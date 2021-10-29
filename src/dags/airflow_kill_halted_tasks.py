"""
An Airflow maintenance DAG that runs monthly and kills off tasks that are running in the background that no longer
correspond to a running task in the DB.
This is useful because when you kill off a DAG Run or Task through the Airflow Web Server, the task still runs in the
background on one of the executors until the task is complete.
"""
import os
import re
from datetime import datetime
from typing import NamedTuple

import pytz
from airflow.models import DAG, DagModel, DagRun, TaskInstance
from airflow.operators.python import PythonOperator
from airflow.utils.db import provide_session
from sqlalchemy import and_

KILL_HALTED_TASKS_DAG_ID = 'airflow_kill_halted_tasks'

# Wheres search command is:  ps -o pid -o cmd -u `whoami` | grep 'airflow run'
full_regex = r'\s*(\w+)\s+(.+)'
airflow_run_regex = r'.*run\s([\w._-]*)\s([\w._-]*)\s([\w:.-]*).*'


class Process(NamedTuple):
    pid: str
    command: str
    dag_id: str
    task_id: str
    execution_date: str
    kill_reason: str

    def to_dict(self):
        return {
            'pid': self.pid,
            'command': self.command,
            'dag_id': self.dag_id,
            'task_id': self.task_id,
            'execution_date': self.execution_date,
            'kill_reason': self.kill_reason,
        }


def parse_process_linux_string(line):
    """
    Parse the linux string returned from the search command run

    :param line: line to parse
    :type line: str
    :return: Process object with the information filled in for readability
    :rtype: Process
    """
    full_regex_match = re.search(full_regex, line)
    command = full_regex_match.group(2).strip()
    airflow_run_regex_match = re.search(airflow_run_regex, command)
    return Process(
        pid=full_regex_match.group(1),
        command=command,
        dag_id=airflow_run_regex_match.group(1),
        task_id=airflow_run_regex_match.group(2),
        execution_date=airflow_run_regex_match.group(3),
        kill_reason='',
    )


@provide_session
def kill_halted_tasks(session=None, **context):
    """
    Stops a task or tasks when the:
    - DAG is missing
    - DAG is not active
    - DagRun is missing
    - DagRun not in the 'running' state
    - TaskInstance is missing
    - TaskInstance not in the 'queued', 'running', or 'up_for_retry' states

    :param session: Airflow session used to query objects to delete
    :param context: context within the Airflow DAG
    """
    log = context['ti'].log

    process_search_command = "ps -o pid -o cmd -u `whoami` | grep 'airflow run'"

    log.info(f"Finding halted tasks\n\tRunning '{process_search_command}' to find Airflow DAG runs")
    search_output = os.popen(process_search_command).read()

    log.info('Filtering out empty lines, grep processes, and this DAGs Run')
    search_output_filtered = [
        line
        for line in search_output.split("\n")
        if line and line.strip()
           and ' grep ' not in line
           and KILL_HALTED_TASKS_DAG_ID not in line
    ]

    def mark_process_to_kill(proc, reason):
        proc.kill_reason = reason
        log.warning(f'Marking {proc.to_dict()} to be killed. Reason: {proc.kill_reason}')
        processes_to_kill.append(proc)

    log.info('Searching through running processes')
    processes_to_kill = []
    for line in search_output_filtered:
        process = parse_process_linux_string(line)

        # Checking to make sure the DAG is available and active
        dag: DagModel = session.query(DagModel).filter(
            DagModel.dag_id == process.dag_id
        ).first()
        if not dag:
            mark_process_to_kill(process, f'DAG, {process.dag_id}, was not found in metastore')
            continue

        if not dag.is_active:
            mark_process_to_kill(process, f'DAG, {process.dag_id}, is disabled')
            continue

        log.info(f'DAG, {dag.dag_id}, exists and is enabled')

        # Checking to make sure the DagRun is available and in a running state
        exec_date_str = process.execution_date.replace('T', ' ')
        # Add milliseconds if they are missing.
        if '.' not in exec_date_str:
            exec_date_str = exec_date_str + '.0'

        execution_date_to_search_for = pytz.utc.localize(
            datetime.strptime(exec_date_str, '%Y-%m-%d %H:%M:%S.%f')
        )
        dag_run: DagRun = session.query(DagRun).filter(
            and_(
                DagRun.dag_id == process.dag_id,
                DagRun.execution_date == execution_date_to_search_for,
            )
        ).first()
        if not dag_run:
            mark_process_to_kill(
                process,
                f"DagRun, '{str(execution_date_to_search_for)}', "
                f'for DAG, {process.dag_id}, was not found in metastore'
            )
            continue

        dag_run_states_required = ['running']
        # is the dag_run in a running state?
        if dag_run.state not in dag_run_states_required:
            mark_process_to_kill(
                process,
                f"DagRun, '{str(execution_date_to_search_for)}', "
                f'for DAG, {dag_run.dag_id}, was not in one of these states: {dag_run_states_required}. '
                f"State of DagRun: '{dag_run.state}'"
            )
            continue

        log.info(
            f'DagRun for DAG, {dag_run.dag_id}, '
            f"with execution_date: '{str(execution_date_to_search_for)}'"
            f'exists and is in one of the required states: {dag_run_states_required}'
        )

        task_instance: TaskInstance = session.query(TaskInstance).filter(
            and_(
                TaskInstance.dag_id == process.dag_id,
                TaskInstance.task_id == process.task_id,
                TaskInstance.execution_date == execution_date_to_search_for,
            )
        ).first()
        if not task_instance:
            mark_process_to_kill(
                process,
                f'TaskInstance, {process.dag_id}.{process.task_id}, '
                f"for '{str(execution_date_to_search_for)}' was not found in metastore."
            )
            continue

        task_instance_states_required = ['queued', 'running', 'up_for_retry']
        if task_instance.state not in task_instance_states_required:
            mark_process_to_kill(
                process,
                f'TaskInstance, {task_instance.dag_id}.{task_instance.task_id}, '
                f"for '{str(execution_date_to_search_for)}' was NOT in required states: {task_instance_states_required}. "
                f"State of TaskInstance: '{task_instance.state}'"
            )
            continue

        log.info(
            f'TaskInstance, {task_instance.dag_id}.{task_instance.task_id}, '
            f"with execution_date: '{str(execution_date_to_search_for)}'"
            f'exists and is in one of the required states: {task_instance_states_required}'
        )

    # Killing the processes
    if processes_to_kill:
        log.info(f'Killing {len(processes_to_kill)} processes')
        for process in processes_to_kill:
            kill_command = f'kill -9 {str(process.pid)}'
            output = os.popen(kill_command).read()
            log.info(f'Kill executed. Output for process, {str(process)}: {str(output)}')

        log.info('All halted tasks have been killed')
    else:
        log.info('No halted tasks found')


with DAG(
        dag_id=KILL_HALTED_TASKS_DAG_ID,
        start_date=datetime(2021, 9, 1),
        schedule_interval='@monthly',
        catchup=False,
        tags=['airflow-maintenance-dags'],
) as kill_halted_tasks_dag:
    kill_halted_tasks_op = PythonOperator(
        task_id='kill_halted_tasks',
        python_callable=kill_halted_tasks,
        provide_context=True,
    )

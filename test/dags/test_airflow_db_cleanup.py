import re
from types import SimpleNamespace
from typing import cast

import pendulum
import pytest
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Context, DagRunState
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
from airflow.sdk.execution_time.xcom import XCom
from airflow.utils.types import DagRunType
from pendulum import UTC, DateTime

from maintenance_dags.airflow_db_cleanup import db_cleanup_dag

EXECUTION_DATE = DateTime(2023, 7, 18, 6, tzinfo=UTC)


@pytest.fixture()
def dag_run():
    dag_run = db_cleanup_dag.create_dagrun(
        run_type=DagRunType.SCHEDULED,
        execution_date=EXECUTION_DATE,
        state=DagRunState.RUNNING,
    )
    return dag_run


@pytest.mark.parametrize('use_variable', [
    pytest.param(True, id='Use variable'),
    pytest.param(False, id='Use settings')
])
def test_get_max_days(mocker, use_variable, monkeypatch):
    variable_value = 5
    setting_value = 10

    mocker.patch('maintenance_dags.settings.DEFAULT_AIRFLOW_AGE_IN_DAYS', setting_value)
    if use_variable:
        days_ago = variable_value
        monkeypatch.setenv('AIRFLOW_VAR_MAX_AIRFLOW_AGE_IN_DAYS', str(variable_value))
    else:
        days_ago = setting_value

    task: PythonOperator = db_cleanup_dag.get_task('calculate_max_date')

    test_now = DateTime.utcnow()
    with pendulum.travel_to(test_now, freeze=True):
        result = task.python_callable(task=task)
    expected = test_now.subtract(days=days_ago).isoformat()
    assert result == expected


def test_airflow_db_cleanup():
    # There's no need to actually run the bash command
    # just make sure the correct XCom gets pulled in
    task_id = f'clean_db'
    # prepare_for_execution makes a new copy, so rendered templates won't carry over to other tests
    task = db_cleanup_dag.get_task(task_id).prepare_for_execution()
    timestamp = '2026-06-06T12:34:56+00:00'

    def xcom_pull(task_ids=None, dag_id=None, key=XCom.XCOM_RETURN_KEY, **kwargs):
        assert dag_id in {None, db_cleanup_dag.dag_id}
        assert task_ids == 'calculate_max_date'
        assert key == XCom.XCOM_RETURN_KEY
        return timestamp

    ti = SimpleNamespace(
        xcom_pull=xcom_pull,
        task=task,
    )
    context = Context(ti=ti, task_instance=ti)  # noqa

    rendered = RuntimeTaskInstance.render_templates(
        self=cast(RuntimeTaskInstance, cast(object, ti)),
        context=context,
    )

    bash_command = re.split(r'\s+', cast(BashOperator, rendered).bash_command)
    assert bash_command == ['airflow', 'db', 'clean', '--clean-before-timestamp', repr(timestamp)]

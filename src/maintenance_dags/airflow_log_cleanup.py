"""
An Airflow maintenance DAG that cleans out the local Airflow log files older than the date specified in
`settings.MAX_LOG_fILE_AGE`.
"""
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.sdk import DAG
from pendulum import DateTime

from maintenance_dags import settings


def x_days_ago(dt, num_days):
    """
    Returns a new date that is num_months before the specified date

    :param dt: date to modify
    :param num_days: number of days to subtract
    :type dt: datetime
    :type num_days: int
    :return: dt - num_months
    :rtype: datetime
    """
    return dt - timedelta(days=num_days)


def check_for_old_log_files(max_age: int, *, task) -> list[str]:
    """
    Check if there are log files older than the specified number of days

    :param max_age: maximum age of a log file in days
    :param task: Airflow task
    :return: whether there are old log files to delete
    """
    log = task.log
    files_to_delete = []
    older_than_date = x_days_ago(DateTime.utcnow(), max_age)

    log.info(f'Looking for log files older than {older_than_date.isoformat()}')
    # We use os.walk instead of os.listdir because there may be subdirectories
    # This avoids adding a directory name to the list of files to delete
    for root, _, files in os.walk(settings.LOG_DIR):
        for filename in files:
            file_name = os.path.join(root, filename)
            last_modified_time = datetime.fromtimestamp(Path(file_name).stat().st_mtime)
            if last_modified_time <= older_than_date:
                files_to_delete.append(file_name)

    if files_to_delete:
        log.info(f'Found {len(files_to_delete)} log files to delete')
    else:
        log.info('No log files found to delete')
    return files_to_delete


def delete_files(files_to_delete: list[str], *, task):
    """Deletes all specified local files"""
    log = task.log

    log.info(f'Deleting {len(files_to_delete)} old files')
    for file_path in files_to_delete:
        if os.path.exists(file_path):
            os.remove(file_path)
        else:
            log.warning(f'File, {file_path}, does not exist!')

    log.info('All old files deleted')


with DAG(
        dag_id='airflow_log_cleanup',
        start_date=datetime(2021, 9, 1),
        schedule='@monthly',
        catchup=False,
        tags={'airflow-maintenance-dags'},
) as log_cleanup_dag:
    check_old_log_files = ShortCircuitOperator(
        task_id='check_old_log_files',
        python_callable=check_for_old_log_files,
        op_kwargs={
            'max_age': settings.MAX_LOG_FILE_AGE,
        },
    )
    delete_old_log_files = PythonOperator(
        task_id='delete_old_log_files',
        python_callable=delete_files,
        op_kwargs={
            'files_to_delete': check_old_log_files.output,
        },
    )

    check_old_log_files >> delete_old_log_files

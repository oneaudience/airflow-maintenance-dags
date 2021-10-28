"""
An Airflow maintenance DAG that cleans out the local Airflow log files older than the date specified in
`settings.MAX_LOG_fILE_AGE`.
"""
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

import settings

XCOM_LOG_FILES_KEY = 'log_files_to_delete'


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


def check_for_old_log_files(xcom_key, max_age, **context):
    """
    Check if there are log files older than the specified number of days

    :param xcom_key: xcom key to store the files to delete under
    :param max_age: maximum age of a log file in days
    :param context: context with the Airflow DAG
    :type xcom_key: str
    :type max_age: int
    :return: whether or not there are old log files to delete
    :rtype: bool
    """
    log = context['ti'].log
    files_to_delete = []
    older_than_date = x_days_ago(datetime.utcnow(), max_age)

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
        context['ti'].xcom_push(key=xcom_key, value=files_to_delete)
        return True

    log.info('No log files found to delete')
    return False


def delete_files(xcom_keys, **context):
    """
    Deletes all local files stored in xcom_keys

    :param xcom_keys: xcom key to pull the files to delete from
    :param context: context with the Airflow DAG
    :type xcom_keys: list[str]
    """
    log = context['ti'].log
    files_to_delete = []
    for xcom_key in xcom_keys:
        xcom_files = context['ti'].xcom_pull(key=xcom_key)
        if xcom_files:
            files_to_delete.extend(xcom_files)

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
        schedule_interval='@monthly',
        catchup=False,
        tags=['airflow-maintenance-dags'],
) as log_cleanup_dag:
    check_old_log_files = ShortCircuitOperator(
        task_id='check_old_log_files',
        provide_context=True,
        python_callable=check_for_old_log_files,
        op_kwargs={
            'xcom_key': XCOM_LOG_FILES_KEY,
            'max_age': settings.MAX_LOG_FILE_AGE,
        },
    )
    delete_old_log_files = PythonOperator(
        task_id='delete_old_log_files',
        provide_context=True,
        python_callable=delete_files,
        op_kwargs={
            'xcom_keys': [XCOM_LOG_FILES_KEY],
        },
    )

    check_old_log_files >> delete_old_log_files

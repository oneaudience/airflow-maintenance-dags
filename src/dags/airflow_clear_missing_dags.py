"""
An Airflow maintenance DAG that will clean out entries in the DAG table once a month.
This ensures that the DAG table doesn't have needless items in it and that the Airflow Web Server displays only those
available DAGs.
"""
import os
from datetime import datetime

from airflow.models import DAG, DagModel
from airflow.operators.python_operator import PythonOperator
from airflow.utils.db import provide_session


@provide_session
def clear_missing_dags(session=None, **context):
    """
    Checks the Airflow DB against existing dag files to clear the Airflow DB of DAGs missing an associated dag file

    :param session: Airflow session used to query objects to delete
    :param context: context within the Airflow DAG
    """
    log = context['ti'].log
    log.info('Clearing all missing DAGs')

    dags = session.query(DagModel).all()

    entries_to_delete = []
    for dag in dags:
        # Check if it is a zip-file
        if dag.fileloc and '.zip/' in dag.fileloc:
            index = dag.fileloc.rfind('.zip/') + len('.zip')
            file_location = dag.fileloc[0:index]
        else:
            file_location = dag.fileloc

        if not file_location:
            log.info(
                f'The file location for {str(dag)} was not found. '
                'Assuming the Python definition file does NOT exist'
            )
            entries_to_delete.append(dag)
        elif not os.path.exists(file_location):
            log.info(f"The Python definition file: {file_location}, for '{str(dag)}' does NOT exist")
            entries_to_delete.append(dag)
        else:
            log.info(f"The Python definition file: {file_location}, for '{str(dag)}' exists")

    if entries_to_delete:
        log.info(f'Deleting {len(entries_to_delete)} DAG(s) from the DB')
        for entry in entries_to_delete:
            session.delete(entry)

        session.commit()
        log.info('All missing DAG(s) have been cleared')
    else:
        log.info(f'No missing DAG(s) found to delete')


with DAG(
        dag_id='airflow_clear_missing_dags',
        start_date=datetime(2021, 9, 1),
        schedule_interval='@monthly',
        catchup=False,
        tags=['airflow-maintenance-dags'],
) as clear_missing_dags_dag:
    clear_missing_dags_op = PythonOperator(
        task_id='clear_missing_dags',
        python_callable=clear_missing_dags,
        provide_context=True,
    )

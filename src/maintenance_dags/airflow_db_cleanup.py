"""
An Airflow maintenance DAG that cleans out the Airflow DB Models entries, specified in `DATABASE_OBJECTS`, once a month
to avoid having too much data in the Airflow MetaStore.
"""

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Variable
from pendulum import DateTime

from maintenance_dags import settings


def get_max_days(*, task):
    """
    Calculate the oldest date allowed. All objects with a date prior to this will be deleted.
    """
    log = task.log
    log.info(f'Using Airflow variable MAX_AIRFLOW_AGE_IN_DAYS or defaulting to {settings.DEFAULT_AIRFLOW_AGE_IN_DAYS}')
    max_days = int(Variable.get('MAX_AIRFLOW_AGE_IN_DAYS', default=settings.DEFAULT_AIRFLOW_AGE_IN_DAYS))
    max_date = DateTime.utcnow().subtract(days=max_days)
    log.info(f'Preparing to delete records older than {max_days} days ({max_date})')
    return max_date.isoformat()


with DAG(
        dag_id='airflow_db_cleanup',
        start_date=DateTime.create(2021, 9, 1),
        schedule='@monthly',
        catchup=False,
        tags={'airflow-maintenance-dags'},
) as db_cleanup_dag:
    calc_max_date = PythonOperator(
        task_id='calculate_max_date',
        python_callable=get_max_days,
    )
    clean_db = BashOperator(
        task_id='clean_db',
        bash_command=f"airflow db clean --clean-before-timestamp '{calc_max_date.output}' --yes"
    )

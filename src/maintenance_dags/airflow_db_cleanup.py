"""
An Airflow maintenance DAG that cleans out the Airflow DB Models entries, specified in `DATABASE_OBJECTS`, once a month
to avoid having too much data in the Airflow MetaStore.
"""
from datetime import datetime, timedelta

import dateutil.parser
from airflow.models import DAG, DagRun, Log, XCom, TaskInstance, Variable
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from airflow.utils.db import provide_session
from sqlalchemy import func, and_

import settings

# List of all the objects that will be deleted. Comment out the DB objects you want to skip.
DATABASE_OBJECTS = {
    'DagRun': {
        'airflow_db_model': DagRun,
        'age_check_column': DagRun.execution_date,
        'keep_last': True,
        'keep_last_filters': [DagRun.external_trigger.is_(False)],
        'keep_last_group_by': DagRun.dag_id,
    },
    'TaskInstance': {
        'airflow_db_model': TaskInstance,
        'age_check_column': TaskInstance.execution_date,
        'keep_last': False,
        'keep_last_filters': None,
        'keep_last_group_by': None,
    },
    'Log': {
        'airflow_db_model': Log,
        'age_check_column': Log.dttm,
        'keep_last': False,
        'keep_last_filters': None,
        'keep_last_group_by': None,
    },
    # XCom can have a custom class, and it's usually an alias for BaseXCom, so don't hard-code this name
    XCom.__name__: {
        'airflow_db_model': XCom,
        'age_check_column': XCom.execution_date,
        'keep_last': False,
        'keep_last_filters': None,
        'keep_last_group_by': None,
    },
}


def get_max_days(**context):
    """
    Push the oldest date allowed to an xcom variable. All objects with a date prior to this will be deleted.

    :param context: context within the Airflow DAG
    """
    log = context['ti'].log
    log.info(f'Using Airflow variable MAX_AIRFLOW_AGE_IN_DAYS or defaulting to {settings.DEFAULT_AIRFLOW_AGE_IN_DAYS}')
    max_days = int(Variable.get('MAX_AIRFLOW_AGE_IN_DAYS', default_var=settings.DEFAULT_AIRFLOW_AGE_IN_DAYS))
    max_date = timezone.utcnow() - timedelta(days=max_days)
    log.info(f'Preparing to delete records older than {max_days} days ({max_date})')
    context['ti'].xcom_push(key='max_date', value=max_date.isoformat())


@provide_session
def cleanup_function(db_class, session=None, **context):
    """
    Clears the Airflow DB of the Model specified in the context parameter `airflow_db_model` before the `max_date`

    :param db_class: The name of the class to clean up
    :param session: Airflow session used to query objects to delete
    :param context: context within the Airflow DAG
    """
    log = context['ti'].log
    max_date = context['ti'].xcom_pull(key='max_date')
    max_date = dateutil.parser.parse(max_date)  # stored as iso8601 str in xcom

    airflow_db_model = DATABASE_OBJECTS[db_class].get('airflow_db_model')
    age_check_column = DATABASE_OBJECTS[db_class].get('age_check_column')
    keep_last = DATABASE_OBJECTS[db_class].get('keep_last')
    keep_last_filters = DATABASE_OBJECTS[db_class].get('keep_last_filters')
    keep_last_group_by = DATABASE_OBJECTS[db_class].get('keep_last_group_by')

    log.info(f'Clearing Airflow DB table of model: {str(airflow_db_model.__name__)} before {max_date.isoformat()}')
    query = session.query(airflow_db_model)

    if keep_last:
        subquery = session.query(func.max(DagRun.execution_date))
        # workaround for MySQL "table specified twice" issue
        # https://github.com/teamclairvoyant/airflow-maintenance-dags/issues/41
        if keep_last_filters:
            for entry in keep_last_filters:
                subquery = subquery.filter(entry)

        if keep_last_group_by:
            subquery = subquery.group_by(keep_last_group_by)

        subquery = subquery.from_self()
        query = query.filter(
            and_(age_check_column.notin_(subquery)),
            and_(age_check_column <= max_date),
        )
    else:
        query = query.filter(age_check_column <= max_date)

    # using bulk delete
    num_deleted = query.delete(synchronize_session=False)
    session.commit()
    log.info(f'{num_deleted} {str(airflow_db_model.__name__)}(s) cleared from DB')


with DAG(
        dag_id='airflow_db_cleanup',
        start_date=datetime(2021, 9, 1),
        schedule_interval='@monthly',
        catchup=False,
        tags=['airflow-maintenance-dags'],
) as db_cleanup_dag:
    calc_max_date = PythonOperator(
        task_id='calculate_max_date',
        provide_context=True,
        python_callable=get_max_days,
    )
    for db_object_name in DATABASE_OBJECTS:
        calc_max_date >> PythonOperator(
            task_id=f"cleanup_{db_object_name}",
            python_callable=cleanup_function,
            op_kwargs={
                'db_class': db_object_name
            },
        )

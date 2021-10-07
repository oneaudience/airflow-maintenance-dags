import os
from datetime import datetime

import pytest
from airflow.models import TaskInstance, DAG, DagModel
from airflow.utils.state import State
from pendulum import Pendulum

from dags.airflow_clear_missing_dags import clear_missing_dags_dag

EXECUTION_DATE = Pendulum(2020, 7, 18, 6)


# DAG_CONFIGS is of the format: dag_id: dag configuration options
# Accounts for the following cases:
#  1. '.zip/' in dag.fileloc
#  2. dag.fileloc is None
#  3. non-existent dag.fileloc
#  4. dag.fileloc exists
DAG_CONFIGS = {
    'zip': {
        'fileloc': '/usr/local/airflow/dags/.zip/hi.txt',
    },
    'none_location': {
        'fileloc': None,
    },
    'empty_location': {
        'fileloc': '',
    },
    'location_does_not_exist': {
        'fileloc': '/usr/local/airflow/dags/blahblah/blah.txt',
    },
    'location_exists': {
        'fileloc': '/usr/local/airflow/dags/location_exists.py',
    },
}


@pytest.fixture(scope='module')
def dagrun():
    dag_run = clear_missing_dags_dag.create_dagrun(
        run_id=f'test_airflow_db_cleanup__{datetime.utcnow()}',
        execution_date=EXECUTION_DATE,
        state=State.RUNNING,
    )
    return dag_run


@pytest.fixture
def prepare_missing_dags(fs, airflow_session):
    for dag_id, dag_config in DAG_CONFIGS.items():
        dag = DagModel(dag_id=dag_id, **dag_config)
        airflow_session.add(dag)

        if dag_id == 'location_exists':
            folder = dag.fileloc.rsplit('/', 1)[0]
            if not os.path.exists(folder):
                fs.create_dir(folder)
            fs.create_file(dag.fileloc)

    airflow_session.commit()


@pytest.mark.usefixtures('prepare_missing_dags')
def test_clear_missing_dags(airflow_session, dagrun):
    ti = TaskInstance(
        task=dagrun.dag.get_task('clear_missing_dags'),
        execution_date=dagrun.execution_date,
    )
    ti.set_state(State.NONE)
    ti.run(ignore_all_deps=True)

    dag_ids = airflow_session.query(DagModel.dag_id) \
        .filter(DagModel.dag_id.in_(DAG_CONFIGS.keys())) \
        .all()

    assert dag_ids == [('location_exists',)]

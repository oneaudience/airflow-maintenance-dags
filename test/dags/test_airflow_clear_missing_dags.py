import os

import pytest
from airflow.models import TaskInstance, DagModel, DagRun
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from pendulum import DateTime, UTC

from maintenance_dags.airflow_clear_missing_dags import clear_missing_dags_dag

EXECUTION_DATE = DateTime(2020, 7, 18, 6, tzinfo=UTC)


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


@pytest.fixture()
def dag_run(airflow_session) -> DagRun:
    return clear_missing_dags_dag.create_dagrun(
        run_type=DagRunType.SCHEDULED,
        execution_date=EXECUTION_DATE,
        state=State.RUNNING,
        session=airflow_session
    )


@pytest.fixture
def prepare_missing_dags(fs, airflow_session):
    fs.add_real_directory(os.path.dirname(clear_missing_dags_dag.fileloc))
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
def test_clear_missing_dags(airflow_session, dag_run):
    ti = TaskInstance(
        task=dag_run.dag.get_task('clear_missing_dags'),
        execution_date=dag_run.execution_date,
    )
    ti.set_state(State.NONE)
    ti.run(ignore_all_deps=True)

    dag_ids = airflow_session.query(DagModel.dag_id) \
        .filter(DagModel.dag_id.in_(DAG_CONFIGS.keys())) \
        .all()

    assert dag_ids == [('location_exists',)]

from copy import copy

import pytest
from airflow.configuration import conf
from airflow.models import DagRun, Log, TaskInstance, XCom, DagTag, DagModel
from airflow.utils.db import create_session, resetdb
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists


def create_database(url):
    """
    Replacement for create_database from sqlalchemy-utils until bug is fixed

    https://github.com/kvesteri/sqlalchemy-utils/issues/432
    """
    url = copy(url)
    database = url.database
    url.database = 'postgres'

    e = create_engine(url, isolation_level='AUTOCOMMIT')
    e.execute(f"CREATE DATABASE {database}")


@pytest.fixture(scope='session')
def airflow_postgres_engine(postgres):
    port = postgres.ports['5432/tcp'][0]
    engine = create_engine(f'postgresql://test_user:test_pass@localhost:{port}/airflowdb')
    if not database_exists(engine.url):
        create_database(engine.url)
    yield engine
    engine.dispose()


@pytest.hookimpl
def pytest_sessionstart(session):
    # Clear out the Airflow db and make sure we're in unit testing mode
    if not conf.getboolean('core', 'unit_test_mode'):
        raise RuntimeError('Not running in unit test mode. Set AIRFLOW__CORE__UNIT_TEST_MODE=true')
    resetdb()


@pytest.fixture
def airflow_session():
    with create_session() as session:
        yield session
        # Truncate all tables
        for model in (DagRun, Log, TaskInstance, XCom, DagTag, DagModel):
            session.query(model).delete()

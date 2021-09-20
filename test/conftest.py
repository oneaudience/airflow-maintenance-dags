from copy import copy
from unittest import mock

import pytest
import responses
from airflow.jobs import BaseJob
from airflow.models import DagRun, Log, TaskInstance, XCom, DagTag, DagModel
from airflow.utils.db import create_session, resetdb
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists

settings_patchers = [
    mock.patch('settings.SQLALCHEMY_DATABASE_URI', 'sqlite:///:memory:')
]


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
    # This will allow dw_hook to function even when S3 mocking is active
    responses._default_mock.add_passthru('http+docker://localhost')
    for patcher in settings_patchers:
        patcher.start()

    with create_session() as sql_session:
        # Have to manually drop the job table until https://issues.apache.org/jira/browse/AIRFLOW-5036 is fixed
        BaseJob.__table__.drop(sql_session.get_bind(), checkfirst=True)
    resetdb(rbac=False)


@pytest.fixture
def airflow_session():
    with create_session() as session:
        yield session
        # Truncate all tables
        for model in (DagRun, Log, TaskInstance, XCom, DagTag, DagModel):
            session.query(model).delete()

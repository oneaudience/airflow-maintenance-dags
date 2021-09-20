import os
from datetime import datetime

import pytest
from airflow.models import TaskInstance, XCom, DagRun
from airflow.utils.state import State
from pendulum import Pendulum

import settings
from dags.airflow_log_cleanup import log_cleanup_dag, XCOM_LOG_FILES_KEY

EXECUTION_DATE = Pendulum(2020, 7, 18, 6)


def create_file_with_st_mtime(py_fake_fs, full_path, dt):
    folder = full_path.rsplit('/', 1)[0]
    if not os.path.exists(folder):
        py_fake_fs.create_dir(folder)
    f = py_fake_fs.create_file(full_path)
    f.stat_result._st_mtime_ns = dt.timestamp() * 1e9  # nanoseconds


@pytest.fixture(scope='module')
def dagrun():
    dag_run = log_cleanup_dag.create_dagrun(
        run_id=f'test_airflow_log_cleanup__{datetime.utcnow()}',
        execution_date=EXECUTION_DATE,
        state=State.RUNNING,
    )
    return dag_run


@pytest.mark.parametrize('log_files_to_create, older_than_date, expected', [
    pytest.param({}, datetime(2021, 1, 1), None, id='no files'),
    pytest.param(
        {'log1.txt': datetime(2020, 1, 1)},
        datetime(2021, 1, 1),
        ['log1.txt'],
        id='single file',
    ),
    pytest.param(
        {'log1.txt': datetime(2020, 1, 1),
         'log2.txt': datetime(2019, 11, 12),
         'this/log3.txt': datetime(2019, 11, 12),
         'that/log4.txt': datetime(2021, 3, 1),
         'that/log5.txt': datetime(2018, 11, 12),
         },
        datetime(2020, 3, 1),
        ['log1.txt', 'log2.txt', 'this/log3.txt', 'that/log5.txt'],
        id='multiple files with subpaths',
    ),
    pytest.param(
        {'log1.txt': datetime(2020, 1, 1),
         'log2.txt': datetime(2019, 11, 12),
         'this/log3.txt': datetime(2019, 11, 12),
         'that/log4.txt': datetime(2021, 3, 1),
         'that/log5.txt': datetime(2018, 11, 12),
         },
        datetime(2005, 3, 1),
        None,
        id='files with nothing to delete',
    ),
])
def test_check_old_log_files(mocker, fs, dagrun, log_files_to_create, older_than_date, expected):
    dagrun: DagRun
    fs.create_dir(settings.LOG_DIR)
    mocker.patch('dags.airflow_log_cleanup.x_days_ago', return_value=older_than_date)

    log_files = []
    for file_name, creation_timestamp in log_files_to_create.items():
        file_name = os.path.join(settings.LOG_DIR, file_name)
        create_file_with_st_mtime(fs, file_name, creation_timestamp)
        log_files.append(file_name)

    ti = TaskInstance(
        task=dagrun.dag.get_task('check_old_log_files'),
        execution_date=dagrun.execution_date,
    )
    ti.set_state(State.NONE)
    ti.run(ignore_all_deps=True)

    xcom_values = XCom.get_one(
        task_id=ti.task_id,
        dag_id=ti.dag_id,
        execution_date=ti.execution_date,
        key=XCOM_LOG_FILES_KEY,
    )
    if expected:
        expected = [
            os.path.join(settings.LOG_DIR, file_name)
            for file_name in expected
        ]
    assert xcom_values == expected
    assert ti.state == State.SUCCESS


@pytest.mark.parametrize('log_paths', [
    pytest.param({}, id='No paths'),
    pytest.param({
        'log1.txt': datetime(2020, 1, 1),
        'log2.txt': datetime(1999, 12, 12),
        'log3.txt': datetime.utcnow(),
    }, id='2 old, 1 new, no subpaths'),
    pytest.param({
        'log1.txt': datetime(2020, 1, 1),
        'this/log1.txt': datetime(2020, 1, 1),
    }, id='subpaths'),
    pytest.param({
        'log1.txt': datetime(2020, 1, 1),
        'this/log2.txt': datetime(2020, 1, 1),
        'that/log3.txt': datetime(2020, 1, 1),
        'that/other/path/log4.txt': datetime(2020, 1, 1),
    }, id='multiple subpaths'),
])
def test_delete_old_files(fs, airflow_session, dagrun, log_paths):
    # All additional_files should remain after the task has completed
    fs.create_dir(settings.LOG_DIR)

    log_file_names = []
    for log_file_name, file_creation_timestamp in log_paths.items():
        full_path = os.path.join(settings.LOG_DIR, log_file_name)
        create_file_with_st_mtime(fs, full_path, file_creation_timestamp)
        log_file_names.append(full_path)

    XCom.set(
        key=XCOM_LOG_FILES_KEY,
        value=log_file_names,
        task_id='check_old_log_files',
        dag_id=log_cleanup_dag.dag_id,
        execution_date=EXECUTION_DATE,
    )
    ti = TaskInstance(
        task=dagrun.dag.get_task('delete_old_log_files'),
        execution_date=dagrun.execution_date,
    )
    ti.set_state(State.NONE)
    ti.run(ignore_all_deps=True)

    for file_name in log_file_names:
        assert not os.path.exists(file_name)
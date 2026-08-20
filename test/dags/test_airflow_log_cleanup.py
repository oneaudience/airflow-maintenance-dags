import os
from datetime import datetime
from types import SimpleNamespace
from typing import cast

import pendulum
import pytest
from airflow.models import DagRun, XCom
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.sdk import Context
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from pendulum import UTC, DateTime

from maintenance_dags import settings
from maintenance_dags.airflow_log_cleanup import log_cleanup_dag

EXECUTION_DATE = DateTime(2024, 7, 18, 6, tzinfo=UTC)


def create_file_with_st_mtime(py_fake_fs, full_path, dt):
    folder = full_path.rsplit('/', 1)[0]
    if not os.path.exists(folder):
        py_fake_fs.create_dir(folder)
    f = py_fake_fs.create_file(full_path)
    f.stat_result._st_mtime_ns = dt.timestamp() * 1e9  # nanoseconds


@pytest.fixture()
def dag_run(airflow_session) -> DagRun:
    return log_cleanup_dag.create_dagrun(
        run_type=DagRunType.SCHEDULED,
        execution_date=EXECUTION_DATE,
        state=State.RUNNING,
        session=airflow_session,
    )


@pytest.mark.parametrize('log_files_to_create, fake_now, expected', [
    pytest.param({}, DateTime.create(2021, 1, 31), [], id='no files'),
    pytest.param(
        {'log1.txt': datetime(2020, 1, 1)},
        DateTime.create(2021, 1, 31),
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
        DateTime.create(2020, 3, 31),
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
        DateTime.create(2005, 3, 31),
        [],
        id='files with nothing to delete',
    ),
])
def test_check_old_log_files(fs, log_files_to_create: dict, fake_now, expected):
    fs.create_dir(settings.LOG_DIR)

    log_files = []
    for file_name, creation_timestamp in log_files_to_create.items():
        file_name = os.path.join(settings.LOG_DIR, file_name)
        create_file_with_st_mtime(fs, file_name, creation_timestamp)
        log_files.append(file_name)

    task: ShortCircuitOperator = log_cleanup_dag.get_task('check_old_log_files')
    with pendulum.travel_to(fake_now, freeze=True):
        result = task.python_callable(**task.op_kwargs, task=task)
    expected = [
        os.path.join(settings.LOG_DIR, file_name)
        for file_name in expected
    ]
    assert sorted(result) == sorted(expected)


@pytest.mark.parametrize('log_paths', [
    pytest.param({}, id='No paths'),
    pytest.param({
        'log1.txt': datetime(2020, 1, 1),
        'log2.txt': datetime(1999, 12, 12),
        'log3.txt': datetime.now(),
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
def test_delete_old_files(fs, log_paths: dict):
    # All additional_files should remain after the task has completed
    fs.create_dir(settings.LOG_DIR)

    log_file_names = []
    for log_file_name, file_creation_timestamp in log_paths.items():
        full_path = os.path.join(settings.LOG_DIR, log_file_name)
        create_file_with_st_mtime(fs, full_path, file_creation_timestamp)
        log_file_names.append(full_path)

    task_id = 'delete_old_log_files'
    # prepare_for_execution makes a new copy, so rendered templates won't carry over to other tests
    task: PythonOperator = log_cleanup_dag.get_task(task_id).prepare_for_execution()

    def xcom_pull(task_ids=None, dag_id=None, key=XCom.XCOM_RETURN_KEY, **kwargs):
        assert task_ids == 'check_old_log_files'
        assert dag_id in {None, log_cleanup_dag.dag_id}
        assert key == XCom.XCOM_RETURN_KEY
        return log_file_names

    ti = SimpleNamespace(
        xcom_pull=xcom_pull,
        task=task,
    )
    context = Context(
        ti=ti,  # noqa
        task_instance=ti,  # noqa
        task=task,
    )
    rendered = cast(PythonOperator, RuntimeTaskInstance.render_templates(
        self=cast(RuntimeTaskInstance, cast(object, ti)),
        context=context,
    ))
    rendered.python_callable(
        **task.op_kwargs,
        task=task,
    )

    for file_name in log_file_names:
        assert not os.path.exists(file_name)
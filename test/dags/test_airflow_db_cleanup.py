from datetime import datetime, timedelta

import pytest
from airflow import DAG
from airflow.models import TaskInstance, XCom, Variable, DagRun, Log
from airflow.operators.dummy import DummyOperator
from airflow.utils.db import provide_session, create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from dateutil.parser import parse
from pendulum import DateTime, UTC

from maintenance_dags.airflow_db_cleanup import db_cleanup_dag, DATABASE_OBJECTS
from test import DateTimeRange

EXECUTION_DATE = DateTime(2020, 7, 18, 6, tzinfo=UTC)


@pytest.fixture()
def dagrun(airflow_session):
    dag_run = db_cleanup_dag.create_dagrun(
        run_id=f'test_airflow_db_cleanup__{datetime.utcnow()}',
        execution_date=EXECUTION_DATE,
        state=State.RUNNING,
    )
    return dag_run


@pytest.fixture
def clear_variables():
    """Clear variables for this test and the next"""
    with create_session() as session:
        session.query(Variable).delete()
    yield
    with create_session() as session:
        session.query(Variable).delete()


@pytest.mark.usefixtures('clear_variables')
@pytest.mark.parametrize('use_variable', [
    pytest.param(True, id='Use variable'),
    pytest.param(False, id='Use settings')
])
def test_get_max_days(mocker, dagrun, use_variable):
    variable_value = 5
    setting_value = 10

    mocker.patch('maintenance_dags.settings.DEFAULT_AIRFLOW_AGE_IN_DAYS', setting_value)
    if use_variable:
        days_ago = variable_value
        Variable.set('MAX_AIRFLOW_AGE_IN_DAYS', variable_value)
    else:
        days_ago = setting_value

    ti = TaskInstance(
        task=db_cleanup_dag.get_task('calculate_max_date'),
        execution_date=EXECUTION_DATE,
    )
    ti.run(ignore_all_deps=True)

    expected_range = DateTimeRange(
        ti.start_date - timedelta(days=days_ago),
        ti.end_date - timedelta(days=days_ago),
    )

    pushed_xcoms = [
        (xcom.key, parse(xcom.value))
        for xcom in XCom.get_many(
            execution_date=ti.execution_date,
            task_ids=ti.task_id,
            dag_ids=ti.dag_id,
        )]
    assert pushed_xcoms == [
        ('max_date', expected_range)
    ]


cleanup_threshold = DateTime(2020, 6, 19, 12, 34, 56, tzinfo=UTC)
# Last midnight to get dropped (most of our DAG runs are at midnight UTC)
dropped_midnight = DateTime(cleanup_threshold.year,
                            cleanup_threshold.month,
                            cleanup_threshold.day,
                            tzinfo=UTC)

# It's hard to compare SQLAlchemy objects that aren't bound to the same session,
# so compare by a specific set of fields instead
compare_fields = {
    DagRun: ('dag_id', 'execution_date', 'run_id', 'external_trigger'),
    Log: ('dttm', 'dag_id', 'task_id', 'execution_date', 'event'),
    TaskInstance: ('task_id', 'dag_id', 'execution_date'),
    XCom: ('key', 'value', 'execution_date', 'task_id', 'dag_id'),
}

# A series of Airflow objects, in the form (class, args, expect_kept)
AIRFLOW_DATA = [
    # DAG runs: for each dag ID, the most recent item for which external_trigger is True will be kept,
    #           regardless of how old it is.
    # Note: There's a false positives bug in the Team Clairvoyant code so that the most recent execution_date
    #       per DagRun is collected into a table, and all execution dates from all DagRuns are kept if in that list,
    #       so for this test data, all execution_dates that need to be deleted should be completely unique
    # dag_1: One scheduled run, recent
    (DagRun, dict(
        dag_id='dag_1',
        execution_date=cleanup_threshold.add(days=1),
        run_id='dag1_run1',
        external_trigger=False,
    ), True),
    # dag_2: One triggered run, recent
    (DagRun, dict(
        dag_id='dag_2',
        execution_date=cleanup_threshold.add(days=5),
        run_id='dag2_run1',
        external_trigger=True
    ), True),
    # dag_3: One scheduled run, old
    (DagRun, dict(
        dag_id='dag_3',
        execution_date=cleanup_threshold.subtract(days=2),
        run_id='dag3_run1',
        external_trigger=False,
    ), True),
    # dag_4: One triggered run, old
    (DagRun, dict(
        dag_id='dag_4',
        execution_date=cleanup_threshold.subtract(days=3),
        run_id='dag4_run1',
        external_trigger=True,
    ), False),
    # dag_5: Multiple scheduled runs, some old, some new
    (DagRun, dict(
        dag_id='dag_5',
        execution_date=dropped_midnight.subtract(days=1),
        run_id='dag5_run1',
        external_trigger=False,
    ), False),
    (DagRun, dict(
        dag_id='dag_5',
        execution_date=dropped_midnight,
        run_id='dag5_run2',
        external_trigger=False,
    ), False),
    (DagRun, dict(
        dag_id='dag_5',
        execution_date=dropped_midnight.add(days=1),
        run_id='dag5_run3',
        external_trigger=False,
    ), True),
    (DagRun, dict(
        dag_id='dag_5',
        execution_date=dropped_midnight.add(days=2),
        run_id='dag5_run4',
        external_trigger=False,
    ), True),
    # dag_6: Mixture of scheduled runs, some old, some new. At least one scheduled run is new.
    (DagRun, dict(
        dag_id='dag_6',
        execution_date=cleanup_threshold.subtract(days=4),
        run_id='dag6_run1',
        external_trigger=True
    ), False),
    (DagRun, dict(
        dag_id='dag_6',
        execution_date=cleanup_threshold.subtract(days=1),
        run_id='dag6_run2',
        external_trigger=True
    ), False),
    (DagRun, dict(
        dag_id='dag_6',
        execution_date=dropped_midnight.subtract(days=2),
        run_id='dag6_run3',
        external_trigger=False
    ), False),
    (DagRun, dict(
        dag_id='dag_6',
        execution_date=cleanup_threshold.add(days=3),
        run_id='dag6_run4',
        external_trigger=True
    ), True),
    (DagRun, dict(
        dag_id='dag_6',
        execution_date=dropped_midnight.add(days=2),
        run_id='dag6_run5',
        external_trigger=False
    ), True),
    # dag_7: Mixture of scheduled and triggered runs, all old, plus one new triggered run
    (DagRun, dict(
        dag_id='dag_7',
        execution_date=dropped_midnight.subtract(days=1, hours=12),
        run_id='dag7_run1',
        external_trigger=False,
    ), False),
    (DagRun, dict(
        dag_id='dag_7',
        execution_date=dropped_midnight.subtract(hours=12),
        run_id='dag7_run2',
        external_trigger=False,
    ), True),
    # The other types just filter based on one column, without any special conditions to keep items
    # TaskInstance; make sure each one matches a DagRun above
    # Note that each task will automatically be added to the DAG during test setup,
    # So no 2 TIs should share a dag_id/task_id pair
    (TaskInstance, dict(
        task_id='task_7',
        dag_id='dag_4',
        execution_date=cleanup_threshold.subtract(days=3)
    ), False),
    (TaskInstance, dict(
        task_id='task_7',
        dag_id='dag_5',
        execution_date=dropped_midnight.subtract(days=1)
    ), False),
    (TaskInstance, dict(
        task_id='task_7',
        dag_id='dag_1',
        execution_date=cleanup_threshold.add(days=1)
    ), True),
    (TaskInstance, dict(
        task_id='task_7',
        dag_id='dag_6',
        execution_date=cleanup_threshold.add(days=3)
    ), True),
    # Log - there are two datetime columns; make sure the right one (dttm) is used for log deletion
    (Log, dict(
        dttm=cleanup_threshold.subtract(hours=1),
        dag_id='log_dag',
        task_id='some_task',
        execution_date=dropped_midnight,
        event='running',
    ), False),
    (Log, dict(
        dttm=cleanup_threshold.add(hours=1),
        dag_id='log_dag',
        task_id='some_task',
        execution_date=dropped_midnight,
        event='success',
    ), True),
    (Log, dict(
        dttm=cleanup_threshold.subtract(hours=2),
        dag_id='log_dag',
        task_id='some_task',
        execution_date=dropped_midnight.add(days=1),
        event='running',
    ), False),
    (Log, dict(
        dttm=cleanup_threshold.add(hours=2),
        dag_id='log_dag',
        task_id='some_task',
        execution_date=dropped_midnight.add(days=1),
        event='success',
    ), True),
    # XComs
    (XCom, dict(
        key='abc',
        value=34567,
        execution_date=dropped_midnight.subtract(days=1),
        dag_id='dag_1',
        task_id='task_1',
    ), False),
    (XCom, dict(
        key='abc',
        value=23456,
        execution_date=dropped_midnight,
        dag_id='dag_1',
        task_id='task_1',
    ), False),
    (XCom, dict(
        key='abc',
        value=12345,
        execution_date=dropped_midnight.add(days=1),
        dag_id='dag_1',
        task_id='task_1',
    ), True),
]


@pytest.fixture
def make_airflow_objects():
    with create_session() as session:
        session.query(DagRun).delete()
        session.query(Log).delete()
        session.query(TaskInstance).delete()
        session.query(XCom).delete()

        # Create a DagModel entry for each dag_id
        dags = {
            dag_id: DAG(dag_id=dag_id, start_date=dropped_midnight.subtract(years=1))
            for dag_id in {obj['dag_id'] for _, obj, _ in AIRFLOW_DATA}
        }
        for dag in dags.values():
            dag.sync_to_db(session=session)

        # Create DagRun entries
        session.add_all([DagRun(**obj_data, run_type=DagRunType.MANUAL if obj_data['external_trigger'] else DagRunType.SCHEDULED) for obj_class, obj_data, _ in AIRFLOW_DATA if obj_class is DagRun])

        # First, the XCom read by these tasks
        XCom.set(
            key='max_date',
            value=cleanup_threshold.isoformat(),
            execution_date=EXECUTION_DATE,
            task_id='calculate_max_date',
            dag_id=db_cleanup_dag.dag_id,
        )

        # Use XCom.set to handle value serialization
        xcoms = [obj_data for obj_class, obj_data, _ in AIRFLOW_DATA if obj_class is XCom]
        for xcom in xcoms:
            XCom.set(**xcom, session=session)

        # TaskInstance and Log have custom constructors
        def make_ti(ti_data):
            task = DummyOperator(task_id=ti_data['task_id'], dag=dags[ti_data['dag_id']])
            return TaskInstance(
                task=task,
                execution_date=ti_data['execution_date']
            )

        tis = [
            make_ti(obj_data)
            for obj_class, obj_data, _ in AIRFLOW_DATA
            if obj_class is TaskInstance
        ]
        session.add_all([ti for ti in tis if ti])

        def make_log(log_data):
            log = Log(task_instance=None, **log_data)
            # dttm is set by __init__, override it
            log.dttm = log_data['dttm']
            return log

        logs = [
            make_log(obj_data)
            for obj_class, obj_data, _ in AIRFLOW_DATA
            if obj_class is Log
        ]
        session.add_all(logs)

    yield
    with create_session() as session:
        session.query(DagRun).delete()
        session.query(Log).delete()
        session.query(TaskInstance).delete()
        session.query(XCom).delete()


@pytest.mark.usefixtures('make_airflow_objects')
@pytest.mark.parametrize(
    'obj',
    [pytest.param(obj, id=f'{class_name}, print') for class_name, obj in DATABASE_OBJECTS.items()] +
    [pytest.param(obj, id=f'{class_name}, no print') for class_name, obj in DATABASE_OBJECTS.items()]
)
@provide_session
def test_airflow_db_cleanup(dagrun, obj, session=None):
    airflow_db_model = obj['airflow_db_model']
    task_id = f'cleanup_{airflow_db_model.__name__}'
    ti = TaskInstance(
        task=db_cleanup_dag.get_task(task_id),
        execution_date=EXECUTION_DATE
    )
    ti.run(ignore_all_deps=True)

    expected = [
        {
            k: v
            for k, v in obj_data.items()
            if k in compare_fields[airflow_db_model]
        }
        for obj_class, obj_data, kept in AIRFLOW_DATA
        if kept and obj_class == airflow_db_model
    ]
    results = [
        {
            f: getattr(obj, f)
            for f in compare_fields[airflow_db_model]
        }
        for obj in session.query(airflow_db_model)
            .filter(airflow_db_model.dag_id != db_cleanup_dag.dag_id)
            .all()
    ]

    assert results == expected


@pytest.mark.usefixtures('make_airflow_objects')
@pytest.mark.parametrize('obj', [pytest.param(obj, id=class_name) for class_name, obj in DATABASE_OBJECTS.items()])
@provide_session
def test_airflow_db_cleanup_no_delete(dagrun, obj, session=None):
    airflow_db_model = obj['airflow_db_model']

    def make_obj(item):
        obj_dict = {f: getattr(item, f) for f in compare_fields[airflow_db_model]}
        return frozenset(obj_dict.items())

    before = {make_obj(obj) for obj in session.query(airflow_db_model).all()}

    task_id = f'cleanup_{airflow_db_model.__name__}'
    ti = TaskInstance(
        task=db_cleanup_dag.get_task(task_id),
        execution_date=EXECUTION_DATE
    )
    ti.run(ignore_all_deps=True)
    after = {
        make_obj(obj)
        for obj in session.query(airflow_db_model)
            .filter(airflow_db_model.dag_id != db_cleanup_dag.dag_id)
            .all()
    }
    # Make sure nothing was deleted
    assert after <= before  # | expect_added

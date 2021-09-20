# airflow-maintenance-dags
A series of DAGs to help maintain and clean up after the operations of an Airflow environment

## DAGs Overview

* log-cleanup
    * A maintenance workflow that you can deploy into Airflow to periodically clean out the task logs to avoid those getting too big.
* db-cleanup
    * A maintenance workflow that you can deploy into Airflow to periodically clean out the DagRun, TaskInstance, Log, XCom, Job DB and SlaMiss entries to avoid having too much data in your Airflow MetaStore.
* clear-missing-dags
    * A maintenance workflow that you can deploy into Airflow to periodically clean out entries in the DAG table of which there is no longer a corresponding Python File for it. This ensures that the DAG table doesn't have needless items in it and that the Airflow Web Server displays only those available DAGs.
* kill-halted-tasks
    * A maintenance workflow that you can deploy into Airflow to periodically kill off tasks that are running in the background that don't correspond to a running task in the DB.
    * This is useful because when you kill off a DAG Run or Task through the Airflow Web Server, the task still runs in the background on one of the executors until the task is complete.


## Setup

### Python Setup
To set up the environment for testing, you must run the following commands
```
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r etc/requirements.dev.txt
```

### Unit tests
#### First-Time Setup
If Airflow verson ~= 1.x:
```
airflow initdb
```

If Airflow verson >= 2.x:
```
airflow db init
```

Once the Airflow DB has been set up properly, we can run the unit tests:
```
export AIRFLOW_HOME=$PWD/src
export PYTHONPATH=$PWD/src
pytest test --cov src
```


## Airflow Log Cleanup

A maintenance workflow that you can deploy into Airflow to periodically clean out the task logs to avoid those getting too big.

- **airflow-log-cleanup.py**: Allows to delete logs by specifying the **number** of worker nodes. Does not guarantee log deletion of all nodes. 
- **airflow-log-cleanup-pwdless-ssh.py**: Allows to delete logs by specifying the list of worker nodes by their hostname. Requires the `airflow` user to have passwordless ssh to access all nodes.

### Deploy

1. Login to the machine running Airflow
2. Navigate to the dags directory
3. Select the DAG to deploy (with or without SSH access) and follow the instructions

#### airflow-log-cleanup.py

1. Copy the airflow-log-cleanup.py file to this dags directory
<!-- TODO: REPLACE-->

       a. Here's a fast way:

                $ wget https://raw.githubusercontent.com/teamclairvoyant/airflow-maintenance-dags/master/log-cleanup/airflow-log-cleanup.py

2. Update the global variables (SCHEDULE_INTERVAL, DAG_OWNER_NAME, ALERT_EMAIL_ADDRESSES, ENABLE_DELETE and NUMBER_OF_WORKERS) in the DAG with the desired values

3. Create and Set the following Variables in the Airflow Web Server (Admin -> Variables)

    - airflow_log_cleanup__max_log_age_in_days - integer - Length to retain the log files if not already provided in the conf. If this is set to 30, the job will remove those files that are 30 days old or older.
    - airflow_log_cleanup__enable_delete_child_log - boolean (True/False) - Whether to delete files from the Child Log directory defined under [scheduler] in the airflow.cfg file

4. Enable the DAG in the Airflow Webserver


## Airflow DB Cleanup

A maintenance workflow that you can deploy into Airflow to periodically clean out the DagRun, TaskInstance, Log, XCom, Job DB and SlaMiss entries to avoid having too much data in your Airflow MetaStore.

### Deploy

1. Login to the machine running Airflow

2. Navigate to the dags directory

3. Copy the airflow-db-cleanup.py file to this dags directory

<!-- TODO: REPLACE-->
       a. Here's a fast way:

                $ wget https://raw.githubusercontent.com/teamclairvoyant/airflow-maintenance-dags/master/db-cleanup/airflow-db-cleanup.py
        
4. Update the global variables (SCHEDULE_INTERVAL, DAG_OWNER_NAME, ALERT_EMAIL_ADDRESSES and ENABLE_DELETE) in the DAG with the desired values

5. Modify the DATABASE_OBJECTS list to add/remove objects as needed. Each dictionary in the list features the following parameters:
    - airflow_db_model: Model imported from airflow.models corresponding to a table in the airflow metadata database
    - age_check_column: Column in the model/table to use for calculating max date of data deletion
    - keep_last: Boolean to specify whether to preserve last run instance
        - keep_last_filters: List of filters to preserve data from deleting during clean-up, such as DAG runs where the external trigger is set to 0. 
        - keep_last_group_by: Option to specify column by which to group the database entries and perform aggregate functions.

6. Create and Set the following Variables in the Airflow Web Server (Admin -> Variables)

    - airflow_db_cleanup__max_db_entry_age_in_days - integer - Length to retain the log files if not already provided in the conf. If this is set to 30, the job will remove those files that are 30 days old or older.

7. Enable the DAG in the Airflow Webserver


## Airflow Clear Missing DAGs

A maintenance workflow that you can deploy into Airflow to periodically clean out entries in the DAG table of which there is no longer a corresponding Python File for it. This ensures that the DAG table doesn't have needless items in it and that the Airflow Web Server displays only those available DAGs.  

### Deploy

1. Login to the machine running Airflow

2. Navigate to the dags directory

3. Copy the airflow-clear-missing-dags.py file to this dags directory

<!-- TODO: REPLACE-->
       a. Here's a fast way:

                $ wget https://raw.githubusercontent.com/teamclairvoyant/airflow-maintenance-dags/master/clear-missing-dags/airflow-clear-missing-dags.py
        
5. Update the global variables (SCHEDULE_INTERVAL, DAG_OWNER_NAME, ALERT_EMAIL_ADDRESSES and ENABLE_DELETE) in the DAG with the desired values

6. Enable the DAG in the Airflow Webserver


## Airflow Kill Halted Tasks

A maintenance workflow that you can deploy into Airflow to periodically kill off tasks that are running in the background that don't correspond to a running task in the DB. 

This is useful because when you kill off a DAG Run or Task through the Airflow Web Server, the task still runs in the background on one of the executors until the task is complete.

### Deploy

1. Login to the machine running Airflow

2. Navigate to the dags directory

3. Copy the airflow-kill-halted-tasks.py file to this dags directory
<!-- TODO: REPLACE-->

       a. Here's a fast way:

                $ wget https://raw.githubusercontent.com/teamclairvoyant/airflow-maintenance-dags/master/kill-halted-tasks/airflow-kill-halted-tasks.py
        
4. Update the global variables in the DAG with the desired values 

5. Enable the DAG in the Airflow Webserver


### Common Issues

#### "Table dag_stats already exists" While Running Unit Tests

*Solution: Delete the configured .db file in src/unittests.cfg*

1. Find unit testing db file
```shell
$ cat src/unittests.cfg | grep sql_alchemy_conn
sql_alchemy_conn = sqlite:///$AIRFLOW_HOME/unittests.db
```

2. Delete the file from the specified path
```shell
rm $AIRFLOW_HOME/unittests.db
```

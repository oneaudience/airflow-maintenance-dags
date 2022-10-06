import os

# This is here so that we can run some tests and import directly into any project that has settings.LOG_DIR anywhere
LOG_DIR = '/usr/local/airflow/logs'

# Anything older than this many days will be deleted from Airflow tables by the maintenance DAG
# This will be overridden by the "MAX_AIRFLOW_AGE_IN_DAYS" Airflow Variable
DEFAULT_AIRFLOW_AGE_IN_DAYS = 360
MAX_LOG_FILE_AGE = 30

SQLALCHEMY_DATABASE_HOST = os.environ.get('SQLALCHEMY_DATABASE_HOST', default='postgres')
SQLALCHEMY_DATABASE_PASS = os.environ.get('SQLALCHEMY_DATABASE_PASS', default='exports')
SQLALCHEMY_DATABASE_USER = os.environ.get('SQLALCHEMY_DATABASE_USER', default='exports')
SQLALCHEMY_DATABASE_NAME = os.environ.get('SQLALCHEMY_DATABASE_NAME', default='exports')

SQLALCHEMY_DATABASE_URI = f'postgresql://{SQLALCHEMY_DATABASE_USER}:{SQLALCHEMY_DATABASE_PASS}@{SQLALCHEMY_DATABASE_HOST}/{SQLALCHEMY_DATABASE_NAME}'

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.operators.bash import BashOperator
from airflow.models.dag import DAG

import os
import pandas as pd

# Create placeholder if doesn't exist
if not os.path.exists('./files/data.csv'):
    d = pd.DataFrame()
    d.to_csv('./files/data.csv')

# NOTE: Generate Presigned URL
url = '# PASTE PRESIGNED URL HERE #'

# Executing Tasks and TaskGroups
with DAG(
    dag_id="TEST_ETL_TASKGROUP",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    default_args={'retries': 2},
    schedule_interval=None,
    catchup=False,
    tags=["workshop"],
) as dag:
    pass

    # ---------- 1. DEFINE YOUR TASKS & TASKGROUPS BELOW HERE ---------- #

    # ---------- 2. DEFINE YOUR CONTROL FLOW BELOW HERE ---------- #
    
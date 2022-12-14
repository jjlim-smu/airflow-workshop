# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pendulum

import os
from etl_code.etl import transform, load

# NOTE: Generate Presigned URL
url = 'https://mybucket-7012.s3.us-east-1.amazonaws.com/bank-customer-churn-prediction.csv?response-content-disposition=inline&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEAoaDmFwLXNvdXRoZWFzdC0xIkcwRQIgNRIAuRoUzv5vFJx9xYwbXIfLMO974OAC3IEkw4ZvNi4CIQDndQQocKSM%2Bu7wFMVr0Zgq9%2FSfD6VxAZA6zOdfqljhryrtAgij%2F%2F%2F%2F%2F%2F%2F%2F%2F%2F8BEAAaDDUwNDEwNTcyOTMwOCIM2DntyKslTvF892RiKsECWguz%2BrmeaWQ6ci4Jzr17SeThFWg%2BV3aJP6W01BIOBL27x6LyKm2kXnxHNvf8Dv3hJOsxlc4w%2F6G3LpvYeGZOnGPKcIhMdpMN2sNwVascY7mC5IPkkt%2Fk4GusN2vxLUsBvEP%2FWcKP%2By7hxc3DXD%2FL86v0Adw2rFoQyZyL0TejsLXQBSt5C2H32vAXqkGsatwBvFnnV9URe1d80TaA78ScJqQ93yhhukOdK9ix%2FsL87nVG4dDvfUgfyp01npR4CQEeP40LEAhsQsZsP4K4kYc9LhaIKrhrArkxr5Eu5XWf3Xxf1MwQqkAKX1rVPFmVTfauH%2B8s5ikIytnJAwwTgBn6%2Bg8tf6oDxhKMp9KiSKPuOwIJQYb1tq%2B0STJzNZqmxTurR6IgF3t8ldYTiweF%2BaE1eXcgGmRMhQQiKm8XMWznVX4nMKuugZkGOrMCSjachoyskKzKimyaP3n5goC4bIHtCHxR4NiqwWA3Pd1tcSXm%2FprNn0q7FMczOgp7%2BfzbrRWQsR%2Fbtna3aMUmLf48YUNf0hMXxW%2FVFk4XgdsRnfmocqfSFVjf4QaZoW5Vza5xeQyL30hJ6kxuM4TeV%2F8lQNMo7oXnL49VgmuQvmt4mN0ANXpGkfyBobGjKry8T06doNci4mLwXaqP0QNu%2BmyvAYr3aXEDuxe12EW4TIGj%2BV1lxgc6fQDxq303Jyah0rBBwGK9iJR%2BZYo0mY4z9PQthEr5bgZJGXJ8HTPz69THo56kqBF8%2F7giH15qaQYLaUhyW1IJsQq8HGk9qiYyV%2BTZ%2BYCpnJSW73%2B83pGGE0iuq5bDjUPo71qqbMOZdPjSyyHsJayCnVm6rC6p4FkH8HDk6w%3D%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20220913T101100Z&X-Amz-SignedHeaders=host&X-Amz-Expires=43200&X-Amz-Credential=ASIAXKXYK7UOLFGK23PG%2F20220913%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=c744959c28892177859760abf67c83648484eec77eaa6e35a83220311833303f'


# ---------- 1. DEFINE YOUR DAG HERE ---------- #
with DAG(
    'TEST_ETL',
    default_args={'retries': 2},
    description='BIA Workshop ETL Tutorial',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule_interval='@daily',
    catchup=False,
    tags=['workshop'],
) as dag:

    # ---------- 2. DEFINE YOUR TASKS HERE ---------- #
    extract_task = BashOperator(
        task_id='extract', 
        bash_command=f"""curl -o $AIRFLOW_HOME/files/data.csv '{url}'"""
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )

    # ---------- 3. DEFINE YOUR CONTROL FLOW HERE ---------- #
    extract_task >> transform_task >> load_task
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pendulum

import os
from etl_code.etl import transform, load

# GET PRESIGNED URL (TO BE SHARED)
url = 'https://mybucket-7012.s3.us-east-1.amazonaws.com/bank-customer-churn-prediction.csv?response-content-disposition=inline&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEMn%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaDmFwLXNvdXRoZWFzdC0xIkcwRQIhAPGYPD7PnU1mtVjCmVT6yFQGk1lGgHhmMYzePr53nuRmAiAUDFk5BbT%2Bi1syiS%2BG3grz1ijrQ5lXFRmecdZPC2UTCyrkAghiEAAaDDUwNDEwNTcyOTMwOCIMxJYMG69oFN0RF2YEKsEC5NP4gGRof6n8YPGo5OIMq16sW5mDGaJpfMSzMMccdnVKHdonmwFsJGKXFai3dyDNv6Fu7kodwn2bfIQsmbhVtrsTbDA7QfUO5Jnm7dApIbvXCmBnIOn9XnGSeARA8zAdvMY3Zvn5u1DVFkkG8Y4h99Kf5EPm7YAIMO0x%2B55IxA0833NRc5yZL93o4HNVRJ%2BpdBcdfNM4atzhd9aQTuUo5vok620VLiLG27Oz04vzKEYp8cs3rCwm1dP4K5KeAkDZDYyR2g3RGGmHdRhwGlKRxj%2BRwGt8NXGE5K5YmV5IhbqjDuVjqoPbYVcMsP%2FyIrgyGGJx0GFhNGFs2tqXDaKO%2BDa1OGg0tCFaFHF4s2v6xJCO1QV3wALaQk%2FlSKQ6mg2LE0XFc1XKWF3E8BtaGznYfHTa8iD4%2BG7RcNB0tFNEb1LsMI2I85gGOrMCx%2BmEcsV7cSvD1szYwGRGMdXWdCnjO6pPqixcIiqbK%2Fuca1esti1DwlkB5FKxHWRjE839C%2BXCTHAzqn8c9i1PVlNaFI3pXK9%2BOu1L3l0pogzorQ2044o13PP4NJR9CsbLQQYWAjBa2d2qGRlFd30XgrcOTUaIunqWZh%2FrNymD7VUnlbjdtWFKDpO8bKK%2BCTqsbxG3lbMqgUTSYisc5jpZoFq5FTqZZ8tmzuoNmnDk2Qf24IJ2z%2FMBGz9syv%2F%2Bh%2BSlX5CiXnX8fgxcJGFCoZKjVJr62AP%2BuwfbdVFByGXWKcfYieU7lVy%2FJQwtAg2Wf8TJRgcmnMlh4g1aP4n%2F%2BO5MBfszuivFNfN9aIXyeJvLLo%2BBsZKccGYQ1KbK9YJPropHu1PvGyU4fx18R7lhMKmOEoXu9w%3D%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20220910T170630Z&X-Amz-SignedHeaders=host&X-Amz-Expires=43199&X-Amz-Credential=ASIAXKXYK7UOLCBVD25N%2F20220910%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=292b9430f54a944ba3c27ba0c91fb0d6ad97a9e0b3a9ab43d54be652b273c9bc'

with DAG(
    'TEST_ETL',
    default_args={'retries': 2},
    description='BIA Workshop ETL Tutorial',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule_interval='@daily',
    catchup=False,
    tags=['workshop'],
) as dag:
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

    extract_task >> transform_task >> load_task
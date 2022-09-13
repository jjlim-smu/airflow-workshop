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
url = 'https://mybucket-7012.s3.us-east-1.amazonaws.com/bank-customer-churn-prediction.csv?response-content-disposition=inline&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEAoaDmFwLXNvdXRoZWFzdC0xIkcwRQIgNRIAuRoUzv5vFJx9xYwbXIfLMO974OAC3IEkw4ZvNi4CIQDndQQocKSM%2Bu7wFMVr0Zgq9%2FSfD6VxAZA6zOdfqljhryrtAgij%2F%2F%2F%2F%2F%2F%2F%2F%2F%2F8BEAAaDDUwNDEwNTcyOTMwOCIM2DntyKslTvF892RiKsECWguz%2BrmeaWQ6ci4Jzr17SeThFWg%2BV3aJP6W01BIOBL27x6LyKm2kXnxHNvf8Dv3hJOsxlc4w%2F6G3LpvYeGZOnGPKcIhMdpMN2sNwVascY7mC5IPkkt%2Fk4GusN2vxLUsBvEP%2FWcKP%2By7hxc3DXD%2FL86v0Adw2rFoQyZyL0TejsLXQBSt5C2H32vAXqkGsatwBvFnnV9URe1d80TaA78ScJqQ93yhhukOdK9ix%2FsL87nVG4dDvfUgfyp01npR4CQEeP40LEAhsQsZsP4K4kYc9LhaIKrhrArkxr5Eu5XWf3Xxf1MwQqkAKX1rVPFmVTfauH%2B8s5ikIytnJAwwTgBn6%2Bg8tf6oDxhKMp9KiSKPuOwIJQYb1tq%2B0STJzNZqmxTurR6IgF3t8ldYTiweF%2BaE1eXcgGmRMhQQiKm8XMWznVX4nMKuugZkGOrMCSjachoyskKzKimyaP3n5goC4bIHtCHxR4NiqwWA3Pd1tcSXm%2FprNn0q7FMczOgp7%2BfzbrRWQsR%2Fbtna3aMUmLf48YUNf0hMXxW%2FVFk4XgdsRnfmocqfSFVjf4QaZoW5Vza5xeQyL30hJ6kxuM4TeV%2F8lQNMo7oXnL49VgmuQvmt4mN0ANXpGkfyBobGjKry8T06doNci4mLwXaqP0QNu%2BmyvAYr3aXEDuxe12EW4TIGj%2BV1lxgc6fQDxq303Jyah0rBBwGK9iJR%2BZYo0mY4z9PQthEr5bgZJGXJ8HTPz69THo56kqBF8%2F7giH15qaQYLaUhyW1IJsQq8HGk9qiYyV%2BTZ%2BYCpnJSW73%2B83pGGE0iuq5bDjUPo71qqbMOZdPjSyyHsJayCnVm6rC6p4FkH8HDk6w%3D%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20220913T101100Z&X-Amz-SignedHeaders=host&X-Amz-Expires=43200&X-Amz-Credential=ASIAXKXYK7UOLFGK23PG%2F20220913%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=c744959c28892177859760abf67c83648484eec77eaa6e35a83220311833303f'

# Executing Tasks and TaskGroups
with DAG(
    dag_id="TEST_ETL_TASKGROUP",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule_interval='@weekly',
    catchup=False,
    tags=["workshop"],
) as dag:

    @task()
    def transform_credit_score(df):
        """
        Aggregate mean and median values for credit score
        """
        summary = {}
        summary['average_credit_score'] = df['credit_score'].agg('mean')
        summary['median_credit_score'] = df['credit_score'].agg('median')
        return summary

    @task()
    def transform_age(df):
        """
        Aggregate mean and median values for age
        """
        summary = {}
        summary['average_age'] = df['age'].agg('mean')
        summary['median_age'] = df['age'].agg('median')
        return summary

    @task()
    def transform_salary(df):
        """
        Aggregate mean and median values for salary
        """
        summary = {}
        summary['average_salary'] = df['estimated_salary'].agg('mean')
        summary['median_salary'] = df['estimated_salary'].agg('median')
        return summary

    @task()
    def transform_balance(df):
        """
        Aggregate mean and median values for balance
        """
        summary = {}
        summary['average_balance'] = df['balance'].agg('mean')
        summary['median_balance'] = df['balance'].agg('median')
        return summary

    @task()
    def transform_gender(df):
        """
        Aggregate mean and median values for gender
        """
        summary = {}
        summary['female_to_male_ratio'] = (df['gender']=='Female').sum() / len(df)
        summary['male_to_female_ratio'] = (df['gender']=='Male').sum() / len(df)
        return summary

    @task_group
    def transform_values():
        """
        TaskGroup to group all transformation tasks
        """
        df = pd.read_csv('./files/data.csv')
        results = (transform_credit_score(df), transform_age(df), transform_balance(df), transform_salary(df), transform_gender(df))
        return results

    @task()
    def load(results: dict):
        """
        #### Load task
        A simple Load task which takes in the result of the Transform task.
        """

        # Load data to parquet file
        df = pd.read_csv('./files/data.csv')
        df.to_parquet('./files/data.parquet')

        # Merge results into one dict
        summary = {}
        for r in results:
            summary.update(r)

        # Export summary to parquet file
        df_summary = pd.DataFrame.from_dict(summary, orient='index')
        df_summary.columns = df_summary.columns.astype(str)
        df_summary.to_parquet('./files/data_summary.parquet')
        print('Successfully load data to parquet file')

    extract_task = BashOperator(
        task_id='extract', 
        bash_command=f"""curl -o $AIRFLOW_HOME/files/data.csv '{url}'"""
    )

    transform_task = transform_values()
    load_task = load(transform_task)

    extract_task >> transform_task >> load_task
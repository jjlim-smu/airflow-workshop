import pendulum
from airflow.decorators import dag, task, task_group
from airflow.operators.bash import BashOperator
from airflow.models.dag import DAG

import os
import pandas as pd

from sqlalchemy import create_engine

# NOTE: Generate Presigned URL
url = 'https://mybucket-7012.s3.us-east-1.amazonaws.com/bank-customer-churn-prediction.csv?response-content-disposition=inline&X-Amz-Security-Token=IQoJb3JpZ2luX2VjELv%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaDmFwLXNvdXRoZWFzdC0xIkgwRgIhAN5HtsRIMgvPdPxvYmLqGj8enkWlshdT4JOPE22RRBIaAiEAk8OYBVNqjH7wPR4ANU61QJWhJwsGmOp3q4s8glmVlZUq5AIIVBAAGgw1MDQxMDU3MjkzMDgiDLUcVZ1u4UGdEQKs4SrBApiHFztozpgpm0QbaUbS2DEFsRyytaUW4TQfK0F9kmgrBnCAc0qb46Nm5rCXoObXNo1i1SYw7gjl3nQD6MEGN9PqNX6N1Gq86aGoedaAM%2Fg7XReF%2FoBAlX16oHQKustNA0yHncnnvblBV9pF44mB8XEWbAsAiRvlxe7npKXIqEs32rP076YEumiFldoSOrL0koL8q13bVM17uHwRikqxKsxfuW3fsSzZW04QfBi0gm%2BF5KjBfrKrpb3lsRbdD%2FOXtW1Gjx5UIWBh8fER9GCJaMImPR5%2Bu4Ri2zYskWhO%2BbHP6V0uEdtBfn8dJSPUSHpnUOJKMQUoUK7faZGDcqe8xa1g2tljvcJeSqrqJVfYfvIH4lgKDC%2BUGdVd246bGzBH9Dyfxp3Z4vyUvNPBhY52XmOHYSgrrXEJPJpqeyGNbQv9KzDT%2F%2B%2BYBjqyAoA4JTLavy77oKQTVf6xhW4%2FMD371XZxf2cb18lNOSst3us3iC4OcBl2zDfjAbveu6uEnVLwjKa8kbHKfMBFtLI0hG9FrhzJE9Gu7eT1qm45VlzDpYLkSksYl%2FtaoGQ12ROlkUoy9XhKNXyfWow81tvrhq8RyXjqGWYr9cdNGi9AOLN01St%2BduNt71y8Yq%2BrFtJC0Hbbfl%2FCnHM7kPNssn%2BjvDigc5AKKbdnfuL9e6jg0A1xBhYKGCFBrwB9eAxIZwtlVQqIp%2B3%2FO5f72qdJZxzP%2BwpuV%2FGHXCm%2BDaQ1TnXIMIE9RjbpqpxoZmGkWAS3ZJlx9WvEBArOWFNN21q8O1qDRhbeSFztSRu2pva17IzwvKSoikSf1yDVZGtAw8WdLu6Z2muE3Cm6Ycn6bsaLmliQKg%3D%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20220910T135311Z&X-Amz-SignedHeaders=host&X-Amz-Expires=43200&X-Amz-Credential=ASIAXKXYK7UONDOGS7GS%2F20220910%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=2e529aec5c464b6314780daf07f0168f0e797880ccd93a20b3f8c76dd582ee3a'

# Executing Tasks and TaskGroups
with DAG(
    dag_id="TEST_ETL_TASKGROUP",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule_interval=None,
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

        # SQLAlchemy Connection
        engine = create_engine('postgresql://airflow:c9f3f98eca043dd34fa0141e100d9690224e6245b6b2f7764fdd1638619c53c6@postgres/airflow')
        # Load data to psql database
        df.to_sql('bank_customer', con=engine, if_exists='append')
        df_summary.to_sql('bank_customer_summary', con=engine, if_exists='append')

        print('Successfully load data to psql db')

    extract_task = BashOperator(
        task_id='extract', 
        bash_command=f"""curl -o $AIRFLOW_HOME/files/data.csv '{url}'"""
    )

    transform_task = transform_values()
    load_task = load(transform_task)

    extract_task >> transform_task >> load_task
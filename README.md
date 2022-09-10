# airflow-workshop

### Pre-Requisites
Install Docker Desktop [https://docs.docker.com/desktop/]

### Setup
```bash
docker-compose -f docker-compose-LocalExecutor.yml up --build -d
```

## ETL Part I

### Python Operation
1. Ingest CSV file from S3 Bucket using BashOperator (to be done later on within the DAG)
2. Perform basic aggregation using PythonOperator
3. Load results to psql database and parquet file

In `etl_code/etl.py`, fill in the following code snippets accordingly:
```python
def transform():
    """Perform basic aggregation of data and export data summary to csv"""
    df = pd.read_csv('./files/data.csv')
    
    summary = {}
    summary['average_credit_score'] = df['credit_score'].agg('mean')
    summary['median_credit_score'] = df['credit_score'].agg('median')
    summary['average_age'] = df['age'].agg('mean')
    summary['median_age'] = df['age'].agg('median')
    summary['average_salary'] = df['estimated_salary'].agg('mean')
    summary['median_salary'] = df['estimated_salary'].agg('median')
    summary['average_balance'] = df['balance'].agg('mean')
    summary['median_balance'] = df['balance'].agg('median')
    summary['female_to_male_ratio'] = (df['gender']=='Female').sum() / len(df)
    summary['male_to_female_ratio'] = (df['gender']=='Male').sum() / len(df)

    df_summary = pd.DataFrame.from_dict(summary, orient='index')

    df_summary.to_csv('data_summary.csv')

def load():
    """Load data to psql db and parquet file"""
    # Load data to psql bank_customer table
    df = pd.read_csv('./files/data.csv')
    df.to_parquet('./files/data.parquet')

    # Load summary data to new table
    df_summary = pd.read_csv('./files/data_summary.csv')
    df_summary.columns = df_summary.columns.astype(str)
    df_summary.to_parquet('./files/data_summary.parquet')
    print('Successfully load data to parquet file')

    # SQLAlchemy Connection
    engine = create_engine('postgresql://airflow:c9f3f98eca043dd34fa0141e100d9690224e6245b6b2f7764fdd1638619c53c6@postgres/airflow')
    # Load data to psql database
    df.to_sql('bank_customer', con=engine, if_exists='append')
    df_summary.to_sql('bank_customer_summary', con=engine, if_exists='append')

    print('Successfully load data to psql db')
```

### Creating a DAG
```python
with DAG(
    'TEST_ETL',
    default_args={'retries': 2},
    description='BIA Workshop ETL Tutorial',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule_interval=None,
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
```

## ETL Part II

In `test_etl_taskgroup.py`, define the tasks and taskgroup as shown below
```python
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
```

Define your control flow as shown below
```python
transform_task = transform_values()
load_task = load(transform_task)

extract_task >> transform_task >> load_task
```

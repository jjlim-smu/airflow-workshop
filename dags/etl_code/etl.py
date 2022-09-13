import pandas as pd

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

    df_summary.to_csv('./files/data_summary.csv')

def load():
    """Load data to parquet file"""
    # Load data to parquet file
    df = pd.read_csv('./files/data.csv')
    df.to_parquet('./files/data.parquet')

    # Load summary data to parquet file
    df_summary = pd.read_csv('./files/data_summary.csv')
    df_summary.columns = df_summary.columns.astype(str)
    df_summary.to_parquet('./files/data_summary.parquet')
    print('Successfully load data to parquet file')

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from time import time, sleep
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector
from datetime import timedelta

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url: str) -> pd.DataFrame:
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    df = pd.read_csv(csv_name)
    
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])

    return df

@task(log_prints=True)
def transform_data(df):

    print(f'pre: missing passenger count {df["passenger_count"].isin([0]).sum()}')
    df = df.loc[df['passenger_count'] != 0]
    print(f'post: missing passenger count {df["passenger_count"].isin([0]).sum()}')

    return df

@task(log_prints=True, retries=3)
def ingest_data(table_name, df):
    
    connection_block = SqlAlchemyConnector.load("de-zoomcamp-pgconnector")
    
    with connection_block.get_connection(begin=False) as engine:

        df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')
        
        list_df = np.array_split(df, np.ceil(df.shape[0]/100000))
        for dataframe in list_df:
            
            t_start = time()
        
            dataframe.to_sql(name=table_name, con=engine, if_exists='append')
            
            t_end = time()
            
            print(f'Inserted another chunk ({dataframe.shape[0]} rows)..., took {(t_end-t_start):.3f} s')

    
@flow(name='Subflow', log_prints=True)
def log_subflow(table_name:str):
    print(f'logging Subflow for: {table_name}')
    
@flow(name='Ingest Flow')
def main_flow(table_name: str):

    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
    
    log_subflow(table_name)
    raw_data = extract_data(url)
    data = transform_data(raw_data)
    ingest_data(table_name, data)

if __name__ == '__main__':
    main_flow("yellow_taxi_trips")
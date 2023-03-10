from pathlib import Path 
import pandas as pd 
import numpy as np
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas dataframe"""

    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(color: str, df = pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""

    if color == 'yellow':
        df.fillna(np.nan)
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        
        trans_dtype = ['Int64', 'datetime64[ns]', 'datetime64[ns]', 'Int64', 'float64', 'Int64','object','Int64','Int64','Int64','float64','float64','float64','float64','float64','float64','float64','float64',]
        
        for column, dt in zip(df.columns, trans_dtype):
            df[column] = df[column].astype(dt)
    elif color == 'green':
        df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
        df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
        
        trans_dtype = ['Int64', 'datetime64[ns]', 'datetime64[ns]', 'object', 'Int64', 'Int64', 'Int64', 'Int64', 'float64', 'float64', 'float64', 'float64', 'float64', 'float64', 'float64', 'float64', 'float64', 'Int64', 'Int64',  'float64']
        
        for column, dt in zip(df.columns, trans_dtype):
            df[column] = df[column].astype(dt)
    
    print(df.shape[0])
    print(f'columns: {df.dtypes}')
    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write dataframe out locally as parquet file"""
    path = Path(f'data/{color}/{dataset_file}.parquet')

    df.to_parquet(path, compression='gzip')

    return path

@task()
def write_gcs(path: Path) -> None:
    """Uploading local parquet file to GCS"""

    gcs_block = GcsBucket.load("zoomcamp-gcs")
    gcs_block.upload_from_path(
        from_path=f"{path}",
        to_path=path
    )

    return

@flow()
def etl_web_to_gcs(month: int, year: int, color: str) -> None:
    """The main ETL function"""
    
    dataset_file = f'{color}_tripdata_{year}-{month:02}'
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(color, df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

@flow()
def etl_web_to_gcs_parent_flow(
    months: list[int] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
    year: int = 2020,
    color: str = 'green'
):  
    for month in months:
        etl_web_to_gcs(month, year, color)

if __name__ == "__main__":
    etl_web_to_gcs_parent_flow()


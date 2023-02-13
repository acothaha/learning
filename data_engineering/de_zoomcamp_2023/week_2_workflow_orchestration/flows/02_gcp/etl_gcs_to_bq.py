from pathlib import Path 
import pandas as pd 
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(log_prints=True, retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trop data from GCS"""
    gcs_path = f'data/{color}/{color}_tripdata_{year}-{month:02}.parquet'
    gcs_block = GcsBucket.load('de-zoomcamp-gcs')
    gcs_block.get_directory(from_path=gcs_path, local_path=f'../week_2_workflow_orchestration')

    return Path(f'{gcs_path}')

@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""

    df = pd.read_parquet(path)
    # print(f'pre: missing passenger count: {df["passenger_count"].isna().sum()}')
    # df["passenger_count"].fillna(0, inplace=True)
    # print(f'post : missing passenger count: {df["passenger_count"].isna().sum()}')

    return df

@task(log_prints=True)
def write_bq(df: pd.DataFrame) -> None:
    """Write data into BigQuery"""

    gcp_credentials_block = GcpCredentials.load("de-zoomcamp-gcp-credential")

    df.to_gbq(
        destination_table='de_zoomcamp.question_3',
        project_id='esoteric-code-377203',
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists='append'
    )



@flow()
def etl_gcs_to_bq(month: int, year: int, color: str):
    """Main ETL flow to load data into Big Query"""

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)


@flow()
def etl_gcs_to_bq_parent_flow(
    months: list[int] = [2, 3],
    year: int = 2019,
    color: str = 'yellow'
):
    for month in months:
        etl_gcs_to_bq(month, year, color)

if __name__ == '__main__':
    months = [2, 3]
    year = 2019
    color = 'yellow'
    etl_gcs_to_bq_parent_flow(months, year, color)
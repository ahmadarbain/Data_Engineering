import pandas as pd
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    
    # if randint(0, 1) > 0:
    #     raise Exception
    df = pd.read_csv(dataset_url, low_memory=False)
    return df

@task()
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"row: {len(df)}")
    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write Dataframe out locally as parquet file"""
    path =  Path(f"Week_2/data/{color}/{dataset_file}.parquet").as_posix()
    df.to_parquet(path, compression="gzip")
    return path

@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("de-zoomcamp", validate=False)
    gcs_block.upload_from_path(from_path=path, to_path=path, timeout=90000)
    return

@flow()
def etl_web_to_gcs(year:int, color:str, months:int):
    """The main ETL Function"""
    dataset_file = f"{color}_tripdata_{year}-{months:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path =  write_local(df_clean, color, dataset_file)
    write_gcs(path) 

if __name__ == '__main__':
    color:str = "green"
    year:int = 2019
    months:int = 4
    etl_web_to_gcs(year, color, months) 

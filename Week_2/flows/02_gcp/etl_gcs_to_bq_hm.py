import pandas as pd
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs(color: str, year: int, month: str):
    """Doownload trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("de-zoomcamp")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"")
    return Path(f"{gcs_path}")


@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    return df

@task(log_prints=True)
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to Big Query"""
    gcp_credentials_block = GcpCredentials.load("de-zcamp")

    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="bionic-kiln-342115",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )

@flow()
def etl_gcs_to_bq(year: int, month: int, color: str) -> None:
    """Main ETL flow to load data into Big Query"""

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)

@flow()
def etl_parent_flow(
    months: list[int] = [1, 2, 3], 
    year: int = 2019, 
    color: str = "yellow"
):
    for month in months:
        etl_gcs_to_bq(year, month, color)
    
if __name__ == "__main__":
    color = "yellow"
    months = [1, 2, 3] 
    year = 2019
    etl_parent_flow(months, year, color)
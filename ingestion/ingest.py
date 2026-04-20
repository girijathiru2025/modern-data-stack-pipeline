import logging
import os
import requests
import pandas as pd

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("ingest")


# downloads the NYC Taxi Trip data (yellow taxi 2023) from NYC Open Data, saves it locally,
def download_raw_file():
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
    local_path = "data/yellow_tripdata_2023-01.parquet"

    os.makedirs("data", exist_ok=True)

    logger.info(f"Downloading from {url}")

    response = requests.get(url, stream=True)
    response.raise_for_status()

    with open(local_path, "wb") as f:
        # download 8kb at a time
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    logger.info(f"Downloaded file to {local_path}")
    return local_path


# from Local file - loads it to Snowflake RAW_DB using snowflake-connector-python
def load_raw_file(local_path: str) -> pd.DataFrame:

    df = pd.read_parquet(local_path)
    logger.info(f"Read {len(df)} rows from {local_path}")

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database="RAW_DB",
        schema="RAW",
        warehouse="LOADING_WH",
        role="LOADER_ROLE",
    )

    try:
        # write_pandas() - snowflake tool bulk loads df using a temp stage internally
        # Returns a tuple containing a success flag, the number of chunks processed, and the total number of rows written

        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name="YELLOW_TAXI_TRIPS_RAW",
            auto_create_table=True,
            overwrite=True,
        )

        if not success:
            raise RuntimeError("write_pandas failed to load data into Snowflake")

        logger.info(f"Loaded {nrows} rows into RAW_DB.RAW.YELLOW_TAXI_TRIPS_RAW")

    finally:
        conn.close()

    return df


if __name__ == "__main__":
    raw_df = load_raw_file(download_raw_file())

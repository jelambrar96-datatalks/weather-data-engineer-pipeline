"""@bruin

name: raw.a01_download_raw_data
type: python
connection: noaa_duckdb

materialization:
  type: table
  strategy: append
  partition_by: date

secrets:
  - key: noaa_duckdb
    inject_as: noaa_duckdb

columns:
  - name: station_id
    type: varchar
    description: Station ID
    checks:
      - name: not_null
  - name: date
    type: timestamp
    description: Observation date (YYYYMMDD)
    checks:
      - name: not_null
  - name: element
    type: varchar
    description: Element type (TMAX, TMIN, PRCP, etc.)
  - name: value
    type: float
    description: Data value
  - name: m_flag
    type: varchar
    description: Measurement flag
  - name: q_flag
    type: varchar
    description: Quality flag
  - name: s_flag
    type: varchar
    description: Source flag
  - name: obs_date
    type: timestamp
    description: Observation date
    checks:
      - name: not_null
  - name: obs_time
    type: varchar
    description: Observation time
  - name: year
    type: integer
    description: Year partition column
    checks:
      - name: not_null

@bruin"""

import os
import pandas as pd
import requests



def download_noaa_ghcn_data(year: int) -> pd.DataFrame | None:
    df = None
    try:
        source_url = f"https://noaa-ghcn-pds.s3.amazonaws.com/csv.gz/{year}.csv.gz"
        df = pd.read_csv(
            source_url, 
            header=None,
            names=[
                "station_id", 
                "date", 
                "element", 
                "value", 
                "m_flag", 
                "q_flag", 
                "s_flag", 
                "obs_time"
            ],
            dtype=str,
            compression="gzip"
        )
        df["year"] = int(year)
        df["obs_date"] = df["date"].copy()
        df["date"] = pd.to_datetime(df["obs_date"], format="%Y%m%d")
        df["value"] = df["value"].astype(float)
    except requests.exceptions.RequestException as e:
        print(f"error downloading data from {year}")
        print(e)
    except Exception as e:
        print(e)
    return df


def materialize():

    print("stating task...")

    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]

    print(f"start_date: {start_date}")
    print(f"end_date: {end_date}")

    start_year = int(start_date.split("-")[0])
    end_year = int(end_date.split("-")[0])

    df = None

    for year in range(start_year, end_year + 1):
        print(f"downloading data from {year}...")
        temp_df = download_noaa_ghcn_data(year)       
        if temp_df is None:
            continue
        print("sucessful downloaded")
        df = temp_df if df is None else pd.concat((df, temp_df), ignore_index=True)

    # pandas filters the null values    
    df = df\
        .dropna(subset=["date", "station_id", "year", "obs_date"])\
        .reset_index(drop=True)

    print("finish to download")
    print("saving data...")
    return df

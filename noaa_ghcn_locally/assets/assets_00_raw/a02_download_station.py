"""@bruin
name: raw.a02_download_station
type: python
connection: noaa_duckdb

materialization:
    type: table
    strategy: create+replace
    cluster_by: ["id"]
description: "Downloads and parses the NOAA GHCN stations metadata from S3."
owner: "jelambrar@gmail.com"


columns:
    - name: id
      type: string
      description: "the station identification code"
      checks:
          - name: not_null
          - name: unique
      primary_key: true
    - name: latitude
      type: float
      description: "latitude of the station (in decimal degrees)."
      checks:
          - name: not_null
          - name: min
            value: -90.0
          - name: max
            value: 90.0
    - name: longitude
      type: float
      description: "longitude of the station (in decimal degrees)."
      checks:
          - name: not_null
          - name: min
            value: -180.0
          - name: max
            value: 180.0
    - name: elevation
      type: float
      description: "elevation of the station in meters."
      checks:
          - name: min
            value: -1000
    - name: state
      type: string
      description: "U.S. postal code for the state (for U.S. and Canadian stations only)."
      checks:
          - name: pattern
            value: "^[a-zA-Z0-9]+$"
    - name: name
      type: string
      description: "name of the station"
      checks:
          - name: not_null
    - name: gsn_flag
      type: string
      description: "GSN Flag - Indicates membership in the Global Historical Climatology Network (GHCN). Possible values: Blank (not a member), Y (member)"
      checks:
          - name: pattern
            value: "^(GSN|)$"
    - name: hcn_crn_flag
      type: string
      description: "HCN/CRN Flag - Indicates membership in U.S. Historical Climatology Network (HCN) or Climate Reference Network (CRN). Possible values: Blank (not a member), HCN (Historical Climatology Network), CRN (Climate Reference Network or Regional Climate Network)"
      checks:
          - name: pattern
            value: "^(HCN|CRN|)$"
    - name: wmo_id
      type: string
      description: "WMO ID"

custom_checks:
    - name: at least 10k stations
      description: "Ensure the stations table has at least 100,000 records"
      query: "SELECT count(*) >= 10000 FROM raw.a02_download_station"
      value: 1
@bruin"""


import io
import requests
import pandas as pd


def materialize():

    url = 'http://noaa-ghcn-pds.s3.amazonaws.com/ghcnd-stations.txt'

    # Download the file content
    response = requests.get(url)
    response.raise_for_status()  # Raises an error for bad status codes

    colspecs = [
        (0, 11),    # ID
        (12, 20),   # LATITUDE
        (21, 30),   # LONGITUDE
        (31, 37),   # ELEVATION
        (38, 40),   # STATE
        (41, 71),   # NAME
        (72, 75),   # GSN FLAG
        (76, 79),   # HCN/CRN FLAG
        (80, 85),   # WMO ID
    ]

    col_names = ['id', 'latitude', 'longitude', 'elevation', 'state', 'name', 'gsn_flag', 'hcn_crn_flag', 'wmo_id']

    df = pd.read_fwf(
        io.StringIO(response.text), # Pass text content as file-like object
        colspecs=colspecs,
        names=col_names,
        header=None
    )

    # Optional: clean up whitespace in string columns
    str_cols = ['id', 'state', 'name', 'gsn_flag', 'hcn_crn_flag', 'wmo_id']
    df[str_cols] = df[str_cols].astype(str).apply(lambda x: x.str.strip())

    return df


if __name__ == '__main__':

    df = materialize()
    print(df.head())
    print(df.dtypes)
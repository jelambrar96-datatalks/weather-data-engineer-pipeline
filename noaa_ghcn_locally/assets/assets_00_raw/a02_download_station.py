"""@bruin
name: raw.a02_download_station
type: python
connection: noaa_duckdb

materialization:
    type: table
    destination: raw.stations
    strategy: create+replace
    cluster_by: ["id"]

columns:
    - name: id
      type: string
      description: "Station ID"
      checks:
          - name: not_null
    - name: latitude
      type: float
      description: "Latitude"
      checks:
          - name: not_null
    - name: longitude
      type: float
      description: "Longitude"
      checks:
          - name: not_null
    - name: elevation
      type: float
      description: "Elevation"
    - name: state
      type: string
      description: "State"
    - name: name
      type: string
      description: "Station Name"
    - name: gsn_flag
      type: string
      description: "GSN Flag"
    - name: hcn_crn_flag
      type: string
      description: "HCN/CRN Flag"
    - name: wmo_id
      type: string
      description: "WMO ID"
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
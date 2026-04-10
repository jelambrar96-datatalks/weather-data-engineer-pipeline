"""@bruin
name: staging.a03_stations
type: python
connection: noaa_duckdb
description: "Staging table for stations dataset"
owner: "jelambrar@gmail.com"

materialization:
    type: table
    strategy: create+replace
    cluster_by: ["country", "province", "id"]

depends:
    - raw.a02_download_station
    - raw.a04_world_provinces

secrets:
  - key: noaa_duckdb


columns:
    - name: id
      type: varchar
      description: "Station ID"
      checks:
          - name: not_null
      primary_key: true
    - name: latitude
      type: float
      description: "Latitude"
      checks:
          - name: not_null
          - name: min
            value: -90
          - name: max
            value: 90
    - name: longitude
      type: float
      description: "Longitude"
      checks:
          - name: not_null
          - name: min
            value: -180
          - name: max
            value: 180
    - name: elevation
      type: float
      description: "Elevation"
      checks:
          - name: min
            value: -1000
    - name: name
      type: varchar
      description: "Station Name"
      checks: 
          - name: not_null
    - name: gsn_flag
      type: varchar
      description: "GSN Flag"
      checks:
        - name: pattern
          value: "^(GSN|)$"
    - name: hcn_crn_flag
      type: varchar
      description: "HCN/CRN Flag"
      checks:
        - name: pattern
          value: "^(HCN|CRN|)$"
    - name: wmo_id
      type: varchar
      description: "WMO ID"
    - name: province
      type: varchar
      description: "Province name"
      checks:
        - name: not_null
    - name: country
      type: varchar
      description: "Country name"
      checks:
        - name: not_null
    - name: iso_code
      type: varchar
      description: "ISO code"
      checks:
        - name: not_null
    - name: iso_cc
      type: varchar
      description: "ISO country code"
      checks:
        - name: not_null
    - name: iso_sub
      type: varchar
      description: "ISO subdivision code"
      checks:
        - name: not_null

custom_checks:
  - name: all rows are unique
    query: "SELECT case when count (*) = count(DISTINCT id) then 1 else 0 end as result FROM staging.a03_stations"
    value: 1
@bruin"""


import os
import duckdb
import json
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point


def materialize():

    noaa_duckdb_connection = json.loads(os.getenv("noaa_duckdb", "{}"))
    path_duckdb_database =  noaa_duckdb_connection["path"]
    conn = duckdb.connect(path_duckdb_database)
    
    df = conn.execute("""
    INSTALL spatial;
    LOAD spatial;
    SELECT 
        id, latitude, longitude, elevation, name, gsn_flag, hcn_crn_flag, wmo_id,
        province, country, iso_code, iso_cc, iso_sub
    FROM (
        -- Step 1: Fast Spatial Join using ST_Intersects (triggers spatial join operator)
        SELECT 
            s.id, s.latitude, s.longitude, s.elevation, s.name, s.gsn_flag, s.hcn_crn_flag, s.wmo_id,
            p.name as province, p.country, p.iso_code, p.iso_cc, p.iso_sub
        FROM raw.a02_download_station s
        JOIN raw.a04_world_provinces p ON ST_Intersects(ST_Point(s.longitude, s.latitude), ST_GeomFromGeoJSON(p.geometry))

        UNION ALL

        -- Step 2: Fallback for unmatched stations (e.g. offshore or boundary issues)
        SELECT 
            u.id, u.latitude, u.longitude, u.elevation, u.name, u.gsn_flag, u.hcn_crn_flag, u.wmo_id,
            n.province, n.country, n.iso_code, n.iso_cc, n.iso_sub
        FROM (
            SELECT *, ST_Point(longitude, latitude) as geom
            FROM raw.a02_download_station s1
            WHERE NOT EXISTS (
                SELECT 1 FROM raw.a04_world_provinces p1 
                WHERE ST_Intersects(ST_Point(s1.longitude, s1.latitude), ST_GeomFromGeoJSON(p1.geometry))
            )
        ) u
        CROSS JOIN (
            SELECT name as province, country, iso_code, iso_cc, iso_sub,
                   ST_GeomFromGeoJSON(geometry) as geom
            FROM raw.a04_world_provinces
        ) n
        QUALIFY ROW_NUMBER() OVER(PARTITION BY u.id ORDER BY ST_Distance(u.geom, n.geom)) = 1
    ) final_stations
    QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY province DESC) = 1;
    """).df()

    return df


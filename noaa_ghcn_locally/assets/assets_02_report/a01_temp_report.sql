/* @bruin
name: report.a01_temp_report
type: duckdb.sql
connection: noaa_duckdb
description: Temperature report
owner: "jelambrar@gmail.com"

materialization:
  type: view

depends:
  - staging.a03_stations
  - staging.a02_pivot_transformed_data

columns:
  - name: id
    type: integer
    description: Unique identifier
    checks:
      - name: not_null
    primary_key: true
  - name: station_id
    type: varchar
    description: Station ID
    checks:
      - name: not_null
  - name: station_name
    type: varchar
    description: Station name
    checks:
      - name: not_null
  - name: latitude
    type: float
    description: Station latitude
    checks:
      - name: not_null
      - name: min
        value: -90
      - name: max
        value: 90
  - name: longitude
    type: float
    description: Station longitude
    checks:
      - name: not_null
      - name: min
        value: -180
      - name: max
        value: 180
  - name: altitude
    type: float
    description: Station altitude (elevation)
    checks:
      - name: not_null
      - name: min
        value: -1000
  - name: province
    type: varchar
    description: Province name
    checks:
      - name: not_null
  - name: country
    type: varchar
    description: Country name
    checks:
      - name: not_null
  - name: iso_code
    type: varchar
    description: ISO code
    checks:
      - name: not_null
  - name: iso_cc
    type: varchar
    description: ISO country code
    checks:
      - name: not_null
  - name: iso_sub
    type: varchar
    description: ISO subdivision code
    checks:
      - name: not_null
  - name: date
    type: timestamp
    description: Observation date
    checks:
      - name: not_null
  - name: TMIN
    type: float
    description: Minimum temperature
    checks:
      - name: min
        value: -100
  - name: TMAX
    type: float
    description: Maximum temperature
    checks:
      - name: min
        value: -100
  - name: TAVG
    type: float
    description: Average temperature
    checks:
      - name: min
        value: -100
  - name: PRCP
    type: float
    description: Precipitation
    checks:
      - name: min
        value: 0
  - name: SNOW
    type: float
    description: Snowfall
    checks:
      - name: min
        value: 0
  - name: SNWD
    type: float
    description: Snow depth
    checks:
      - name: min
        value: 0

custom_checks:
  - name: all rows are unique
    value: 1
    query: SELECT case when count (*) = count(DISTINCT id) then 1 else 0 end as result FROM report.a01_temp_report

@bruin */

SELECT
    t.id,
    s.id AS station_id,
    s.name AS station_name,
    s.latitude,
    s.longitude,
    s.elevation AS altitude,
    s.province,
    s.country,
    s.iso_code,
    s.iso_cc,
    s.iso_sub,
    t.date,
    t.TMIN,
    t.TMAX,
    t.TAVG,
    t.PRCP,
    t.SNOW,
    t.SNWD
FROM staging.a03_stations s
JOIN staging.a02_pivot_transformed_data t ON s.id = t.station_id

/* @bruin
name: report.a01_temp_report
type: duckdb.sql
connection: noaa_duckdb

materialization:
  type: view

depends:
  - raw.a02_download_station
  - staging.a02_pivot_transformed_data

columns:
  - name: station_id
    type: varchar
    description: Station ID
  - name: station_name
    type: varchar
    description: Station name
  - name: latitude
    type: float
    description: Station latitude
  - name: longitude
    type: float
    description: Station longitude
  - name: altitude
    type: float
    description: Station altitude (elevation)
  - name: province
    type: varchar
    description: Province name
  - name: country
    type: varchar
    description: Country name
  - name: iso_code
    type: varchar
    description: ISO code
  - name: iso_cc
    type: varchar
    description: ISO country code
  - name: iso_sub
    type: varchar
    description: ISO subdivision code
  - name: date
    type: timestamp
    description: Observation date
  - name: TMIN
    type: float
    description: Minimum temperature
  - name: TMAX
    type: float
    description: Maximum temperature
  - name: TAVG
    type: float
    description: Average temperature
  - name: PRCP
    type: float
    description: Precipitation
  - name: SNOW
    type: float
    description: Snowfall
  - name: SNWD
    type: float
    description: Snow depth
@bruin */

SELECT
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

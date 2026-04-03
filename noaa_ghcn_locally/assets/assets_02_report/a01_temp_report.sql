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
  - name: latitude
    type: float
    description: Station latitude
  - name: longitude
    type: float
    description: Station longitude
  - name: altitude
    type: float
    description: Station altitude (elevation)
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
    s.latitude,
    s.longitude,
    s.elevation AS altitude,
    t.date,
    t.TMIN,
    t.TMAX,
    t.TAVG,
    t.PRCP,
    t.SNOW,
    t.SNWD
FROM raw.a02_download_station s
JOIN staging.a02_pivot_transformed_data t ON s.id = t.station_id

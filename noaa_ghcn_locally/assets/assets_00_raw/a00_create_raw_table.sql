"""@bruin
name: raw.noaa_ghcn
type: duckdb.sql
connection: noaa_duckdb

materialization:
    type: table
    strategy: create+replace
    destination: raw.noaa_ghcn
    partition_by: date

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
    type: varchar
    description: Observation date
    checks:
      - name: not_null
  - name: obs_time
    type: varchar
    description: Observation time
  - name: year
    type: integer
    description: Year partition column
@bruin"""

SELECT 
    CAST(NULL AS VARCHAR) AS station_id,
    CAST(NULL AS TIMESTAMP) AS date,
    CAST(NULL AS VARCHAR) AS element,
    CAST(NULL AS FLOAT) AS value,
    CAST(NULL AS VARCHAR) AS m_flag,
    CAST(NULL AS VARCHAR) AS q_flag,
    CAST(NULL AS VARCHAR) AS s_flag,
    CAST(NULL AS VARCHAR) AS obs_date,
    CAST(NULL AS VARCHAR) AS obs_time,
    CAST(NULL AS INTEGER) AS year
WHERE 1 = 0;
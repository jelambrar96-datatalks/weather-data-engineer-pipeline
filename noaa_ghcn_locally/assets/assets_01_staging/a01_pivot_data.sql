/* @bruin

name: staging.a01_pivot_data
type: duckdb.sql
description: Staging table for pivot data
connection: noaa_duckdb

materialization:
  type: table
  strategy: create+replace
  partition_by: date
  cluster_by:
    - station_id

depends:
  - raw.a01_download_raw_data
  - raw.a03_source_priority
owner: jelambrar@gmail.com

secrets:
  - key: noaa_duckdb
    inject_as: noaa_duckdb

columns:
  - name: id
    type: integer
    description: Observation id
    primary_key: true
  - name: date
    type: timestamp
    description: Observation date
    checks:
      - name: not_null
  - name: station_id
    type: varchar
    description: Station ID
    checks:
      - name: not_null
  - name: PRCP
    type: float
    description: Precipitation (mm)
    checks:
      - name: min
        value: 0
  - name: SNOW
    type: float
    description: Snowfall (mm)
    checks:
      - name: min
        value: 0
  - name: SNWD
    type: float
    description: Snow depth (mm)
    checks:
      - name: min
        value: 0
  - name: TMAX
    type: float
    description: Maximum temperature (degrees C)
    checks:
      - name: min
        value: -100
  - name: TMIN
    type: float
    description: Minimum temperature (degrees C)
    checks:
      - name: min
        value: -100
  - name: ACMC
    type: float
    description: Average cloudiness midnight to midnight from 30-second ceilometer data (percent)
    checks:
      - name: min
        value: 0
  - name: ACMH
    type: float
    description: Average cloudiness midnight to midnight from manual observations (percent)
    checks:
      - name: min
        value: 0
  - name: ACSC
    type: float
    description: Average cloudiness sunrise to sunset from 30-second ceilometer data (percent)
    checks:
      - name: min
        value: 0
  - name: ACSH
    type: float
    description: Average cloudiness sunrise to sunset from manual observations (percent)
    checks:
      - name: min
        value: 0
  - name: AWDR
    type: float
    description: Average daily wind direction (degrees)
    checks:
      - name: min
        value: 0
  - name: AWND
    type: float
    description: Average daily wind speed (meters per second)
    checks:
      - name: min
        value: 0
  - name: DAEV
    type: integer
    description: Number of days included in the multiday evaporation total (MDEV)
    checks:
      - name: min
        value: 0
  - name: DAPR
    type: integer
    description: Number of days included in the multiday precipitation total (MDPR)
    checks:
      - name: min
        value: 0
  - name: DASF
    type: integer
    description: Number of days included in the multiday snowfall total (MDSF)
    checks:
      - name: min
        value: 0
  - name: DATN
    type: integer
    description: Number of days included in the multiday minimum temperature (MDTN)
    checks:
      - name: min
        value: 0
  - name: DATX
    type: integer
    description: Number of days included in the multiday maximum temperature (MDTX)
    checks:
      - name: min
        value: 0
  - name: DAWM
    type: integer
    description: Number of days included in the multiday wind movement (MDWM)
    checks:
      - name: min
        value: 0
  - name: DWPR
    type: integer
    description: Number of days with non-zero precipitation included in multiday precipitation total (MDPR)
    checks:
      - name: min
        value: 0
  - name: EVAP
    type: float
    description: Evaporation of water from evaporation pan (mm)
    checks:
      - name: min
        value: -100
  - name: FMTM
    type: varchar
    description: Time of fastest mile or fastest 1-minute wind (hours and minutes, i.e., HHMM)
  - name: FRGB
    type: float
    description: Base of frozen ground layer (cm)
    checks:
      - name: min
        value: 0
  - name: FRGT
    type: float
    description: Top of frozen ground layer (cm)
    checks:
      - name: min
        value: 0
  - name: FRTH
    type: float
    description: Thickness of frozen ground layer (cm)
    checks:
      - name: min
        value: 0
  - name: GAHT
    type: float
    description: Difference between river and gauge height (cm)
  - name: MDEV
    type: float
    description: Multiday evaporation total (mm; use with DAEV)
    checks:
      - name: min
        value: 0
  - name: MDPR
    type: float
    description: Multiday precipitation total (mm; use with DAPR and DWPR, if available)
    checks:
      - name: min
        value: 0
  - name: MDSF
    type: float
    description: Multiday snowfall total
    checks:
      - name: min
        value: 0
  - name: MDTN
    type: float
    description: Multiday minimum temperature (degrees C; use with DATN)
    checks:
      - name: min
        value: -100
  - name: MDTX
    type: float
    description: Multiday maximum temperature (degrees C; use with DATX)
    checks:
      - name: min
        value: -100
  - name: MDWM
    type: float
    description: Multiday wind movement (km)
    checks:
      - name: min
        value: 0
  - name: MNPN
    type: float
    description: Daily minimum temperature of water in an evaporation pan (degrees C)
    checks:
      - name: min
        value: -100
  - name: MXPN
    type: float
    description: Daily maximum temperature of water in an evaporation pan (degrees C)
    checks:
      - name: min
        value: -100
  - name: PGTM
    type: varchar
    description: Peak gust time (hours and minutes, i.e., HHMM)
  - name: PSUN
    type: float
    description: Daily percent of possible sunshine (percent)
    checks:
      - name: min
        value: 0
      - name: max
        value: 100
  - name: TAVG
    type: float
    description: Average temperature (degrees C)
    checks:
      - name: min
        value: -100
  - name: THIC
    type: float
    description: Thickness of ice on water (mm)
    checks:
      - name: min
        value: 0
  - name: TOBS
    type: float
    description: Temperature at the time of observation (degrees C)
    checks:
      - name: min
        value: -100
  - name: TSUN
    type: float
    description: Daily total sunshine (minutes)
    checks:
      - name: min
        value: 0
  - name: WDF1
    type: float
    description: Direction of fastest 1-minute wind (degrees)
    checks:
      - name: min
        value: 0
  - name: WDF2
    type: float
    description: Direction of fastest 2-minute wind (degrees)
    checks:
      - name: min
        value: 0
  - name: WDF5
    type: float
    description: Direction of fastest 5-second wind (degrees)
    checks:
      - name: min
        value: 0
  - name: WDFG
    type: float
    description: Direction of peak wind gust (degrees)
    checks:
      - name: min
        value: 0
  - name: WDFI
    type: float
    description: Direction of highest instantaneous wind (degrees)
    checks:
      - name: min
        value: 0
  - name: WDFM
    type: float
    description: Fastest mile wind direction (degrees)
    checks:
      - name: min
        value: 0
  - name: WDMV
    type: float
    description: 24-hour wind movement (km)
    checks:
      - name: min
        value: 0
  - name: WESD
    type: float
    description: Water equivalent of snow on the ground (mm)
    checks:
      - name: min
        value: 0
  - name: WESF
    type: float
    description: Water equivalent of snowfall (mm)
    checks:
      - name: min
        value: 0
  - name: WSF1
    type: float
    description: Fastest 1-minute wind speed (meters per second)
    checks:
      - name: min
        value: 0
  - name: WSF2
    type: float
    description: Fastest 2-minute wind speed (meters per second)
    checks:
      - name: min
        value: 0
  - name: WSF5
    type: float
    description: Fastest 5-second wind speed (meters per second)
    checks:
      - name: min
        value: 0
  - name: WSFG
    type: float
    description: Peak gust wind speed (meters per second)
    checks:
      - name: min
        value: 0
  - name: WSFI
    type: float
    description: Highest instantaneous wind speed (meters per second)
    checks:
      - name: min
        value: 0
  - name: WSFM
    type: float
    description: Fastest mile wind speed (meters per second)
    checks:
      - name: min
        value: 0
  - name: WT01
    type: boolean
    description: Fog, ice fog, or freezing fog (may include heavy fog)
    checks:
      - name: accepted_values
        value:
          - "true"
          - "false"
  - name: WT02
    type: boolean
    description: Heavy fog or heaving freezing fog (not always distinguished from fog)
    checks:
      - name: accepted_values
        value:
          - "true"
          - "false"
  - name: WT03
    type: boolean
    description: Thunder
    checks:
      - name: accepted_values
        value:
          - "true"
          - "false"
  - name: WT04
    type: boolean
    description: Ice pellets, sleet, snow pellets, or small hail
    checks:
      - name: accepted_values
        value:
          - "true"
          - "false"
  - name: WT05
    type: boolean
    description: Hail (may include small hail)
    checks:
      - name: accepted_values
        value:
          - "true"
          - "false"
  - name: WT06
    type: boolean
    description: Glaze or rime
    checks:
      - name: accepted_values
        value:
          - "true"
          - "false"
  - name: WT07
    type: boolean
    description: Dust, volcanic ash, blowing dust, blowing sand, or blowing obstruction
    checks:
      - name: accepted_values
        value:
          - "true"
          - "false"
  - name: WT08
    type: boolean
    description: Smoke or haze
    checks:
      - name: accepted_values
        value:
          - "true"
          - "false"
  - name: WT09
    type: boolean
    description: Blowing or drifting snow
    checks:
      - name: accepted_values
        value:
          - "true"
          - "false"
  - name: WT10
    type: boolean
    description: Tornado, waterspout, or funnel cloud
    checks:
      - name: accepted_values
        value:
          - "true"
          - "false"
  - name: WT11
    type: boolean
    description: High or damaging winds
    checks:
      - name: accepted_values
        value:
          - "true"
          - "false"
  - name: WT12
    type: boolean
    description: Blowing spray
    checks:
      - name: accepted_values
        value:
          - "true"
          - "false"
  - name: WT13
    type: boolean
    description: Mist
    checks:
      - name: accepted_values
        value:
          - "true"
          - "false"
  - name: WT14
    type: boolean
    description: Drizzle
    checks:
      - name: accepted_values
        value:
          - "true"
          - "false"
  - name: WT15
    type: boolean
    description: Freezing drizzle
    checks:
      - name: accepted_values
        value:
          - "true"
          - "false"
  - name: WT16
    type: boolean
    description: Rain (may include freezing rain, drizzle, and freezing drizzle)
    checks:
      - name: accepted_values
        value:
          - "true"
          - "false"
  - name: WT17
    type: boolean
    description: Freezing rain
    checks:
      - name: accepted_values
        value:
          - "true"
          - "false"
  - name: WT18
    type: boolean
    description: Snow, snow pellets, snow grains, or ice crystals
    checks:
      - name: accepted_values
        value:
          - "true"
          - "false"
  - name: WT19
    type: boolean
    description: Unknown source of precipitation
    checks:
      - name: accepted_values
        value:
          - "true"
          - "false"
  - name: WT20
    type: boolean
    description: Ground fog
    checks:
      - name: accepted_values
        value:
          - "true"
          - "false"
  - name: WT21
    type: boolean
    description: Ground fog
    checks:
      - name: accepted_values
        value:
          - "true"
          - "false"
  - name: WT22
    type: boolean
    description: Ice fog or freezing fog
    checks:
      - name: accepted_values
        value:
          - "true"
          - "false"

custom_checks:
  - name: all rows are unique
    value: 1
    query: SELECT case when count (*) = count(DISTINCT id) then 1 else 0 end as result FROM staging.a01_pivot_data

@bruin */

WITH raw_data AS (
    SELECT * FROM raw.a01_download_raw_data
    WHERE 1 = 1 
        AND date IS NOT NULL
        AND station_id IS NOT NULL
        AND element IS NOT NULL
        AND value IS NOT NULL
        AND s_flag IS NOT NULL
        -- Applying checks as filters
        AND (
            (element = 'PRCP' AND TRY_CAST(value AS FLOAT) >= 0) OR
            (element = 'SNOW' AND TRY_CAST(value AS FLOAT) >= 0) OR
            (element = 'SNWD' AND TRY_CAST(value AS FLOAT) >= 0) OR
            (element = 'TMAX' AND TRY_CAST(value AS FLOAT) >= -100) OR
            (element = 'TMIN' AND TRY_CAST(value AS FLOAT) >= -100) OR
            (element = 'ACMC' AND TRY_CAST(value AS FLOAT) >= 0) OR
            (element = 'ACMH' AND TRY_CAST(value AS FLOAT) >= 0) OR
            (element = 'ACSC' AND TRY_CAST(value AS FLOAT) >= 0) OR
            (element = 'ACSH' AND TRY_CAST(value AS FLOAT) >= 0) OR
            (element = 'AWDR' AND TRY_CAST(value AS FLOAT) >= 0) OR
            (element = 'AWND' AND TRY_CAST(value AS FLOAT) >= 0) OR
            (element IN ('DAEV', 'DAPR', 'DASF', 'DATN', 'DATX', 'DAWM', 'DWPR') AND TRY_CAST(value AS INTEGER) >= 0) OR
            (element = 'EVAP' AND TRY_CAST(value AS FLOAT) >= 0) OR
            (element = 'FRGB' AND TRY_CAST(value AS FLOAT) >= 0) OR
            (element = 'FRGT' AND TRY_CAST(value AS FLOAT) >= 0) OR
            (element = 'FRTH' AND TRY_CAST(value AS FLOAT) >= 0) OR
            (element = 'MDEV' AND TRY_CAST(value AS FLOAT) >= 0) OR
            (element = 'MDPR' AND TRY_CAST(value AS FLOAT) >= 0) OR
            (element = 'MDSF' AND TRY_CAST(value AS FLOAT) >= 0) OR
            (element = 'MDTN' AND TRY_CAST(value AS FLOAT) >= -100) OR
            (element = 'MDTX' AND TRY_CAST(value AS FLOAT) >= -100) OR
            (element = 'MDWM' AND TRY_CAST(value AS FLOAT) >= 0) OR
            (element = 'MNPN' AND TRY_CAST(value AS FLOAT) >= -100) OR
            (element = 'MXPN' AND TRY_CAST(value AS FLOAT) >= -100) OR
            (element = 'PSUN' AND TRY_CAST(value AS FLOAT) BETWEEN 0 AND 100) OR
            (element = 'TAVG' AND TRY_CAST(value AS FLOAT) >= -100) OR
            (element = 'THIC' AND TRY_CAST(value AS FLOAT) >= 0) OR
            (element = 'TOBS' AND TRY_CAST(value AS FLOAT) >= -100) OR
            (element = 'TSUN' AND TRY_CAST(value AS FLOAT) >= 0) OR
            (element LIKE 'WD%' AND TRY_CAST(value AS FLOAT) >= 0) OR
            (element LIKE 'WS%' AND TRY_CAST(value AS FLOAT) >= 0) OR
            (element LIKE 'WT%' AND TRY_CAST(value AS VARCHAR) IN ('0', '1', 'true', 'false')) OR
            (element IN ('FMTM', 'PGTM', 'GAHT')) -- Elements without specific range checks in metadata
        )
),
prioritized_data AS (
    SELECT
        d.date,
        d.station_id,
        d.element,
        d.value,
        d.s_flag,
        COALESCE(p.priority, 999) AS priority,
        ROW_NUMBER() OVER (
            PARTITION BY d.date, d.station_id, d.element
            ORDER BY COALESCE(p.priority, 999) ASC
        ) AS rn
    FROM raw_data d
    LEFT JOIN raw.a03_source_priority p
        ON d.s_flag = p.source
),
filtered_data AS (
    SELECT
        date,
        station_id,
        element,
        value,
        s_flag
    FROM prioritized_data
    WHERE rn = 1
),
pivoted AS (
    SELECT
        date,
        station_id,
        MAX(CASE WHEN element = 'PRCP' THEN TRY_CAST(value AS FLOAT) END) AS PRCP,
        MAX(CASE WHEN element = 'SNOW' THEN TRY_CAST(value AS FLOAT) END) AS SNOW,
        MAX(CASE WHEN element = 'SNWD' THEN TRY_CAST(value AS FLOAT) END) AS SNWD,
        MAX(CASE WHEN element = 'TMAX' THEN TRY_CAST(value AS FLOAT) END) AS TMAX,
        MAX(CASE WHEN element = 'TMIN' THEN TRY_CAST(value AS FLOAT) END) AS TMIN,
        MAX(CASE WHEN element = 'ACMC' THEN TRY_CAST(value AS FLOAT) END) AS ACMC,
        MAX(CASE WHEN element = 'ACMH' THEN TRY_CAST(value AS FLOAT) END) AS ACMH,
        MAX(CASE WHEN element = 'ACSC' THEN TRY_CAST(value AS FLOAT) END) AS ACSC,
        MAX(CASE WHEN element = 'ACSH' THEN TRY_CAST(value AS FLOAT) END) AS ACSH,
        MAX(CASE WHEN element = 'AWDR' THEN TRY_CAST(value AS FLOAT) END) AS AWDR,
        MAX(CASE WHEN element = 'AWND' THEN TRY_CAST(value AS FLOAT) END) AS AWND,
        MAX(CASE WHEN element = 'DAEV' THEN TRY_CAST(value AS INTEGER) END) AS DAEV,
        MAX(CASE WHEN element = 'DAPR' THEN TRY_CAST(value AS INTEGER) END) AS DAPR,
        MAX(CASE WHEN element = 'DASF' THEN TRY_CAST(value AS INTEGER) END) AS DASF,
        MAX(CASE WHEN element = 'DATN' THEN TRY_CAST(value AS INTEGER) END) AS DATN,
        MAX(CASE WHEN element = 'DATX' THEN TRY_CAST(value AS INTEGER) END) AS DATX,
        MAX(CASE WHEN element = 'DAWM' THEN TRY_CAST(value AS INTEGER) END) AS DAWM,
        MAX(CASE WHEN element = 'DWPR' THEN TRY_CAST(value AS INTEGER) END) AS DWPR,
        MAX(CASE WHEN element = 'EVAP' THEN TRY_CAST(value AS FLOAT) END) AS EVAP,
        MAX(CASE WHEN element = 'FMTM' THEN value END) AS FMTM,
        MAX(CASE WHEN element = 'FRGB' THEN TRY_CAST(value AS FLOAT) END) AS FRGB,
        MAX(CASE WHEN element = 'FRGT' THEN TRY_CAST(value AS FLOAT) END) AS FRGT,
        MAX(CASE WHEN element = 'FRTH' THEN TRY_CAST(value AS FLOAT) END) AS FRTH,
        MAX(CASE WHEN element = 'GAHT' THEN TRY_CAST(value AS FLOAT) END) AS GAHT,
        MAX(CASE WHEN element = 'MDEV' THEN TRY_CAST(value AS FLOAT) END) AS MDEV,
        MAX(CASE WHEN element = 'MDPR' THEN TRY_CAST(value AS FLOAT) END) AS MDPR,
        MAX(CASE WHEN element = 'MDSF' THEN TRY_CAST(value AS FLOAT) END) AS MDSF,
        MAX(CASE WHEN element = 'MDTN' THEN TRY_CAST(value AS FLOAT) END) AS MDTN,
        MAX(CASE WHEN element = 'MDTX' THEN TRY_CAST(value AS FLOAT) END) AS MDTX,
        MAX(CASE WHEN element = 'MDWM' THEN TRY_CAST(value AS FLOAT) END) AS MDWM,
        MAX(CASE WHEN element = 'MNPN' THEN TRY_CAST(value AS FLOAT) END) AS MNPN,
        MAX(CASE WHEN element = 'MXPN' THEN TRY_CAST(value AS FLOAT) END) AS MXPN,
        MAX(CASE WHEN element = 'PGTM' THEN value END) AS PGTM,
        MAX(CASE WHEN element = 'PSUN' THEN TRY_CAST(value AS FLOAT) END) AS PSUN,
        MAX(CASE WHEN element = 'TAVG' THEN TRY_CAST(value AS FLOAT) END) AS TAVG,
        MAX(CASE WHEN element = 'THIC' THEN TRY_CAST(value AS FLOAT) END) AS THIC,
        MAX(CASE WHEN element = 'TOBS' THEN TRY_CAST(value AS FLOAT) END) AS TOBS,
        MAX(CASE WHEN element = 'TSUN' THEN TRY_CAST(value AS FLOAT) END) AS TSUN,
        MAX(CASE WHEN element = 'WDF1' THEN TRY_CAST(value AS FLOAT) END) AS WDF1,
        MAX(CASE WHEN element = 'WDF2' THEN TRY_CAST(value AS FLOAT) END) AS WDF2,
        MAX(CASE WHEN element = 'WDF5' THEN TRY_CAST(value AS FLOAT) END) AS WDF5,
        MAX(CASE WHEN element = 'WDFG' THEN TRY_CAST(value AS FLOAT) END) AS WDFG,
        MAX(CASE WHEN element = 'WDFI' THEN TRY_CAST(value AS FLOAT) END) AS WDFI,
        MAX(CASE WHEN element = 'WDFM' THEN TRY_CAST(value AS FLOAT) END) AS WDFM,
        MAX(CASE WHEN element = 'WDMV' THEN TRY_CAST(value AS FLOAT) END) AS WDMV,
        MAX(CASE WHEN element = 'WESD' THEN TRY_CAST(value AS FLOAT) END) AS WESD,
        MAX(CASE WHEN element = 'WESF' THEN TRY_CAST(value AS FLOAT) END) AS WESF,
        MAX(CASE WHEN element = 'WSF1' THEN TRY_CAST(value AS FLOAT) END) AS WSF1,
        MAX(CASE WHEN element = 'WSF2' THEN TRY_CAST(value AS FLOAT) END) AS WSF2,
        MAX(CASE WHEN element = 'WSF5' THEN TRY_CAST(value AS FLOAT) END) AS WSF5,
        MAX(CASE WHEN element = 'WSFG' THEN TRY_CAST(value AS FLOAT) END) AS WSFG,
        MAX(CASE WHEN element = 'WSFI' THEN TRY_CAST(value AS FLOAT) END) AS WSFI,
        MAX(CASE WHEN element = 'WSFM' THEN TRY_CAST(value AS FLOAT) END) AS WSFM,
        MAX(CASE WHEN element = 'WT01' THEN TRY_CAST(value AS BOOLEAN) END) AS WT01,
        MAX(CASE WHEN element = 'WT02' THEN TRY_CAST(value AS BOOLEAN) END) AS WT02,
        MAX(CASE WHEN element = 'WT03' THEN TRY_CAST(value AS BOOLEAN) END) AS WT03,
        MAX(CASE WHEN element = 'WT04' THEN TRY_CAST(value AS BOOLEAN) END) AS WT04,
        MAX(CASE WHEN element = 'WT05' THEN TRY_CAST(value AS BOOLEAN) END) AS WT05,
        MAX(CASE WHEN element = 'WT06' THEN TRY_CAST(value AS BOOLEAN) END) AS WT06,
        MAX(CASE WHEN element = 'WT07' THEN TRY_CAST(value AS BOOLEAN) END) AS WT07,
        MAX(CASE WHEN element = 'WT08' THEN TRY_CAST(value AS BOOLEAN) END) AS WT08,
        MAX(CASE WHEN element = 'WT09' THEN TRY_CAST(value AS BOOLEAN) END) AS WT09,
        MAX(CASE WHEN element = 'WT10' THEN TRY_CAST(value AS BOOLEAN) END) AS WT10,
        MAX(CASE WHEN element = 'WT11' THEN TRY_CAST(value AS BOOLEAN) END) AS WT11,
        MAX(CASE WHEN element = 'WT12' THEN TRY_CAST(value AS BOOLEAN) END) AS WT12,
        MAX(CASE WHEN element = 'WT13' THEN TRY_CAST(value AS BOOLEAN) END) AS WT13,
        MAX(CASE WHEN element = 'WT14' THEN TRY_CAST(value AS BOOLEAN) END) AS WT14,
        MAX(CASE WHEN element = 'WT15' THEN TRY_CAST(value AS BOOLEAN) END) AS WT15,
        MAX(CASE WHEN element = 'WT16' THEN TRY_CAST(value AS BOOLEAN) END) AS WT16,
        MAX(CASE WHEN element = 'WT17' THEN TRY_CAST(value AS BOOLEAN) END) AS WT17,
        MAX(CASE WHEN element = 'WT18' THEN TRY_CAST(value AS BOOLEAN) END) AS WT18,
        MAX(CASE WHEN element = 'WT19' THEN TRY_CAST(value AS BOOLEAN) END) AS WT19,
        MAX(CASE WHEN element = 'WT20' THEN TRY_CAST(value AS BOOLEAN) END) AS WT20,
        MAX(CASE WHEN element = 'WT21' THEN TRY_CAST(value AS BOOLEAN) END) AS WT21,
        MAX(CASE WHEN element = 'WT22' THEN TRY_CAST(value AS BOOLEAN) END) AS WT22
    FROM filtered_data
    GROUP BY date, station_id
)
SELECT
    row_number() OVER () AS id,
    date,
    station_id,
    -- Convert tenths to actual units (divide by 10)
    PRCP / 10.0 AS PRCP,
    SNOW,
    SNWD,
    TMAX / 10.0 AS TMAX,
    TMIN / 10.0 AS TMIN,
    ACMC,
    ACMH,
    ACSC,
    ACSH,
    AWDR,
    AWND / 10.0 AS AWND,
    DAEV, DAPR, DASF, DATN, DATX, DAWM, DWPR,
    EVAP / 10.0 AS EVAP,
    FMTM, FRGB, FRGT, FRTH,
    GAHT,
    MDEV / 10.0 AS MDEV,
    MDPR / 10.0 AS MDPR,
    MDSF,
    MDTN / 10.0 AS MDTN,
    MDTX / 10.0 AS MDTX,
    MDWM,
    MNPN / 10.0 AS MNPN,
    MXPN / 10.0 AS MXPN,
    PGTM, PSUN,
    TAVG / 10.0 AS TAVG,
    THIC / 10.0 AS THIC,
    TOBS / 10.0 AS TOBS,
    TSUN,
    WDF1, WDF2, WDF5, WDFG, WDFI, WDFM, WDMV,
    WESD / 10.0 AS WESD,
    WESF / 10.0 AS WESF,
    WSF1 / 10.0 AS WSF1,
    WSF2 / 10.0 AS WSF2,
    WSF5 / 10.0 AS WSF5,
    WSFG / 10.0 AS WSFG,
    WSFI / 10.0 AS WSFI,
    WSFM / 10.0 AS WSFM,
    WT01, WT02, WT03, WT04, WT05, WT06, WT07, WT08,
    WT09, WT10, WT11, WT12, WT13, WT14, WT15, WT16,
    WT17, WT18, WT19, WT20, WT21, WT22,
FROM pivoted;

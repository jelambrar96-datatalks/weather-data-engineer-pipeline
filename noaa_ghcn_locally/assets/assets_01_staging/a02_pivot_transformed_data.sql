/* @bruin
name: staging.a02_pivot_transformed_data
type: duckdb.sql
connection: noaa_duckdb
description: Pivot transformed data from staging.a01_pivot_data
owner: "jelambrar@gmail.com"

materialization:
  type: table
  strategy: create+replace
  partition_by: date
  cluster_by: ["station_id"]

depends:
  - staging.a01_pivot_data

columns:
  - name: id
    type: integer
    description: Unique identifier
    checks:
      - name: not_null
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
  - name: EVAP
    type: float
    description: Evaporation of water from evaporation pan (mm)
    checks:
      - name: min
        value: 0
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
    checks:
      - name: min
        value: 0
  - name: MDEV
    type: float
    description: Distributed daily evaporation from multiday total (mm)
    checks:
      - name: min
        value: 0
  - name: MDPR
    type: float
    description: Distributed daily precipitation from multiday total (mm)
    checks:
      - name: min
        value: 0
  - name: MDSF
    type: float
    description: Distributed daily snowfall from multiday total (mm)
    checks:
      - name: min
        value: 0
  - name: MDTN
    type: float
    description: Distributed daily minimum temperature from multiday total (degrees C)
    checks:
      - name: min
        value: -100
  - name: MDTX
    type: float
    description: Distributed daily maximum temperature from multiday total (degrees C)
    checks:
      - name: min
        value: -100
  - name: MDWM
    type: float
    description: Distributed daily wind movement from multiday total (km)
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
        value: [true, false]
  - name: WT02
    type: boolean
    description: Heavy fog or heaving freezing fog (not always distinguished from fog)
    checks:
      - name: accepted_values
        value: [true, false]
  - name: WT03
    type: boolean
    description: Thunder
    checks:
      - name: accepted_values
        value: [true, false]
  - name: WT04
    type: boolean
    description: Ice pellets, sleet, snow pellets, or small hail
    checks:
      - name: accepted_values
        value: [true, false]
  - name: WT05
    type: boolean
    description: Hail (may include small hail)
    checks:
      - name: accepted_values
        value: [true, false]
  - name: WT06
    type: boolean
    description: Glaze or rime
    checks:
      - name: accepted_values
        value: [true, false]
  - name: WT07
    type: boolean
    description: Dust, volcanic ash, blowing dust, blowing sand, or blowing obstruction
    checks:
      - name: accepted_values
        value: [true, false]
  - name: WT08
    type: boolean
    description: Smoke or haze
    checks:
      - name: accepted_values
        value: [true, false]
  - name: WT09
    type: boolean
    description: Blowing or drifting snow
    checks:
      - name: accepted_values
        value: [true, false]
  - name: WT10
    type: boolean
    description: Tornado, waterspout, or funnel cloud
    checks:
      - name: accepted_values
        value: [true, false]
  - name: WT11
    type: boolean
    description: High or damaging winds
    checks:
      - name: accepted_values
        value: [true, false]
  - name: WT12
    type: boolean
    description: Blowing spray
    checks:
      - name: accepted_values
        value: [true, false]
  - name: WT13
    type: boolean
    description: Mist
    checks:
      - name: accepted_values
        value: [true, false]
  - name: WT14
    type: boolean
    description: Drizzle
    checks:
      - name: accepted_values
        value: [true, false]
  - name: WT15
    type: boolean
    description: Freezing drizzle
    checks:
      - name: accepted_values
        value: [true, false]
  - name: WT16
    type: boolean
    description: Rain (may include freezing rain, drizzle, and freezing drizzle)
    checks:
      - name: accepted_values
        value: [true, false]
  - name: WT17
    type: boolean
    description: Freezing rain
    checks:
      - name: accepted_values
        value: [true, false]
  - name: WT18
    type: boolean
    description: Snow, snow pellets, snow grains, or ice crystals
    checks:
      - name: accepted_values
        value: [true, false]
  - name: WT19
    type: boolean
    description: Unknown source of precipitation
    checks:
      - name: accepted_values
        value: [true, false]
  - name: WT20
    type: boolean
    description: Ground fog
    checks:
      - name: accepted_values
        value: [true, false]
  - name: WT21
    type: boolean
    description: Ground fog
    checks:
      - name: accepted_values
        value: [true, false]
  - name: WT22
    type: boolean
    description: Ice fog or freezing fog
    checks:
      - name: accepted_values
        value: [true, false]

custom_checks:
  - name: all rows are unique
    value: 1
    query: SELECT case when count (*) = count(DISTINCT id) then 1 else 0 end as result FROM staging.a02_pivot_transformed_data

@bruin */

WITH source_data AS (
    SELECT * FROM staging.a01_pivot_data
    WHERE 1 = 1 
        AND date IS NOT NULL AND station_id IS NOT NULL
        AND date >= '{{ start_date }}' 
        AND date <= '{{ end_date }}'
),

-- Expand multiday records into individual days
-- For each multiday record, generate N rows (one for each day in the period)
multiday_expanded AS (
    SELECT
        date::DATE - INTERVAL '1 day' * (day_offset + 1) AS date,
        station_id,
        -- Calculate daily average and distribute to previous days
        CASE WHEN DAEV > 0 AND DAEV IS NOT NULL THEN MDEV / DAEV ELSE NULL END AS MDEV,
        CASE WHEN DAPR > 0 AND DAPR IS NOT NULL THEN MDPR / DAPR ELSE NULL END AS MDPR,
        CASE WHEN DASF > 0 AND DASF IS NOT NULL THEN MDSF / DASF ELSE NULL END AS MDSF,
        CASE WHEN DATN > 0 AND DATN IS NOT NULL THEN MDTN / DATN ELSE NULL END AS MDTN,
        CASE WHEN DATX > 0 AND DATX IS NOT NULL THEN MDTX / DATX ELSE NULL END AS MDTX,
        CASE WHEN DAWM > 0 AND DAWM IS NOT NULL THEN MDWM / DAWM ELSE NULL END AS MDWM
    FROM source_data
    CROSS JOIN UNNEST(GENERATE_SERIES(0,
        COALESCE(NULLIF(GREATEST(
            COALESCE(DAEV, 0),
            COALESCE(DAPR, 0),
            COALESCE(DASF, 0),
            COALESCE(DATN, 0),
            COALESCE(DATX, 0),
            COALESCE(DAWM, 0)
        ), 0) - 1, 0)
    )) AS t(day_offset)
    WHERE DAEV > 0 OR DAPR > 0 OR DASF > 0 OR DATN > 0 OR DATX > 0 OR DAWM > 0
),

-- Aggregate distributed values by date and station
multiday_aggregated AS (
    SELECT
        date::TIMESTAMP AS date,
        station_id,
        SUM(MDEV) AS MDEV,
        SUM(MDPR) AS MDPR,
        SUM(MDSF) AS MDSF,
        SUM(MDTN) AS MDTN,
        SUM(MDTX) AS MDTX,
        SUM(MDWM) AS MDWM
    FROM multiday_expanded
    GROUP BY date, station_id
),

-- Original records without multiday values (set MD* to NULL when DA* > 0)
base_records AS (
    SELECT
        date,
        station_id,
        PRCP,
        SNOW,
        SNWD,
        TMAX,
        TMIN,
        ACMC,
        ACMH,
        ACSC,
        ACSH,
        AWDR,
        AWND,
        -- Set MD* to NULL if corresponding DA* > 0 (will be filled from multiday_aggregated)
        CASE WHEN DAEV > 0 AND DAEV IS NOT NULL THEN NULL ELSE MDEV END AS MDEV,
        CASE WHEN DAPR > 0 AND DAPR IS NOT NULL THEN NULL ELSE MDPR END AS MDPR,
        CASE WHEN DASF > 0 AND DASF IS NOT NULL THEN NULL ELSE MDSF END AS MDSF,
        CASE WHEN DATN > 0 AND DATN IS NOT NULL THEN NULL ELSE MDTN END AS MDTN,
        CASE WHEN DATX > 0 AND DATX IS NOT NULL THEN NULL ELSE MDTX END AS MDTX,
        CASE WHEN DAWM > 0 AND DAWM IS NOT NULL THEN NULL ELSE MDWM END AS MDWM,
        EVAP,
        FMTM,
        FRGB,
        FRGT,
        FRTH,
        GAHT,
        MNPN,
        MXPN,
        PGTM,
        PSUN,
        TAVG,
        THIC,
        TOBS,
        TSUN,
        WDF1,
        WDF2,
        WDF5,
        WDFG,
        WDFI,
        WDFM,
        WDMV,
        WESD,
        WESF,
        WSF1,
        WSF2,
        WSF5,
        WSFG,
        WSFI,
        WSFM,
        WT01,
        WT02,
        WT03,
        WT04,
        WT05,
        WT06,
        WT07,
        WT08,
        WT09,
        WT10,
        WT11,
        WT12,
        WT13,
        WT14,
        WT15,
        WT16,
        WT17,
        WT18,
        WT19,
        WT20,
        WT21,
        WT22
    FROM source_data
),

-- Combine base records with distributed multiday values
combined AS (
    SELECT
        COALESCE(b.date, m.date) AS date,
        COALESCE(b.station_id, m.station_id) AS station_id,
        b.PRCP,
        b.SNOW,
        b.SNWD,
        b.TMAX,
        b.TMIN,
        b.ACMC,
        b.ACMH,
        b.ACSC,
        b.ACSH,
        b.AWDR,
        b.AWND,
        -- Use distributed value if available, otherwise keep base value
        COALESCE(m.MDEV, b.MDEV) AS MDEV,
        COALESCE(m.MDPR, b.MDPR) AS MDPR,
        COALESCE(m.MDSF, b.MDSF) AS MDSF,
        COALESCE(m.MDTN, b.MDTN) AS MDTN,
        COALESCE(m.MDTX, b.MDTX) AS MDTX,
        COALESCE(m.MDWM, b.MDWM) AS MDWM,
        b.EVAP,
        b.FMTM,
        b.FRGB,
        b.FRGT,
        b.FRTH,
        b.GAHT,
        b.MNPN,
        b.MXPN,
        b.PGTM,
        b.PSUN,
        b.TAVG,
        b.THIC,
        b.TOBS,
        b.TSUN,
        b.WDF1,
        b.WDF2,
        b.WDF5,
        b.WDFG,
        b.WDFI,
        b.WDFM,
        b.WDMV,
        b.WESD,
        b.WESF,
        b.WSF1,
        b.WSF2,
        b.WSF5,
        b.WSFG,
        b.WSFI,
        b.WSFM,
        b.WT01,
        b.WT02,
        b.WT03,
        b.WT04,
        b.WT05,
        b.WT06,
        b.WT07,
        b.WT08,
        b.WT09,
        b.WT10,
        b.WT11,
        b.WT12,
        b.WT13,
        b.WT14,
        b.WT15,
        b.WT16,
        b.WT17,
        b.WT18,
        b.WT19,
        b.WT20,
        b.WT21,
        b.WT22
    FROM base_records b
    FULL OUTER JOIN multiday_aggregated m
        ON b.date = m.date AND b.station_id = m.station_id
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY station_id, date) AS id,
    *
FROM combined
WHERE date IS NOT NULL
  AND station_id IS NOT NULL
ORDER BY station_id, date

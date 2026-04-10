/* @bruin
name: report.a02_weather_report
type: duckdb.sql
connection: noaa_duckdb
description: Weather report
owner: "jelambrar@gmail.com"

materialization:
  type: view

depends:
  - report.a01_temp_report

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
  - name: weather_symbol
    type: varchar
    description: Weather symbol
    checks:
      - name: accepted_values
        values: ['❄️🌨️ Heavy snow', '🌨️ Moderate snow', '⛄ Snow on ground', '⛈️ Heavy rain', '🌧️ Moderate rain', '🌦️ Light rain', '☀️🌡️ Hot', '☀️ Sunny', '🌤️ Cold and clear', '🥶 Below freezing', '❓ Unclassified']
  - name: climate_code
    type: int
    description: Climate code
    checks:
      - name: min
        value: -2
      - name: max
        value: 7

custom_checks:
  - name: all rows are unique
    value: 1
    query: SELECT case when count (*) = count(DISTINCT id) then 1 else 0 end as result FROM report.a02_weather_report

  - name: all weather symbols are valid
    value: 1
    query: |
      SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
      FROM (
          SELECT weather_symbol
          FROM report.a02_weather_report
          GROUP BY weather_symbol
          HAVING COUNT(DISTINCT climate_code) > 1
      ) AS invalid_mappings

@bruin */



SELECT
    id,
    station_id,
    station_name,
    latitude,
    longitude,
    altitude,
    country,
    province,
    iso_code,
    iso_cc,
    iso_sub,
    date,
    TMIN,
    TMAX,
    TAVG,
    PRCP,
    SNOW,
    SNWD,

    -- Weather symbol classification
    CASE
        -- Heavy snow
        WHEN SNOW IS NOT NULL AND SNOW > 10 THEN '❄️🌨️ Heavy snow'
        -- Moderate snow
        WHEN SNOW IS NOT NULL AND SNOW > 0 AND SNOW <= 10 THEN '🌨️ Moderate snow'
        -- Snow on ground (no new precipitation)
        WHEN SNWD IS NOT NULL AND SNWD > 0 AND (SNOW IS NULL OR SNOW = 0) THEN '⛄ Snow on ground'
        -- Heavy rain
        WHEN PRCP IS NOT NULL AND PRCP > 10 THEN '⛈️ Heavy rain'
        -- Moderate rain
        WHEN PRCP IS NOT NULL AND PRCP > 2 AND PRCP <= 10 THEN '🌧️ Moderate rain'
        -- Light rain
        WHEN PRCP IS NOT NULL AND PRCP > 0 AND PRCP <= 2 THEN '🌦️ Light rain'
        -- Hot and sunny
        WHEN PRCP = 0 AND TAVG IS NOT NULL AND TAVG > 30 THEN '☀️🌡️ Hot'
        -- Sunny
        WHEN PRCP = 0 AND TAVG IS NOT NULL AND TAVG > 15 THEN '☀️ Sunny'
        -- Cold and clear
        WHEN PRCP = 0 AND TAVG IS NOT NULL AND TAVG > 0 AND TAVG <= 15 THEN '🌤️ Cold and clear'
        -- Below freezing and clear
        WHEN PRCP = 0 AND TAVG IS NOT NULL AND TAVG <= 0 THEN '🥶 Below freezing'
        -- Insufficient data
        ELSE '❓ Unclassified'
    END AS weather_symbol,

    -- Numeric code for plotting (useful in Python/matplotlib)
    CASE
        WHEN SNOW IS NOT NULL AND SNOW > 10                             THEN 7
        WHEN SNOW IS NOT NULL AND SNOW > 0 AND SNOW <= 10              THEN 6
        WHEN SNWD IS NOT NULL AND SNWD > 0                             THEN 5
        WHEN PRCP IS NOT NULL AND PRCP > 10                            THEN 4
        WHEN PRCP IS NOT NULL AND PRCP > 2 AND PRCP <= 10             THEN 3
        WHEN PRCP IS NOT NULL AND PRCP > 0 AND PRCP <= 2              THEN 2
        WHEN PRCP = 0 AND TAVG > 30                                    THEN 1
        WHEN PRCP = 0 AND TAVG > 15                                    THEN 0
        WHEN PRCP = 0 AND TAVG <= 15 AND TAVG > 0                     THEN -1
        WHEN PRCP = 0 AND TAVG <= 0                                    THEN -2
        ELSE NULL
    END AS climate_code

FROM report.a01_temp_report
ORDER BY station_id, date;
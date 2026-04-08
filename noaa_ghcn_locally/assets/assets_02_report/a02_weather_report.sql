/* @bruin
name: report.a02_weather_report
type: duckdb.sql
connection: noaa_duckdb

materialization:
  type: view

depends:
  - report.a01_temp_report

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
  - name: weather_symbol
    type: varchar
    description: Weather symbol
  - name: climate_code
    type: int
    description: Climate code
@bruin */



SELECT
    station_id,
    station_name,
    latitude,
    longitude,
    altitude,
    country,
    province,
    iso_code,
    iso_cc,
    iso_sub
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
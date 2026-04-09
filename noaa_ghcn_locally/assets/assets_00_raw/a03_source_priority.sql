/* @bruin
name: raw.a03_source_priority
type: duckdb.sql
connection: noaa_duckdb
description: "Source priority table for GHCN-D data"
owner: "jelambrar@gmail.com"

materialization:
  type: table
  strategy: create+replace
  cluster_by: ["priority"]

columns:
    - name: source
      type: varchar
      description: "Source of the data"
      primary_key: true
      checks:
        - name: not_null
        - name: unique
    - name: priority
      type: integer
      description: "Priority of the source"
      checks:
        - name: not_null
        - name: min
          value: 1
        - name: max
          value: 28
        - name: unique

custom_checks:
  - name: all rows are unique
    query: "SELECT case when count (*) = count(DISTINCT source) then 1 else 0 end as result FROM raw.a03_source_priority"
    value: 1
@bruin */


SELECT  'Z' AS source,  1 AS priority UNION ALL
SELECT  'R' AS source,  2 AS priority UNION ALL
SELECT  '0' AS source,  3 AS priority UNION ALL
SELECT  '6' AS source,  4 AS priority UNION ALL
SELECT  'C' AS source,  5 AS priority UNION ALL
SELECT  'X' AS source,  6 AS priority UNION ALL
SELECT  'W' AS source,  7 AS priority UNION ALL
SELECT  'K' AS source,  8 AS priority UNION ALL
SELECT  '7' AS source,  9 AS priority UNION ALL
SELECT  'F' AS source,  10 AS priority UNION ALL
SELECT  'B' AS source,  11 AS priority UNION ALL
SELECT  'M' AS source,  12 AS priority UNION ALL
SELECT  'r' AS source,  13 AS priority UNION ALL
SELECT  'E' AS source,  14 AS priority UNION ALL
SELECT  'z' AS source,  15 AS priority UNION ALL
SELECT  'u' AS source,  16 AS priority UNION ALL
SELECT  'b' AS source,  17 AS priority UNION ALL
SELECT  's' AS source,  18 AS priority UNION ALL
SELECT  'a' AS source,  19 AS priority UNION ALL
SELECT  'G' AS source,  20 AS priority UNION ALL
SELECT  'Q' AS source,  21 AS priority UNION ALL
SELECT  'I' AS source,  22 AS priority UNION ALL
SELECT  'A' AS source,  23 AS priority UNION ALL
SELECT  'N' AS source,  24 AS priority UNION ALL
SELECT  'T' AS source,  25 AS priority UNION ALL
SELECT  'U' AS source,  26 AS priority UNION ALL
SELECT  'H' AS source,  27 AS priority UNION ALL
SELECT  'S' AS source,  28 AS priority;

"""@bruin
name: raw.a04_world_provinces
type: python
connection: noaa_duckdb
description: "World provinces (admin-1) dataset from Natural Earth in GeoJSON format"
owner: "jelambrar@gmail.com"

materialization:
    type: table
    destination: raw.a04_world_provinces
    strategy: create+replace

columns:
    - name: f_id
      type: integer
      description: "Feature ID"
      checks:
        - name: not_null
      primary_key: true
    - name: object_id
      type: integer
      description: "Object ID"
      checks:
        - name: not_null
    - name: name
      type: string
      description: "Province name"
      checks:
        - name: not_null
    - name: country
      type: string
      description: "Country name"
      checks:
        - name: not_null
    - name: iso_code
      type: string
      description: "ISO code"
      checks:
        - name: not_null
    - name: iso_cc
      type: string
      description: "ISO country code"
      checks:
        - name: not_null
    - name: iso_sub
      type: string
      description: "ISO subdivision code"
      checks:
        - name: not_null
    - name: geometry
      type: string
      description: "Geometry in GeoJSON format"
      checks:
        - name: not_null

custom_checks:
  - name: all rows are unique
    query: "SELECT case when count (*) = count(DISTINCT f_id) then 1 else 0 end as result FROM raw.a04_world_provinces"
    value: 1
@bruin"""

import json
import requests
import pandas as pd


def materialize():
    """
    Downloads the world provinces (admin-1) dataset from Natural Earth in GeoJSON format
    and returns a pandas DataFrame.
    """
    # url = 'https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_110m_admin_1_states_provinces.geojson'
    url = 'https://hub.arcgis.com/api/v3/datasets/56633b40c1744109a265af1dba673535_0/downloads/data?format=geojson&spatialRefId=4326&where=1%3D1'
    
    # Download the GeoJSON data
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    
    # Flatten features into a list of dictionaries
    rows = []
    for feature in data.get('features', []):
        properties = feature.get('properties', {})
        geometry = feature.get('geometry', {})
        
        # Extract relevant fields and the geometry as a JSON string
        row = {
            'f_id': properties.get('FID'),
            'object_id': properties.get('OBJECTID'),
            'name': properties.get('NAME'),
            'country': properties.get('COUNTRY'),
            'iso_code': properties.get('ISO_CODE'),
            'iso_cc': properties.get('ISO_CC'),
            'iso_sub': properties.get('ISO_SUB'),
            'geometry': json.dumps(geometry)
        }
        rows.append(row)
    
    # Create a pandas DataFrame
    df = pd.DataFrame(rows)
    df.dropna(inplace=True)
    
    return df


if __name__ == '__main__':
    df = materialize()
    print(f"Downloaded {len(df)} provinces.")
    print(df.head())

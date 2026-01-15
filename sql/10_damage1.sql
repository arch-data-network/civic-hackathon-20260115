/**************************************************************************************************************************
    Radon data from estimates created by Microsoft AI for Good Lab
    https://www.arcgis.com/home/item.html?id=604493cabe9d4006b637d42a6a52d04e
**************************************************************************************************************************/

use schema ARCHDATA_CIVIC.RAW;
use warehouse ARCHDATA_CIVIC_WH;
use role ARCHDATA_CIVIC_DEV;

-- use role ACCOUNTADMIN;
-- -- 1. Define the network rule
-- CREATE OR REPLACE NETWORK RULE arcgis_network_rule
--   MODE = EGRESS
--   TYPE = HOST_PORT
--   VALUE_LIST = ('arcgis.com','services9.arcgis.com');

-- -- 2. Create the External Access Integration
-- CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION arcgis_access_int
--   ALLOWED_NETWORK_RULES = (arcgis_network_rule)
--   ENABLED = TRUE;

CREATE OR REPLACE PROCEDURE fetch_damage()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.12
PACKAGES = ('snowflake-snowpark-python', 'requests')
EXTERNAL_ACCESS_INTEGRATIONS = (arcgis_access_int)
HANDLER = 'main'
AS
$$
import requests
import json
import os

def main(session):
    stage_name='@SOURCE_FILES'
    base_url = "https://services9.arcgis.com/PJam36xXQCkmUnAU/ArcGIS/rest/services/predicted_damage_St_Louis_model_364_v6/FeatureServer/0/query"
    
    # Base parameters for the ArcGIS request
    params = {
        "where": "1=1",
        "outFields": "*",
        "returnGeometry": "true",
        "f": "pjson",
        "resultRecordCount": 2000,
        "resultOffset": 0
    }
    
    row_count = 0
    page = 0
    has_more = True
    
    while has_more:
        # 1. Fetch data from API
        response = requests.get(base_url, params=params)
        data = response.json()
        
        # 2. Check for errors or empty responses
        if "features" not in data:
            return f"Error: {json.dumps(data)}"
        
        # 3. Save current page to a local temp file
        file_name = f"damage_{page}.json"
        local_path = f"/tmp/{file_name}"
        
        with open(local_path, "w") as f:
            json.dump(data, f)
            
        # 4. Upload file to the Snowflake Stage
        session.file.put(local_path, stage_name, overwrite=True, auto_compress=True)
        
        # 5. Check if we should keep going
        # ArcGIS returns 'exceededTransferLimit': true if more records exist
        has_more = data.get("exceededTransferLimit", False)
        
        # Fallback: stop if we get 0 features (safety check)
        if len(data.get("features", [])) == 0:
            has_more = False
            
        row_count += len(data.get("features", []))
        params["resultOffset"] += 2000
        page += 1
        
        # Clean up local file to save space in the proc container
        os.remove(local_path)

    return f"Successfully loaded {page} files containing approximately {row_count} records to {stage_name}."
$$;

call FETCH_DAMAGE();

list @SOURCE_FILES pattern = '.*damage_.*';

/**
    Load the raw JSON
**/
CREATE OR REPLACE TABLE raw_damage_data (
    json_data VARIANT
);

COPY INTO raw_damage_data
FROM @source_files
PATTERN = '.*damage_.*'
FILE_FORMAT = (TYPE = 'JSON');

select * from RAW_DAMAGE_DATA;

create or replace table DAMAGE_RESULTS as
SELECT
    f.value:attributes:fid::NUMBER AS fid,
    f.value:attributes:id::NUMBER AS id,
    f.value:attributes:Shape__Area::NUMBER(12,6) AS shape_area,
    f.value:attributes:Shape__Length::NUMBER(12,6) AS shape_length,
    f.value:attributes:damage_pct_0m::NUMBER(12,6) AS damage_pct_0m,
    f.value:attributes:damage_pct_10m::NUMBER(12,6) AS damage_pct_10m,
    f.value:attributes:damage_pct_20m::NUMBER(12,6) AS damage_pct_20m,
    f.value:attributes:damaged::NUMBER AS damaged,
    f.value:attributes:unknown_pct::NUMBER(12,6) AS unknown_pct,
    -- Extracts the geometry object for geospatial analysis
    f.value:geometry AS geometry_data 
FROM 
    raw_damage_data,
    LATERAL FLATTEN(input => json_data:features) f;

select * from DAMAGE_RESULTS;


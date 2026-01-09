/**************************************************************************************************************************
    Radon data from State of Missouri
    https://gis.mo.gov/arcgis/rest/services/DHSS/EPHT_radonSchool/MapServer/0/query
    https://gis.mo.gov/arcgis/rest/services/DHSS/EPHT_radonResidential/MapServer/0/query
**************************************************************************************************************************/

use schema ARCHDATA_CIVIC.RAW;
use warehouse ARCHDATA_CIVIC_WH;
use role ARCHDATA_CIVIC_DEV;

-- use role ACCOUNTADMIN;
-- -- 1. Define the network rule
-- CREATE OR REPLACE NETWORK RULE mo_gis_network_rule
--   MODE = EGRESS
--   TYPE = HOST_PORT
--   VALUE_LIST = ('gis.mo.gov');

-- -- 2. Create the External Access Integration
-- CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION mo_gis_access_int
--   ALLOWED_NETWORK_RULES = (mo_gis_network_rule)
--   ENABLED = TRUE;

CREATE OR REPLACE PROCEDURE fetch_mo_radon_data()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.12
PACKAGES = ('snowflake-snowpark-python', 'requests')
EXTERNAL_ACCESS_INTEGRATIONS = (mo_gis_access_int)
HANDLER = 'main'
AS
$$
import requests
import json
import os

def main(session):
    stage_name='@SOURCE_FILES'
    base_url = "https://gis.mo.gov/arcgis/rest/services/DHSS/EPHT_radonResidential/MapServer/0/query"
    
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
        file_name = f"radon_data_page_{page}.json"
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

call FETCH_MO_RADON_DATA();

list @SOURCE_FILES pattern = '.*radon_data_page.*';

/**
    Load the raw JSON
**/
CREATE OR REPLACE TABLE raw_radon_data (
    json_data VARIANT
);

COPY INTO raw_radon_data
FROM @source_files
PATTERN = '.*radon_data_page.*'
FILE_FORMAT = (TYPE = 'JSON');

select * from RAW_RADON_DATA;

create or replace table RADON_TEST_RESULTS as
SELECT
    f.value:attributes:OBJECTID::NUMBER AS object_id,
    f.value:attributes:City::STRING AS city,
    f.value:attributes:Zip::STRING AS zip_code,
    f.value:attributes:Analysis_Date::NUMBER AS date,
    f.value:attributes:Final_Result::NUMBER AS final_result,
    f.value:attributes:County::STRING AS county,
    f.value:attributes:Loc_Code::STRING AS loc_code,
    f.value:attributes:Test_Year::NUMBER AS test_year,
    f.value:attributes:Valid_Test::STRING AS valid_test,
    -- Extracts the geometry object for geospatial analysis
    f.value:geometry AS geometry_data 
FROM 
    raw_radon_data,
    LATERAL FLATTEN(input => json_data:features) f;

select * from RADON_TEST_RESULTS;

select count(*) from RADON_TEST_RESULTS;



/**
Note that Google Gemini was helpful in understanding ArcGIS and writing code to download the data:

I've got a link to an arcgis map. Is there a way to easily download the data points being shown on that map? https://mohealth.maps.arcgis.com/apps/webappviewer/index.html?id=8c78df9a427a4536ab915e08f4def37d

Create me a Snowflake Python stored procedure that pages through increments of 2000 for this GET url, fetches the JSON data that returned, and stores the results in stage files.
https://gis.mo.gov/arcgis/rest/services/DHSS/EPHT_radonResidential/MapServer/0/query?where=1%3D1&text=&objectIds=&time=&timeRelation=esriTimeRelationOverlaps&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&distance=&units=esriSRUnit_Foot&relationParam=&outFields=*&returnGeometry=true&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&havingClause=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&historicMoment=&returnDistinctValues=false&resultOffset=0&resultRecordCount=&returnExtentOnly=false&sqlFormat=none&datumTransformation=&parameterValues=&rangeValues=&quantizationParameters=&featureEncoding=esriDefault&f=pjson

Great. What's the best way to load this into a table in snowflake? I want to load all the individual attributes into separate columns with the data from the "features" list.
**/
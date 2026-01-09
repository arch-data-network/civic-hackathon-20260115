/**************************************************************************************************************************
    Flood data from FEMA
    Interactive maps and planning resources developed by University of Missouri Extension that compile information on 
    flood hazard zones, historical flooding frequency, and flood-related risk across Missouri.
    https://catalog.data.gov/dataset/enviroatlas-st-louis-mo-people-and-land-cover-in-floodplains-by-block-group6?utm_source=chatgpt.com
    
    Raw data from FEMA for this map was extracted from files provide by FEMA for St. Louis City.
    https://msc.fema.gov/portal/advanceSearch#searchresultsanchor

**************************************************************************************************************************/

use schema ARCHDATA_CIVIC.RAW;
use warehouse ARCHDATA_CIVIC_WH;
use role ARCHDATA_CIVIC_DEV;


CREATE OR REPLACE FUNCTION PY_LOAD_SHP(PATH_TO_FILE STRING, SHP_FILENAME STRING)
RETURNS TABLE (WKB BINARY, PROPERTIES OBJECT)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('fiona', 'shapely', 'snowflake-snowpark-python')
HANDLER = 'ShapeFileReader'
AS $$
from shapely.geometry import shape
from snowflake.snowpark.files import SnowflakeFile
from fiona.io import ZipMemoryFile

class ShapeFileReader:
    def process(self, path_to_file: str, shp_filename: str):
        # Open the zipped file from the stage
        with SnowflakeFile.open(path_to_file, 'rb') as f:
            # Fiona can treat the zip in memory as a virtual file system
            with ZipMemoryFile(f) as zip_container:
                with zip_container.open(shp_filename) as collection:
                    for record in collection:
                        if record['geometry'] is not None:
                            # Convert geometry to WKB and properties to a dict
                            yield (shape(record['geometry']).wkb, dict(record['properties']))
$$;

select BUILD_SCOPED_FILE_URL(@SOURCE_FILES, 'NFHL/290385_20160315.zip');


CREATE OR REPLACE TABLE ST_LOUIS_FLOOD_ZONES AS
SELECT 
    TO_GEOGRAPHY(WKB) as geom,                -- Convert WKB to Snowflake Geography
    PROPERTIES as raw_metadata                -- Keep the rest of the attributes
FROM TABLE(PY_LOAD_SHP(
    BUILD_SCOPED_FILE_URL(@SOURCE_FILES, 'NFHL/290385_20160315.zip'), 
    'S_FLD_HAZ_AR.shp'
));


/**************************************************************************************************************************
    Sample Queries
**************************************************************************************************************************/
select * from ST_LOUIS_FLOOD_ZONES;

select
    RAW_METADATA:FLD_ZONE::STRING as FLD_ZONE,
    RAW_METADATA:ZONE_SUBTY::STRING as ZONE_SUBTY,
    count(1) AS ROW_COUNT
from 
    ST_LOUIS_FLOOD_ZONES
group by all;




/**************************************************************************************************************************

Here's a map of flood areas in the St. Louis, MO region. I'd like to try to extract the raw geo data from the regions on 
this map. As geojson or some other reasonable geo format. This appears to be a Microsoft mapping service. Is there any way 
to extract the shape data from this: 

https://allthingsmissouri.org/missouri-maps/?bbox=-10189248.45%2C4576229.01%2C-9891511.72%2C4744707.51&
fs=1&l=%5B%7B
%22id%22%3A%22arcgis-light-gray%22%2C
%22v%22%3A1%2C%22lb%22%3A1%2C
%22b%22%3A1%7D%2C%7B
%22id%22%3A%2210121%22%2C
%22v%22%3A1%2C
%22def%22%3A%5B%22%22%5D%7D%2C%7B
%22id%22%3A%2254444%22%2C
%22op%22%3A0.7%2C
%22v%22%3A1%2C
%22def%22%3A%5B%22%22%5D%7D%2C%7B
%22id%22%3A%2238801%22%2C%22v%22%3A1%2C
%22def%22%3A%5B%22%22%5D%7D%2C%7B
%22id%22%3A%22r2%22%2C%22v%22%3A1%7D%2C%7B
%22id%22%3A%22r8%22%2C%22op%22%3A0.4%2C
%22v%22%3A1%7D%2C%7B
%22id%22%3A%22r3%22%2C
%22op%22%3A0.8%2C
%22v%22%3A1%7D%2C%7B
%22id%22%3A%22r6%22%2C
%22op%22%3A0.9%7D%2C%7B
%22id%22%3A%2254598%22%2C
%22op%22%3A0.8%2C
%22v%22%3A1%2C
%22def%22%3A%5B%22%22%5D%7D%2C%7B
%22id%22%3A
%22r15%22%7D%2C%7B
%22id%22%3A
%22r15%22%7D%5D

[
  {"id": "arcgis-light-gray", "v": 1, "lb": 1, "b": 1},
  {"id": "10121", "v": 1, "def": [""]},
  {"id": "54444", "v": 1, "op": 0.7, "def": [""]},
  {"id": "38801", "v": 1, "def": [""]},
  {"id": "r2", "v": 1},
  {"id": "r8", "v": 1, "op": 0.4},
  {"id": "r3", "v": 1, "op": 0.8},
  {"id": "r6", "op": 0.9},
  {"id": "54598", "v": 1, "op": 0.8, "def": [""]},
  {"id": "r15"},
  {"id": "r15"}
]

54444: FEMA National Flood Hazard Layer (Flood Zones).
54598: Flood Hazard Areas (likely the 1% annual chance/100-year floodplain).
10121: Likely a reference layer for Hydrography (rivers/streams).

** GO TO **
https://msc.fema.gov/portal/advanceSearch#searchresultsanchor

Got two zip files with SHP data and uploaded to @SOURCE_FILES/NFHL/:
* 290385_20160315.zip
* NFHL_29_20251225.zip

**************************************************************************************************************************/


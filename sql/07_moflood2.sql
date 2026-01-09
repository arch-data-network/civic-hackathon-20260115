/**************************************************************************************************************************
    Flood data from the EPA
    https://catalog.data.gov/dataset/enviroatlas-st-louis-mo-people-and-land-cover-in-floodplains-by-block-group6?utm_source=chatgpt.com

    Unzipped raw data files and metadata have been uploaded to Snowflake in @SOURCE_FILES/EPA
**************************************************************************************************************************/

use schema ARCHDATA_CIVIC.RAW;
use warehouse ARCHDATA_CIVIC_WH;
use role ARCHDATA_CIVIC_DEV;


CREATE OR REPLACE TABLE SLMO_FLOODPLAIN (
    BGRP VARCHAR(12) PRIMARY KEY COMMENT 'Census Block Group Identifier; A concatenation of state, county, tract, and block group FIPS codes.',
    
    -- 1% Annual Chance Flood Hazard Data
    FP1_LAND_M NUMBER(38, 0) COMMENT 'Total land area in 1% Annual Chance Flood Hazard area (m2). Value of -99997 indicates data could not be calculated.',
    FP1_LAND_P FLOAT COMMENT 'Percent land area in 1% Annual Chance Flood Hazard area.',
    FP1_IMP_M NUMBER(38, 0) COMMENT 'Total impervious surface in 1% Annual Chance Flood Hazard area (m2). -99999: No floodplain/people; -99997: No FEMA coverage.',
    FP1_IMP_P FLOAT COMMENT 'Percent impervious surface in 1% Annual Chance Flood Hazard land area.',
    FP1_POP_C FLOAT COMMENT 'Total dasymetric population in 1% Annual Chance Flood Hazard area (person).',
    FP1_POP_P FLOAT COMMENT 'Percent dasymetric population in 1% Annual Chance Flood Hazard area.',
    
    -- 0.2% Annual Chance Flood Hazard Data
    FP02_LAND_M NUMBER(38, 0) COMMENT 'Total land area in 0.2% Annual Chance Flood Hazard area (m2). Value of -99997 indicates data could not be calculated.',
    FP02_LAND_P FLOAT COMMENT 'Percent land area in 0.2% Annual Chance Flood Hazard area.',
    FP02_IMP_M NUMBER(38, 0) COMMENT 'Total impervious surface in 0.2% Annual Chance Flood Hazard area (m2). -99999: No floodplain/people; -99997: No FEMA coverage.',
    FP02_IMP_P FLOAT COMMENT 'Percent impervious surface in 0.2% Annual Chance Flood Hazard land area.',
    FP02_POP_C FLOAT COMMENT 'Total dasymetric population in 0.2% Annual Chance Flood Hazard area (person).',
    FP02_POP_P FLOAT COMMENT 'Percent dasymetric population in 0.2% Annual Chance Flood Hazard area.'
) 
COMMENT = 'EnviroAtlas - St. Louis, MO - People and Land Cover in Floodplains by Block Group. Data sourced from US EPA and FEMA NFHL.';


-- Create the File Format to handle the CSV structure
CREATE OR REPLACE FILE FORMAT SLMO_CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('', 'NULL')
    FIELD_OPTIONALLY_ENCLOSED_BY = '"';

-- Execute the data load
COPY INTO SLMO_FLOODPLAIN
FROM @SOURCE_FILES/EPA/SLMO_Floodplain.csv
FILE_FORMAT = (FORMAT_NAME = 'SLMO_CSV_FORMAT')
ON_ERROR = 'ABORT_STATEMENT';

select * from SLMO_FLOODPLAIN
where bgrp like '29510%';

select * from PUBLIC_DATA.PUBLIC_DATA_FREE.GEOGRAPHY_CHARACTERISTICS
where geo_id like '%29510%';


/**************************************************************************************************************************
    Examples
**************************************************************************************************************************/

// 1. Combine the floodplain data with geography characteristics to get us some info about that location
// NOTE:
// g.geo_id = geoId/29510127400   --- data is tract level only (one level above block group)
// x.bgrp   =       295101013001  --- data is block group level
select 
    x.*, g.*
from 
    SLMO_FLOODPLAIN x join
    PUBLIC_DATA.PUBLIC_DATA_FREE.GEOGRAPHY_CHARACTERISTICS g on 'geoId/'||left(x.bgrp,11) = g.geo_id
where
    g.RELATIONSHIP_TYPE = 'coordinates_geojson'
order by
    x.bgrp
;
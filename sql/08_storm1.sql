/**************************************************************************************************************************
    Storm Damage Data from NOAA
    https://apps.dat.noaa.gov/StormDamage/DamageViewer/?cw=rlx&level=8&center=-81.39,38.54

    This includes damage report details shared with NOAA after the event.
    Data was downloaded on 2026-01-09 and converted from KMZ to GeoJSON using gdal offline before uploading to Snowflake.

    ogr2ogr -f GeoJSON storm_202501516.json storm_survey.kmz
**************************************************************************************************************************/

use schema ARCHDATA_CIVIC.RAW;
use warehouse ARCHDATA_CIVIC_WH;
use role ARCHDATA_CIVIC_DEV;


-- 1. We start with very raw data... just the JSON in a single cell
CREATE OR REPLACE TABLE raw_noaa_dat (
    raw_json VARIANT
);

COPY INTO raw_noaa_dat
FROM @"ARCHDATA_CIVIC"."RAW"."SOURCE_FILES"/NOAA/storm_202501516.json
FILE_FORMAT = (TYPE = JSON);

-- 2. Flatten the GeoJSON features into the columns
create or replace table XML_NOAA_DAT as
SELECT 
    ROW_NUMBER() OVER (ORDER BY 1) as item_number,
    TO_GEOGRAPHY(value:geometry) AS location, -- Converts GeoJSON geometry to Snowflake GEOGRAPHY
    REGEXP_REPLACE(
        TRIM(value:properties:description::STRING),
        '<[0-9 ]',
        '&lt;') AS description_str,
    value:properties:Name::STRING AS event_name,
    CHECK_XML(description_str) AS description_xml_check,
    CASE WHEN description_xml_check IS NULL
        THEN PARSE_XML(description_str)
        ELSE NULL END AS description_xml,
    value:properties:altitudeMode::STRING AS altitude_mode,
    value:properties:tessellate:STRING AS tessellate,
    value:properties:extrude:STRING AS extrude,
    value:properties:visibility:STRING AS visibility
FROM raw_noaa_dat,
LATERAL FLATTEN(input => raw_json:features);


-- 3. Parse the XML/HTML description field table into key/value pairs for JSON
CREATE OR REPLACE TABLE json_noaa_dat AS
SELECT
    item_number,
    ANY_VALUE(location) AS location,
    OBJECT_AGG(
        LOWER(REGEXP_REPLACE(
            REGEXP_REPLACE(value:"$"[0]:"$", '<[^>]*>', ''),
            ' ', '_'))::STRING,
        value:"$"[1]:"$"::VARIANT
    ) AS properties
FROM xml_noaa_dat,
LATERAL FLATTEN(input => description_xml:"$")
GROUP BY item_number;

-- 4. And extract specific table elements we expect to have in the HTML into separate columns
CREATE OR REPLACE TABLE noaa_dat AS
SELECT
    item_number,
    location,
    properties:comments::STRING AS comments,
    properties:damage_dir::STRING AS damage_dir,
    properties:deaths::NUMBER AS deaths,
    properties:degree_of_damage::STRING AS degree_of_damage,
    properties:"estimated_windspeed_(mph)"::STRING AS estimated_windspeed_mph,
    properties:ef_rating::STRING AS ef_rating,
    TRY_TO_TIMESTAMP(properties:event_date::STRING, 'MM/DD/YYYY HH24:MI UTC') AS event_date,
    properties:event_id::STRING event_id,
    properties:injuries::NUMBER injuries,
    properties:lat::NUMBER AS lat,
    properties:lon::NUMBER AS lon,
    properties:office::STRING AS office,
    properties:qc::STRING AS qc,
    TRY_TO_TIMESTAMP(properties:survey_date::STRING, 'MM/DD/YYYY HH24:MI UTC') AS survey_date,
    properties:survey_type::STRING AS survey_type,
    properties    
FROM json_noaa_dat;


-- Example 1: Estimate percent damage using Snowflake AI
with dods as (
    select degree_of_damage, comments, count(*)
    from noaa_dat
    group by all
)
select
    degree_of_damage,
    comments,
    AI_COMPLETE(
        'llama3-70b',
        CONCAT(
            'Convert this tornado damage description into a single decimal percentage (0.0 to 1.0) ',
            'representing the severity of structural destruction. Return ONLY the number. ',
            'Description: ', degree_of_damage, ' + ', comments))::NUMBER(3,2) AS damage_estimate
from dods
;

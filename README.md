# Data Prep for Arch Data Network Civic Hackathon

## Background
The Arch Data Network, in collaboration with TechSTL and CIVIC CITY, are hosting a mini civic hackthon and edcuational event on January 16, 2026. This event is centered around disaster preparedness for the St. Louis region and recovery efforts following the May 16, 2025 EF3 tornado that did major damage to the region.

You can read more about the event here:
* https://archdatanetwork.org/
* https://www.zeffy.com/en-US/ticketing/st-louis-civic-mini-hackathon
* https://docs.google.com/document/d/1wXzprZZcRsJwwnxc2TpBKNujxa0v8DqvtFZ5dPi__Hc/edit?tab=t.0

## Scripts in the sql/ directory

* **01_acs1.sql**
    * Data for St. Louis City and St. Louis County from the American Community Survey
    * Data extracts from Snowflake Public Data
    * Saved into `ACS_STL` and `ACS_SLC`
* **02_cdc1.sql**
    * Life Expectancy at Birth data downlowned via stored procedure from CDC website
    * Saved into `CENSUS_LIFE_EXPECTANCY` and `CENSUS_LIFE_TABLE_DETAILS`
* **03_radon1.sql**
    * Residential radon test results data downloaded via stored procedure from ArcGIS
    * Saved JSON into raw table
    * Transformed into tabular format as `RADON_TEST_RESULTS`
* **04_reca1.sql**
    * ZIP codes covered by the Radiation Exposure Compensation Act
    * Saved into `RECA_ZIP_CODES`
* **05_usace1.sql**
    * Radiological Site Status Maps
    * Data extracted from Appendix tables in PDF file (not super clean)
    * Saved into tables named `FUSRAP_TABLE_<page>_<table>` for pages 262-501
    * https://www.mvs.usace.army.mil/Portals/54/docs/fusrap/docs/FSNCounty_2.pdf
    * Also created a Cortex Search Service to allow for LLM interaction
* **06_moflood1.sql**
    * Flood data from FEMA
    * Data extracted from SHP files and loaded to `ST_LOUIS_FLOOD_ZONES` table
* **07_moflood2.sql**
    * Flood data from EPA
    * Extracted from NFHL data and loaded to `SLMO_FLOODPLAIN` table
* **08_storm1.sql**
    * Storm damage data collected through NOAA and available through the NOAA DAT
    * https://apps.dat.noaa.gov/StormDamage/DamageViewer/?cw=rlx&level=8&center=-81.39,38.54
    * Manually filtered and downloaded and then parsed in Snowflake
* **99_reference.sql**
    * Replication of `GEOGRAPHY` tables from Snowflake Public Data
    * Inclusion of all ARCHDATA_CIVIC tables in data share
* **99_share_out.sql**
    * Instructions for granting other accounts access to the share

## Sample Queries

### ACS: Employment Rate by Year

```sql
/**
    1. Employment Rate by Year
    This query pulls to the total population 16+ and the employed civilian labor force
    to compute an employment percentage. You'll see this matches the employment rate
    statistic for St. Louis City here: https://data.census.gov/profile/St._Louis_city,_Missouri?g=050XX00US29510
    
    DATE	TOTAL	EMPLOYED	PCT_EMPLOYED
    2022-12-31	239897	153877	64.14294468
    2023-12-31	237461	155864	65.637725774
    2024-12-31	234852	149323	63.581745099
**/

with facts as (
    select
        -- Here I've used the VARIABLE code to make the query shorter...
        -- but you have to assume I've mapped code to meaning correctly.
        case 
            when a.VARIABLE = 'B23025_004E_1YR' then 'EMPLOYED'
            when a.VARIABLE = 'B23025_001E_1YR' then 'TOTAL'
            else 'NA' end as VARIABLE,
        x.DATE,
        x.VALUE        
    from
        -- This join isn't strictly necessary because the ACS_SLC table has
        -- VARIABLE_NAME on it for filtering, but this is to help the reader
        -- understand the relationships
        ARCHDATA_CIVIC.RAW.ACS_STL x JOIN
        PUBLIC_DATA.PUBLIC_DATA_FREE.AMERICAN_COMMUNITY_SURVEY_ATTRIBUTES a ON x.VARIABLE_NAME = a.VARIABLE_NAME
    where
        -- a.VARIABLE_NAME like 'Employment Status For The Population 16 Years And Over: Population%'
        -- Here I've used VARIABLE_NAME to help the reader understand what we're selecting
        a.VARIABLE_NAME in (
            'Employment Status For The Population 16 Years And Over: Population | Total, 1yr Estimate',
            'Employment Status For The Population 16 Years And Over: Population | In labor force | Civilian labor force | Employed, 1yr Estimate'
        )
)
-- Now pivot our metrics into separate columns so we can see them by year
-- and do calculations on them.
select
    DATE,
    "'TOTAL'" as TOTAL,
    "'EMPLOYED'" as EMPLOYED,
    EMPLOYED / TOTAL * 100 as PCT_EMPLOYED
from facts
    pivot (sum(VALUE) for VARIABLE in (any order by VARIABLE))
order by DATE;
```


### ACS: Population Breakdown by Race

```sql
/**
    2. Population Breakdown by Race
    This query gives us a breakdown of population by race in the city of St. Louis
    and how it has changed by year.
    
    GEO_ID	DATE	'American Indian and Alaska Native alone'	'Asian alone'	'Black or African American alone'	'Native Hawaiian and Other Pacific Islander alone'	'Some Other Race alone'	'Total'	'Two or More Races'	'White alone'
    geoId/29510	2022-12-31	564	11769	123674	306	7299	286578	38763	134545
    geoId/29510	2023-12-31	1397	10811	117535	197	7641	281754	52828	130679
    geoId/29510	2024-12-31	1559	11627	116491	198	7079	279695	49587	131020
**/
with facts as (
    select
        x.GEO_ID,
        split(x.VARIABLE_NAME, ' | ') as NAME_PARTS1,
        split(NAME_PARTS1[1], ', ') as NAME_PARTS2,
        NAME_PARTS2[0]::VARCHAR as RACE,
        x.VARIABLE,
        x.VARIABLE_NAME,
        x.DATE,
        x.VALUE        
    from
        ARCHDATA_CIVIC.RAW.ACS_STL x
    where
        -- a.VARIABLE_NAME like 'Race: Population%'
        x.VARIABLE like 'B02001%1YR'
),
facts_race as (
    select GEO_ID, RACE, DATE, VALUE
    from facts
)
-- This will give you all race breakdowns as individual columns.
-- Note that when Snowflake does a PIVOT, the resulting column names
-- are the entire value of that column including single-quotes.
-- Use something like "'race'" to reference a specific column from the pivot.
-- See 1. Employment Rate by Year for an example.
select *
from facts_race
    pivot (sum(VALUE) for RACE in (any order by RACE))
order by DATE;
```

### CDC: Life Expectancy at Birth

```sql
/**
    1. Life Expectancy at Birth for tracts in St. Louis City
    From the previous research, we know that St. Louis City tracts start with:
    '29510%'
    29 = Missouri
    510 = St. Louis City County

    We're joining in the GEOGRAPHY table for completeness to see the GEO_NAME.
**/
select 
    x.*, g.*
from 
    CENSUS_LIFE_EXPECTANCY x join
    PUBLIC_DATA.PUBLIC_DATA_FREE.GEOGRAPHY_CHARACTERISTICS g on 'geoId/'||x.tract_id = g.geo_id
where 
    x.TRACT_ID like '29510%';
```

### ACE: FUSRAP RAG - Cortex Search Service

```sql
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'FUSRAP_SEARCH_SERVICE',
    '{
        "query": "What are the contamination levels in the North St. Louis County site?",
        "columns": ["chunk_text", "file_name"],
        "limit": 5
    }'
);
```

### FEMA: Missouri Flood Zones

```sql
select
    RAW_METADATA:FLD_ZONE::STRING as FLD_ZONE,
    RAW_METADATA:ZONE_SUBTY::STRING as ZONE_SUBTY,
    count(1) AS ROW_COUNT
from 
    ST_LOUIS_FLOOD_ZONES
group by all;
```

### EPA: Missouri Flood Plains

```sql
-- 1. Combine the floodplain data with geography characteristics to get us some info about that location
-- NOTE:
-- g.geo_id = geoId/29510127400   --- data is tract level only (one level above block group)
-- x.bgrp   =       295101013001  --- data is block group level

select 
    x.*, g.*
from 
    SLMO_FLOODPLAIN x join
    PUBLIC_DATA.PUBLIC_DATA_FREE.GEOGRAPHY_CHARACTERISTICS g on 'geoId/'||left(x.bgrp,11) = g.geo_id
where
    g.RELATIONSHIP_TYPE = 'coordinates_geojson'
order by
    x.bgrp;
```

### NOAA: Storm Damage Report

```sql
-- 1. Estimate percent damage using Snowflake AI
with dods as (
    select degree_of_damage, count(*)
    from noaa_dat
    group by all
)
select
    degree_of_damage,
    AI_COMPLETE(
        'claude-4-sonnet',
        CONCAT(
            'Convert this tornado damage description into a single decimal percentage (0.0 to 1.0) ',
            'representing the severity of structural destruction. Return ONLY the number. ',
            'Description: ', degree_of_damage))::NUMBER(3,2) AS damage_estimate
from dods;
```

## Object List

### `ACS_SLC`, `ACS_STL`
These are American Community Survey responses for the `geo_id` values that represent St. Louis City and St. Louis County.  See example queries and online documentation from the Census / American Community Survey website.

### `CENSUS_LIFE_EXPECTANCY`
Life expectancy at birth at tract-level granularity.

### `FUSRAP_TABLE_<page>_<table>`
Each page in the FUSRAP appendix contains a table of data with detailed compound and radiation measurements. Each table on each page of the PDF is extracted into it's own table. You may need to union tables together for tables that span multiple pages.

### `GEOGRAPHY_CHARACTERISTICS`, `GEOGRAPHY_HIERARCHY`
The GEOGRAPHY tables all come directly from Snowflake's Public Data Free marketplace listing. These tables contain information about various standard geographies include ZIP, tract, etc.

### `NOAA_DAT`
Final version of cleaned up NOAA data on destruction from the May 16, 2025 EF3 tornado in St. Louis.

### `RADON_TEST_RESULTS`
Radon testing results from around the St. Louis region.

### `RECA_ZIP_CODES`
A list of ZIP codes that are eligible for RECA.

### `SLMO_FLOODPLAN`, `ST_LOUIS_FLOOD_ZONES`
Various data describing the flood risk in the St. Louis region.

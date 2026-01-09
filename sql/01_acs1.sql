/**************************************************************************************************************************
    Demographic & Housing Data
    a) St. Louis City profile + per zip code
       1) https://data.census.gov/profile/St._Louis_city,_Missouri?g=050XX00US29510
       2) https://data.census.gov/profile?g=160XX00US2965000
    b) St. Louis County profile + county subdivisions
       1) https://data.census.gov/profile/St._Louis_County,_Missouri?g=050XX00US29189
       2) https://data.census.gov/profile?g=050XX00US29189
**************************************************************************************************************************/

use schema ARCHDATA_CIVIC.RAW;
use warehouse ARCHDATA_CIVIC_WH;
use role ARCHDATA_CIVIC_DEV;


/** a.1. St. Louis City Profile

    Most of this data appears to come from a couple of sources:
    * 2024 American Community Survey 1-Year Estimates
    * 2020 Decennial Census
    * 2023 Economic Surveys Business Patterns

    These are available through Snowflake's Free Public Data offering.
    https://app.snowflake.com/marketplace/listing/GZTSZ290BV255/snowflake-public-data-products-snowflake-public-data-free

    Documentation:
    https://data-docs.snowflake.com/foundations/products/snowflake-foundations-paid/
**/

// Finding St. Louis GEO_ID values
select *
from PUBLIC_DATA.PUBLIC_DATA_FREE.GEOGRAPHY_INDEX
where GEO_NAME ilike '%St. Louis%';

/** Relevant geo_id ranges:
    geoId/29189                     St. Louis County                County <-- Missouri
    geoId/29189####00               Census Tract ####               CensusTract  (2101 - 2221)
    geoId/29189######               Census Tract ####.##            CensusTract  (2101.01 - 2221.00)
    geoId/29510                     St. Louis                       County <-- St. Louis City County
    geoId/29510                     Census Tract ####               County <-- St. Louis City
    censusBlockGroup/291892108063   Block Group 3; Census Tract 2108.06; St. Louis County; Missouri	CensusBlockGroup
    geoId/1722255                   East St. Louis                  City
    geoId/1722268                   East St. Louis Township         City
    geoId/C41180                    St. Louis, MO-IL Metro Area     CensusCoreBasedStatisticalArea

    DO NOT USE
    geoId/2671000                   St. Louis                       City <-- Michigan
    geoId/27137                     St. Louis County                County <-- Minnesota
**/


/**
    In the ARCHDATA_CIVIC.RAW schema, all table names follow the pattern [DATASET]_[REGION].
    DATASET may be things like ACS for the American Community Survey data.
    REGION may be things like STL for St. Louis City and SLC for St. Louis County or ESL for East St. Louis or MO for Missouri State

    PUBLIC_DATA.PUBLIC_DATA_FREE
    * GEOGRAPHY_CHARACTERISTICS has geojson and wkt
    * AMERICAN_COMMUNITY_SURVEY_ATTRIBUTES has attribute descriptions

**/

// Copy over just the St. Louis City Data
create table ARCHDATA_CIVIC.RAW.ACS_STL as
select * from PUBLIC_DATA.PUBLIC_DATA_FREE.AMERICAN_COMMUNITY_SURVEY_TIMESERIES
where GEO_ID like 'geoId/29510%'
   or GEO_ID like 'censusBlockGroup/29510%';

// Copy over just the St. Louis County Data
create table ARCHDATA_CIVIC.RAW.ACS_SLC as
select * from PUBLIC_DATA.PUBLIC_DATA_FREE.AMERICAN_COMMUNITY_SURVEY_TIMESERIES
where GEO_ID like 'geoId/29189%'
   or GEO_ID like 'gensusBlockGroup/29189%';


/**************************************************************************************************************************
    Examples:
    1. Employment Rate by Year
    2. Population by Race by Year
**************************************************************************************************************************/

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
order by DATE
;



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
order by date
;

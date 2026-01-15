/**
    Data from the city of St. Louis
**/


USE ROLE archdata_civic_dev;
USE SCHEMA archdata_civic.raw;
USE WAREHOUSE archdata_civic_wh;


CREATE OR REPLACE TABLE ACTIONSTL_NEEDS_BY_NEIGHBORHOOD_MAY_AUG_2025 (
    NHD_NAME STRING
        COMMENT 'Neighborhood name (NHD_NAME).',

    HD_ASSISTANCE_WITH_TEMPORARY_STORAGE_OF_HOUSEHOLD_ITEMS NUMBER(38,0)
        COMMENT 'Count of needs/requests for assistance with temporary storage of household items (housing damage-related).',

    HD_DEBRIS_REMOVAL NUMBER(38,0)
        COMMENT 'Count of needs/requests for debris removal (housing damage-related).',

    HD_MOLD_REMEDIATION_ASSISTANCE NUMBER(38,0)
        COMMENT 'Count of needs/requests for mold remediation assistance (housing damage-related).',

    HD_SIGNIFICANT_HOUSING_DAMAGE_NEEDS_HOME_REPAIR NUMBER(38,0)
        COMMENT 'Count of needs/requests indicating significant housing damage / needs home repair (housing damage-related).',

    HD_TARPING_AND_BOARDING_ASSISTANCE NUMBER(38,0)
        COMMENT 'Count of needs/requests for tarping and boarding assistance (housing damage-related).',

    HS_HOUSING_ASSISTANCE_TEMPORARY_SHELTER_INTERIM_TO_LONG_SERVICE NUMBER(38,0)
        COMMENT 'Count of needs/requests for housing assistance (temporary/shelter/interim-to-longer service).',

    HS_POWER_OUTAGE NUMBER(38,0)
        COMMENT 'Count of needs/requests related to power outage (housing/shelter-related).',

    HS_SHELTER NUMBER(38,0)
        COMMENT 'Count of needs/requests for shelter (housing/shelter-related).',

    HS_UTILITIES_ASSISTANCE_DEPOSITS_MONTHLY_EXPENSES_FOR_WATER_ELECTRIC_GAS_PHONES NUMBER(38,0)
        COMMENT 'Count of needs/requests for utilities assistance (deposits/monthly expenses for water/electric/gas/phones).',

    SF_ESSENTIAL_CLOTHING NUMBER(38,0)
        COMMENT 'Count of needs/requests for essential clothing (supplies/food-related).',

    SF_ESSENTIAL_FURNITURE_AND_OR_APPLIANCES NUMBER(38,0)
        COMMENT 'Count of needs/requests for essential furniture and/or appliances (supplies/food-related).',

    SF_FOOD_DESCRIBE_BELOW NUMBER(38,0)
        COMMENT 'Count of needs/requests for food (field labeled “describe below” in source).',

    SF_HOUSEHOLD_GOODS NUMBER(38,0)
        COMMENT 'Count of needs/requests for household goods (supplies/food-related).',

    SF_HYGIENE_OR_TOILETRIES NUMBER(38,0)
        COMMENT 'Count of needs/requests for hygiene or toiletries (supplies/food-related).',

    SF_INFANT_SUPPLIES NUMBER(38,0)
        COMMENT 'Count of needs/requests for infant supplies (supplies/food-related).',

    SF_SUPPLY_DROP_NEEDED_RESIDENT_CANNOT_PICK_UP NUMBER(38,0)
        COMMENT 'Count of needs/requests for supply drop needed because resident cannot pick up (supplies/food-related).'
)
COMMENT = 'ActionSTL needs summary by neighborhood (source file: "ActionSTL Needs Summary by Neighborhood (May - August 2025).csv"). Values are summed counts per neighborhood for May–Aug 2025.';


COPY INTO ARCHDATA_CIVIC.RAW.ACTIONSTL_NEEDS_BY_NEIGHBORHOOD_MAY_AUG_2025
FROM '@SOURCE_FILES/ActionSTL Needs Summary by Neighborhood (May - August 2025).csv'
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)
ON_ERROR = ABORT_STATEMENT;
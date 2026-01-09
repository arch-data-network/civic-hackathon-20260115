/**************************************************************************************************************************
    RECA Data

    Publicly available list of ZIP codes designated as eligible under the Radiation Exposure Compensation Act (RECA) in 
    the St. Louis region. These ZIP codes, primarily in North St. Louis County and parts of the City, are associated with 
    historical exposure pathways linked to Manhattan Project–era radioactive waste, including contamination along 
    Coldwater Creek. The dataset is used to identify areas recognized for compensation eligibility and to contextualize 
    environmental health and land-use risks; it does not represent parcel-level contamination or current exposure levels.

    https://www.justice.gov/civil/reca
    https://stlouiscountymo.gov/st-louis-county-government/county-executive/reca-saint-louis-county/
**************************************************************************************************************************/

use schema ARCHDATA_CIVIC.RAW;
use warehouse ARCHDATA_CIVIC_WH;
use role ARCHDATA_CIVIC_DEV;


-- Create the table
CREATE OR REPLACE TABLE reca_zip_codes (
    zip_code STRING COMMENT '5-digit zip code for RECA eligible area',
    region_label STRING COMMENT 'The specific County or City label'
);

-- Insert St. Louis County data
INSERT INTO reca_zip_codes (zip_code, region_label) VALUES
('63031', 'St. Louis County'),
('63033', 'St. Louis County'),
('63034', 'St. Louis County'),
('63042', 'St. Louis County'),
('63043', 'St. Louis County'),
('63044', 'St. Louis County'),
('63045', 'St. Louis County'),
('63074', 'St. Louis County'),
('63114', 'St. Louis County'),
('63121', 'St. Louis County'),
('63134', 'St. Louis County'),
('63135', 'St. Louis County'),
('63138', 'St. Louis County'),
('63140', 'St. Louis County'),
('63145', 'St. Louis County');

-- Insert St. Charles County and St. Louis City data
INSERT INTO reca_zip_codes (zip_code, region_label) VALUES
('63102', 'St. Louis City'),
('63147', 'St. Louis City'),
('63304', 'St. Charles County'),
('63341', 'St. Charles County'),
('63368', 'St. Charles County'),
('63367', 'St. Charles County');
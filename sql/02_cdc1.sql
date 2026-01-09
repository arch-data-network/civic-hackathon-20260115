/**************************************************************************************************************************
    Small Area Life Expectacy Data from the CDC
    https://www.cdc.gov/nchs/nvss/usaleep/usaleep.html

    This page has a table of CSV files with Life Expectancy Files and Abridged Period Life Table Files.    
**************************************************************************************************************************/

use schema ARCHDATA_CIVIC.RAW;
use warehouse ARCHDATA_CIVIC_WH;
use role ARCHDATA_CIVIC_DEV;

-- Create a stage where we can dump the raw files
-- use role ARCHDATA_CIVIC_DEV;
CREATE OR REPLACE STAGE ARCHDATA_CIVIC.RAW.SOURCE_FILES
	DIRECTORY = ( ENABLE = true ) 
	ENCRYPTION = ( TYPE = 'SNOWFLAKE_SSE' );

-- Create external access integration
-- use role ACCOUNTADMIN;

-- CREATE OR REPLACE NETWORK RULE cdc_access_rule
--   MODE = EGRESS
--   TYPE = HOST_PORT
--   VALUE_LIST = ('www.cdc.gov', 'ftp.cdc.gov');

-- CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION cdc_integration
--   ALLOWED_NETWORK_RULES = (cdc_access_rule)
--   ENABLED = true;

-- GRANT USAGE ON INTEGRATION cdc_integration TO ROLE ARCHDATA_CIVIC_DEV;


-- Create function to get links
use role ARCHDATA_CIVIC_DEV;

CREATE OR REPLACE PROCEDURE download_life_expectancy_files()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.12
PACKAGES = ('requests', 'beautifulsoup4', 'pandas', 'snowflake-snowpark-python')
EXTERNAL_ACCESS_INTEGRATIONS = (cdc_integration)
HANDLER = 'get_usaleep'
AS
$$
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from urllib.parse import urljoin, urlparse
import snowflake.snowpark as snowpark
import tempfile
import os

def get_usaleep(session):
    try:
        # Target URL
        url = "https://www.cdc.gov/nchs/nvss/usaleep/usaleep.html#life-expectancy"
        
        # Set up headers to mimic a browser request
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # Get the webpage
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        # Parse HTML
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find all USALEEP that might be downloadable CSV files
        file_extensions = ['.CSV']
        file_names = ['USALEEP']
        links_found = []
        
        # Find all anchor tags with href attributes
        for link in soup.find_all('a', href=True):
            href = link['href']
            link_text = link.get_text(strip=True)
            
            # Convert relative URLs to absolute URLs
            full_url = urljoin(url, href)
            
            # Check if link contains file extensions we're interested in
            if any(pat in href.upper() for pat in file_names):
                if any(ext in href.upper() for ext in file_extensions):
                    links_found.append({
                        'link_text': link_text,
                        'url': full_url,
                        'file_type': next((ext for ext in file_extensions if ext in href.lower()), 'unknown')
                    })


        # Remove duplicates
        unique_links = []
        seen_urls = set()
        for link in links_found:
            if link['url'] not in seen_urls:
                unique_links.append(link)
                seen_urls.add(link['url'])

        # Create results table
        results = []
        download_count = 0

        # Download results and store in stage files
        for link in unique_links:
            try:
                # Download the file
                file_response = requests.get(link['url'], headers=headers, timeout=30)
                file_response.raise_for_status()
                
                # Get filename
                filename = None
                if not filename:
                    filename = urlparse(link['url']).path.split('/')[-1]
                    if not filename or '.' not in filename:
                        filename = f"cdc_file_{download_count + 1}{link['file_type']}"
                
                # Create a temporary file to write the content
                with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                    temp_file.write(file_response.content)
                    temp_file_path = temp_file.name
                
                try:
                    # Define the stage path
                    stage_path = f"@source_files/{filename}"
                    
                    # Use Snowflake's PUT command to upload file to stage
                    session.file.put(f'file://{temp_file_path}', f'@source_files/{filename}')
                                        
                finally:
                    # Clean up temporary file
                    if os.path.exists(temp_file_path):
                        os.unlink(temp_file_path)
                
                results.append({
                    'filename': filename,
                    'stage_path': stage_path,
                    'url': link['url'],
                    'link_text': link['link_text'],
                    'file_type': link['file_type']
                })
                
                download_count += 1
                
            except Exception as e:
                results.append({
                    'filename': 'N/A',
                    'stage_path': 'N/A',
                    'url': link['url'],
                    'link_text': link['link_text'],
                    'file_type': link['file_type'],
                    'original_size_bytes': 0,
                    'stage_size_bytes': 0,
                    'upload_status': f'FAILED: {str(e)[:200]}'
                })
  
        
        # Create DataFrame and return results
        if results:
            df = pd.DataFrame(results)
            # Convert to Snowpark DataFrame for easier handling
            snowpark_df = session.create_dataframe(df)
            
            # You could save this to a table if needed
            snowpark_df.write.save_as_table("CDC_DOWNLOAD_RESULTS", mode="overwrite")
            
            return f"Successfully processed {len(results)} links. Sample results: {results[:3]}"
        else:
            return "No downloadable files found on the specified page."
            
    except Exception as e:
        return f"Error occurred: {str(e)}"
$$;


call DOWNLOAD_LIFE_EXPECTANCY_FILES();

select * from CDC_DOWNLOAD_RESULTS;

list @SOURCE_FILES pattern = '.*A.CSV.*';


/**************************************************************************************************************************
    Because this is relatively small data, we aren't going to filter to just STL and SLC data.
    These files contain all data for the entire US.
**************************************************************************************************************************/

CREATE OR REPLACE TABLE census_life_expectancy (
    tract_id STRING COMMENT 'Concatenation of 2-digit state FIPS code, 3-digit county FIPS code, and 6-digit census tract number',
    state2kx STRING COMMENT 'Census 2010 FIPS State Code (2-digit numeric with leading zeros significant)',
    cnty2kx STRING COMMENT 'Census 2010 FIPS County Code (3-digit numeric with leading zeros significant)',
    tract2kx STRING COMMENT 'Census 2010 Tract (contains leading zeros with the decimal point implied)',
    life_expectancy NUMBER COMMENT 'Life expectancy at birth',
    std_error_life_expectancy NUMBER COMMENT 'Standard error of life expectancy at birth',
    life_table_flag NUMBER COMMENT 'Flag for age-specific death rate source: 1=Observed, 2=Predicted, 3=Combination'
);


COPY INTO census_life_expectancy
FROM @SOURCE_FILES
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('', 'NULL')
    COMPRESSION = AUTO
    SKIP_BLANK_LINES = TRUE
)
PATTERN = '.*_A\.CSV.*'
ON_ERROR = 'ABORT_STATEMENT';



CREATE OR REPLACE TABLE census_life_table_details (
    tract_id STRING COMMENT 'Concatenation of 2-digit state FIPS code, 3-digit county FIPS code, and 6-digit census tract number',
    state2kx STRING COMMENT 'Census 2010 FIPS State Code (2-digit numeric with leading zeros significant)',
    cnty2kx STRING COMMENT 'Census 2010 FIPS County Code (3-digit numeric with leading zeros significant)',
    tract2kx STRING COMMENT 'Census 2010 Tract (contains leading zeros with the decimal point implied)',
    age_group STRING COMMENT 'The age interval between two exact ages, x and x+n',
    nqx NUMBER COMMENT 'Probability of dying between ages x and x+n',
    lx NUMBER COMMENT 'Number surviving to age x',
    ndx NUMBER COMMENT 'Number dying between ages x and x+n',
    nlx NUMBER COMMENT 'Person-years lived between ages x and x+n',
    tx NUMBER COMMENT 'Total number of person-years lived above age x',
    ex NUMBER COMMENT 'Expectation of life at age x',
    se_nqx NUMBER COMMENT 'Standard error of the probability of dying between ages x and x+n',
    se_ex NUMBER COMMENT 'Standard error of life expectancy at age x'
);

COPY INTO census_life_table_details
FROM @SOURCE_FILES
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('', 'NULL')
    COMPRESSION = AUTO
    SKIP_BLANK_LINES = TRUE
)
PATTERN = '.*_B\.CSV.*'
ON_ERROR = 'ABORT_STATEMENT';

/**************************************************************************************************************************
    Examples:
    1. Life Expectancy for tracts in STL
**************************************************************************************************************************/

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
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
    * Saved into ARCHDATA_CIVIC.RAW.ACS_STL and ARCHDATA_CIVIC.RAW.ACS_SLC
* **02_cdc1.sql**
    * Life Expectancy at Birth data downlowned via stored procedure from CDC website
    * Saved into CENSUS_LIFE_EXPECTANCY and CENSUS_LIFE_TABLE_DETAILS
* **03_radon1.sql**
    * Residential radon test results data downloaded via stored procedure from ArcGIS
    * Saved JSON into raw table
    * Transformed into tabular format as RADON_TEST_RESULTS
* **04_reca1.sql**
    * ZIP codes covered by the Radiation Exposure Compensation Act
    * Saved into RECA_ZIP_CODES
* **05_usace1.sql**
    * Radiological Site Status Maps
    * Data extracted from Appendix tables in PDF file (not super clean)
    * Saved into tables named `FUSRAP_TABLE_<page>_<table>` for pages 262-501
    * https://www.mvs.usace.army.mil/Portals/54/docs/fusrap/docs/FSNCounty_2.pdf
    * Also created a Cortex Search Service to allow for LLM interaction
* **06_moflood1.sql**
    * Flood data from FEMA
    * Data extracted from SHP files and loaded to ST_LOUIS_FLOOD_ZONES table
* **07_moflood2.sql**
    * Flood data from EPA
    * Extracted from NFHL data and loaded to SLMO_FLOODPLAIN table
* **99_reference.sql**
    * Replication of GEOGRAPHY tables from Snowflake Public Data
    * Inclusion of all ARCHDATA_CIVIC tables in data share
* **99_share_out.sql**
    * Instructions for granting other accounts access to the share
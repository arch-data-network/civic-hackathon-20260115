/**************************************************************************************************************************
    Radiological Site Status Maps

    This is a site status report published by the U.S. Army Corps of Engineers documenting cleanup activities in 
    North St. Louis County. The maps classify areas by remediation status like released for use, excavation completed, 
    under remediation, under evaluation, or requiring further action. Because these materials are published as static 
    images in PDFs, they are not searchable or usable at the address level, limiting their usefulness for residents, 
    planners, and community organizations.

    * FUSRAP_TABLE_<page>_<table>
    For structured data, we ran an auotomated process to extract Appendex data tables by page into separate tables.
    You'll probably want to process the data from there if you need to use it.

    * FUSRAP_DOC_CHUNKS
    We've also created a Cortex Search Service to make these data available to LLMs through Snowflake AI features
    like Snowflake Intelligence agents.
**************************************************************************************************************************/

use schema ARCHDATA_CIVIC.RAW;
use warehouse ARCHDATA_CIVIC_WH;
use role ARCHDATA_CIVIC_DEV;


-- use role ACCOUNTADMIN;

-- -- 1. Create a Network Rule for the USACE domain
-- CREATE OR REPLACE NETWORK RULE usace_network_rule
--   MODE = EGRESS
--   TYPE = HOST_PORT
--   VALUE_LIST = ('www.mvs.usace.army.mil');

-- -- 2. Create an External Access Integration
-- CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION usace_access_int
--   ALLOWED_NETWORK_RULES = (usace_network_rule)
--   ENABLED = TRUE;

-- GRANT USAGE ON INTEGRATION usace_access_int TO ROLE archdata_civic_dev;

use role ARCHDATA_CIVIC_DEV;


CREATE OR REPLACE PROCEDURE IMPORT_FUSRAP_PDF_TABLES(stage_file STRING, pages ARRAY)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests', 'pdfplumber', 'pandas')
EXTERNAL_ACCESS_INTEGRATIONS = (usace_access_int)
HANDLER = 'main'
AS
$$
import io
import pdfplumber
import pandas as pd

def main(session, stage_file, pages):
    file_stream = session.file.get_stream(stage_file)
    tables_created = []
    errors = {}
    dfs = {}
    
    settings = {
        "vertical_strategy": "text", 
        "horizontal_strategy": "text",
        "snap_tolerance": 4,
    }
    
    # 2. Extract tables using pdfplumber
    # with pdfplumber.open(stage_file) as pdf:
    with pdfplumber.open(io.BytesIO(file_stream.read())) as pdf:
    
        page_number = 0
    
        for page in pdf.pages:
            page_number += 1
            table_index = 0
    
            try:
    
                # Start looking on page 260
                if page_number in pages or len(pages) == 0:
                    extracted_tables = page.extract_tables(table_settings=settings)
                    
                    for tbl in extracted_tables:
                        table_index += 1
    
                        print(f'Processing page {page_number}, table {table_index}...')
    
                        # Convert list of lists to DataFrame
                        # We assume the first row is the header
                        if not tbl or len(tbl) < 10:
                            continue
    
                        # Skip header rows until we have a blank row
                        for r in range(10):
                            if ''.join(tbl[r]).strip() == '':
                                break
    
                        # Combine multi-line headers
                        cols = tbl[r+1]
                        for r in range(r+1, 10):
                            for n, c in enumerate(tbl[r]):
                                cols[n] = cols[n] + ' ' + c
                            if not(tbl[r][0] is None or tbl[r][0].strip() == ''):
                                break
                            
                        print(cols)
                        df = pd.DataFrame(tbl[4:], columns=cols)
                        
                        # Basic cleaning: remove newlines from headers and data
                        df.columns = [str(c).replace('\n', ' ').strip()[:80] for c in df.columns]
    
                        # Fix empty column names
                        cnames = list(df.columns)
                        for num,c in enumerate(cnames):
                            if c == 'None' or c is None:
                                cnames[num] = f'COL{num:03d}'
    
                        # Fix duplicate column names
                        for num,c in enumerate(cnames):
                            if c in set(cnames[num+1:]):
                                cnames[num] = f'c_{num:03d}'
    
                        df.columns = cnames
                        
                        df = df.replace('\n', ' ', regex=True)
                        
                        # Sanitize table name (replace spaces/special chars)
                        table_name = f"FUSRAP_TABLE_{page_number:03d}_{table_index:03d}"
                        
                        # 3. Write to Snowflake
                        # auto_create_table=True handles the DDL automatically
                        dfs [table_name] = {
                            'page_number': page_number,
                            'table_index': table_index,
                            'data': df,
                            'result': 'Table extracted successfully'
                        }
    
                        # auto_create_table=True handles the DDL automatically
                        session.write_pandas(
                            df, 
                            table_name, 
                            auto_create_table=True, 
                            overwrite=True, 
                            quote_identifiers=True
                        )
                        
                        tables_created.append(table_name)
    
    
            except Exception as e:
                errors[table_name] = {
                    'page_number': page_number,
                    'table_index': table_index,
                    'result': f'Error: {e}'
                }

    if len(errors) > 0:
        session.write_pandas(
            pd.DataFrame(errors),
            'FUSRAP_ERRORS',
            auto_create_table = True,
            overwrite = True,
            quote_identifiers = False
        )
    
    return f"Successfully created tables: {', '.join(tables_created)}"
$$;

call DROP_FUSRAP_TABLES();
call CLEANUP_TEMP_STAGES();
call IMPORT_FUSRAP_PDF_TABLES('@SOURCE_FILES/FUSRAP/FSNCounty_2.pdf',ARRAY_GENERATE_RANGE(262, 502));

select * from FUSRAP_TABLE_262_001;

/**
    Clean up all FUSRAP tables
**/
CREATE OR REPLACE PROCEDURE DROP_FUSRAP_TABLES()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  tables_dropped ARRAY DEFAULT ARRAY_CONSTRUCT();
  table_name VARCHAR;
  table_cursor CURSOR FOR 
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = CURRENT_SCHEMA()
    AND table_name ILIKE 'FUSRAP%';
BEGIN
  FOR record IN table_cursor DO
    table_name := record.table_name;
    EXECUTE IMMEDIATE 'DROP TABLE IF EXISTS ' || table_name;
    tables_dropped := ARRAY_APPEND(tables_dropped, table_name);
  END FOR;
  
  RETURN 'Dropped tables: ' || ARRAY_TO_STRING(tables_dropped, ', ');
END;
$$;

call DROP_FUSRAP_TABLES();

/**
    Drop all temporary stages from snowpark
**/
CREATE OR REPLACE PROCEDURE CLEANUP_TEMP_STAGES()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  cur CURSOR FOR SELECT stage_catalog, stage_schema, stage_name 
                 FROM information_schema.stages 
                 WHERE UPPER(stage_name) LIKE 'SNOWPARK_TEMP_STAGE%';
  stage_cat VARCHAR;
  stage_sch VARCHAR;
  stage_nm VARCHAR;
  dropped_count INTEGER DEFAULT 0;
BEGIN
  FOR record IN cur DO
    stage_cat := record.stage_catalog;
    stage_sch := record.stage_schema;
    stage_nm := record.stage_name;
    
    EXECUTE IMMEDIATE 'DROP STAGE IF EXISTS ' || stage_cat || '.' || stage_sch || '.' || stage_nm;
    dropped_count := dropped_count + 1;
  END FOR;
  
  RETURN 'Dropped ' || dropped_count || ' temporary stages';
END;
$$;

call CLEANUP_TEMP_STAGES();


/**
    Stored procedure to loop through all SNOWPARK_TEMP_FILE_FORMAT formats and drop them
**/
CREATE OR REPLACE PROCEDURE DROP_TEMP_FILE_FORMATS()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  res STRING DEFAULT '';
  cur CURSOR FOR SELECT FILE_FORMAT_NAME, FILE_FORMAT_CATALOG, FILE_FORMAT_SCHEMA 
                 FROM INFORMATION_SCHEMA.FILE_FORMATS 
                 WHERE FILE_FORMAT_NAME LIKE 'SNOWPARK_TEMP_FILE_FORMAT%';
  format_name VARCHAR;
  format_catalog VARCHAR;
  format_schema VARCHAR;
BEGIN
  FOR record IN cur DO
    format_name := record.FILE_FORMAT_NAME;
    format_catalog := record.FILE_FORMAT_CATALOG;
    format_schema := record.FILE_FORMAT_SCHEMA;
    
    EXECUTE IMMEDIATE 'DROP FILE FORMAT ' || format_catalog || '.' || format_schema || '.' || format_name;
    res := res || format_name || ' dropped; ';
  END FOR;
  
  RETURN COALESCE(res, 'No temporary file formats found to drop');
END;
$$;

call DROP_TEMP_FILE_FORMATS();


/**************************************************************************************************************************
    Cortex Search Service

    This service can be used through Snowflake Intelligence or APIs to answer questions based on the knowledge in the PDF
    files. This is Snowflake's framework for RAG.
**************************************************************************************************************************/
-- 1. Create a table to hold the searchable chunks
CREATE OR REPLACE TABLE FUSRAP_DOC_CHUNKS AS
WITH raw_text AS (
    SELECT 
        relative_path,
        -- Use the 'LAYOUT' mode to better preserve table structures found in FUSRAP docs
        SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
            '@SOURCE_FILES', 
            relative_path, 
            {'mode': 'LAYOUT'}
        ):content::VARCHAR as extracted_text
    FROM DIRECTORY(@SOURCE_FILES)
    -- Filter to only process files within the FUSRAP folder
    WHERE relative_path LIKE 'FUSRAP/%'
)
SELECT 
    relative_path as file_name,
    chunk.value::VARCHAR as chunk_text,
    SHA2(chunk.value::VARCHAR) as chunk_id
FROM raw_text,
LATERAL FLATTEN(
    input => SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(
        extracted_text, 
        'markdown', 
        1500,       
        200         
    )
) chunk;

-- 2. Create Cortex Search Service
CREATE OR REPLACE CORTEX SEARCH SERVICE FUSRAP_SEARCH_SERVICE
  ON chunk_text               
  ATTRIBUTES file_name        
  WAREHOUSE = ARCHDATA_CIVIC_WH      
  TARGET_LAG = '1 hour'       
  EMBEDDING_MODEL = 'snowflake-arctic-embed-l-v2.0' 
AS (
    SELECT 
        chunk_text, 
        file_name 
    FROM FUSRAP_DOC_CHUNKS
);

-- 3. Test
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'FUSRAP_SEARCH_SERVICE',
    '{
        "query": "What are the contamination levels in the North St. Louis County site?",
        "columns": ["chunk_text", "file_name"],
        "limit": 5
    }'
);


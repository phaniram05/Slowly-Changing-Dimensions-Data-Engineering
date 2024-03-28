**************************
SLOWLY CHANGING DIMENSIONS
**************************

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE DATABASE SCD_TYPE2;

USE DATABASE SCD_TYPE2;
USE SCHEMA PUBLIC;

-- SUPPLIER_RAW Table

CREATE OR REPLACE TABLE SUPPLIER_RAW
(
    supplier_key NUMBER,
    supplier_code VARCHAR(25),
    supplier_name VARCHAR(200),
    supplier_state VARCHAR(25)      -- column of interest for SCD type-2
)

-- SUPPLIER_LANDING table
CREATE OR REPLACE TABLE SUPPLIER_LANDING
(
    supplier_key NUMBER,
    supplier_code VARCHAR(25),
    supplier_name VARCHAR(200),
    supplier_state VARCHAR(25)
)

-- SUPPLIER_STAGING table
CREATE OR REPLACE TABLE SUPPLIER_STAGING
(
    supplier_key NUMBER,
    supplier_code VARCHAR(25),
    supplier_name VARCHAR(200),
    supplier_state VARCHAR(25),
    start_date TIMESTAMP_NTZ,
    end_date TIMESTAMP_NTZ,
    current_flag VARCHAR(1)         -- 'Y' or 'N'
)


-- SUPPLIER_MASTER table
CREATE OR REPLACE TABLE SUPPLIER_MASTER
(
    supplier_key NUMBER,
    supplier_code VARCHAR(25),
    supplier_name VARCHAR(200),
    supplier_state VARCHAR(25)
)


-- Create a stream to track changes on landing table (Change Data Capture)

CREATE OR REPLACE SCHEMA SCD_TYPE2.STREAMS;

CREATE OR REPLACE STREAM SCD_TYPE2.STREAMS.supplier_cdc_landing ON TABLE SCD_TYPE2.PUBLIC.SUPPLIER_LANDING;

SHOW STREAMS;

-- Create a FILE FORMAT schema

CREATE OR REPLACE SCHEMA SCD_TYPE2.FILE_FORMATS;

CREATE OR REPLACE FILE FORMAT FILE_FORMAT_CSV
COMPRESSION = 'AUTO'
FIELD_DELIMITER = ','
RECORD_DELIMITER = '\n'
SKIP_HEADER = 0
FIELD_OPTIONALLY_ENCLOSED_BY = '\047';


-- Create a stage

CREATE OR REPLACE SCHEMA SCD_TYPE2.INTERNAL_STAGES;

CREATE OR REPLACE STAGE supplier_internal_stage
FILE_FORMAT = SCD_TYPE2.FILE_FORMATS.FILE_FORMAT_CSV;

-- Use snowsql to load the suppliers.csv file into the staging area
-- PUT file:///Users/beingrampopuri/Desktop/suppliers.csv @SCD_TYPE2.INTERNAL_STAGES.supplier_internal_stage

LIST @SCD_TYPE2.INTERNAL_STAGES.supplier_internal_stage; -- file present


-- COPY COMMAND to load the data from file into raw table


COPY INTO SCD_TYPE2.PUBLIC.SUPPLIER_RAW
FROM @SCD_TYPE2.INTERNAL_STAGES.supplier_internal_stage/suppliers.csv.gz
PURGE = TRUE;

-- Data is present inside SUPPLIER_RAW table
SELECT * FROM SCD_TYPE2.PUBLIC.SUPPLIER_RAW;

-- Load data from supplier_raw into supplier_landing

MERGE INTO SCD_TYPE2.PUBLIC.supplier_landing SL
USING (SELECT * FROM SCD_TYPE2.PUBLIC.supplier_raw) SR
ON SL.supplier_code = SR.supplier_code
WHEN MATCHED
     AND(
        SL.supplier_state != SR.supplier_state
        OR
        SL.supplier_name != SR.supplier_name
        OR
        SL.supplier_key != SR.supplier_key
     )

THEN UPDATE
     SET 
     SL.supplier_state = SR.supplier_state,
     SL.supplier_name = SR.supplier_name,
     SL.supplier_key = SR.supplier_key

WHEN NOT MATCHED THEN
INSERT (supplier_key, supplier_state, supplier_name, supplier_code)
VALUES (SR.supplier_key, SR.supplier_state, SR.supplier_name, SR.supplier_code);


SELECT * FROM SCD_TYPE2.PUBLIC.supplier_landing;

-- Note that we have a stream "supplier_cdc_landing" created on top of Landing table
-- Check if it captured the change in data.

SELECT * FROM SCD_TYPE2.STREAMS.supplier_cdc_landing; -- Yes, data is present.

/*
SUPPLIER_KEY	SUPPLIER_CODE	SUPPLIER_NAME	SUPPLIER_STATE	METADATA$ACTION	METADATA$ISUPDATE	METADATA$ROW_ID
    1	              A101	       Virat Kohli	    Delhi	      INSERT	         FALSE	fef326c1c7ded02ac0de7a883f9371a5b4d5aba4
2	A102	MS Dhoni	Ranchi	INSERT	FALSE	d6ba50454fcdc4871da597f1540e6afd4d08a919
3	A103	Pujara	Gujarat	INSERT	FALSE	da8b41538235e7dbc350583fb7db5da11dec0101
4	A104	Bumrah	Mumbai	INSERT	FALSE	db51d7192bf6a716b812ca4efe82d5854564cb1d
5	A105	Rohit Sharma	Hyderabad	INSERT	FALSE	132007ff7cc4373bc18973a2b7a528d70f6088f3
6	A106	Dravid	Karnataka	INSERT	FALSE	ebdeb326edfbfb3d4981903ccc7a443e24f39407

*/

-- All the data that is captured is INSERT type.


MERGE INTO SUPPLIER_STAGING SS
USING (SELECT * FROM SCD_TYPE2.STREAMS.supplier_cdc_landing) LS
ON (SS.SUPPLIER_CODE = LS.SUPPLIER_CODE AND SS.SUPPLIER_STATE = LS.SUPPLIER_STATE)
WHEN MATCHED AND (LS.METADATA$ACTION = 'DELETE')
     THEN 
     UPDATE SET END_DATE = to_timestamp_ntz(CURRENT_TIMESTAMP()), CURRENT_FLAG = 'N'

WHEN NOT MATCHED AND (LS.METADATA$ACTION = 'INSERT')
     THEN
     INSERT (supplier_key, supplier_code, supplier_name, supplier_state, start_date, end_date, current_flag)
     VALUES (LS.supplier_key, LS.supplier_code, LS.supplier_name, LS.supplier_state, TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()), NULL, 'Y');


SELECT * FROM SUPPLIER_STAGING;

-- Populate into end user report table (MASTER TABLE)

INSERT OVERWRITE INTO SUPPLIER_MASTER
SELECT supplier_key, supplier_code, supplier_name, supplier_state FROM SUPPLIER_STAGING WHERE CURRENT_FLAG = 'Y';

SELECT * FROM SUPPLIER_MASTER;

**************

-- Scenario 2: New file loaded with 2 updated records and 2 new inserted records
--             A105, A106 - Updated State to Tamilnadu & A107, A108 gets inserted

LIST @SCD_TYPE2.INTERNAL_STAGES.supplier_internal_stage; -- Nothing can be seen as we set PURGE = TRUE

-- Truncate raw table and load it with fresh table

TRUNCATE TABLE SUPPLIER_RAW;

-- Upload the "suppliers_v2.csv" file into the internal stage using below command from snowsql
-- PUT file:///Users/beingrampopuri/Desktop/suppliers_v2.csv @SCD_TYPE2.INTERNAL_STAGES.supplier_internal_stage;

LIST @SCD_TYPE2.INTERNAL_STAGES.supplier_internal_stage; -- Stage shows the uploaded file.

-- From this stage, copy the data into the supplier_raw table.

COPY INTO SUPPLIER_RAW
FROM @SCD_TYPE2.INTERNAL_STAGES.supplier_internal_stage
PURGE = FALSE;

SELECT * FROM SUPPLIER_RAW;

-- Load data from supplier_raw into supplier_landing

MERGE INTO SCD_TYPE2.PUBLIC.supplier_landing SL
USING (SELECT * FROM SCD_TYPE2.PUBLIC.supplier_raw) SR
ON SL.supplier_code = SR.supplier_code
WHEN MATCHED
     AND(
        SL.supplier_state != SR.supplier_state
        OR
        SL.supplier_name != SR.supplier_name
        OR
        SL.supplier_key != SR.supplier_key
     )

THEN UPDATE
     SET 
     SL.supplier_state = SR.supplier_state,
     SL.supplier_name = SR.supplier_name,
     SL.supplier_key = SR.supplier_key

WHEN NOT MATCHED THEN
INSERT (supplier_key, supplier_state, supplier_name, supplier_code)
VALUES (SR.supplier_key, SR.supplier_state, SR.supplier_name, SR.supplier_code);

-- check in landing table for 2 updates A106, A107 & 2 inserts A107, A108
SELECT * FROM SUPPLIER_LANDING;

-- check in landing table stream for change data capture (CDC)

SELECT * FROM SCD_TYPE2.STREAMS.supplier_cdc_landing;

/*
SUPPLIER_KEY	SUPPLIER_CODE	SUPPLIER_NAME	SUPPLIER_STATE	METADATA$ACTION	METADATA$ISUPDATE	METADATA$ROW_ID
7	A107	Pujara	Saurasthra	INSERT	FALSE	89666fc1341236f609bde541c7cc8665051620b4
8	A108	Hanuma Vihari	Andhra Pradesh	INSERT	FALSE	87327c399e26b470b8f61ab0e8ab91c2a7223ac7
5	A105	Rohit Sharma	Tamilnadu	INSERT	TRUE	132007ff7cc4373bc18973a2b7a528d70f6088f3
6	A106	Dravid	Tamilnadu	INSERT	TRUE	ebdeb326edfbfb3d4981903ccc7a443e24f39407
5	A105	Rohit Sharma	Hyderabad	DELETE	TRUE	132007ff7cc4373bc18973a2b7a528d70f6088f3
6	A106	Dravid	Karnataka	DELETE	TRUE	ebdeb326edfbfb3d4981903ccc7a443e24f39407

*/

-- Every insert - 1 record in stream
-- Every update - 2 records (1 delete + 1 insert) in stream


-- Landing to Staging Data Movement based on CDC

MERGE INTO SUPPLIER_STAGING SS
USING (SELECT * FROM SCD_TYPE2.STREAMS.supplier_cdc_landing) LS
ON (SS.SUPPLIER_CODE = LS.SUPPLIER_CODE AND SS.SUPPLIER_STATE = LS.SUPPLIER_STATE)
WHEN MATCHED
     AND (LS.METADATA$ACTION = 'DELETE') 
     THEN 
     UPDATE SET SS.END_DATE = TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()), CURRENT_FLAG = 'N'
WHEN NOT MATCHED 
     AND (LS.METADATA$ACTION = 'INSERT') 
     THEN 
     INSERT (supplier_key, supplier_code, supplier_name, supplier_state, start_date, end_date, current_flag)
     VALUES (LS.supplier_key, LS.supplier_code, LS.supplier_name, LS.supplier_state, TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()), NULL, 'Y');

-- Verify the staging table
SELECT * FROM SUPPLIER_STAGING ORDER BY SUPPLIER_KEY;

/*
SUPPLIER_KEY	SUPPLIER_CODE	SUPPLIER_NAME	SUPPLIER_STATE	START_DATE	                 END_DATE	   CURRENT_FLAG
    1	          A101	           Virat Kohli	    Delhi	      2024-03-26 23:41:54.500		               Y
    2	          A102	           MS Dhoni	        Ranchi	      2024-03-26 23:41:54.500		               Y
    3	          A103	           Pujara	        Gujarat	      2024-03-26 23:41:54.500		               Y
    4	          A104	           Bumrah	        Mumbai	      2024-03-26 23:41:54.500		               Y
    5	          A105	           Rohit Sharma	    Tamilnadu	  2024-03-27 00:05:43.782		               Y
    5	          A105	           Rohit Sharma	    Hyderabad	  2024-03-26 23:41:54.500	2024-03-27 00:05:43.782	N
    6	          A106	           Dravid	        Tamilnadu	  2024-03-27 00:05:43.782		               Y
    6	          A106	           Dravid	        Karnataka	  2024-03-26 23:41:54.500	2024-03-27 00:05:43.782	N
    7	          A107	           Pujara	        Saurasthra	  2024-03-27 00:05:43.782		               Y
    8	          A108	           Hanuma Vihari	Andhra Pradesh	2024-03-27 00:05:43.782		               Y

*/




-- Push the latest data into SUPPLIER_MASTER
INSERT OVERWRITE INTO SUPPLIER_MASTER
SELECT supplier_key, supplier_code, supplier_name, supplier_state FROM SUPPLIER_STAGING WHERE CURRENT_FLAG = 'Y';

SELECT * FROM SUPPLIER_MASTER ORDER BY SUPPLIER_KEY; -- All latest data

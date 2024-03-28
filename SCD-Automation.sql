-- Automate SCD Type-2 Using Task and Stream

USE ROLE ACCOUNTADMIN;
USE DATABASE SCD_TYPE2;
USE SCHEMA PUBLIC;

-- Truncate tables to start afresh

TRUNCATE TABLE SCD_TYPE2.PUBLIC.SUPPLIER_RAW;
TRUNCATE TABLE SCD_TYPE2.PUBLIC.SUPPLIER_LANDING;
TRUNCATE TABLE SCD_TYPE2.PUBLIC.SUPPLIER_STAGING;
TRUNCATE TABLE SCD_TYPE2.PUBLIC.SUPPLIER_MASTER;

-- Create a stream

CREATE OR REPLACE STREAM SCD_TYPE2.STREAMS.supplier_cdc_landing ON TABLE SCD_TYPE2.PUBLIC.SUPPLIER_LANDING;

SHOW STREAMS;

/*
    ETL Steps needed to be performed:

    1. Truncate RAW TABLE before every load
    2. Load Data from RAW FILE to RAW TABLE & Remove the file from INTERNAL STAGE
    3. Update LANDING TABLE with the latest data from RAW TABLE
    4. Transform LANDING TABLE into STAGING TABLE based on the DELTA IDENTIFIED in stream and compared against staging.
    5. Update MASTER TABLE from STAGING TABLE processing only CURRENT_FLAG = 'Y' records.

*/

CREATE OR REPLACE SCHEMA SCD_TYPE2.TASKS;

-- Task 1: Truncate raw table
CREATE OR REPLACE TASK SCD_TYPE2.TASKS.task1_truncate_raw_table
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '1 minute'
AS
TRUNCATE TABLE SCD_TYPE2.PUBLIC.SUPPLIER_RAW;


-- Task 2: Load data from internal stage file to raw table

CREATE OR REPLACE TASK SCD_TYPE2.TASKS.task2_load_raw_table
    WAREHOUSE = COMPUTE_WH
    AFTER SCD_TYPE2.TASKS.task1_truncate_raw_table
AS
COPY INTO SCD_TYPE2.PUBLIC.SUPPLIER_RAW
FROM @SCD_TYPE2.INTERNAL_STAGES.supplier_internal_stage
PURGE = TRUE;

-- Task 3: Update LANDING TABLE based on latest data from raw table

CREATE OR REPLACE TASK SCD_TYPE2.TASKS.task3_modify_landing
    WAREHOUSE = COMPUTE_WH
    AFTER SCD_TYPE2.TASKS.task2_load_raw_table
AS
MERGE INTO SCD_TYPE2.PUBLIC.supplier_landing SL
USING (SELECT * FROM SCD_TYPE2.PUBLIC.supplier_raw) SR
ON SL.supplier_code = SR.supplier_code
WHEN MATCHED
     AND(SL.supplier_state != SR.supplier_state
        OR
        SL.supplier_name != SR.supplier_name
        OR
        SL.supplier_key != SR.supplier_key)
THEN UPDATE
     SET 
     SL.supplier_state = SR.supplier_state,
     SL.supplier_name = SR.supplier_name,
     SL.supplier_key = SR.supplier_key

WHEN NOT MATCHED THEN
INSERT (supplier_key, supplier_state, supplier_name, supplier_code)
VALUES (SR.supplier_key, SR.supplier_state, SR.supplier_name, SR.supplier_code);


-- Task 4: Transform LANDING TABLE into STAGING based on the DELTA CAPTURED VIA STREAM on LANDING TABLE

CREATE OR REPLACE TASK SCD_TYPE2.TASKS.task4_delta_update_landing
    WAREHOUSE = COMPUTE_WH
    AFTER SCD_TYPE2.TASKS.task3_modify_landing
AS
MERGE INTO SCD_TYPE2.PUBLIC.SUPPLIER_STAGING SS
USING (SELECT * FROM SCD_TYPE2.STREAMS.supplier_cdc_landing) LS
ON (SS.SUPPLIER_CODE = LS.SUPPLIER_CODE AND SS.SUPPLIER_STATE = LS.SUPPLIER_STATE) -- SCD column of Interest: SUPPLIER_STATE
WHEN MATCHED AND (LS.METADATA$ACTION = 'DELETE')
     THEN 
     UPDATE SET END_DATE = to_timestamp_ntz(CURRENT_TIMESTAMP()), CURRENT_FLAG = 'N'

WHEN NOT MATCHED AND (LS.METADATA$ACTION = 'INSERT')
     THEN
     INSERT (supplier_key, supplier_code, supplier_name, supplier_state, start_date, end_date, current_flag)
     VALUES (LS.supplier_key, LS.supplier_code, LS.supplier_name, LS.supplier_state, TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()), NULL, 'Y');

-- Task 5: Overwrite MASTER table with latest 'Y' flag data from staging

CREATE OR REPLACE TASK SCD_TYPE2.TASKS.task5_master_refresh
    WAREHOUSE = COMPUTE_WH
    AFTER SCD_TYPE2.TASKS.task4_delta_update_landing
AS
INSERT OVERWRITE INTO SCD_TYPE2.PUBLIC.SUPPLIER_MASTER
SELECT supplier_key, supplier_code, supplier_name, supplier_state FROM SCD_TYPE2.PUBLIC.SUPPLIER_STAGING WHERE CURRENT_FLAG = 'Y';

-- Show tasks

CUST_DB.CUST_VIEWS -- By default, all tasks are SUSPENDED.

-- Resume all tasks from BACKWARDS ** Really imp

ALTER TASK TASK5_MASTER_REFRESH RESUME;
ALTER TASK TASK4_DELTA_UPDATE_LANDING RESUME;
ALTER TASK TASK3_MODIFY_LANDING RESUME;
ALTER TASK TASK2_LOAD_RAW_TABLE RESUME;
ALTER TASK TASK1_TRUNCATE_RAW_TABLE RESUME;

SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY ORDER BY COMPLETED_TIME DESC;

ALTER TASK TASK1_TRUNCATE_RAW_TABLE SUSPEND;
ALTER TASK TASK2_LOAD_RAW_TABLE SUSPEND;
ALTER TASK TASK3_MODIFY_LANDING SUSPEND;
ALTER TASK TASK4_DELTA_UPDATE_LANDING SUSPEND;
ALTER TASK TASK5_MASTER_REFRESH SUSPEND;






-- *********** Driver Code *************

LIST @SCD_TYPE2.INTERNAL_STAGES.supplier_internal_stage; -- there is some file inside
RM @SCD_TYPE2.INTERNAL_STAGES.supplier_internal_stage; -- remove data from internal stage
LIST @SCD_TYPE2.INTERNAL_STAGES.supplier_internal_stage; -- nothing present.

-- Upload csv file "supplier.csv" into internal stage
-- PUT file:///Users/beingrampopuri/Desktop/suppliers.csv @SCD_TYPE2.INTERNAL_STAGES.supplier_internal_stage

-- We are not checking the RAW table as it gets truncated as and when landing table is filled.

SELECT * FROM SCD_TYPE2.PUBLIC.SUPPLIER_LANDING ORDER BY SUPPLIER_KEY;

SELECT * FROM SCD_TYPE2.STREAMS.supplier_cdc_landing; -- Stream data once used will be GONE permanently.

SELECT * FROM SCD_TYPE2.PUBLIC.SUPPLIER_STAGING ORDER BY SUPPLIER_KEY;
SELECT * FROM SCD_TYPE2.PUBLIC.SUPPLIER_MASTER ORDER BY SUPPLIER_KEY;

SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY ORDER BY COMPLETED_TIME DESC;

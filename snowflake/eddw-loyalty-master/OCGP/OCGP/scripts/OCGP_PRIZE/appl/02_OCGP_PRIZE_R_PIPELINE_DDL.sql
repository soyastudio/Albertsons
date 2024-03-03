--liquibase formatted sql
--changeset SYSTEM:EDM_OCGP_PRIZE_R_PIPELINE_DDL runOnChange:true splitStatements:false OBJECT_TYPE:pipe

USE DATABASE <<EDM_DB_NAME_R>>;
USE SCHEMA DW_APPL;

--creation of file format
CREATE OR REPLACE FILE FORMAT CSV_OCGP
TYPE = CSV 
COMPRESSION = 'AUTO' 
FIELD_DELIMITER = ','
RECORD_DELIMITER = '\\n'
SKIP_HEADER = 1 
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
TRIM_SPACE = FALSE 
ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE 
ESCAPE = '\\';

---creation of stage
Create or replace stage EDM_OCGP_STAGE_<<ENV>>BLOB_INC 
storage_integration = <<STORAGE_INTEGRATION>>
url = <<STAGE_URL>>;

----------Creation of pipes
Create or replace pipe EDM_OCGP_PRIZE_PIPE_<<ENV>>BLOB_INC
auto_ingest = true
integration = <<PIPE_INTEGRATION>>
as
COPY INTO <<EDM_DB_NAME_R>>.DW_R_LOYALTY.OCGP_PRIZE_FLAT 
FROM
(select  $1 ,$2 ,$3 ,$4 ,$5 ,$6 ,$7 ,$8 ,$9 ,$10 ,$11 ,$12 ,$13 ,$14 ,$15 ,$16 ,metadata$filename ,current_timestamp
from @EDM_OCGP_STAGE_<<ENV>>BLOB_INC/OCGP/
)
file_format = CSV_OCGP
pattern='.*Prize_2.*.csv'
on_error = 'SKIP_FILE';

Create or replace pipe EDM_OCGP_PRIZE_INVENTORY_STATUS_PIPE_<<ENV>>BLOB_INC 
auto_ingest = true
integration = <<PIPE_INTEGRATION>>
as
COPY INTO <<EDM_DB_NAME_R>>.DW_R_LOYALTY.OCGP_PRIZE_INVENTORY_STATUS_FLAT 
FROM
(select  $1 ,$2 ,$3 ,metadata$filename ,current_timestamp
from @EDM_OCGP_STAGE_<<ENV>>BLOB_INC/OCGP/
)
file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
pattern='.*Prize_Inventory.*.csv'
on_error = 'SKIP_FILE';


------------------creation of Streams
Create or replace stream OCGP_PRIZE_FLAT_R_STREAM ON TABLE <<EDM_DB_NAME_R>>.DW_R_LOYALTY.OCGP_PRIZE_FLAT;

Create or replace stream OCGP_PRIZE_INVENTORY_STATUS_FLAT_R_STREAM ON TABLE <<EDM_DB_NAME_R>>.DW_R_LOYALTY.OCGP_PRIZE_INVENTORY_STATUS_FLAT;

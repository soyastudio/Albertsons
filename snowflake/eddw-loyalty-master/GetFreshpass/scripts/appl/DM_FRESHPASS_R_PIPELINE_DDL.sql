--liquibase formatted sql
--changeset SYSTEM:EDM_FRESHPASS_R_PIPELINE_DDL runOnChange:true splitStatements:false OBJECT_TYPE:pipe

USE DATABASE <<EDM_DB_NAME_R>>;
USE SCHEMA DW_APPL;

--creation of file format
CREATE OR REPLACE FILE FORMAT CSV_FRESHPASS
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
Create or replace stage EDM_FRESHPASS_DIRECTFEEDS_STAGE_<<ENV>>BLOB_INC 
storage_integration = <<STORAGE_INTEGRATION>>
url = <<STAGE_URL>>;

----------Creation of pipes
Create or replace pipe FRESHPASS_SUBSCRIPTION_DISCOUNT_PIPE_<<ENV>>BLOB_INC
auto_ingest = true
integration = <<PIPE_INTEGRATION>>
as
COPY INTO <<EDM_DB_NAME_R>>.DW_R_LOYALTY.FRESHPASS_SUBSCRIPTION_DISCOUNT_FLAT 
FROM
(select  $1 ,$2 ,$3 ,$4 ,$7 ,$8 ,$9 ,$10 ,$13 ,$14 ,$15 ,$16, $17,$18,$19, $20 ,$5 ,$6 ,$12 ,$11 ,metadata$filename ,current_timestamp
from @EDM_FRESHPASS_DIRECTFEEDS_STAGE_<<ENV>>BLOB_INC/Freshpass/
)
file_format = CSV_FRESHPASS
pattern='.*SubscriptionDiscountTypeDetail.*.csv'
on_error = 'SKIP_FILE';

Create or replace pipe EDM_FRESHPASS_SUBSCRIPTION_HOUSEHOLD_DISCOUNT_PIPE_<<ENV>>BLOB_INC 
auto_ingest = true
integration = <<PIPE_INTEGRATION>>
as
COPY INTO <<EDM_DB_NAME_R>>.DW_R_LOYALTY.FRESHPASS_SUBSCRIPTION_HOUSEHOLD_DISCOUNT_FLAT 
FROM
(select $1 ,$2, $7 ,$8 ,$3 ,$4, $5, $6, metadata$filename ,current_timestamp
from @EDM_FRESHPASS_DIRECTFEEDS_STAGE_<<ENV>>BLOB_INC/Freshpass/
)
file_format = CSV_FRESHPASS
pattern='.*SubscriptionHouseholdDiscount.*.csv'
on_error = 'SKIP_FILE';

Create or replace pipe EDM_FRESHPASS_SUBSCRIPTION_HOUSEHOLD_LIFESAVINGS_PIPE_<<ENV>>BLOB_INC 
auto_ingest = true
integration = <<PIPE_INTEGRATION>>
as
COPY INTO <<EDM_DB_NAME_R>>.DW_R_LOYALTY.FRESHPASS_SUBSCRIPTION_HOUSEHOLD_LIFESAVINGS_FLAT 
FROM
(select $1 ,$2 ,$3 ,$4 ,$5 ,$6 ,$7 ,$8 ,$9 ,$10 ,$11 ,$12 ,$13 ,$14, $15, metadata$filename ,current_timestamp
from @EDM_FRESHPASS_DIRECTFEEDS_STAGE_<<ENV>>BLOB_INC/Freshpass/
)
file_format = CSV_FRESHPASS
pattern='.*SubscriptionHouseholdLifeSavings.*.csv'
on_error = 'SKIP_FILE';


------------------creation of Streams
Create or replace stream FRESHPASS_SUBSCRIPTION_DISCOUNT_FLAT_R_STREAM ON TABLE <<EDM_DB_NAME_R>>.DW_R_LOYALTY.FRESHPASS_SUBSCRIPTION_DISCOUNT_FLAT;

Create or replace stream FRESHPASS_SUBSCRIPTION_HOUSEHOLD_DISCOUNT_FLAT_R_STREAM ON TABLE <<EDM_DB_NAME_R>>.DW_R_LOYALTY.FRESHPASS_SUBSCRIPTION_HOUSEHOLD_DISCOUNT_FLAT;

Create or replace stream FRESHPASS_SUBSCRIPTION_HOUSEHOLD_LIFESAVINGS_FLAT_R_STREAM ON TABLE <<EDM_DB_NAME_R>>.DW_R_LOYALTY.FRESHPASS_SUBSCRIPTION_HOUSEHOLD_LIFESAVINGS_FLAT;

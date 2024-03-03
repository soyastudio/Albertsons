--liquibase formatted sql
--changeset SYSTEM:ESED_UBER_ORDER_SNOWPIPE_PRODBLOB_INC runOnChange:true splitStatements:false OBJECT_TYPE:SP

use database <<EDM_DB_NAME_R>>;
use schema <<EDM_DB_NAME_R>>.DW_APPL;

create or replace pipe ESED_UBER_ORDER_SNOWPIPE<<BLOB_ENV>>BLOB_INC 
auto_ingest=true integration='EDDW<<ENV1>>SNOWPIPEINTEGRATION'
as COPY INTO <<EDM_DB_NAME_R>>.DW_R_LOYALTY.uber_order_info
FROM
(select
metadata$filename,current_timestamp(), $1,$2,$3,$4,$5,$6,$7
from @ESED_UBER_STAGE<<BLOB_ENV>>BLOB_INC/Uber
 )
 file_format = (
 type = csv 
 TIMESTAMP_FORMAT = 'AUTO'
 DATE_FORMAT = 'AUTO'
 null_if = '' 
 FIELD_OPTIONALLY_ENCLOSED_BY='"' 
 FIELD_DELIMITER = "|"
   skip_header = 1
) 
pattern='.*order.*[.]csv'
on_error='SKIP_FILE';



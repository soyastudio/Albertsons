--liquibase formatted sql
--changeset SYSTEM:ESED_UBER_CUSTOMER_SNOWPIPE_PRODBLOB_INC runOnChange:true splitStatements:false OBJECT_TYPE:PIPE
use database <<EDM_DB_NAME_R>>;
use schema <<EDM_DB_NAME_R>>.DW_APPL;

Create or replace pipe ESED_UBER_CUSTOMER_SNOWPIPE_PRODBLOB_INC
auto_ingest = true
integration = EDDW_PRD_SNOWPIPEINTEGRATION
as
COPY INTO <<EDM_DB_NAME_R>>.DW_R_LOYALTY.uber_customer_info
FROM
(select
metadata$filename, $1,$2,$3,$4,$5
from @ESED_UBER_STAGE_PRODBLOB_INC/Uber
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
pattern='.*customer.*[.]csv'
on_error='SKIP_FILE'

--rollback DROP PIPE <<EDM_DB_NAME_R>>.DW_APPL.ESED_UBER_CUSTOMER_SNOWPIPE_PRODBLOB_INC

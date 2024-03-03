--liquibase formatted sql
--changeset SYSTEM:EPE_OFFEROMS_JSON_TASK runOnChange:true splitStatements:false OBJECT_TYPE:TASK
use database <<EDM_DB_NAME_OUT>>;
use schema <<EDM_DB_NAME_OUT>>.DW_APPL;

CREATE OR REPLACE TASK EPE_OFFEROMS_JSON_TASK
WAREHOUSE='EDM_ADMIN_WH'
SCHEDULE='1 minute'
WHEN SYSTEM$STREAM_HAS_DATA('<<EDM_DB_NAME>>.DW_APPL.GetOfferOMS_Flat_C_Stream')
AS CALL SP_JSON_EPE_OMS_OFFER();

ALTER TASK EPE_OFFEROMS_JSON_TASK RESUME;
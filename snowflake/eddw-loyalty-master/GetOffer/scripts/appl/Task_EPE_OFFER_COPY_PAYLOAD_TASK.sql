--liquibase formatted sql
--changeset SYSTEM:EPE_OFFER_COPY_PAYLOAD_TASK runOnChange:true splitStatements:false OBJECT_TYPE:TASK
use database <<EDM_DB_NAME_OUT>>;
use schema <<EDM_DB_NAME_OUT>>.DW_APPL;

create or replace task EPE_OFFER_COPY_PAYLOAD_TASK
	warehouse=PROD_INGESTION_SMALL_WH
	schedule='1 minute'
	when SYSTEM$STREAM_HAS_DATA('EPE_OFFER_JSON_O_STREAM')
	as CALL SP_EPE_OFFER_COPY_PAYLOAD();
  
ALTER TASK EPE_OFFER_COPY_PAYLOAD_TASK RESUME;

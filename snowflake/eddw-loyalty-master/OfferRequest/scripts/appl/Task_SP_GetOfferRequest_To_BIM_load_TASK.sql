--liquibase formatted sql
--changeset SYSTEM:SP_GetOfferRequest_To_BIM_load_TASK runOnChange:true splitStatements:false OBJECT_TYPE:TASK
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_PRODUCT;

create or replace task SP_GETOFFERREQUEST_TO_BIM_LOAD_SP_TASK
	warehouse=PROD_INGESTION_SMALL_WH
	schedule='1 minute'
	when SYSTEM$STREAM_HAS_DATA('EDM_REFINED_PRD.DW_APPL.GetOfferRequest_FLAT_SP_R_STREAM')
	as CALL EDM_CONFIRMED_PRD.DW_APPL.sp_GetOfferRequest_To_BIM_load_SP();
  
ALTER TASK SP_GETOFFERREQUEST_TO_BIM_LOAD_TASK RESUME;

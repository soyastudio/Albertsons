--liquibase formatted sql
--changeset SYSTEM:Task runOnChange:true splitStatements:false OBJECT_TYPE:SP

USE DATABASE EDM_REFINED_PRD;
USE SCHEMA dw_appl; 

DROP TASK IF EXISTS EDM_REFINED_PRD.DW_R_PRODUCT.ESED_OFFERREQUEST_R_TASK;

create or replace task ESED_OFFERREQUEST_R_TASK
	warehouse=PROD_INGESTION_BIG_WH
	schedule='1 minutes'
	when SYSTEM$STREAM_HAS_DATA('EDM_REFINED_PRD.DW_R_PRODUCT.ESED_OfferRequest_R_STREAM')
	as call sp_GetOfferRequest_To_FLAT_load_Rerun();
	
alter task ESED_OFFERREQUEST_R_TASK resume;

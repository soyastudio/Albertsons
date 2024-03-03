--liquibase formatted sql
--changeset SYSTEM:Task runOnChange:true splitStatements:false OBJECT_TYPE:SP

use database EDM_ANALYTICS_PRD;
use schema dw_appl;

Drop task if exists EDM_ANALYTICS_PRD.dw_retail_exp.SP_GETOFFERREQUEST_TO_ANALYTICS_LOAD_TASK;

create or replace task SP_GETOFFERREQUEST_TO_ANALYTICS_LOAD_TASK
	warehouse=PROD_DELIVERY_BIG_WH
	schedule='2 minutes'
	when SYSTEM$STREAM_HAS_DATA('EDM_CONFIRMED_PRD.DW_C_PRODUCT.GetOfferRequest_Flat_C_Stream')
	as CALL SP_GETOFFERREQUEST_TO_ANALYTICAL_LOAD();
	
alter task SP_GETOFFERREQUEST_TO_ANALYTICS_LOAD_TASK resume;

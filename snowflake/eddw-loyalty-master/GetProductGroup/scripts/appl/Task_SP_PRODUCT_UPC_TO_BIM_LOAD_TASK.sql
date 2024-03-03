--liquibase formatted sql
--changeset SYSTEM:SP_PRODUCT_UPC_TO_BIM_LOAD_TASK runOnChange:true splitStatements:false OBJECT_TYPE:TASK

use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

create or replace task SP_PRODUCT_UPC_TO_BIM_LOAD_TASK
	warehouse=PROD_INGESTION_SMALL_WH
	schedule='1 minutes'
	QUERY_TAG='{"OBJECT_TYPE":"TASK", "OBJECT_NAME":"sp_Product_UPC_To_BIM_load_task", 
				"APPCODE":"OCMC", "APPID":"DSE", "SUBJECTAREA":"DCAT", "BOD": "Product_UPC", 
				"PROCESS": "C_PIPELINE_INC_LOAD", "COMMENT": "Task Tag"}'
	when SYSTEM$STREAM_HAS_DATA('EDM_REFINED_PRD.DW_APPL.GetRuntimeProductsUPC_Flat_R_STREAM')
	as CALL sp_GetProductUPC_To_BIM_load();

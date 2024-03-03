--liquibase formatted sql
--changeset SYSTEM:SP_ProductGroup_To_BIM_load_TASK runOnChange:true splitStatements:false OBJECT_TYPE:TASK

USE DATABASE EDM_CONFIRMED_PRD;
USE SCHEMA dw_appl;

create or replace task SP_PRODUCTGROUP_TO_BIM_LOAD_TASK
	warehouse=PROD_INGESTION_BIG_WH
	schedule='1 minutes'
	when SYSTEM$STREAM_HAS_DATA('EDM_REFINED_PRD.DW_R_PRODUCT.ProductGroup_Flat_R_STREAM')
	as CALL SP_ProductGroup_To_BIM_load();
	
alter task SP_PRODUCTGROUP_TO_BIM_LOAD_TASK resume;

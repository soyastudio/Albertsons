--liquibase formatted sql
--changeset SYSTEM:Task runOnChange:true splitStatements:false OBJECT_TYPE:SP

use database EDM_CONFIRMED_PRD;
use schema EDM_CONFIRMED_PRD.DW_APPL;

create or replace task SP_FACT_MULTICLIP_REPORT_LOAD_TASK
	warehouse=PROD_INGESTION_SMALL_WH
	schedule='1 minutes'
	when SYSTEM$STREAM_HAS_DATA('EDM_CONFIRMED_PRD.dw_appl.FACT_MULTICLIP_REPORT_Stream')
	as CALL SP_LOAD_FACT_MULTICLIP_REPORT();
	
ALTER TASK SP_FACT_MULTICLIP_REPORT_LOAD_TASK RESUME;

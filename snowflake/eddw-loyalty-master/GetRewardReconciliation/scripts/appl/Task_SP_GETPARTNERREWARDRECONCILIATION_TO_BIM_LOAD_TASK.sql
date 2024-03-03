--liquibase formatted sql
--changeset SYSTEM:SP_GETPARTNERREWARDRECONCILIATION_TO_BIM_LOAD_TASK runOnChange:true splitStatements:false OBJECT_TYPE:TASK
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

create or replace task SP_GETPARTNERREWARDRECONCILIATION_TO_BIM_LOAD_TASK
	warehouse=PROD_INGESTION_MEDIUM_WH
	schedule='1 minutes'
	when SYSTEM$STREAM_HAS_DATA('EDM_REFINED_PRD.DW_APPL.GetPartnerRewardReconciliation_Flat_R_STREAM')
	as CALL SP_GetPartnerRewardReconciliation_To_BIM_load();
  
ALTER TASK SP_GETPARTNERREWARDRECONCILIATION_TO_BIM_LOAD_TASK RESUME;

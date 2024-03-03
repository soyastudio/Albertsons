--liquibase formatted sql
--changeset SYSTEM:Task runOnChange:true splitStatements:false OBJECT_TYPE:SP

use database EDM_REFINED_PRD;
use schema DW_APPL;

create or replace task ESED_REWARDTRANSACTION_JSON_R_TASK
	warehouse=PROD_INGESTION_SMALL_WH
	schedule='1 minutes'	
	when SYSTEM$STREAM_HAS_DATA('ESED_RewardTransaction_Json_R_STREAM')
	as call SP_GETREWARDTRANSACTION_TO_FLAT_JSON_LOAD();
	
alter task ESED_REWARDTRANSACTION_JSON_R_TASK resume;

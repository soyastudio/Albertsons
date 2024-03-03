--liquibase formatted sql
--changeset SYSTEM:JSON_TASK runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME_R>>;
use schema DW_APPL;



create or replace stream ESED_REWARDTRANSACTION_JSON_R_STREAM on table <<EDM_DB_NAME_R>>.DW_r_LOYALTY.ESED_REWARDTRANSACTION_JSON;

create or replace task ESED_REWARDTRANSACTION_JSON_R_TASK
	warehouse=<<ENV1>>_INGESTION_SMALL_WH
	schedule='1 minutes'
	when SYSTEM$STREAM_HAS_DATA('ESED_RewardTransaction_Json_R_STREAM')
	as call SP_GETREWARDTRANSACTION_TO_FLAT_JSON_LOAD();

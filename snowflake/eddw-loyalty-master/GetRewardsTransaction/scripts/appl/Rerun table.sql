--liquibase formatted sql
--changeset SYSTEM:Rerun_table runOnChange:true splitStatements:false OBJECT_TYPE:TABLE

USE DATABaSE EDM_REFINED_PRD;
USE SCHEMA DW_R_STAGE;

create or replace table ESED_REWARDTRANSACTION_JSON_RERUN
AS select * from EDM_REFINED_PRD.dw_appl.ESED_RewardTransaction_Json_R_STREAM where 1=2;

alter table ESED_REWARDTRANSACTION_JSON_RERUN ALTER COLUMN METADATA$ACTION VARCHAR, METADATA$ROW_ID VARCHAR;

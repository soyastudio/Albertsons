--liquibase formatted sql
--changeset SYSTEM:r_task_resume runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME_R>>;
use schema DW_APPL;

ALTER TASK ESED_REWARDTRANSACTION_JSON_R_TASK RESUME;

ALTER TASK ESED_REWARDTRANSACTION_R_TASK RESUME;

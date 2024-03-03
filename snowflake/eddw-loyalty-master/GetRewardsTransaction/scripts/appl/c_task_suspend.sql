--liquibase formatted sql
--changeset SYSTEM:c_task_suspend runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema DW_APPL;

ALTER TASK SP_GETREWARDTRANSACTION_TO_BIM_LOAD_TASK SUSPEND;


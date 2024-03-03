--liquibase formatted sql
--changeset SYSTEM:Task runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME_C>>;
use schema <<EDM_DB_NAME_C>>.DW_APPL;


ALTER TASK SP_F_ORDER_TRANSACTION_LOAD_TASK SUSPEND;

ALTER TASK SP_F_ORDER_TRANSACTION_LOAD_TASK 
SET SCHEDULE = 'USING CRON 0 */3 * * * Asia/Kolkata'
 ;
 

ALTER TASK SP_F_ORDER_TRANSACTION_LOAD_TASK RESUME;

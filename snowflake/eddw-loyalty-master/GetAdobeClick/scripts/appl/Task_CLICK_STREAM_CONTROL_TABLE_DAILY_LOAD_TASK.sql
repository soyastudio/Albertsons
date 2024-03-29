--liquibase formatted sql
--changeset SYSTEM:CLICK_STREAM_CONTROL_TABLE_DAILY_LOAD_TASK runOnChange:true splitStatements:false OBJECT_TYPE:TASK
use database EDM_CONFIRMED_PRD;
use schema DW_APPL;

CREATE OR REPLACE TASK CLICK_STREAM_CONTROL_TABLE_DAILY_LOAD_TASK
WAREHOUSE='EDM_ADMIN_WH'
SCHEDULE='120 minute'
WHEN SYSTEM$STREAM_HAS_DATA('EDM_CONFIRMED_PRD.DW_APPL.CLICK_HIT_DATA_C_STREAM')
AS call sp_GetAdobeClickHitData_LOAD_CLICK_STREAM_Control_Table('daily');

ALTER TASK CLICK_STREAM_CONTROL_TABLE_DAILY_LOAD_TASK RESUME;
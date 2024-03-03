--liquibase formatted sql
--changeset SYSTEM:CLICK_STREAM_PLUGIN_C_TASK runOnChange:true splitStatements:false OBJECT_TYPE:TASK
use database EDM_CONFIRMED_PRD;
use schema DW_APPL;

CREATE OR REPLACE TASK CLICK_STREAM_PLUGIN_C_TASK
WAREHOUSE='EDM_ADMIN_WH'
SCHEDULE='480 minute'
AS call SP_GetAdobeClickHitData_TO_BIM_LOAD_Click_Stream_Plugin('EDM_VIEWS_PRD.DW_VIEWS.CLICK_PLUGIN','EDM_CONFIRMED_PRD','DW_C_USER_ACTIVITY','DW_C_STAGE');

ALTER TASK CLICK_STREAM_PLUGIN_C_TASK RESUME;
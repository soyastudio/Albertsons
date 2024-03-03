--liquibase formatted sql
--changeset SYSTEM:CLICK_STREAM_SHOP_LOAD_TASK runOnChange:true splitStatements:false OBJECT_TYPE:TASK
use database EDM_CONFIRMED_PRD;
use schema DW_APPL;

CREATE OR REPLACE TASK CLICK_STREAM_SHOP_LOAD_TASK
WAREHOUSE='EDM_ADMIN_WH'
SCHEDULE='1 minute'
WHEN SYSTEM$STREAM_HAS_DATA('EDM_CONFIRMED_PRD.DW_APPL.CLICK_HIT_SHOP_C_STREAM')
AS call SP_GetAdobeClickHitData_TO_BIM_LOAD_Click_Stream_Shop('EDM_CONFIRMED_PRD.DW_APPL.CLICK_HIT_SHOP_C_STREAM','EDM_CONFIRMED_PRD','DW_C_USER_ACTIVITY','DW_C_STAGE');

ALTER TASK CLICK_STREAM_SHOP_LOAD_TASK RESUME;
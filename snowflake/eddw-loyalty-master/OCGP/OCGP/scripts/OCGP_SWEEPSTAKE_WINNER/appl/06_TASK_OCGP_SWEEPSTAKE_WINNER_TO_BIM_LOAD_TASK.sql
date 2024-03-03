--liquibase formatted sql
--changeset SYSTEM:SP_OCGP_SWEEPSTAKE_WINNER_TO_BIM_LOAD_TASK runOnChange:true splitStatements:false OBJECT_TYPE:TASK

use database <<EDM_DB_NAME>>;
use schema DW_APPL;

CREATE OR REPLACE TASK SP_OCGP_SWEEPSTAKE_WINNER_TO_BIM_LOAD_TASK
WAREHOUSE='<<ENVFULL>>_INGESTION_SMALL_WH'
SCHEDULE='720 minutes'
QUERY_TAG = '{"OBJECT_TYPE":"TASK", "OBJECT_NAME":"SP_OCGP_SWEEPSTAKE_WINNER_TO_BIM_LOAD_TASK", "APPCODE":"EMAQ", "OBJECT_CATEGORY":"INBOUND",
"TECHNICAL_CONTACT":"LOYALTY"}'
WHEN SYSTEM$STREAM_HAS_DATA('<<EDM_DB_NAME_R>>.DW_APPL.OCGP_SWEEPSTAKE_WINNER_FLAT_R_STREAM')
AS CALL SP_OCGP_SWEEPSTAKE_WINNER_TO_BIM_LOAD('<<EDM_DB_NAME_R>>.DW_APPL.OCGP_SWEEPSTAKE_WINNER_FLAT_R_STREAM','<<EDM_DB_NAME>>','DW_C_LOYALTY','DW_C_STAGE');

ALTER TASK SP_OCGP_SWEEPSTAKE_WINNER_TO_BIM_LOAD_TASK RESUME;

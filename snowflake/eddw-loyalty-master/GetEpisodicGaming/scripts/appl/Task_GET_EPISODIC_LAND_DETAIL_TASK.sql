--liquibase formatted sql
--changeset SYSTEM:GET_EPISODIC_LAND_DETAIL_TASK runOnChange:true splitStatements:false OBJECT_TYPE:TASK
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE TASK GET_EPISODIC_LAND_DETAIL_TASK
WAREHOUSE='EDM_ADMIN_WH'
SCHEDULE='1 minutes'
WHEN SYSTEM$STREAM_HAS_DATA('EDM_REFINED_PRD.DW_APPL.EPISODIC_LANDDETAILS_FLAT_R_STREAM')
AS CALL SP_GETEPISODIC_TO_BIM_LOAD_EPISODIC_LAND_DETAIL('EDM_REFINED_PRD.DW_APPL.EPISODIC_LANDDETAILS_FLAT_R_STREAM','EDM_CONFIRMED_PRD','DW_C_LOYALTY','DW_C_STAGE');

ALTER TASK GET_EPISODIC_LAND_DETAIL_TASK RESUME;

--liquibase formatted sql
--changeset SYSTEM:Create_TASK_For_Foodstorm runOnChange:true splitStatements:false OBJECT_TYPE:TASK
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE TASK GET_FOOD_STORM_TASK
		  WAREHOUSE = '<<WH>>'
		  SCHEDULE = '1 minutes'
		  QUERY_TAG = '{"OBJECT_TYPE":"TASK", "OBJECT_NAME":"GET_FOOD_STORM_TASK", "APPCODE":"PSFS", "TECHNICAL_CONTACT":"LOYALTY"}'
		  WHEN 
		  SYSTEM$STREAM_HAS_DATA('<<EDM_DB_NAME_R>>.DW_APPL.GetFOODSTORM_Flat_R_STREAM')
		  AS 
		  CALL SP_GetFoodstorm_TO_BIM_LOAD();


ALTER TASK GET_FOOD_STORM_TASK RESUME;

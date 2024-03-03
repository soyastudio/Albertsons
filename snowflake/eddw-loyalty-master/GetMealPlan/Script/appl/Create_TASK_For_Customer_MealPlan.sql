--liquibase formatted sql
--changeset SYSTEM:Create_TASK_For_Customer_MealPlan runOnChange:true splitStatements:false OBJECT_TYPE:TASK
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

ALTER TASK GET_MEAL_PLAN_CUSTOMER_TASK SUSPEND;
CREATE OR REPLACE TASK GET_MEAL_PLAN_CUSTOMER_TASK
              WAREHOUSE = '<<WH>>'
              SCHEDULE = 'USING CRON 0 20 * * * Asia/Kolkata'
			        QUERY_TAG = '{"OBJECT_TYPE":"TASK", "OBJECT_NAME":"GET_MEAL_PLAN_CUSTOMER_TASK", "APPCODE":"DIRM", "TECHNICAL_CONTACT":"LOYALTY"}'
              WHEN 
              SYSTEM$STREAM_HAS_DATA('<<EDM_DB_NAME_R>>.DW_APPL.GetMeal_Plan_Customer_Flat_R_STREAM')
              AS 
              CALL SP_GetMeal_Plan_Customer_TO_BIM_LOAD_Meal_Plan_Customer('<<EDM_DB_NAME_R>>.DW_APPL.GetMeal_Plan_Customer_Flat_R_STREAM','<<EDM_DB_NAME>>','DW_C_LOYALTY','DW_C_STAGE');

ALTER TASK GET_MEAL_PLAN_CUSTOMER_TASK RESUME;

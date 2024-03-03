USE DATABASE EDM_CONFIRMED_PRD;
USE SCHEMA DW_APPL;

CREATE OR REPLACE TASK SP_Txn_NPS_Survey_Faulty_Data_LOAD_Task
    WAREHOUSE = 'PROD_INGESTION_MEDIUM_WH'
    SCHEDULE = 'USING CRON 0 0 * * * MST'
	QUERY_TAG = '{"OBJECT_TYPE":"TASK", "OBJECT_NAME":"SP_Txn_NPS_Survey_Faulty_Data_LOAD_Task", "APPCODE":"EDDW"}'
    AS
    call EDM_CONFIRMED_PRD.DW_APPL.SP_Txn_NPS_Survey_Faulty_Data_LOAD();
    
ALTER TASK SP_Txn_NPS_Survey_Faulty_Data_LOAD_Task RESUME;
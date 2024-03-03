--liquidbase formatted sql
--changeset SYSTEM:ORDER_INFO runOnChange: true splitStatements:false OBJECT_TYPE:TASK

USE DATABASE <<TGT_EDM_DB_NAME>>;
USE SCHEMA <<SP_DEPLOY_SCHEMA>>;

CREATE OR REPLACE TASK <<TGT_EDM_DB_NAME>>.<<SP_DEPLOY_SCHEMA>>.CATALINA_OUTBOUND_ORDER_INFO_LOAD_TASK
    WAREHOUSE = <<WAREHOUSE>>
    SCHEDULE = 'USING CRON 0 2 * * * MST'
        QUERY_TAG = '{"OBJECT_TYPE":"TASK", "OBJECT_NAME":"SP_ORDER_INFO_Incremental", "APPCODE":"EDDW"}'
    AS
    call CATALINA_OUTBOUND.DW_APPL.SP_ORDER_INFO_Incremental();
    
ALTER TASK CATALINA_OUTBOUND_ORDER_INFO_LOAD_TASK RESUME;

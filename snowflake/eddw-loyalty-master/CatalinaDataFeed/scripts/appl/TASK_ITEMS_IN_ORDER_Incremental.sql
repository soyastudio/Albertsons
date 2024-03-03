--liquidbase formatted sql
--changeset SYSTEM:ITEMS_IN_ORDER runOnChange: true splitStatements:false OBJECT_TYPE:TASK

USE DATABASE <<TGT_EDM_DB_NAME>>;
USE SCHEMA <<SP_DEPLOY_SCHEMA>>;

CREATE OR REPLACE TASK <<TGT_EDM_DB_NAME>>.<<SP_DEPLOY_SCHEMA>>.CATALINA_OUTBOUND_ITEMS_IN_ORDER_LOAD_TASK
    WAREHOUSE = <<WAREHOUSE>>
    SCHEDULE = 'USING CRON 0 2 * * * MST'
        QUERY_TAG = '{"OBJECT_TYPE":"TASK", "OBJECT_NAME":"SP_ITEMS_IN_ORDER_Incremental", "APPCODE":"EDDW"}'
    AS
    call CATALINA_OUTBOUND.DW_APPL.SP_ITEMS_IN_ORDER_Incremental();
    
ALTER TASK CATALINA_OUTBOUND_ITEMS_IN_ORDER_LOAD_TASK RESUME;

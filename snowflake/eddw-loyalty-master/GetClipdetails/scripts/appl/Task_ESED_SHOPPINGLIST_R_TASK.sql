--liquibase formatted sql
--changeset SYSTEM:ESED_SHOPPINGLIST_R_TASK runOnChange:true splitStatements:false OBJECT_TYPE:TASK
use database <<EDM_DB_NAME_R>>;
use schema <<EDM_DB_NAME_R>>.DW_APPL;

CREATE OR REPLACE TASK ESED_SHOPPINGLIST_R_TASK
WAREHOUSE='EDM_ADMIN_WH'
SCHEDULE='1 minutes'
WHEN SYSTEM$STREAM_HAS_DATA('EDM_REFINED_PRD.DW_APPL.ESED_ShoppingList_Temp_R_STREAM')
AS call sp_GetShoppingList_To_FLAT_load();

ALTER TASK ESED_SHOPPINGLIST_R_TASK RESUME;
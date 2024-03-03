--liquibase formatted sql
--changeset SYSTEM:Create_PIPE_for_Foodstorm runOnChange:true splitStatements:false OBJECT_TYPE:PIPE
USE DATABASE <<EDM_DB_NAME_R>>;
USE SCHEMA <<EDM_DB_NAME_R>>.DW_APPL;

CREATE OR REPLACE PIPE EDM_FOODSTORM_PIPE_<<ENV>>BLOB_INC
auto_ingest = true
integration = '<<EDM_PIPE_INTEGRATION>>'
as
COPY INTO <<EDM_DB_NAME_R>>.DW_R_LOYALTY.GETFOODSTORM_FLAT
FROM
(select $1 ,$2 ,$3 ,$4 ,$5 ,$6 ,$7 ,$8 ,$9 ,$10 ,$11 ,$12 ,$13 ,$14 ,$15 ,$16 ,$17 ,$18 ,$19 ,$20 ,$21 ,$22 ,current_timestamp ,metadata$filename
from @EDM_FOOD_STORM_STAGE_<<ENV>>BLOB_INC/Foodstorm/
)
file_format = 'CSV_FOOD_STORM'
pattern = '.*POSExportFoodStorm.*[.]*'
on_error = 'SKIP_FILE'; 

alter pipe <<EDM_DB_NAME_R>>.DW_APPL.EDM_FOODSTORM_PIPE_<<ENV>>BLOB_INC SET TAG <<EDM_PIPE_DB>>.OBJECTTAG.APPCODE = 'PSFS', <<EDM_PIPE_DB>>.OBJECTTAG.TECHNICAL_CONTACT = 'LOYALTY';

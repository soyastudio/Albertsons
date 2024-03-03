--liquibase formatted sql
--changeset SYSTEM:CLICK_STREAM_PLUGIN runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database EDM_VIEWS_PRD;
use schema DW_VIEWS;

CREATE OR REPLACE VIEW Click_Stream_Plugin
(
Plugin_Nm comment 'No longer used. List of numeric ID’s that correspond to plugins available within the browser. Uses plugins.tsv lookup'
,Plugin_Id comment 'Numeric ID’s that correspond to plugins available within the browser. Uses plugins.tsv lookup.'
,DW_FIRST_EFFECTIVE_DT comment'The timestamp the record was inserted. For update Primary Keys this values is used from the prior record of the primary key'
,DW_LAST_EFFECTIVE_DT comment'for the current record this is 12/31/9999 24.00.00.0000. For updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 micro second'
,DW_CREATE_TS comment'When a record is created this would be the current timestamp'
,DW_LAST_UPDATE_TS comment'When a record is updated this would be the current timestamp'
,DW_LOGICAL_DELETE_IND comment'Set to True when we receive a delete record for the primary key, else False'
,DW_SOURCE_CREATE_NM comment'The data source name of this insert'
,DW_SOURCE_UPDATE_NM comment'The data source name of this update or delete'
,DW_CURRENT_VERSION_IND comment'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 day'

)
COPY GRANTS comment = 'View For Click_Stream_Stream_Plugin' AS 
SELECT
 Plugin_Nm
,Plugin_Id
,DW_First_Effective_Dt
,DW_Last_Effective_Dt
,DW_CREATE_TS
,DW_LAST_UPDATE_TS
,DW_LOGICAL_DELETE_IND
,DW_SOURCE_CREATE_NM
,DW_SOURCE_UPDATE_NM
,DW_CURRENT_VERSION_IND
FROM 
EDM_CONFIRMED_PRD.DW_C_USER_ACTIVITY.Click_Stream_Plugin;
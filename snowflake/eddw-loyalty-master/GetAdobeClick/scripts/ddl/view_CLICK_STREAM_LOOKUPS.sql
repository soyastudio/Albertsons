--liquibase formatted sql
--changeset SYSTEM:CLICK_STREAM_LOOKUPS runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database EDM_VIEWS_PRD;
use schema DW_VIEWS;

CREATE OR REPLACE VIEW CLICK_STREAM_LOOKUPS
(
lookup_id				COMMENT 'Unique Key generated for each record in the Adobe transaction data'
, refresh_dt			COMMENT 'When a record is created this would be the current timestamp'
, refresh_intervals		COMMENT 'Job Scheduling time intervals'
, description			COMMENT 'Load Description details'
, attribute1			COMMENT 'Future enhancement'			
, attribute2			COMMENT 'Future enhancement'
, attrubute3			COMMENT 'Future enhancement'
, attribute4			COMMENT 'Future enhancement'
, attribute5			COMMENT 'Future enhancement'
, dw_create_ts			COMMENT'When a record is created this would be the current timestamp'
, dw_last_update_ts		COMMENT'When a record is created this would be the current timestamp'
, dw_source_create_nm	COMMENT'The data source name of this insert'
, dw_source_update_nm	COMMENT'The data source name of this update or delete'                         
)
COPY GRANTS
comment = 'VIEW For click_stream_lookups' 
AS
SELECT
lookup_id
, refresh_dt
, refresh_intervals
, description
, attribute1
, attribute2
, attrubute3
, attribute4
, attribute5
, dw_create_ts
, dw_last_update_ts
, dw_source_create_nm
, dw_source_update_nm
FROM  EDM_CONFIRMED_PRD.DW_C_USER_ACTIVITY.CLICK_STREAM_LOOKUPS;
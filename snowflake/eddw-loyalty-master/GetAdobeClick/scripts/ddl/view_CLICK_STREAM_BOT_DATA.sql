--liquibase formatted sql
--changeset SYSTEM:CLICK_STREAM_BOT_DATA runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database EDM_VIEWS_PRD;
use schema DW_VIEWS;

CREATE OR REPLACE VIEW CLICK_STREAM_BOT_DATA
(
bot_id                      	COMMENT 'Unique Key generated for each record in the Adobe transaction data'
, click_stream_integration_id 	COMMENT 'Unique Key generated for each record in the Adobe transaction data'
, ip                          	COMMENT 'The users IP Address i.e. 65.208.210.98 - Expires After Visit'
, guid                        	COMMENT 'GUID# of the Retail_Customer'
, visitor_id                  	COMMENT 'Unique identifier for a visitor as identified by Adobe'
, visit_id                    	COMMENT 'Unique identifier for a visit as identified by Adobe'
, ip_guid_status              	COMMENT 'Identified as IP and Customer'
, dw_create_ts 			COMMENT'When a record is created this would be the current timestamp'
, dw_last_update_ts 		COMMENT'When a record is updated this would be the current timestamp'
, dw_source_create_nm 		COMMENT'The data source name of this insert'
, dw_source_update_nm 		COMMENT'The data source name of this update or delete'                            
)
COPY GRANTS
comment = 'VIEW For click_stream_bot_data' 
AS
SELECT
bot_id
, click_stream_integration_id
, ip
, guid
, visitor_id
, visit_id
, ip_guid_status
, dw_create_ts
, dw_last_update_ts
, dw_source_create_nm
, dw_source_update_nm
FROM  EDM_CONFIRMED_PRD.DW_C_USER_ACTIVITY.CLICK_STREAM_BOT_DATA;
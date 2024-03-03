--liquibase formatted sql
--changeset SYSTEM:CLICK_STREAM_BOT_DATA runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database EDM_CONFIRMED_PRD;
use schema DW_C_USER_ACTIVITY;

create or replace TABLE CLICK_STREAM_BOT_DATA (
	BOT_ID NUMBER(38,0) autoincrement COMMENT 'Unique Key generated for each record in the Adobe transaction data',
	CLICK_STREAM_INTEGRATION_ID VARCHAR(16777216) COMMENT 'Unique Key generated for each record in the Adobe transaction data',
	IP VARCHAR(16777216) COMMENT 'The users IP Address i.e. 65.208.210.98 - Expires After Visit',
	GUID VARCHAR(16777216) COMMENT 'GUID# of the Retail_Customer',
	VISITOR_ID VARCHAR(16777216) COMMENT 'Unique identifier for a visitor as identified by Adobe',
	VISIT_ID VARCHAR(16777216) COMMENT 'Unique identifier for a visit as identified by Adobe',
	IP_GUID_STATUS VARCHAR(100) COMMENT 'Identified as IP and Customer',
	DW_CREATE_TS TIMESTAMP_LTZ(9) DEFAULT CAST(CONVERT_TIMEZONE('UTC', CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_TZ(9))) AS TIMESTAMP_NTZ(9)) COMMENT 'When a record is created this would be the current timestamp',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) DEFAULT CAST(CONVERT_TIMEZONE('UTC', CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_TZ(9))) AS TIMESTAMP_NTZ(9)) COMMENT 'When a record is updated this would be the current timestamp',
	DW_SOURCE_CREATE_NM VARCHAR(255) DEFAULT CURRENT_USER() COMMENT 'The data source name of this insert',
	DW_SOURCE_UPDATE_NM VARCHAR(255) DEFAULT CURRENT_USER() COMMENT 'The data source name of this update or delete'
);
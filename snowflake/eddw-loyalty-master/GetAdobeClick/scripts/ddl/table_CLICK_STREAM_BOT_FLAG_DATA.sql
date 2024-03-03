--liquibase formatted sql
--changeset SYSTEM:CLICK_STREAM_BOT_FLAG_DATA runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database EDM_CONFIRMED_PRD;
use schema DW_C_USER_ACTIVITY;

create or replace TABLE CLICK_STREAM_BOT_FLAG_DATA (
	BOT_ID NUMBER(38,0) autoincrement COMMENT 'Unique Key generated for each record in the Adobe transaction data',
	IP_GUID VARCHAR(16777216) COMMENT 'The users IP Address i.e. 65.208.210.98 - Expires After Visit',
	BOT_FLAG VARCHAR(255) COMMENT 'Identified as BOT flag',
	FLAG VARCHAR(255) COMMENT 'Identified as IP and Customer ',
	ATTRIBUTE1 VARCHAR(16777216) COMMENT 'Future enhancement',
	ATTRIBUTE2 VARCHAR(16777216) COMMENT 'Future enhancement',
	ATTRUBUTE3 VARCHAR(16777216) COMMENT 'Future enhancement',
	ATTRIBUTE4 TIMESTAMP_LTZ(9) COMMENT 'Future enhancement',
	ATTRIBUTE5 TIMESTAMP_LTZ(9) COMMENT 'Future enhancement',
	DW_CREATE_TS TIMESTAMP_LTZ(9) DEFAULT CAST(CONVERT_TIMEZONE('UTC', CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_TZ(9))) AS TIMESTAMP_NTZ(9)) COMMENT 'When a record is created this would be the current timestamp',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) DEFAULT CAST(CONVERT_TIMEZONE('UTC', CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_TZ(9))) AS TIMESTAMP_NTZ(9)) COMMENT 'When a record is updated this would be the current timestamp',
	DW_SOURCE_CREATE_NM VARCHAR(255) DEFAULT CURRENT_USER() COMMENT 'The data source name of this insert',
	DW_SOURCE_UPDATE_NM VARCHAR(255) DEFAULT CURRENT_USER() COMMENT 'The data source name of this update or delete'
);
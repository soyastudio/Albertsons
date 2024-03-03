--liquibase formatted sql
--changeset SYSTEM:CLICK_STREAM_LOOKUPS runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database EDM_CONFIRMED_PRD;
use schema DW_C_USER_ACTIVITY;

create or replace TABLE CLICK_STREAM_LOOKUPS (
	LOOKUP_ID NUMBER(38,0) autoincrement COMMENT 'Unique Key generated for each record in the Adobe transaction data',
	REFRESH_DT TIMESTAMP_LTZ(9) COMMENT 'When a record is created this would be the current timestamp',
	REFRESH_INTERVALS NUMBER(38,0) COMMENT 'Job Scheduling time intervals',
	DESCRIPTION VARCHAR(16777216) COMMENT 'Load Description details',
	ATTRIBUTE1 VARCHAR(16777216) COMMENT 'Future enhancement',
	ATTRIBUTE2 VARCHAR(16777216) COMMENT 'Future enhancement',
	ATTRUBUTE3 VARCHAR(16777216) COMMENT 'Future enhancement',
	ATTRIBUTE4 TIMESTAMP_LTZ(9) COMMENT 'Future enhancement',
	ATTRIBUTE5 TIMESTAMP_LTZ(9) COMMENT 'Future enhancement',
	DW_CREATE_TS TIMESTAMP_LTZ(9) DEFAULT CAST(CONVERT_TIMEZONE('UTC', CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_TZ(9))) AS TIMESTAMP_NTZ(9)) COMMENT 'When a record is created this would be the current timestamp',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) DEFAULT CAST(CONVERT_TIMEZONE('UTC', CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_TZ(9))) AS TIMESTAMP_NTZ(9)) COMMENT 'When a record is created this would be the current timestamp',
	DW_SOURCE_CREATE_NM VARCHAR(255) DEFAULT CURRENT_USER() COMMENT 'The data source name of this insert',
	DW_SOURCE_UPDATE_NM VARCHAR(255) DEFAULT CURRENT_USER() COMMENT 'The data source name of this update or delete'
);
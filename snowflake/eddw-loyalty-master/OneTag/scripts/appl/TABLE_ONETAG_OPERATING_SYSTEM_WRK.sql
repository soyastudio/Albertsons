--liquibase formatted sql
--changeset SYSTEM:CLICK_STREAM_OPERATING_SYSTEM_WRK runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database EDM_CONFIRMED_<<ENV>>;
use schema DW_C_STAGE;

create or replace TRANSIENT TABLE CLICK_STREAM_OPERATING_SYSTEM_WRK (
	OPERATING_SYSTEM_CD VARCHAR NULL,
	OPERATING_SYSTEM_TYPE_CD VARCHAR NULL,
	PLATFORM_CD VARCHAR NULL,
	DW_CREATE_TS TIMESTAMP NULL,
	DW_LAST_UPDATE_TS TIMESTAMP NULL,
	DW_LOGICAL_DELETE_IND BOOLEAN NULL,
	DW_SOURCE_CREATE_NM VARCHAR(255) NULL,
	DW_SOURCE_UPDATE_NM VARCHAR(255) NULL,
	DW_CURRENT_VERSION_IND BOOLEAN NULL 
);

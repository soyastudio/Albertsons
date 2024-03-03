--liquibase formatted sql
--changeset SYSTEM:OMS_Store_Group_Store runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema DW_C_PRODUCT;
create or replace TABLE OMS_STORE_GROUP (
	STORE_GROUP_ID NUMBER(38,0) NOT NULL,
	DW_FIRST_EFFECTIVE_TS TIMESTAMP_LTZ(9) NOT NULL,
	DW_LAST_EFFECTIVE_TS TIMESTAMP_LTZ(9) NOT NULL,
	STORE_GROUP_NM VARCHAR(16777216),
	STORE_GROUP_DSC VARCHAR(16777216),
	CREATE_TS TIMESTAMP_LTZ(9),
	UPDATE_TS TIMESTAMP_LTZ(9),
	CREATE_USER_ID VARCHAR(16777216),
	CREATE_FIRST_NM VARCHAR(16777216),
	CREATE_LAST_NM VARCHAR(16777216),
	UPDATE_USER_ID VARCHAR(16777216),
	UPDATE_FIRST_NM VARCHAR(16777216),
	DW_CREATE_TS TIMESTAMP_LTZ(9),
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9),
	DW_LOGICAL_DELETE_IND BOOLEAN,
	DW_SOURCE_CREATE_NM VARCHAR(255),
	DW_SOURCE_UPDATE_NM VARCHAR(255),
	DW_CURRENT_VERSION_IND BOOLEAN,
	primary key (STORE_GROUP_ID, DW_FIRST_EFFECTIVE_TS, DW_LAST_EFFECTIVE_TS)
)COMMENT='This table contains information about OMS_STORE_GROUP'
;
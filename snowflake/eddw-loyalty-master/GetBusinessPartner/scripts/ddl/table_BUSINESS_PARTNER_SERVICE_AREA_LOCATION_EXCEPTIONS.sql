--liquibase formatted sql
--changeset SYSTEM:BUSINESS_PARTNER_SERVICE_AREA_LOCATION_EXCEPTIONS runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_STAGE;

create or replace TABLE BUSINESS_PARTNER_SERVICE_AREA_LOCATION_EXCEPTIONS (
	BUSINESS_PARTNER_INTEGRATION_ID VARCHAR(16777216),
	PARTNER_NM VARCHAR(16777216),
	SERVICE_AREA_LOCATION_TYPE_CD VARCHAR(16777216),
	SERVICE_AREA_LOCATION_VALUE_TXT VARCHAR(16777216),
	FILENAME VARCHAR(16777216),
	DW_LOGICAL_DELETE_IND BOOLEAN,
	CREATIONDT VARCHAR(16777216),
	DML_TYPE VARCHAR(5),
	SAMEDAY_CHG_IND BOOLEAN,
	EXCEPTION_REASON VARCHAR(1000),
	DW_CREATE_TS TIMESTAMP_LTZ(9)
);
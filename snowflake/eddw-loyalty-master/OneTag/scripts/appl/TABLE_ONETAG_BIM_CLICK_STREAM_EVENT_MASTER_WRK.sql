--liquibase formatted sql
--changeset SYSTEM:CUSTOMER_SESSION_EVENT_MASTER runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database EDM_CONFIRMED_<<ENV>>;
use schema DW_C_STAGE;

create or replace TABLE CUSTOMER_SESSION_EVENT_MASTER (
	EVENT_ID VARCHAR NOT NULL,
	EVENT_TS TIMESTAMP NOT NULL,
	SESSION_ID VARCHAR NULL,
	PAGE_INTEGRATION_ID NUMBER NULL,
	EVENT_NM VARCHAR NULL,
	VISITOR_INTEGRATION_ID NUMBER NULL,
	VISITOR_ID VARCHAR NULL,
	HOUSEHOLD_ID NUMBER NULL,
	CLUB_CARD_NBR NUMBER NULL,
	RETAIL_CUSTOMER_UUID VARCHAR NULL,
	ADOBE_VISITOR_ID VARCHAR NULL,
	EVENT_TYPE_CD VARCHAR NULL,
	EVENT_DT DATE NULL,
	SESSION_SEQUENCE_NBR NUMBER NULL,
	BANNER_NM VARCHAR NULL,
	Division_Id VARCHAR(10) NULL,
	DIVISION_NM VARCHAR NULL,
	OPERATING_SYSTEM_INTEGRATION_ID NUMBER NULL,
	FACILITY_NBR VARCHAR NULL,
	FACILITY_INTEGRATION_ID NUMBER NULL,
	POSTAL_ZONE_CD VARCHAR NULL,
	APP_VERSION_CD VARCHAR NULL,
	PAGE_URL_TXT VARCHAR NULL,
	DEVICE_NM VARCHAR NULL,
	DW_CREATE_TS TIMESTAMP NULL,
	DW_LAST_UPDATE_TS TIMESTAMP NULL,
	DW_LOGICAL_DELETE_IND BOOLEAN NULL,
	DW_SOURCE_CREATE_NM VARCHAR(255) NULL,
	DW_SOURCE_UPDATE_NM VARCHAR(255) NULL,
	DW_CURRENT_VERSION_IND BOOLEAN NULL
);

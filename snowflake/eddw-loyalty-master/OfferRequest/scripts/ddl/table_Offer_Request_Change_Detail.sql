--liquibase formatted sql
--changeset SYSTEM:Offer_Request_Change_Detail runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_PURCHASING;
create or replace TABLE OFFER_REQUEST_CHANGE_DETAIL (	
	OFFER_REQUEST_ID NUMBER(38,0) NOT NULL COMMENT 'Request ID from Offer Request',
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL COMMENT 'First inserted Date in table',
	DW_LAST_EFFECTIVE_DT DATE NOT NULL COMMENT 'Last SCD change Timestamp',
	CHANGE_TYPE_CD VARCHAR(50) COMMENT 'Codes for Request IDs',
	CHANGE_TYPE_DSC VARCHAR(500) COMMENT 'Description for Request IDs',
	CHANGE_TYPE_QTY NUMBER(38,0) COMMENT 'Type Quantity for Request IDs',
	CHANGE_CATEGORY_CD VARCHAR(50) COMMENT 'Category for Request IDs',
	CHANGE_CATEGORY_DSC VARCHAR(500) COMMENT 'Category description for Request IDs',
	CHANGE_CATEGORY_QTY NUMBER(38,0) COMMENT 'Category Quantity for Request IDs',
	REASON_TYPE_CD VARCHAR(50) COMMENT 'Reason Type Code for Offer Request IDs',
	REASON_TYPE_DSC VARCHAR(5000) COMMENT 'Reason Type Description for Offer Request IDs',
	REASON_COMMENT_TXT VARCHAR(500) COMMENT 'Comment Text for Offer IDs',
	CHANGE_BY_TYPE_USER_ID VARCHAR(255) COMMENT 'Changes done by User ID',
	CHANGE_BY_TYPE_FIRST_NM VARCHAR(20) COMMENT 'First name of the User',
	CHANGE_BY_TYPE_LAST_NM VARCHAR(20) COMMENT 'Last Name of the User',
	CHANGE_BY_TYPE_TS TIMESTAMP_LTZ(9) NOT NULL COMMENT 'Changes done at Timestamp',
	DW_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'File creation date',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'File late update timestamp',
	DW_LOGICAL_DELETE_IND BOOLEAN COMMENT 'Indicator for Delete',
	DW_SOURCE_CREATE_NM VARCHAR(255) COMMENT 'Source File name',
	DW_SOURCE_UPDATE_NM VARCHAR(255) COMMENT 'Source file name after SCD',
	DW_CURRENT_VERSION_IND BOOLEAN COMMENT 'True for Active Records',
	primary key (OFFER_REQUEST_ID, CHANGE_BY_TYPE_TS, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT)
);

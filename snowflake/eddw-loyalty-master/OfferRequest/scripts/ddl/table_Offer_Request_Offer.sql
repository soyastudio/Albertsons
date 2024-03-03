--liquibase formatted sql
--changeset SYSTEM:Offer_Request_Offer runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_PURCHASING;

create or replace TABLE OFFER_REQUEST_OFFER (
	OFFER_EXTERNAL_ID VARCHAR(20) NOT NULL,
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL,
	DW_LAST_EFFECTIVE_DT DATE NOT NULL,
	COPIENT_OFFER_ID NUMBER(38,0),
	OFFER_REQUEST_ID NUMBER(38,0) NOT NULL,
	OFFER_ID NUMBER(38,0) NOT NULL,
	USER_INTERFACE_UNIQUE_ID NUMBER(38,0) NOT NULL,
	PRODUCT_GROUP_VERSION_ID NUMBER(38,0),
	DISCOUNT_VERSION_ID NUMBER(38,0),
	DISCOUNT_ID NUMBER(38,0),
	ATTACHED_OFFER_STATUS_TYPE_CD VARCHAR(16777216),
	ATTACHED_OFFER_STATUS_DSC VARCHAR(50),
	ATTACHED_OFFER_STATUS_EFFECTIVE_TS TIMESTAMP_LTZ(9),
	APPLIED_PROGRAM_NM VARCHAR(100),
	PROGRAM_APPLIED_IND VARCHAR(5),
	DISTINCT_ID VARCHAR(16777216),
	DW_CREATE_TS TIMESTAMP_LTZ(9),
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9),
	DW_LOGICAL_DELETE_IND BOOLEAN,
	DW_SOURCE_CREATE_NM VARCHAR(255),
	DW_SOURCE_UPDATE_NM VARCHAR(255),
	DW_CURRENT_VERSION_IND BOOLEAN,
	INSTANT_WIN_VERSION_ID NUMBER(38,0),
	OFFER_RANK_NBR NUMBER(38,0),
	constraint XPKATTACHEDOFFER primary key (OFFER_EXTERNAL_ID, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT)
);
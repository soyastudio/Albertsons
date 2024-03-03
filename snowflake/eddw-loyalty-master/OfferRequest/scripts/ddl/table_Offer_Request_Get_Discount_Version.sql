--liquibase formatted sql
--changeset SYSTEM:Offer_Request_Get_Discount_Version runOnChange:true splitStatements:false OBJECT_TYPE:TABLE

use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_PURCHASING;

create or replace TABLE OFFER_REQUEST_GET_DISCOUNT_VERSION (
	OFFER_REQUEST_ID NUMBER(38,0) NOT NULL,
	USER_INTERFACE_UNIQUE_ID NUMBER(38,0) NOT NULL,
	DISCOUNT_VERSION_ID NUMBER(38,0) NOT NULL,
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL,
	DW_LAST_EFFECTIVE_DT DATE NOT NULL,
	PRODUCT_GROUP_ID NUMBER(38,0) NOT NULL,
	AIR_MILE_PROGRAM_ID NUMBER(38,0),
	AIR_MILE_PROGRAM_NM VARCHAR(200),
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9),
	DW_LOGICAL_DELETE_IND BOOLEAN,
	DW_SOURCE_CREATE_NM VARCHAR(255),
	DW_SOURCE_UPDATE_NM VARCHAR(255),
	DW_CURRENT_VERSION_IND BOOLEAN,
	DW_CREATE_TS TIMESTAMP_LTZ(9),
	primary key (OFFER_REQUEST_ID, USER_INTERFACE_UNIQUE_ID, DISCOUNT_VERSION_ID, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT, PRODUCT_GROUP_ID)
);

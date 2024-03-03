--liquibase formatted sql
--changeset SYSTEM:Offer_Request_Product_Group_Tier runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_PURCHASING;

create or replace TABLE OFFER_REQUEST_PRODUCT_GROUP_TIER (
	OFFER_REQUEST_ID NUMBER(38,0) NOT NULL,
	USER_INTERFACE_UNIQUE_ID NUMBER(38,0) NOT NULL,
	TIER_LEVEL_ID NUMBER(38,0) NOT NULL,
	PRODUCT_GROUP_ID NUMBER(38,0) NOT NULL,
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL,
	DW_LAST_EFFECTIVE_DT DATE NOT NULL,
	TIER_LEVEL_AMT NUMBER(14,4),
	DW_CREATE_TS TIMESTAMP_LTZ(9),
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9),
	DW_LOGICAL_DELETE_IND BOOLEAN,
	DW_SOURCE_CREATE_NM VARCHAR(255),
	DW_SOURCE_UPDATE_NM VARCHAR(255),
	DW_CURRENT_VERSION_IND BOOLEAN,
	primary key (OFFER_REQUEST_ID, USER_INTERFACE_UNIQUE_ID, TIER_LEVEL_ID, PRODUCT_GROUP_ID, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT)
);
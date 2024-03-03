--liquibase formatted sql
--changeset SYSTEM:Offer_Request_Discount_Tier runOnChange:true splitStatements:false OBJECT_TYPE:TABLE

use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_PURCHASING;

create or replace TABLE OFFER_REQUEST_DISCOUNT_TIER (
	OFFER_REQUEST_ID NUMBER(38,0) NOT NULL,
	USER_INTERFACE_UNIQUE_ID NUMBER(38,0) NOT NULL,
	DISCOUNT_VERSION_ID NUMBER(38,0) NOT NULL,
	DISCOUNT_ID NUMBER(38,0) NOT NULL,
	TIER_LEVEL_NBR NUMBER(38,0) NOT NULL,
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL,
	DW_LAST_EFFECTIVE_DT DATE NOT NULL,
	PRODUCT_GROUP_ID NUMBER(38,0) NOT NULL,
	DISCOUNT_AMT NUMBER(14,4),
	LIMIT_QTY NUMBER(38,0),
	LIMIT_WT NUMBER(14,4),
	LIMIT_VOL NUMBER(14,4),
	UNIT_OF_MEASURE_CD VARCHAR(10),
	UNIT_OF_MEASURE_NM VARCHAR(25),
	LIMIT_AMT NUMBER(14,4),
	REWARD_QTY NUMBER(38,0),
	RECEIPT_TXT VARCHAR(500),
	DISCOUNT_UP_TO_QTY NUMBER(38,0),
	DW_CREATE_TS TIMESTAMP_LTZ(9),
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9),
	DW_LOGICAL_DELETE_IND BOOLEAN,
	DW_SOURCE_CREATE_NM VARCHAR(255),
	DW_SOURCE_UPDATE_NM VARCHAR(255),
	DW_CURRENT_VERSION_IND BOOLEAN,
	primary key (OFFER_REQUEST_ID, USER_INTERFACE_UNIQUE_ID, DISCOUNT_VERSION_ID, DISCOUNT_ID, TIER_LEVEL_NBR, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT, PRODUCT_GROUP_ID)
);
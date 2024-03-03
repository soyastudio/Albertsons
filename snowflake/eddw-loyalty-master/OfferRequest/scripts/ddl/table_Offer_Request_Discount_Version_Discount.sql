--liquibase formatted sql
--changeset SYSTEM:Offer_Request_Discount_Version_Discount runOnChange:true splitStatements:false OBJECT_TYPE:TABLE

use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_PURCHASING;

create or replace TABLE OFFER_REQUEST_DISCOUNT_VERSION_DISCOUNT (
	OFFER_REQUEST_ID NUMBER(38,0) NOT NULL,
	USER_INTERFACE_UNIQUE_ID NUMBER(38,0) NOT NULL,
	DISCOUNT_VERSION_ID NUMBER(38,0) NOT NULL,
	DISCOUNT_ID NUMBER(38,0) NOT NULL,
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL,
	DW_LAST_EFFECTIVE_DT DATE NOT NULL,
	PRODUCT_GROUP_ID NUMBER(38,0) NOT NULL,
	DISCOUNT_TYPE_CD VARCHAR(50),
	DISCOUNT_TYPE_DSC VARCHAR(250),
	DISCOUNT_TYPE_SHORT_DSC VARCHAR(50),
	BENEFIT_VALUE_TYPE_CODE VARCHAR(50),
	BENEFIT_VALUE_TYPE_DSC VARCHAR(250),
	BENEFIT_VALUE_TYPE_SHORT_DSC VARCHAR(50),
	BENEFIT_VALUE_QTY NUMBER(14,4),
	INCLUDED_PRODUCT_GROUP_ID NUMBER(38,0),
	INCLUDED_PRODUCT_GROUP_NM VARCHAR(200),
	EXCLUDED_PRODUCT_GROUP_ID NUMBER(38,0),
	EXCLUDED_PRODUCT_GROUP_NM VARCHAR(200),
	CHARGEBACK_DEPARTMENT_ID VARCHAR(16777216),
	CHARGEBACK_DEPARTMENT_NM VARCHAR(50),
	DISPLAY_ORDER_NBR NUMBER(38,0),
	DW_CREATE_TS TIMESTAMP_LTZ(9),
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9),
	DW_LOGICAL_DELETE_IND BOOLEAN,
	DW_SOURCE_CREATE_NM VARCHAR(255),
	DW_SOURCE_UPDATE_NM VARCHAR(255),
	DW_CURRENT_VERSION_IND BOOLEAN,
	primary key (OFFER_REQUEST_ID, USER_INTERFACE_UNIQUE_ID, DISCOUNT_VERSION_ID, DISCOUNT_ID, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT, PRODUCT_GROUP_ID)
);
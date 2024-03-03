--liquibase formatted sql
--changeset SYSTEM:Partner_Grocery_Order_tender runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_LOYALTY;

create or replace TABLE PARTNER_GROCERY_ORDER_TENDER (
	ORDER_ID VARCHAR(50) NOT NULL COMMENT 'Partner created Order ID, Must be numeric (not alphanumeric).  This is Unique for each Grocery Order placed by the  Customers through Partner channels.',
	PARTNER_GROCERY_ORDER_CUSTOMER_INTEGRATION_ID NUMBER(38,0) NOT NULL COMMENT 'Partner_Grocery_Order_Customer_Integration_Id',
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL COMMENT 'The date that this division instance became effective.',
	DW_LAST_EFFECTIVE_DT DATE NOT NULL COMMENT 'The last date that this division instance was effective. Thid date for the current instance will be 9999/12/31.',
	APPROVAL_CD VARCHAR(10) COMMENT 'Credit card approval code (6 digit code)',
	DW_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'DW_CREATE_TS',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'DW_LAST_UPDATE_TS',
	DW_LOGICAL_DELETE_IND BOOLEAN COMMENT 'DW_LOGICAL_DELETE_IND',
	DW_SOURCE_CREATE_NM VARCHAR(255) COMMENT 'DW_SOURCE_CREATE_NM',
	DW_SOURCE_UPDATE_NM VARCHAR(255) COMMENT 'DW_SOURCE_UPDATE_NM',
	DW_CURRENT_VERSION_IND BOOLEAN COMMENT 'DW_CURRENT_VERSION_IND',
	MASKED_CREDIT_CARD_NBR VARCHAR(16777216) COMMENT 'Masked Credit Card Number in 123456XXXXXX1234 format',
	primary key (ORDER_ID, PARTNER_GROCERY_ORDER_CUSTOMER_INTEGRATION_ID, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT)
);


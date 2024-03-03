--liquibase formatted sql
--changeset SYSTEM:Partner_Grocery_Order_Customer runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_LOYALTY;

create or replace TABLE PARTNER_GROCERY_ORDER_CUSTOMER (
	PARTNER_GROCERY_ORDER_CUSTOMER_INTEGRATION_ID NUMBER(38,0) NOT NULL COMMENT 'Partner created id for user/Customer.  This is Unique ID of the Customer. This is Partner created id for the user, unique per each Customer.',
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL COMMENT 'DW_First_Effective_Dt',
	DW_LAST_EFFECTIVE_DT DATE NOT NULL COMMENT 'DW_Last_Effective_Dt',
	SOURCE_CUSTOMER_ID VARCHAR(150) COMMENT 'Source_Customer_Id',
	LOYALTY_PHONE_NBR VARCHAR(12) COMMENT 'Loyalty Phone Number of the Customer.  This Phone Number is used to tag the Loyalty information of the Customer.',
	FIRST_NM VARCHAR(50) COMMENT 'First Name of the Customer.',
	LAST_NM VARCHAR(50) COMMENT 'Last Name of the Customer.',
	EMAIL_ADDRESS_TXT VARCHAR(100) COMMENT 'Email Id (adddress) of the Customer.',
	CONTACT_PHONE_NBR VARCHAR(12) COMMENT 'Contact Phone Number (Full Phone Number) of the Customer.  Phone number provided by user (Customer), does not have to be the same number as the loyalty Phone Number. ',
	RETAIL_CUSTOMER_UUID VARCHAR(16777216) COMMENT 'Retail_Customer_UUID',
	DW_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'DW_CREATE_TS',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'DW_LAST_UPDATE_TS',
	DW_LOGICAL_DELETE_IND BOOLEAN COMMENT 'DW_LOGICAL_DELETE_IND',
	DW_SOURCE_CREATE_NM VARCHAR(255) COMMENT 'DW_SOURCE_CREATE_NM',
	DW_SOURCE_UPDATE_NM VARCHAR(255) COMMENT 'DW_SOURCE_UPDATE_NM',
	DW_CURRENT_VERSION_IND BOOLEAN COMMENT 'DW_CURRENT_VERSION_IND',
	primary key (PARTNER_GROCERY_ORDER_CUSTOMER_INTEGRATION_ID, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT)
);

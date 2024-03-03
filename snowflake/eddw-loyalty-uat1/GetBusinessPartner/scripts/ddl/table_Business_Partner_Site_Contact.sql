--liquibase formatted sql
--changeset SYSTEM:Business_Partner_Site_Contact runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_LOYALTY;

create or replace TABLE BUSINESS_PARTNER_SITE_CONTACT (
	BUSINESS_PARTNER_INTEGRATION_ID NUMBER(38,0) NOT NULL COMMENT 'Business_Partner_Integration_Id',
	CONTACT_TYPE_CD VARCHAR(50) NOT NULL COMMENT 'Contact_Type_Cd',
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL COMMENT 'DW_First_Effective_Dt',
	DW_LAST_EFFECTIVE_DT DATE NOT NULL COMMENT 'DW_Last_Effective_Dt',
	CONTACT_TYPE_DSC VARCHAR(250) COMMENT 'Contact_Type_Dsc',
	CONTACT_TYPE_SHORT_DSC VARCHAR(50) COMMENT 'Contact_Type_Short_Dsc',
	CONTACT_NM VARCHAR(16777216) COMMENT 'Contact_Nm',
	CONTACT_PHONE_NBR VARCHAR(16777216) COMMENT 'Contact_Phone_Nbr',
	EMAIL_ADDRESS_TXT VARCHAR(16777216) COMMENT 'Email_Address_txt',
	DW_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'DW_CREATE_TS',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'DW_LAST_UPDATE_TS',
	DW_LOGICAL_DELETE_IND BOOLEAN COMMENT 'DW_LOGICAL_DELETE_IND',
	DW_SOURCE_CREATE_NM VARCHAR(255) COMMENT 'DW_SOURCE_CREATE_NM',
	DW_SOURCE_UPDATE_NM VARCHAR(255) COMMENT 'DW_SOURCE_UPDATE_NM',
	DW_CURRENT_VERSION_IND BOOLEAN COMMENT 'DW_CURRENT_VERSION_IND',
	primary key (BUSINESS_PARTNER_INTEGRATION_ID, CONTACT_TYPE_CD, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT)
);
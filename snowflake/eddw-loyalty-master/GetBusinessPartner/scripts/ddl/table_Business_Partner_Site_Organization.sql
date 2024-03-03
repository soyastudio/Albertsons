--liquibase formatted sql
--changeset SYSTEM:Business_Partner_Site_Organization runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_LOYALTY;

create or replace TABLE BUSINESS_PARTNER_SITE_ORGANIZATION (
	BUSINESS_PARTNER_INTEGRATION_ID NUMBER(38,0) NOT NULL COMMENT 'Business_Partner_Integration_Id',
	PARTNER_SITE_ORGANIZATION_TYPE_CD VARCHAR(50) NOT NULL COMMENT 'Partner_Site_Organization_Type_Cd',
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL COMMENT 'DW_First_Effective_Dt',
	DW_LAST_EFFECTIVE_DT DATE NOT NULL COMMENT 'DW_Last_Effective_Dt',
	PARTNER_SITE_ORGANIZATION_VALUE_TXT VARCHAR(50) COMMENT 'Partner_Site_Organization_Value_Txt',
	DW_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'DW_CREATE_TS',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'DW_LAST_UPDATE_TS',
	DW_LOGICAL_DELETE_IND BOOLEAN COMMENT 'DW_LOGICAL_DELETE_IND',
	DW_SOURCE_CREATE_NM VARCHAR(255) COMMENT 'DW_SOURCE_CREATE_NM',
	DW_SOURCE_UPDATE_NM VARCHAR(255) COMMENT 'DW_SOURCE_UPDATE_NM',
	DW_CURRENT_VERSION_IND BOOLEAN COMMENT 'DW_CURRENT_VERSION_IND',
	primary key (BUSINESS_PARTNER_INTEGRATION_ID, PARTNER_SITE_ORGANIZATION_TYPE_CD, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT)
);

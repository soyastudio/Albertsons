--liquibase formatted sql
--changeset SYSTEM:Business_Partner runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_LOYALTY;

create or replace TABLE BUSINESS_PARTNER (
	BUSINESS_PARTNER_INTEGRATION_ID NUMBER(38,0) NOT NULL COMMENT 'Business_Partner_Integration_Id',
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL COMMENT 'DW_First_Effective_Dt',
	DW_LAST_EFFECTIVE_DT DATE NOT NULL COMMENT 'DW_Last_Effective_Dt',
	PARTNER_ID VARCHAR(50) COMMENT 'Partner_Id',
	PARTNER_SITE_ID VARCHAR(50) COMMENT 'Partner_Site_Id',
	PARTNER_PARTICIPANT_ID VARCHAR(50) COMMENT 'Partner_Participant_Id',
	INTERNAL_PARNTER_IND BOOLEAN COMMENT 'Internal_Parnter_Ind',
	VENDOR_ID VARCHAR(20) COMMENT 'Vendor_Id',
	WHOLESALE_CUSTOMER_NBR NUMBER(38,0) COMMENT 'Wholesale_Customer_Nbr',
	CUSTOMER_SITE_NBR VARCHAR(50) COMMENT 'Customer_Site_Nbr',
	PARTNER_SITE_NM VARCHAR(250) COMMENT 'Partner_Site_Nm',
	PARTNER_SITE_TYPE_CD VARCHAR(50) COMMENT 'Partner_Site_Type_Cd',
	PARTNER_SITE_TYPE_DSC VARCHAR(250) COMMENT 'Partner_Site_Type_Dsc',
	PARTNER_SITE_TYPE_SHORT_DSC VARCHAR(50) COMMENT 'Partner_Site_Type_Short_Dsc',
	PARTNER_SITE_ACTIVE_IND BOOLEAN COMMENT 'Partner_Site_Active_Ind',
	PARTNER_SITE_STATUS_TYPE_CD VARCHAR(16777216) COMMENT 'Partner_Site_Status_Type_Cd',
	PARTNER_SITE_STATUS_DSC VARCHAR(50) COMMENT 'Partner_Site_Status_Dsc',
	PARTNER_SITE_STATUS_EFFECTIVE_TS TIMESTAMP_LTZ(9) COMMENT 'Partner_Site_Status_Effective_Ts',
	PARTNER_SITE_ADDRESS_USAGE_TYPE_CD VARCHAR(20) COMMENT 'Partner_Site_Address_Usage_Type_Cd',
	PARTNER_SITE_ADDRESS_LINE1_TXT VARCHAR(100) COMMENT 'Partner_Site_Address_Line1_txt',
	PARTNER_SITE_ADDRESS_LINE2_TXT VARCHAR(100) COMMENT 'Partner_Site_Address_Line2_txt',
	PARTNER_SITE_ADDRESS_LINE3_TXT VARCHAR(100) COMMENT 'Partner_Site_Address_Line3_txt',
	PARTNER_SITE_ADDRESS_LINE4_TXT VARCHAR(100) COMMENT 'Partner_Site_Address_Line4_txt',
	PARTNER_SITE_ADDRESS_LINE5_TXT VARCHAR(100) COMMENT 'Partner_Site_Address_Line5_txt',
	PARTNER_SITE_CITY_NM VARCHAR(50) COMMENT 'Partner_Site_City_Nm',
	PARTNER_SITE_COUNTY_NM VARCHAR(50) COMMENT 'Partner_Site_County_Nm',
	PARTNER_SITE_COUNTY_CD VARCHAR(20) COMMENT 'Partner_Site_County_Cd',
	PARTNER_SITE_POSTAL_ZONE_CD VARCHAR(20) COMMENT 'Partner_Site_Postal_Zone_Cd',
	PARTNER_SITE_STATE_CD VARCHAR(20) COMMENT 'Partner_Site_State_Cd',
	PARTNER_SITE_STATE_NM VARCHAR(50) COMMENT 'Partner_Site_State_Nm',
	PARTNER_SITE_COUNTRY_CD VARCHAR(20) COMMENT 'Partner_Site_Country_Cd',
	PARTNER_SITE_COUNTRY_NM VARCHAR(50) COMMENT 'Partner_Site_Country_Nm',
	PARTNER_SITE_LATITUDE_DGR NUMBER(21,18) COMMENT 'Partner_Site_Latitude_Dgr',
	PARTNER_SITE_LONGITUDE_DGR NUMBER(21,18) COMMENT 'Partner_Site_Longitude_Dgr',
	PARTNER_SITE_TIMEZONE_CD VARCHAR(16777216) COMMENT 'Partner_Site_TimeZone_Cd',
	PARTNER_SITE_PHONE1_NBR VARCHAR(50) COMMENT 'Partner_Site_Phone1_Nbr',
	PARTNER_SITE_PHONE2_NBR VARCHAR(50) COMMENT 'Partner_Site_Phone2_Nbr',
	PARTNER_SITE_PHONE3_NBR VARCHAR(50) COMMENT 'Partner_Site_Phone3_Nbr',
	PARTNER_SITE_FAX_NBR VARCHAR(16777216) COMMENT 'Partner_Site_Fax_Nbr',
	PARTNER_SITE_COMMENT_TXT VARCHAR(2000) COMMENT 'Partner_Site_Comment_Txt',
	PARTNER_SITE_EFFECTIVE_TIME_PERIOD_TYPE_CD VARCHAR(16777216) COMMENT 'Partner_Site_Effective_Time_Period_Type_Cd',
	PARTNER_SITE_FIRST_EFFECTIVE_TS DATE COMMENT 'Partner_Site_First_Effective_Ts',
	PARTNER_SITE_LAST_EFFECTIVE_TS DATE COMMENT 'Partner_Site_Last_Effective_Ts',
	PARTNER_SITE_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'Partner_Site_Create_Ts',
	PARTNER_SITE_CREATE_USER_ID VARCHAR(50) COMMENT 'Partner_Site_Create_User_Id',
	PARTNER_SITE_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'Partner_Site_Update_Ts',
	PARTNER_SITE_UPDATE_USER_ID VARCHAR(50) COMMENT 'Partner_Site_Update_User_Id',
	PARTNER_EFFECTIVE_TIME_PERIOD_TYPE_CD VARCHAR(50) COMMENT 'Partner_Effective_Time_Period_Type_Cd',
	PARTNER_FIRST_EFFECTIVE_TS DATE COMMENT 'Partner_First_Effective_Ts',
	PARTNER_LAST_EFFECTIVE_TS DATE COMMENT 'Partner_Last_Effective_Ts',
	PARTNER_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'Partner_Create_Ts',
	PARTNER_CREATE_USER_ID VARCHAR(50) COMMENT 'Partner_Create_User_Id',
	PARTNER_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'Partner_Update_Ts',
	PARTNER_UPDATE_USER_ID VARCHAR(50) COMMENT 'Partner_Update_User_Id',
	DW_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'DW_CREATE_TS',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'DW_LAST_UPDATE_TS',
	DW_LOGICAL_DELETE_IND BOOLEAN COMMENT 'DW_LOGICAL_DELETE_IND',
	DW_SOURCE_CREATE_NM VARCHAR(255) COMMENT 'DW_SOURCE_CREATE_NM',
	DW_SOURCE_UPDATE_NM VARCHAR(255) COMMENT 'DW_SOURCE_UPDATE_NM',
	DW_CURRENT_VERSION_IND BOOLEAN COMMENT 'DW_CURRENT_VERSION_IND',
	primary key (BUSINESS_PARTNER_INTEGRATION_ID, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT),
	unique (PARTNER_ID, PARTNER_SITE_ID, PARTNER_PARTICIPANT_ID),
	unique (PARTNER_PARTICIPANT_ID, PARTNER_SITE_ID)
);
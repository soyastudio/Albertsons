--liquibase formatted sql
--changeset SYSTEM:Business_Partner runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view BUSINESS_PARTNER(
	BUSINESS_PARTNER_INTEGRATION_ID COMMENT 'Business_Partner_Integration_Id',
	DW_FIRST_EFFECTIVE_DT COMMENT 'DW First Effective Date',
	DW_LAST_EFFECTIVE_DT COMMENT 'DW Last Effective Date',
	PARTNER_ID COMMENT 'Partner_Id',
	INTERNAL_PARNTER_IND COMMENT 'Internal_Parnter_Ind',
	VENDOR_ID COMMENT 'Vendor_Id',
	WHOLESALE_CUSTOMER_NBR COMMENT 'Wholesale_Customer_Nbr',
	CUSTOMER_SITE_NBR COMMENT 'Customer_Site_Nbr',
	PARTNER_SITE_ID COMMENT 'Partner_Site_Id',
	PARTNER_PARTICIPANT_ID COMMENT 'Partner_Participant_Id',
	PARTNER_SITE_NM COMMENT 'Partner_Site_Nm',
	PARTNER_SITE_TYPE_CD COMMENT 'Partner_Site_Type_Cd',
	PARTNER_SITE_TYPE_DSC COMMENT 'Partner_Site_Type_Dsc',
	PARTNER_SITE_TYPE_SHORT_DSC COMMENT 'Partner_Site_Type_Short_Dsc',
	PARTNER_SITE_ACTIVE_IND COMMENT 'Partner_Site_Active_Ind',
	PARTNER_SITE_STATUS_TYPE_CD COMMENT 'Partner_Site_Status_Type_Cd',
	PARTNER_SITE_STATUS_DSC COMMENT 'Partner_Site_Status_Dsc',
	PARTNER_SITE_STATUS_EFFECTIVE_TS COMMENT 'Partner_Site_Status_Effective_Ts',
	PARTNER_SITE_ADDRESS_USAGE_TYPE_CD COMMENT 'Partner_Site_Address_Usage_Type_Cd',
	PARTNER_SITE_ADDRESS_LINE1_TXT COMMENT 'Partner_Site_Address_Line1_txt',
	PARTNER_SITE_ADDRESS_LINE2_TXT COMMENT 'Partner_Site_Address_Line2_txt',
	PARTNER_SITE_ADDRESS_LINE3_TXT COMMENT 'Partner_Site_Address_Line3_txt',
	PARTNER_SITE_ADDRESS_LINE4_TXT COMMENT 'Partner_Site_Address_Line4_txt',
	PARTNER_SITE_ADDRESS_LINE5_TXT COMMENT 'Partner_Site_Address_Line5_txt    ',
	PARTNER_SITE_CITY_NM COMMENT 'Partner_Site_City_Nm',
	PARTNER_SITE_COUNTY_NM COMMENT 'Partner_Site_County_Nm',
	PARTNER_SITE_COUNTY_CD COMMENT 'Partner_Site_County_Cd',
	PARTNER_SITE_POSTAL_ZONE_CD COMMENT 'Partner_Site_Postal_Zone_Cd',
	PARTNER_SITE_STATE_CD COMMENT 'Partner_Site_State_Cd',
	PARTNER_SITE_STATE_NM COMMENT 'Partner_Site_State_Nm',
	PARTNER_SITE_COUNTRY_CD COMMENT 'Partner_Site_Country_Cd',
	PARTNER_SITE_COUNTRY_NM COMMENT 'Partner_Site_Country_Nm',
	PARTNER_SITE_LATITUDE_DGR COMMENT 'Partner_Site_Latitude_Dgr',
	PARTNER_SITE_LONGITUDE_DGR COMMENT 'Partner_Site_Longitude_Dgr',
	PARTNER_SITE_TIMEZONE_CD COMMENT 'Partner_Site_TimeZone_Cd',
	PARTNER_SITE_PHONE1_NBR COMMENT 'Partner_Site_Phone1_Nbr',
	PARTNER_SITE_PHONE2_NBR COMMENT 'Partner_Site_Phone2_Nbr',
	PARTNER_SITE_PHONE3_NBR COMMENT 'Partner_Site_Phone3_Nbr',
	PARTNER_SITE_FAX_NBR COMMENT 'Partner_Site_Fax_Nbr',
	PARTNER_SITE_COMMENT_TXT COMMENT 'Partner_Site_Comment_Txt',
	PARTNER_SITE_EFFECTIVE_TIME_PERIOD_TYPE_CD COMMENT 'Partner_Site_Effective_Time_Period_Type_Cd',
	PARTNER_SITE_FIRST_EFFECTIVE_TS COMMENT 'Partner_Site_First_Effective_Ts',
	PARTNER_SITE_LAST_EFFECTIVE_TS COMMENT 'Partner_Site_Last_Effective_Ts',
	PARTNER_SITE_CREATE_TS COMMENT 'Partner_Site_Create_Ts',
	PARTNER_SITE_CREATE_USER_ID COMMENT 'Partner_Site_Create_User_Id',
	PARTNER_SITE_UPDATE_TS COMMENT 'Partner_Site_Update_Ts',
	PARTNER_SITE_UPDATE_USER_ID COMMENT 'Partner_Site_Update_User_Id',
	PARTNER_EFFECTIVE_TIME_PERIOD_TYPE_CD COMMENT 'Partner_Effective_Time_Period_Type_Cd',
	PARTNER_FIRST_EFFECTIVE_TS COMMENT 'Partner_First_Effective_Ts',
	PARTNER_LAST_EFFECTIVE_TS COMMENT 'Partner_Last_Effective_Ts',
	PARTNER_CREATE_TS COMMENT 'Partner_Create_Ts',
	PARTNER_CREATE_USER_ID COMMENT 'Partner_Create_User_Id',
	PARTNER_UPDATE_TS COMMENT 'Partner_Update_Ts',
	PARTNER_UPDATE_USER_ID COMMENT 'Partner_Update_User_Id ',
	DW_CREATE_TS COMMENT 'DW CREATE TS',
	DW_LAST_UPDATE_TS COMMENT 'DW LAST UPDATE TS',
	DW_LOGICAL_DELETE_IND COMMENT 'DW LOGICAL DELETE INDICATOR',
	DW_SOURCE_CREATE_NM COMMENT 'DW SOURCE CREATE NM',
	DW_SOURCE_UPDATE_NM COMMENT 'DW SOURCE UPDATE NM',
	DW_CURRENT_VERSION_IND COMMENT 'DW CURRENT VERSION INDICATOR'
) COMMENT='VIEW for Business_Partner'
 as
select
Business_Partner_Integration_Id,
 DW_First_Effective_Dt,
 DW_Last_Effective_Dt,
 Partner_Id,
Internal_Parnter_Ind    ,
Vendor_Id               ,
Wholesale_Customer_Nbr    ,
Customer_Site_Nbr       ,
Partner_Site_Id         ,
Partner_Participant_Id    ,
Partner_Site_Nm         ,
Partner_Site_Type_Cd    ,
Partner_Site_Type_Dsc    ,
Partner_Site_Type_Short_Dsc    ,
Partner_Site_Active_Ind    ,
Partner_Site_Status_Type_Cd    ,
Partner_Site_Status_Dsc    ,
Partner_Site_Status_Effective_Ts    ,
Partner_Site_Address_Usage_Type_Cd    ,
Partner_Site_Address_Line1_txt    ,
Partner_Site_Address_Line2_txt    ,
Partner_Site_Address_Line3_txt    ,
Partner_Site_Address_Line4_txt    ,
Partner_Site_Address_Line5_txt    ,
Partner_Site_City_Nm    ,
Partner_Site_County_Nm    ,
Partner_Site_County_Cd    ,
Partner_Site_Postal_Zone_Cd    ,
Partner_Site_State_Cd    ,
Partner_Site_State_Nm    ,
Partner_Site_Country_Cd    ,
Partner_Site_Country_Nm    ,
Partner_Site_Latitude_Dgr   ,
Partner_Site_Longitude_Dgr    ,
Partner_Site_TimeZone_Cd    ,
Partner_Site_Phone1_Nbr,
Partner_Site_Phone2_Nbr,
Partner_Site_Phone3_Nbr,
Partner_Site_Fax_Nbr    ,
Partner_Site_Comment_Txt    ,
Partner_Site_Effective_Time_Period_Type_Cd    ,
Partner_Site_First_Effective_Ts    ,
Partner_Site_Last_Effective_Ts    ,
Partner_Site_Create_Ts    ,
Partner_Site_Create_User_Id    ,
Partner_Site_Update_Ts    ,
Partner_Site_Update_User_Id    ,
Partner_Effective_Time_Period_Type_Cd    ,
Partner_First_Effective_Ts    ,
Partner_Last_Effective_Ts    ,
Partner_Create_Ts       ,
Partner_Create_User_Id    ,
Partner_Update_Ts       ,
Partner_Update_User_Id ,					 
DW_CREATE_TS    ,
DW_LAST_UPDATE_TS    ,
DW_LOGICAL_DELETE_IND    ,
DW_SOURCE_CREATE_NM    ,
DW_SOURCE_UPDATE_NM    ,
DW_CURRENT_VERSION_IND   
from <<EDM_DB_NAME>>.DW_C_LOYALTY.Business_Partner;

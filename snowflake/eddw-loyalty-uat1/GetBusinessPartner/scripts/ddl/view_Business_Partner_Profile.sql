--liquibase formatted sql
--changeset SYSTEM:Business_Partner_Profile runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view BUSINESS_PARTNER_PROFILE(
	BUSINESS_PARTNER_INTEGRATION_ID COMMENT 'Business Partner Integration Id',
	PARTNER_NM COMMENT 'Partner Nm',
	DW_FIRST_EFFECTIVE_DT COMMENT 'DW First Effective Date',
	DW_LAST_EFFECTIVE_DT COMMENT 'DW Last Effective Date',
	PARTNER_TYPE_CD COMMENT 'Partner_Type_Cd         ',
	PARTNER_TYPE_DSC COMMENT 'Partner_Type_Dsc        ',
	PARTNER_TYPE_SHORT_DSC COMMENT 'Partner_Type_Short_Dsc    ',
	PARTNER_ADDRESS_USAGE_TYPE_CD COMMENT 'Partner_Address_Usage_Type_Cd    ',
	PARTNER_ADDRESS_LINE1_TXT COMMENT 'Partner_Address_Line1_Txt    ',
	PARTNER_ADDRESS_LINE2_TXT COMMENT 'Partner_Address_Line2_Txt    ',
	PARTNER_ADDRESS_LINE3_TXT COMMENT 'Partner_Address_Line3_Txt    ',
	PARTNER_ADDRESS_LINE4_TXT COMMENT 'Partner_Address_Line4_Txt    ',
	PARTNER_ADDRESS_LINE5_TXT COMMENT 'Partner_Address_Line5_Txt    ',
	PARTNER_CONTACT_CITY_NM COMMENT 'Partner_Contact_City_Nm    ',
	PARTNER_CONTACT_COUNTY_NM COMMENT 'Partner_Contact_County_Nm    ',
	PARTNER_CONTACT_COUNTY_CD COMMENT 'Partner_Contact_County_Cd    ',
	PARTNER_CONTACT_POSTAL_ZONE_CD COMMENT 'Partner_Contact_Postal_Zone_Cd    ',
	PARTNER_CONTACT_STATE_CD COMMENT 'Partner_Contact_State_Cd    ',
	PARTNER_CONTACT_STATE_NM COMMENT 'Partner_Contact_State_Nm    ',
	PARTNER_CONTACT_COUNTRY_CD COMMENT 'Partner_Contact_Country_Cd    ',
	PARTNER_CONTACT_COUNTRY_NM COMMENT 'Partner_Contact_Country_Nm    ',
	PARTNER_CONTACT_LATITUDE_DGR COMMENT 'Partner_Contact_Latitude_Dgr    ',
	PARTNER_CONTACT_LONGITUDE_DGR COMMENT 'Partner_Contact_Longitude_Dgr    ',
	PARTNER_CONTACT_TIMEZONE_CD COMMENT 'Partner_Contact_TimeZone_Cd    ',
	PARTNER_CONTACT_PHONE1_NBR COMMENT 'Partner_Contact_Phone1_Nbr    ',
	PARTNER_CONTACT_PHONE2_NBR COMMENT 'Partner_Contact_Phone2_Nbr    ',
	PARTNER_CONTACT_PHONE3_NBR COMMENT 'Partner_Contact_Phone3_Nbr    ',
	PARTNER_CONTACT_FAX_NBR COMMENT 'Partner_Contact_Fax_Nbr    ',
	PARTNER_STATUS_TYPE_CD COMMENT 'Partner_Status_Type_Cd    ',
	PARTNER_STATUS_DSC COMMENT 'Partner_Status_Dsc      ',
	PARTNER_STATUS_EFFECTIVE_TS COMMENT 'Partner_Status_Effective_Ts    ',
	SERVICE_LEVEL_CD COMMENT 'Service_Level_Cd        ',
	SERVICE_LEVEL_DSC COMMENT 'Service_Level_Dsc       ',
	SERVICE_LEVEL_SHORT_DSC COMMENT 'Service_Level_Short_Dsc    ',
	SERVICE_LEVEL_ACTIVITY_CD COMMENT 'Service_Level_Activity_Cd    ',
	SERVICE_LEVEL_ACTIVITY_DSC COMMENT 'Service_Level_Activity_Dsc    ',
	SERVICE_LEVEL_ACTIVITY_SHORT_DSC COMMENT 'Service_Level_Activity_Short_Dsc    ',
	BUSINESS_CONTRACT_ID COMMENT 'Business_Contract_Id    ',
	BUSINESS_CONTRACT_NM COMMENT 'Business_Contract_Nm    ',
	BUSINESS_CONTRACT_DSC COMMENT 'Business_Contract_Dsc    ',
	BUSINESS_CONTRACT_START_DT COMMENT 'Business_Contract_Start_Dt  ',
	BUSINESS_CONTRACT_END_DT COMMENT 'Business_Contract_End_Dt  ',
	CONTRACT_BY_USER_ID COMMENT 'Contract_By_User_Id     ',
	CONTRACT_BY_FIRST_NM COMMENT 'Contract_By_First_Nm    ',
	CONTRACT_BY_LAST_NM COMMENT 'Contract_By_Last_Nm     ',
	REASON_CD COMMENT 'Reason_Cd               ',
	REASON_DSC COMMENT 'Reason_Dsc              ',
	REASON_SHORT_DSC COMMENT 'Reason_Short_Dsc        ',
	CONTRACT_BY_CREATE_TS COMMENT 'Contract_By_Create_Ts    ',
	CONTRACT_THRESHOLD_ORDER_LIMIT_CNT COMMENT 'Contract_Threshold_Order_Limit_Cnt    ',
	CONTRACT_THRESHOLD_MAXIMUM_ITEM_CNT COMMENT 'Contract_Threshold_Maximum_Item_Cnt    ',
	CONTRACT_THRESHOLD_MINIMUM_ITEM_CNT COMMENT 'Contract_Threshold_Minimum_Item_Cnt    ',
	CONTRACT_THRESHOLD_MINIMUM_TOTE_CNT COMMENT 'Contract_Threshold_Minimum_Tote_Cnt    ',
	CONTRACT_THRESHOLD_MAXIMUM_TOTE_CNT COMMENT 'Contract_Threshold_Maximum_Tote_Cnt    ',
	CONTRACT_THRESHOLD_ORDER_ALLOCATION_PCT COMMENT 'Contract_Threshold_Order_Allocation_Pct    ',
	CONTRACT_THRESHOLD_MILEAGE_NBR COMMENT 'Contract_Threshold_Mileage_Nbr    ',
	PARTNER_PROFILE_EFFECTIVE_TIME_PERIOD_TYPE_CD COMMENT 'Partner_Profile_Effective_Time_Period_Type_Cd    ',
	PARTNER_PROFILE_EFFECTIVE_TIME_PERIOD_FIRST_EFFECTIVE_TS COMMENT 'Partner_Profile_Effective_Time_Period_First_Effective_Ts    ',
	PARTNER_PROFILE_EFFECTIVE_TIME_PERIOD_LAST_EFFECTIVE_TS COMMENT 'Partner_Profile_Effective_Time_Period_Last_Effective_Ts    ',
	DW_CREATE_TS COMMENT 'DW CREATE TS',
	DW_LAST_UPDATE_TS COMMENT 'DW LAST UPDATE TS',
	DW_LOGICAL_DELETE_IND COMMENT 'DW LOGICAL DELETE INDICATOR',
	DW_SOURCE_CREATE_NM COMMENT 'DW SOURCE CREATE NM',
	DW_SOURCE_UPDATE_NM COMMENT 'DW SOURCE UPDATE NM',
	DW_CURRENT_VERSION_IND COMMENT 'DW CURRENT VERSION INDICATOR'
) COMMENT='VIEW for Business_Partner_Profile'
 as
select
Business_Partner_Integration_Id,
 Partner_Nm  ,
 DW_First_Effective_Dt,
 DW_Last_Effective_Dt,
 Partner_Type_Cd         ,
 Partner_Type_Dsc        ,
 Partner_Type_Short_Dsc    ,
 Partner_Address_Usage_Type_Cd    ,
 Partner_Address_Line1_Txt    ,
 Partner_Address_Line2_Txt    ,
 Partner_Address_Line3_Txt    ,
 Partner_Address_Line4_Txt    ,
 Partner_Address_Line5_Txt    ,
 Partner_Contact_City_Nm    ,
 Partner_Contact_County_Nm    ,
 Partner_Contact_County_Cd    ,
 Partner_Contact_Postal_Zone_Cd    ,
 Partner_Contact_State_Cd    ,
 Partner_Contact_State_Nm    ,
 Partner_Contact_Country_Cd    ,
 Partner_Contact_Country_Nm    ,
 Partner_Contact_Latitude_Dgr    ,
 Partner_Contact_Longitude_Dgr    ,
 Partner_Contact_TimeZone_Cd    ,
 Partner_Contact_Phone1_Nbr    ,
 Partner_Contact_Phone2_Nbr    ,
 Partner_Contact_Phone3_Nbr    ,
 Partner_Contact_Fax_Nbr    ,
 Partner_Status_Type_Cd    ,
 Partner_Status_Dsc      ,
 Partner_Status_Effective_Ts    ,
 Service_Level_Cd        ,
 Service_Level_Dsc       ,
 Service_Level_Short_Dsc    ,
 Service_Level_Activity_Cd    ,
 Service_Level_Activity_Dsc    ,
 Service_Level_Activity_Short_Dsc    ,
 Business_Contract_Id    ,
 Business_Contract_Nm    ,
 Business_Contract_Dsc    ,
 Business_Contract_Start_Dt  ,
 Business_Contract_End_Dt  ,
 Contract_By_User_Id     ,
 Contract_By_First_Nm    ,
 Contract_By_Last_Nm     ,
 Reason_Cd               ,
 Reason_Dsc              ,
 Reason_Short_Dsc        ,
 Contract_By_Create_Ts    ,
 Contract_Threshold_Order_Limit_Cnt    ,
 Contract_Threshold_Maximum_Item_Cnt    ,
 Contract_Threshold_Minimum_Item_Cnt    ,
 Contract_Threshold_Minimum_Tote_Cnt    ,
 Contract_Threshold_Maximum_Tote_Cnt    ,
 Contract_Threshold_Order_Allocation_Pct    ,
 Contract_Threshold_Mileage_Nbr    ,
 Partner_Profile_Effective_Time_Period_Type_Cd    ,
 Partner_Profile_Effective_Time_Period_First_Effective_Ts    ,
 Partner_Profile_Effective_Time_Period_Last_Effective_Ts    ,
DW_CREATE_TS    ,
DW_LAST_UPDATE_TS    ,
DW_LOGICAL_DELETE_IND    ,
DW_SOURCE_CREATE_NM    ,
DW_SOURCE_UPDATE_NM    ,
DW_CURRENT_VERSION_IND   
from <<EDM_DB_NAME>>.DW_C_LOYALTY.Business_Partner_Profile;

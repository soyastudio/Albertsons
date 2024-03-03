--liquibase formatted sql
--changeset SYSTEM:Business_Partner_Site_Contact runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view BUSINESS_PARTNER_SITE_CONTACT(
	BUSINESS_PARTNER_INTEGRATION_ID COMMENT 'Business_Partner_Integration_Id',
	CONTACT_TYPE_CD COMMENT 'Contact_Type_Cd',
	DW_FIRST_EFFECTIVE_DT COMMENT 'DW_First_Effective_Dt',
	DW_LAST_EFFECTIVE_DT COMMENT 'DW_Last_Effective_Dt',
	CONTACT_TYPE_DSC COMMENT 'Contact_Type_Dsc',
	CONTACT_TYPE_SHORT_DSC COMMENT 'Contact_Type_Short_Dsc',
	CONTACT_NM COMMENT 'Contact_Nm',
	CONTACT_PHONE_NBR COMMENT 'Contact_Phone_Nbr',
	EMAIL_ADDRESS_TXT COMMENT 'Email_Address_txt',
	DW_CREATE_TS COMMENT 'DW_CREATE_TS',
	DW_LAST_UPDATE_TS COMMENT 'DW_LAST_UPDATE_TS',
	DW_LOGICAL_DELETE_IND COMMENT 'DW_LOGICAL_DELETE_IND',
	DW_SOURCE_CREATE_NM COMMENT 'DW_SOURCE_CREATE_NM',
	DW_SOURCE_UPDATE_NM COMMENT 'DW_SOURCE_UPDATE_NM',
	DW_CURRENT_VERSION_IND COMMENT 'DW_CURRENT_VERSION_IND'
) COMMENT='VIEW for Business_Partner_Site_Contact'
 as
SELECT
	Business_Partner_Integration_Id	 
   ,Contact_Type_Cd       			
   ,DW_First_Effective_Dt 			
   ,DW_Last_Effective_Dt  			
   ,Contact_Type_Dsc      			
   ,Contact_Type_Short_Dsc  		
   ,Contact_Nm            			
   ,Contact_Phone_Nbr     			
   ,Email_Address_txt     			
   ,DW_CREATE_TS          			
   ,DW_LAST_UPDATE_TS     			
   ,DW_LOGICAL_DELETE_IND  		
   ,DW_SOURCE_CREATE_NM   			
   ,DW_SOURCE_UPDATE_NM   			
   ,DW_CURRENT_VERSION_IND  			 
FROM  <<EDM_DB_NAME>>.DW_C_Loyalty.Business_Partner_Site_Contact ;

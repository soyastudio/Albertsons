--liquibase formatted sql
--changeset SYSTEM:Business_Partner_Service_Override runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view BUSINESS_PARTNER_SERVICE_OVERRIDE(
	BUSINESS_PARTNER_INTEGRATION_ID COMMENT 'Business_Partner_Integration_Id',
	PARTNER_NM COMMENT 'Partner_Nm',
	OVERRIDE_TYPE_CD COMMENT 'Override_Type_Cd',
	DW_FIRST_EFFECTIVE_DT COMMENT 'DW_First_Effective_Dt',
	DW_LAST_EFFECTIVE_DT COMMENT 'DW_Last_Effective_Dt',
	OVERRIDE_IND COMMENT 'Override_Ind',
	OVERRIDE_TYPE_DSC COMMENT 'Override_Type_Dsc',
	OVERRIDE_TYPE_SHORT_DSC COMMENT 'Override_Type_Short_Dsc',
	OVERRIDE_REASON_TYPE_CD COMMENT 'Override_Reason_Type_Cd',
	OVERRIDE_REASON_TYPE_DSC COMMENT 'Override_Reason_Type_Dsc',
	OVERRIDE_REASON_TYPE_SHORT_DSC COMMENT 'Override_Reason_Type_Short_Dsc',
	DW_CREATE_TS COMMENT 'DW_CREATE_TS',
	DW_LAST_UPDATE_TS COMMENT 'DW_LAST_UPDATE_TS',
	DW_LOGICAL_DELETE_IND COMMENT 'DW_LOGICAL_DELETE_IND',
	DW_SOURCE_CREATE_NM COMMENT 'DW_SOURCE_CREATE_NM',
	DW_SOURCE_UPDATE_NM COMMENT 'DW_SOURCE_UPDATE_NM',
	DW_CURRENT_VERSION_IND COMMENT 'DW_CURRENT_VERSION_IND'
) COMMENT='VIEW for Business_Partner_Service_Override'
 as
SELECT
	Business_Partner_Integration_Id
   ,Partner_Nm            			
   ,Override_Type_Cd      			
   ,DW_First_Effective_Dt 			
   ,DW_Last_Effective_Dt  			
   ,Override_Ind          			
   ,Override_Type_Dsc     			
   ,Override_Type_Short_Dsc  		
   ,Override_Reason_Type_Cd  		
   ,Override_Reason_Type_Dsc 		
   ,Override_Reason_Type_Short_Dsc 
   ,DW_CREATE_TS          			
   ,DW_LAST_UPDATE_TS     			
   ,DW_LOGICAL_DELETE_IND 			
   ,DW_SOURCE_CREATE_NM   			
   ,DW_SOURCE_UPDATE_NM   			
   ,DW_CURRENT_VERSION_IND 
FROM  <<EDM_DB_NAME>>.DW_C_Loyalty.Business_Partner_Service_Override ;

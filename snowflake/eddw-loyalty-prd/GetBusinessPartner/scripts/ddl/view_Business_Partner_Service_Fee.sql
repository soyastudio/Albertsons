--liquibase formatted sql
--changeset SYSTEM:Business_Partner_Service_Fee runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view BUSINESS_PARTNER_SERVICE_FEE(
	BUSINESS_PARTNER_INTEGRATION_ID COMMENT 'Business_Partner_Integration_Id',
	PARTNER_NM COMMENT 'Partner_Nm',
	SERVICE_FEE_TYPE_CD COMMENT 'Service_Fee_Type_Cd',
	DW_FIRST_EFFECTIVE_DT COMMENT 'DW_First_Effective_Dt',
	DW_LAST_EFFECTIVE_DT COMMENT 'DW_Last_Effective_Dt',
	SERVICE_FEE_CATEGORY_CD COMMENT 'Service_Fee_Category_Cd',
	SERVICE_FEE_AMT COMMENT 'Service_Fee_Amt',
	SERVICE_FEE_ITEM_ID COMMENT 'Service_Fee_Item_Id',
	DW_CREATE_TS COMMENT 'DW_CREATE_TS',
	DW_LAST_UPDATE_TS COMMENT 'DW_LAST_UPDATE_TS',
	DW_LOGICAL_DELETE_IND COMMENT 'DW_LOGICAL_DELETE_IND',
	DW_SOURCE_CREATE_NM COMMENT 'DW_SOURCE_CREATE_NM',
	DW_SOURCE_UPDATE_NM COMMENT 'DW_SOURCE_UPDATE_NM',
	DW_CURRENT_VERSION_IND COMMENT 'DW_CURRENT_VERSION_IND'
) COMMENT='VIEW for Business_Partner_Service_Fee'
 as
SELECT
	Business_Partner_Integration_Id
   ,Partner_Nm            			
   ,Service_Fee_Type_Cd   			
   ,DW_First_Effective_Dt 			
   ,DW_Last_Effective_Dt  			
   ,Service_Fee_Category_Cd  		
   ,Service_Fee_Amt       			
   ,Service_Fee_Item_Id   			
   ,DW_CREATE_TS          			
   ,DW_LAST_UPDATE_TS     			
   ,DW_LOGICAL_DELETE_IND 			
   ,DW_SOURCE_CREATE_NM   			
   ,DW_SOURCE_UPDATE_NM   			
   ,DW_CURRENT_VERSION_IND  		
FROM  <<EDM_DB_NAME>>.DW_C_Loyalty.Business_Partner_Service_Fee ;

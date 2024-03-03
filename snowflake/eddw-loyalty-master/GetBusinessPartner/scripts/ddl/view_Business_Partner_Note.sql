--liquibase formatted sql
--changeset SYSTEM:Business_Partner_Note runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view BUSINESS_PARTNER_NOTE(
	BUSINESS_PARTNER_INTEGRATION_ID COMMENT 'Business_Partner_Integration_Id',
	PARTNER_NM COMMENT 'Partner_Nm',
	NOTES_TYPE_CD COMMENT 'Notes_Type_Cd',
	DW_FIRST_EFFECTIVE_DT COMMENT 'DW_First_Effective_Dt',
	DW_LAST_EFFECTIVE_DT COMMENT 'DW_Last_Effective_Dt',
	NOTES_TYPE_TXT COMMENT 'Notes_Type_Txt',
	DW_CREATE_TS COMMENT 'DW_CREATE_TS',
	DW_LAST_UPDATE_TS COMMENT 'DW_LAST_UPDATE_TS',
	DW_LOGICAL_DELETE_IND COMMENT 'DW_LOGICAL_DELETE_IND',
	DW_SOURCE_CREATE_NM COMMENT 'DW_SOURCE_CREATE_NM',
	DW_SOURCE_UPDATE_NM COMMENT 'DW_SOURCE_UPDATE_NM',
	DW_CURRENT_VERSION_IND COMMENT 'DW_CURRENT_VERSION_IND'
) COMMENT='VIEW for Business_Partner_Note'
 as
SELECT
	Business_Partner_Integration_Id
   ,Partner_Nm            			
   ,Notes_Type_Cd         			
   ,DW_First_Effective_Dt 			
   ,DW_Last_Effective_Dt  			
   ,Notes_Type_Txt        			
   ,DW_CREATE_TS          			
   ,DW_LAST_UPDATE_TS     			
   ,DW_LOGICAL_DELETE_IND 			
   ,DW_SOURCE_CREATE_NM   			
   ,DW_SOURCE_UPDATE_NM   			
   ,DW_CURRENT_VERSION_IND  		
FROM  <<EDM_DB_NAME>>.DW_C_Loyalty.Business_Partner_Note ;

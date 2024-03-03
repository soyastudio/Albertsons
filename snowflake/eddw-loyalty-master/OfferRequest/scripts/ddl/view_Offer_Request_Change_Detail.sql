--liquibase formatted sql
--changeset SYSTEM:Offer_Request_Change_Detail runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database EDM_VIEWS_PRD;
use schema EDM_VIEWS_PRD.DW_VIEWS;

create or replace view OFFER_REQUEST_CHANGE_DETAIL(
	OFFER_REQUEST_ID COMMENT 'Request ID from Offer Request',
	DW_FIRST_EFFECTIVE_DT COMMENT 'First inserted Date in table',
	DW_LAST_EFFECTIVE_DT COMMENT 'Last SCD change Timestamp',
	CHANGE_TYPE_CD COMMENT 'Codes for Request IDs',
	CHANGE_TYPE_DSC COMMENT 'Description for Request IDs',
	CHANGE_TYPE_QTY COMMENT 'Type Quantity for Request IDs',
	CHANGE_CATEGORY_CD COMMENT 'Category for Request IDs',
	CHANGE_CATEGORY_DSC COMMENT 'Category description for Request IDs',
	CHANGE_CATEGORY_QTY COMMENT 'Category Quantity for Request IDs',
	REASON_TYPE_CD COMMENT 'Reason Type Code for Offer Request IDs',
	REASON_TYPE_DSC COMMENT 'Reason Type Description for Offer Request IDs',
	REASON_COMMENT_TXT COMMENT 'Comment Text for Offer IDs',
	CHANGE_BY_TYPE_USER_ID COMMENT 'Changes done by User ID',
	CHANGE_BY_TYPE_FIRST_NM COMMENT 'First name of the User',
	CHANGE_BY_TYPE_LAST_NM COMMENT 'Last Name of the User',
	CHANGE_BY_TYPE_TS COMMENT 'Changes done at Timestamp',
	DW_CREATE_TS COMMENT 'File creation date',
	DW_LAST_UPDATE_TS COMMENT 'File late update timestamp',
	DW_LOGICAL_DELETE_IND COMMENT 'Indicator for Delete',
	DW_SOURCE_CREATE_NM COMMENT 'Source File name',
	DW_SOURCE_UPDATE_NM COMMENT 'Source file name after SCD',
	DW_CURRENT_VERSION_IND COMMENT 'True for Active Records'
) COMMENT='VIEW for Offer_Request_Change_Detail'
 as
SELECT
	  Offer_Request_Id      	
     ,DW_First_Effective_Dt 	
     ,DW_Last_Effective_Dt  	
     ,Change_Type_Cd        	
     ,Change_Type_Dsc       	
     ,Change_Type_Qty       	
     ,Change_Category_Cd    	
     ,Change_Category_Dsc   	
     ,Change_Category_Qty   	
     ,Reason_Type_Cd        	
     ,Reason_Type_Dsc       	
     ,Reason_Comment_Txt    	
     ,Change_By_Type_User_Id 
     ,Change_By_Type_First_Nm
     ,Change_By_Type_Last_Nm 
     ,Change_By_Type_Ts     	
     ,DW_CREATE_TS          	
     ,DW_LAST_UPDATE_TS     	
     ,DW_LOGICAL_DELETE_IND  
     ,DW_SOURCE_CREATE_NM   	
     ,DW_SOURCE_UPDATE_NM   	
     ,DW_CURRENT_VERSION_IND 
FROM  EDM_CONFIRMED_PRD.DW_C_PURCHASING.Offer_Request_Change_Detail;
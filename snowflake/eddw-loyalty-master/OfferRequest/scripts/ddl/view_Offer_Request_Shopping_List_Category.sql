--liquibase formatted sql
--changeset SYSTEM:Offer_Request_Shopping_List_Category runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database EDM_VIEWS_PRD;
use schema EDM_VIEWS_PRD.DW_VIEWS;

create or replace view OFFER_REQUEST_SHOPPING_LIST_CATEGORY(
	OFFER_REQUEST_ID COMMENT 'Offer_Request_Id',
	USER_INTERFACE_UNIQUE_ID COMMENT 'User_Interface_Unique_Id',
	SHOPPING_LIST_CATEGORY_CD COMMENT 'Shopping_List_Category_Cd',
	DW_FIRST_EFFECTIVE_DT COMMENT 'DW_First_Effective_Dt',
	DW_LAST_EFFECTIVE_DT COMMENT 'DW_Last_Effective_Dt',
	SHOPPING_LIST_CATEGORY_DSC COMMENT 'Shopping_List_Category_Dsc',
	SHOPPING_LIST_CATEGORY_SHORT_DSC COMMENT 'Shopping_List_Category_Short_Dsc',
	DW_CREATE_TS COMMENT 'DW_CREATE_TS',
	DW_LAST_UPDATE_TS COMMENT 'DW_LAST_UPDATE_TS',
	DW_LOGICAL_DELETE_IND COMMENT 'DW_LOGICAL_DELETE_IND',
	DW_SOURCE_CREATE_NM COMMENT 'DW_SOURCE_CREATE_NM',
	DW_SOURCE_UPDATE_NM COMMENT 'DW_SOURCE_UPDATE_NM',
	DW_CURRENT_VERSION_IND COMMENT 'DW_CURRENT_VERSION_IND'
) COMMENT='View For Offer_Request_Shopping_List_Category'
 as
SELECT 
  Offer_Request_Id ,
  User_Interface_Unique_Id ,  
  Shopping_List_Category_Cd ,
  DW_First_Effective_Dt ,
  DW_Last_Effective_Dt ,
  Shopping_List_Category_Dsc ,
  Shopping_List_Category_Short_Dsc ,
  DW_CREATE_TS ,
  DW_LAST_UPDATE_TS ,
  DW_LOGICAL_DELETE_IND ,
  DW_SOURCE_CREATE_NM , 
  DW_SOURCE_UPDATE_NM ,
  DW_CURRENT_VERSION_IND   
FROM EDM_CONFIRMED_PRD.DW_C_PURCHASING.Offer_Request_Shopping_List_Category;
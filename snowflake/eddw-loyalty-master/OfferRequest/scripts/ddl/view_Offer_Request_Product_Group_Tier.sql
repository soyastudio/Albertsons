--liquibase formatted sql
--changeset SYSTEM:Offer_Request_Product_Group_Tier runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database EDM_VIEWS_PRD;
use schema EDM_VIEWS_PRD.DW_VIEWS;

create or replace view OFFER_REQUEST_PRODUCT_GROUP_TIER(
	OFFER_REQUEST_ID COMMENT 'Offer_Request_Id',
	USER_INTERFACE_UNIQUE_ID COMMENT 'User_Interface_Unique_Id',
	TIER_LEVEL_ID COMMENT 'Tier_Level_Id',
	PRODUCT_GROUP_ID COMMENT 'Product_Group_Id',
	DW_FIRST_EFFECTIVE_DT COMMENT 'DW_First_Effective_Dt',
	DW_LAST_EFFECTIVE_DT COMMENT 'DW_Last_Effective_Dt',
	TIER_LEVEL_AMT COMMENT 'Tier_Level_Amt',
	DW_CREATE_TS COMMENT 'DW_CREATE_TS',
	DW_LAST_UPDATE_TS COMMENT 'DW_LAST_UPDATE_TS',
	DW_LOGICAL_DELETE_IND COMMENT 'DW_LOGICAL_DELETE_IND',
	DW_SOURCE_CREATE_NM COMMENT 'DW_SOURCE_CREATE_NM',
	DW_SOURCE_UPDATE_NM COMMENT 'DW_SOURCE_UPDATE_NM',
	DW_CURRENT_VERSION_IND COMMENT 'DW_CURRENT_VERSION_IND'
) COMMENT='View For Offer_Request_Product_Group_Tier'
 as
SELECT 
  Offer_Request_Id ,
  User_Interface_Unique_Id ,
  Tier_Level_Id ,
  Product_Group_Id ,
  DW_First_Effective_Dt ,
  DW_Last_Effective_Dt ,
  Tier_Level_Amt ,	
  DW_CREATE_TS ,
  DW_LAST_UPDATE_TS ,
  DW_LOGICAL_DELETE_IND ,
  DW_SOURCE_CREATE_NM ,
  DW_SOURCE_UPDATE_NM ,
  DW_CURRENT_VERSION_IND   
FROM EDM_CONFIRMED_PRD.DW_C_PURCHASING.Offer_Request_Product_Group_Tier;
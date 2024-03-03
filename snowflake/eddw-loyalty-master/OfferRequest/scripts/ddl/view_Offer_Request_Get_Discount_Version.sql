--liquibase formatted sql
--changeset SYSTEM:Offer_Request_Get_Discount_Version runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database EDM_VIEWS_PRD;
use schema EDM_VIEWS_PRD.DW_VIEWS;

create or replace view OFFER_REQUEST_GET_DISCOUNT_VERSION(
	OFFER_REQUEST_ID COMMENT 'Offer_Request_Id',
	USER_INTERFACE_UNIQUE_ID COMMENT 'User_Interface_Unique_Id',
	DISCOUNT_VERSION_ID COMMENT 'Discount_Version_Id',
	DW_FIRST_EFFECTIVE_DT COMMENT 'DW_First_Effective_Dt',
	DW_LAST_EFFECTIVE_DT COMMENT 'DW_Last_Effective_Dt',
	PRODUCT_GROUP_ID COMMENT 'Product_Group_Id',
	AIR_MILE_PROGRAM_ID COMMENT 'Air_Mile_Program_Id',
	AIR_MILE_PROGRAM_NM COMMENT 'Air_Mile_Program_Nm',
	DW_LAST_UPDATE_TS COMMENT 'DW_LAST_UPDATE_TS ',
	DW_LOGICAL_DELETE_IND COMMENT 'DW_LOGICAL_DELETE_IND',
	DW_SOURCE_CREATE_NM COMMENT 'DW_SOURCE_CREATE_NM',
	DW_SOURCE_UPDATE_NM COMMENT 'DW_SOURCE_UPDATE_NM',
	DW_CURRENT_VERSION_IND COMMENT 'DW_CURRENT_VERSION_IND',
	DW_CREATE_TS COMMENT 'DW_CREATE_TS'
) COMMENT='View For Offer_Request_Get_Discount_Version'
 as 
Select
Offer_Request_Id   ,
User_Interface_Unique_Id  ,
Discount_Version_Id   ,
DW_First_Effective_Dt  ,
DW_Last_Effective_Dt  ,
Product_Group_Id      ,
Air_Mile_Program_Id   ,
Air_Mile_Program_Nm   ,
DW_LAST_UPDATE_TS     ,
DW_LOGICAL_DELETE_IND ,
DW_SOURCE_CREATE_NM   ,
DW_SOURCE_UPDATE_NM   ,
DW_CURRENT_VERSION_IND ,
DW_CREATE_TS       
From EDM_CONFIRMED_PRD.DW_C_PURCHASING.Offer_Request_Get_Discount_Version;
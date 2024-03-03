--liquibase formatted sql
--changeset SYSTEM:Offer_Request_Store_Group runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database EDM_VIEWS_PRD;
use schema EDM_VIEWS_PRD.DW_VIEWS;

create or replace view OFFER_REQUEST_STORE_GROUP(
	OFFER_REQUEST_ID COMMENT 'Offer_Request_Id ',
	USER_INTERFACE_UNIQUE_ID COMMENT 'User_Interface_Unique_Id',
	STORE_GROUP_ID COMMENT 'Store_Group_Id ',
	STORE_GROUP_TYPE_CD COMMENT 'Store_Group_Type_Cd',
	DW_FIRST_EFFECTIVE_DT COMMENT 'DW_First_Effective_Dt',
	DW_LAST_EFFECTIVE_DT COMMENT 'DW_Last_Effective_Dt',
	STORE_GROUP_TYPE_DSC COMMENT 'Store_Group_Type_Dsc',
	STORE_GROUP_TYPE_SHORT_DSC COMMENT 'Store_Group_Type_Short_Dsc',
	DW_CREATE_TS COMMENT 'DW_CREATE_TS',
	DW_LAST_UPDATE_TS COMMENT 'DW_LAST_UPDATE_TS',
	DW_LOGICAL_DELETE_IND COMMENT 'DW_LOGICAL_DELETE_IND',
	DW_SOURCE_CREATE_NM COMMENT 'DW_SOURCE_CREATE_NM',
	DW_SOURCE_UPDATE_NM COMMENT 'DW_SOURCE_UPDATE_NM',
	DW_CURRENT_VERSION_IND COMMENT 'DW_CURRENT_VERSION_IND'
) COMMENT='View For Offer_Request_Store_Group'
 as 
Select
Offer_Request_Id   ,
User_Interface_Unique_Id  ,
Store_Group_Id  ,
Store_Group_Type_Cd      ,
DW_First_Effective_Dt  ,
DW_Last_Effective_Dt  ,
Store_Group_Type_Dsc      ,
Store_Group_Type_Short_Dsc    ,
DW_CREATE_TS          ,
DW_LAST_UPDATE_TS     ,
DW_LOGICAL_DELETE_IND ,
DW_SOURCE_CREATE_NM   ,
DW_SOURCE_UPDATE_NM   ,
DW_CURRENT_VERSION_IND 
From EDM_CONFIRMED_PRD.DW_C_PURCHASING.Offer_Request_Store_Group;
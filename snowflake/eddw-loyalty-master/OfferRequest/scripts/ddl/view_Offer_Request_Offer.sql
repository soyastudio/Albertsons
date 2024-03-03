--liquibase formatted sql
--changeset SYSTEM:Offer_Request_Offer runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database EDM_VIEWS_PRD;
use schema EDM_VIEWS_PRD.DW_VIEWS;

create or replace view OFFER_REQUEST_OFFER(
	OFFER_EXTERNAL_ID COMMENT 'Offer_External_Id',
	DW_FIRST_EFFECTIVE_DT COMMENT 'DW_First_Effective_Dt',
	DW_LAST_EFFECTIVE_DT COMMENT 'DW_Last_Effective_Dt',
	COPIENT_OFFER_ID COMMENT 'Copient_Offer_Id',
	OFFER_REQUEST_ID COMMENT 'Offer_Request_Id',
	OFFER_ID COMMENT 'Offer_Id',
	USER_INTERFACE_UNIQUE_ID COMMENT 'User_Interface_Unique_Id',
	PRODUCT_GROUP_VERSION_ID COMMENT 'Product_Group_Version_Id',
	DISCOUNT_VERSION_ID COMMENT 'Discount_Version_Id',
	INSTANT_WIN_VERSION_ID COMMENT 'Instant_Win_Version_Id',
	DISCOUNT_ID COMMENT 'Discount_Id',
	ATTACHED_OFFER_STATUS_TYPE_CD COMMENT 'Attached_Offer_Status_Type_Cd',
	ATTACHED_OFFER_STATUS_DSC COMMENT 'Attached_Offer_Status_Dsc',
	ATTACHED_OFFER_STATUS_EFFECTIVE_TS COMMENT 'Attached_Offer_Status_Effective_Ts',
	APPLIED_PROGRAM_NM COMMENT 'Applied_Program_Nm',
	PROGRAM_APPLIED_IND COMMENT 'Program_Applied_Ind',
	DISTINCT_ID COMMENT 'Distinct_Id',
	OFFER_RANK_NBR COMMENT 'Offer_Rank_Nbr',
	DW_CREATE_TS COMMENT 'DW_CREATE_TS',
	DW_LAST_UPDATE_TS COMMENT 'DW_LAST_UPDATE_TS',
	DW_LOGICAL_DELETE_IND COMMENT 'DW_LOGICAL_DELETE_IND',
	DW_SOURCE_CREATE_NM COMMENT 'DW_SOURCE_CREATE_NM',
	DW_SOURCE_UPDATE_NM COMMENT 'DW_SOURCE_UPDATE_NM',
	DW_CURRENT_VERSION_IND COMMENT 'DW_CURRENT_VERSION_IND'
) COMMENT='View For Offer_Request_Offer'
 as
Select
Offer_External_Id     ,
DW_First_Effective_Dt  ,
DW_Last_Effective_Dt  ,
Copient_Offer_Id      ,
Offer_Request_Id      ,
Offer_Id              ,
User_Interface_Unique_Id  ,
Product_Group_Version_Id  ,
Discount_Version_Id   ,
Instant_Win_Version_Id  ,
Discount_Id           ,
Attached_Offer_Status_Type_Cd  ,
Attached_Offer_Status_Dsc  ,
Attached_Offer_Status_Effective_Ts  ,
Applied_Program_Nm    ,
Program_Applied_Ind   ,
Distinct_Id           ,
Offer_Rank_Nbr        ,
DW_CREATE_TS          ,
DW_LAST_UPDATE_TS     ,
DW_LOGICAL_DELETE_IND  ,
DW_SOURCE_CREATE_NM   ,
DW_SOURCE_UPDATE_NM   ,
DW_CURRENT_VERSION_IND  
From EDM_CONFIRMED_PRD.DW_C_PURCHASING.Offer_Request_Offer;
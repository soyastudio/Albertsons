--liquibase formatted sql
--changeset SYSTEM:Offer_Request_Discount_Tier runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database EDM_VIEWS_PRD;
use schema EDM_VIEWS_PRD.DW_VIEWS;

create or replace view OFFER_REQUEST_DISCOUNT_TIER(
	OFFER_REQUEST_ID COMMENT 'Offer_Request_Id',
	USER_INTERFACE_UNIQUE_ID COMMENT 'User_Interface_Unique_Id',
	DISCOUNT_VERSION_ID COMMENT 'Discount_Version_Id',
	DISCOUNT_ID COMMENT 'Discount_Id',
	TIER_LEVEL_NBR COMMENT 'Tier_Level_Nbr',
	DW_FIRST_EFFECTIVE_DT COMMENT 'DW_First_Effective_Dt',
	DW_LAST_EFFECTIVE_DT COMMENT 'DW_Last_Effective_Dt',
	PRODUCT_GROUP_ID COMMENT 'Product_Group_Id',
	DISCOUNT_AMT COMMENT 'Discount_Amt',
	LIMIT_QTY COMMENT 'Limit_Qty',
	LIMIT_WT COMMENT 'Limit_Wt ',
	LIMIT_VOL COMMENT 'Limit_Vol',
	UNIT_OF_MEASURE_CD COMMENT 'Unit_Of_Measure_Cd',
	UNIT_OF_MEASURE_NM COMMENT 'Unit_Of_Measure_Nm',
	LIMIT_AMT COMMENT 'Limit_Amt ',
	REWARD_QTY COMMENT 'Reward_Qty',
	RECEIPT_TXT COMMENT 'Receipt_Txt',
	DISCOUNT_UP_TO_QTY COMMENT 'Discount_Up_to_Qty',
	DW_CREATE_TS COMMENT 'DW_CREATE_TS',
	DW_LAST_UPDATE_TS COMMENT 'DW_LAST_UPDATE_TS',
	DW_LOGICAL_DELETE_IND COMMENT 'DW_LOGICAL_DELETE_IND',
	DW_SOURCE_CREATE_NM COMMENT 'DW_SOURCE_CREATE_NM',
	DW_SOURCE_UPDATE_NM COMMENT 'DW_SOURCE_UPDATE_NM',
	DW_CURRENT_VERSION_IND COMMENT 'DW_CURRENT_VERSION_IND'
) COMMENT='View For Offer_Request_Discount_Tier'
 as 
Select
Offer_Request_Id      ,
User_Interface_Unique_Id  ,
Discount_Version_Id   ,
Discount_Id           ,
Tier_Level_Nbr        ,
DW_First_Effective_Dt  ,
DW_Last_Effective_Dt  ,
Product_Group_Id      ,
Discount_Amt          ,
Limit_Qty             ,
Limit_Wt              ,
Limit_Vol             ,
Unit_Of_Measure_Cd    ,
Unit_Of_Measure_Nm    ,
Limit_Amt             ,
Reward_Qty            ,
Receipt_Txt           ,
Discount_Up_to_Qty    ,
DW_CREATE_TS          ,
DW_LAST_UPDATE_TS     ,
DW_LOGICAL_DELETE_IND ,
DW_SOURCE_CREATE_NM   ,
DW_SOURCE_UPDATE_NM   ,
DW_CURRENT_VERSION_IND 

From EDM_CONFIRMED_PRD.DW_C_PURCHASING.Offer_Request_Discount_Tier;
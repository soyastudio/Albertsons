--liquibase formatted sql
--changeset SYSTEM:OMS_Offer_Benefit_Discount_Tier runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view OMS_OFFER_BENEFIT_DISCOUNT_TIER(
	OMS_OFFER_ID COMMENT 'OMS Offer',
	DISCOUNT_ID COMMENT 'Discount ID',
	DISCOUNT_TIER_ID COMMENT 'Tier ID',
	DW_FIRST_EFFECTIVE_DT COMMENT 'First effective Date',
	DW_LAST_EFFECTIVE_DT COMMENT 'Last effective Date',
	DISCOUNT_TIER_LEVEL_NBR COMMENT 'Discount tier nbr',
	DISCOUNT_TIER_AMT COMMENT 'Discount amt',
	DISCOUNT_TIER_UP_TO_NBR COMMENT 'Discount tier nbr',
	ITEM_LIMIT_QTY COMMENT 'Item limit',
	WEIGHT_LIMIT_QTY COMMENT 'Weight Limit',
	DOLLAR_LIMIT_AMT COMMENT 'Dollar limit',
	RECEIPT_TXT COMMENT 'Receipt txt',
	DW_CREATE_TS COMMENT 'DW Current Timestamp',
	DW_LAST_UPDATE_TS COMMENT 'DW Last update Timestamp',
	DW_SOURCE_CREATE_NM COMMENT 'DW source Create Name',
	DW_LOGICAL_DELETE_IND COMMENT 'DW logical delete Ind',
	DW_CURRENT_VERSION_IND COMMENT 'DW Current version Indicator',
	DW_SOURCE_UPDATE_NM COMMENT 'DW Source update Name'
) COMMENT='VIEW for OMS_Offer_Benefit_Discount_Tier'
 as 
SELECT
 OMS_Offer_Id           ,
 Discount_Id            ,
 Discount_Tier_Id       ,
 DW_First_Effective_Dt  ,
 DW_Last_Effective_Dt  ,
 Discount_Tier_Level_Nbr    ,
 Discount_Tier_Amt      ,
 Discount_Tier_Up_To_Nbr    ,
 Item_Limit_Qty          ,
 Weight_Limit_Qty        ,
 Dollar_Limit_Amt       ,
 Receipt_Txt             ,
 DW_CREATE_TS            ,
 DW_LAST_UPDATE_TS       ,
 DW_SOURCE_CREATE_NM     ,
 DW_LOGICAL_DELETE_IND    ,
 DW_CURRENT_VERSION_IND    ,
 DW_SOURCE_UPDATE_NM     
FROM <<EDM_DB_NAME>>.DW_C_PRODUCT.OMS_Offer_Benefit_Discount_Tier;

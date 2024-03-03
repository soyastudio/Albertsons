--liquibase formatted sql
--changeset SYSTEM:OMS_Offer_Benefit_Discount runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view OMS_OFFER_BENEFIT_DISCOUNT(
	OMS_OFFER_ID COMMENT 'OMS Offer',
	DISCOUNT_ID COMMENT 'Discount ID',
	DW_FIRST_EFFECTIVE_DT COMMENT 'First effective date',
	DW_LAST_EFFECTIVE_DT COMMENT 'Last effective date',
	BENEFIT_VALUE_TYPE_CD COMMENT 'Benefit value type',
	BENEFIT_VALUE_TYPE_DSC COMMENT 'Benefit value desc',
	DISCOUNT_TYPE_CD COMMENT 'Discount type cd',
	DISCOUNT_DSC COMMENT 'Discount dsc',
	CHARGEBACK_DSC COMMENT 'Chargeback dsc',
	BEST_DEAL_IND COMMENT 'Best deal',
	ALLOW_NEGATIVE_IND COMMENT 'Allow negative',
	FLEX_NEGATIVE_IND COMMENT 'Flex negative',
	INCLUDED_PRODUCT_GROUP_ID COMMENT 'Included PG',
	EXCLUDED_PRODUCT_GROUP_ID COMMENT 'Excluded PG',
	DW_CREATE_TS COMMENT 'DW Current Timestamp',
	DW_LAST_UPDATE_TS COMMENT 'DW Last update Timestamp',
	DW_SOURCE_CREATE_NM COMMENT 'DW source Create Name',
	DW_LOGICAL_DELETE_IND COMMENT 'DW logical delete Ind',
	DW_CURRENT_VERSION_IND COMMENT 'DW Current version Indicator',
	DW_SOURCE_UPDATE_NM COMMENT 'DW Source update Name'
) COMMENT='VIEW for OMS_Offer_Benefit_Discount'
 as 
SELECT
 OMS_Offer_Id           ,
 Discount_Id            ,
 DW_First_Effective_Dt  ,
 DW_Last_Effective_Dt  ,
 Benefit_Value_Type_Cd    ,
 Benefit_Value_Type_Dsc    ,
 Discount_Type_Cd        ,
 Discount_Dsc            ,
 Chargeback_Dsc          ,
 Best_Deal_Ind           ,
 Allow_Negative_Ind      ,
 Flex_Negative_Ind       ,
 Included_Product_Group_Id    ,
 Excluded_Product_Group_Id    ,
 DW_CREATE_TS            ,
 DW_LAST_UPDATE_TS       ,
 DW_SOURCE_CREATE_NM     ,
 DW_LOGICAL_DELETE_IND    ,
 DW_CURRENT_VERSION_IND    ,
 DW_SOURCE_UPDATE_NM     
FROM <<EDM_DB_NAME>>.DW_C_PRODUCT.OMS_Offer_Benefit_Discount;

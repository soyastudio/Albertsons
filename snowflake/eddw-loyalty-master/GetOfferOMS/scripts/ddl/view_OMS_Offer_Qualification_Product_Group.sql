--liquibase formatted sql
--changeset SYSTEM:OMS_Offer_Qualification_Product_Group runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view OMS_OFFER_QUALIFICATION_PRODUCT_GROUP(
	OMS_OFFER_ID COMMENT 'OMS Offer',
	PRODUCT_GROUP_ID COMMENT 'PG',
	DW_FIRST_EFFECTIVE_DT COMMENT 'First effective Date',
	DW_LAST_EFFECTIVE_DT COMMENT 'Last effective Date',
	EXCLUDED_OMS_PRODUCT_GROUP_ID COMMENT 'Excluded PG',
	QUANTITY_UNIT_TYPE_DSC COMMENT 'Quantity unit type',
	QUANTITY_UNIT_DSC COMMENT 'Quantity dsc',
	CONJUNCTION_TXT COMMENT 'Conjunction',
	MINIMUM_PURCHASE_AMT COMMENT 'Minimum amt',
	UNIQUE_PRODUCT_IND COMMENT 'UPI',
	INHERITED_FROM_OFFER_REQUEST_IND COMMENT ' From Offer request',
	DW_CREATE_TS COMMENT 'DW Current Timestamp',
	DW_LAST_UPDATE_TS COMMENT 'DW Last update Timestamp',
	DW_SOURCE_CREATE_NM COMMENT 'DW source Create Name',
	DW_LOGICAL_DELETE_IND COMMENT 'DW logical delete Ind',
	DW_CURRENT_VERSION_IND COMMENT 'DW Current version Indicator',
	DW_SOURCE_UPDATE_NM COMMENT 'DW Source update Name'
) COMMENT='VIEW for OMS_Offer_Qualification_Product_Group'
 as
SELECT
 OMS_Offer_Id           ,
 Product_Group_Id       ,
 DW_First_Effective_Dt  ,
 DW_Last_Effective_Dt  ,
 Excluded_OMS_Product_Group_Id    ,
 Quantity_Unit_Type_Dsc    ,
 Quantity_Unit_Dsc       ,
 Conjunction_Txt         ,
 Minimum_Purchase_Amt   ,
 Unique_Product_Ind      ,
 Inherited_From_Offer_Request_Ind    ,
 DW_CREATE_TS            ,
 DW_LAST_UPDATE_TS       ,
 DW_SOURCE_CREATE_NM     ,
 DW_LOGICAL_DELETE_IND    ,
 DW_CURRENT_VERSION_IND    ,
 DW_SOURCE_UPDATE_NM     
FROM <<EDM_DB_NAME>>.DW_C_PRODUCT.OMS_Offer_Qualification_Product_Group;

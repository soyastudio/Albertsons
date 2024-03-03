--liquibase formatted sql
--changeset SYSTEM:view_OMS_PRODUCT_GROUP_UPC_MANUFACTURER runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view OMS_PRODUCT_GROUP_UPC_MANUFACTURER(
	PRODUCT_GROUP_ID COMMENT 'PRODUCT GROUP ID',
	UPC_MANUFACTURER_ID COMMENT 'UPC MANUFACTURER ID',
	DW_FIRST_EFFECTIVE_TS COMMENT 'DW FIRST EFFECTIVE TS',
	DW_LAST_EFFECTIVE_TS COMMENT 'DW LAST EFFECTIVE TS',
	DW_CREATE_TS COMMENT 'DW CREATE TS',
	DW_LAST_UPDATE_TS COMMENT 'DW LAST UPDATE TS',
	DW_LOGICAL_DELETE_IND COMMENT 'DW LOGICAL DELETE IND',
	DW_SOURCE_CREATE_NM COMMENT 'DW SOURCE CREATE NM',
	DW_SOURCE_UPDATE_NM COMMENT 'DW SOURCE UPDATE NM',
	DW_CURRENT_VERSION_IND COMMENT 'DW CURRENT VERSION IND'
) COMMENT='VIEW for OMS_PRODUCT_GROUP_UPC_MANUFACTURER'
 as
select 
PRODUCT_GROUP_ID,
UPC_MANUFACTURER_ID,
DW_FIRST_EFFECTIVE_TS,
DW_LAST_EFFECTIVE_TS,
DW_CREATE_TS,
DW_LAST_UPDATE_TS,
DW_LOGICAL_DELETE_IND,
DW_SOURCE_CREATE_NM,
DW_SOURCE_UPDATE_NM,
DW_CURRENT_VERSION_IND

 from <<EDM_DB_NAME>>.DW_C_PRODUCT.OMS_PRODUCT_GROUP_UPC_MANUFACTURER;

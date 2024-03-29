--liquibase formatted sql
--changeset SYSTEM:OMS_Offer_Benefit_Discount runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_PRODUCT;

create or replace TABLE OMS_OFFER_BENEFIT_DISCOUNT (
	OMS_OFFER_ID NUMBER(38,0) NOT NULL COMMENT 'OMS Offer Id',
	DISCOUNT_ID NUMBER(38,0) NOT NULL COMMENT 'Discount Id',
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL COMMENT 'DW First Effective Dt',
	DW_LAST_EFFECTIVE_DT DATE NOT NULL COMMENT 'DW Last Effective Dt',
	BENEFIT_VALUE_TYPE_CD VARCHAR(16777216) COMMENT 'Benefit Value Type Cd',
	BENEFIT_VALUE_TYPE_DSC VARCHAR(16777216) COMMENT 'Benefit Value Type Dsc',
	DISCOUNT_TYPE_CD VARCHAR(16777216) COMMENT 'Discount Type Cd',
	DISCOUNT_DSC VARCHAR(16777216) COMMENT 'Discount Dsc',
	CHARGEBACK_DSC VARCHAR(16777216) COMMENT 'Chargeback Dsc',
	BEST_DEAL_IND BOOLEAN COMMENT 'Best Deal Ind',
	ALLOW_NEGATIVE_IND BOOLEAN COMMENT 'Allow Negative Ind',
	FLEX_NEGATIVE_IND BOOLEAN COMMENT 'Flex Negative Ind',
	INCLUDED_PRODUCT_GROUP_ID NUMBER(38,0) COMMENT 'Included Product Group Id',
	EXCLUDED_PRODUCT_GROUP_ID NUMBER(38,0) COMMENT 'Excluded Product Group Id',
	DW_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'DW CREATE TS',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'DW LAST UPDATE TS',
	DW_SOURCE_CREATE_NM VARCHAR(255) COMMENT 'DW SOURCE CREATE NM',
	DW_LOGICAL_DELETE_IND BOOLEAN COMMENT 'DW LOGICAL DELETE IND',
	DW_CURRENT_VERSION_IND BOOLEAN COMMENT 'DW CURRENT VERSION IND',
	DW_SOURCE_UPDATE_NM VARCHAR(255) COMMENT 'DW SOURCE UPDATE NM',
	primary key (OMS_OFFER_ID, DISCOUNT_ID, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT)
);

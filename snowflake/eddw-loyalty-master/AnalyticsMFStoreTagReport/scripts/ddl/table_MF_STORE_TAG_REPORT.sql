--liquibase formatted sql
--changeset SYSTEM:MF_STORE_TAG_REPORT runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database EDM_CONFIRMED_PRD;
use schema DW_C_PRODUCT;

create or replace TABLE MF_STORE_TAG_REPORT (
	OFFER_STATUS_DSC VARCHAR(16777216) COMMENT 'Status of the offer. Defines the different stages of an offer and its effective date',
	OMS_OFFER_REGION_NM VARCHAR(16777216) COMMENT 'Name of the offer region',
	CHARGEBACK_VENDOR_NM VARCHAR(16777216) COMMENT 'Name of the chargeback vendor',
	HEADLINE_TXT VARCHAR(16777216) COMMENT 'Headline text of offer',
	PRODUCT_GROUP_NM VARCHAR(16777216) COMMENT 'Name of the product group',
	OFFER_PROTOTYPE_CD VARCHAR(16777216) COMMENT 'Prototype code of the offer',
	AGGREGATOR_OFFER_ID VARCHAR(16777216) COMMENT 'Offer Aggregate Identifier',
	OMS_OFFER_ID VARCHAR(16777216) COMMENT 'Internal Offer Id of OMS offer',
	UPC_CD VARCHAR(14) COMMENT 'UPC number e.g: 3520450097',
	UPC_DSC VARCHAR(16777216) COMMENT 'Description of UPC',
	DISPLAY_EFFECTIVE_START_DT DATE COMMENT 'Date from when the offer will be displayed',
	DISPLAY_EFFECTIVE_END_DT DATE COMMENT 'Date till when the offer will be displayed',
	OFFER_PROTOTYPE_DSC VARCHAR(16777216) COMMENT 'Prototype description of the offer',
	BENEFIT_VALUE_TYPE_DSC VARCHAR(16777216) COMMENT 'Description of benefit value type',
	DISCOUNT_TIER_AMT NUMBER(38,3) COMMENT 'Amount of the discount tier',
	DISCOUNT_TIER_ID NUMBER(38,0) COMMENT 'ID of discount tier',
	ITEM_LIMIT_QTY NUMBER(38,0) COMMENT 'Quantity of the item limit',
	WEIGHT_LIMIT_QTY NUMBER(38,0) COMMENT 'Quantity of the weight limit',
	PRICE_TITLE_TXT VARCHAR(16777216) COMMENT 'Price title text of the offer',
	BRAND_SIZE_DSC VARCHAR(16777216) COMMENT 'Description of the offer brand size',
	USAGE_LIMIT_TYPE_PER_USER_DSC VARCHAR(16777216) COMMENT 'Description of the usage limit for the Offer',
	STORE_TAG_AMT NUMBER(38,3) COMMENT 'Offer Amount on the store tag of the offer',
	STORE_TAG_COMMENTS_TXT VARCHAR(16777216) COMMENT 'Comments on the store tag',
	POD_OFFER_DETAIL_DSC VARCHAR(250) COMMENT 'Description of POD offer details',
	TIER_LEVEL_AMT NUMBER(14,4) COMMENT 'Amount of the tier level',
	UNIT_OF_MEASURE_DSC VARCHAR(100) COMMENT 'Description of the unit of measure',
	MIN_QTY_TO_BUY VARCHAR(16777216) COMMENT 'To pick the minimun quantity',
	PROGRAM_CD VARCHAR(16777216) COMMENT 'Program Code of the offer',
	EXTERNAL_OFFER_ID VARCHAR(16777216) COMMENT 'External Offer Id from OMS Offer'
);
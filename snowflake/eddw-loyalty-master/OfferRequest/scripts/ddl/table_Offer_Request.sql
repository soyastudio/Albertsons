--liquibase formatted sql
--changeset SYSTEM:Offer_Request runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema DW_C_PURCHASING;

create or replace TABLE OFFER_REQUEST (
	OFFER_REQUEST_ID NUMBER(38,0) NOT NULL,
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL,
	DW_LAST_EFFECTIVE_DT DATE NOT NULL,
	DELIVERY_CHANNEL_TYPE_CD VARCHAR(10),
	DELIVERY_CHANNEL_TYPE_DSC VARCHAR(50),
	OFFER_REQUEST_DEPARTMENT_NM VARCHAR(16777216),
	OFFER_NM VARCHAR(200),
	OFFER_REQUEST_DSC VARCHAR(16777216),
	CUSTOMER_SEGMENT_INFO_TXT VARCHAR(200),
	OFFER_ITEM_SIZE_DSC VARCHAR(100),
	VENDOR_PROMOTION_NOPA_START_DT DATE,
	VENDOR_PROMOTION_NOPA_END_DT DATE,
	VENDOR_PROMOTION_BILLING_OPTION_TYPE_CD VARCHAR(50),
	VENDOR_PROMOTION_BILLING_OPTION_TYPE_DSC VARCHAR(250),
	VENDOR_PROMOTION_BILLING_OPTION_TYPE_SHORT_DSC VARCHAR(50),
	VENDOR_PROMOTION_NOPA_ASSIGN_STATUS_TYPE_CD VARCHAR(16777216),
	VENDOR_PROMOTION_NOPA_ASSIGN_STATUS_EFFECTIVE_TS TIMESTAMP_LTZ(9),
	VENDOR_PROMOTION_NOPA_ASSIGN_STATUS_DSC VARCHAR(50),
	VENDOR_PROMOTION_ALLOWANCE_TYPE_CD VARCHAR(50),
	VENDOR_PROMOTION_ALLOWANCE_TYPE_DSC VARCHAR(250),
	VENDOR_PROMOTION_ALLOWANCE_TYPE_SHORT_DSC VARCHAR(50),
	VENDOR_PROMOTION_BILLED_IND VARCHAR(5),
	PROMOTION_PROGRAM_TYPE_CD VARCHAR(50),
	PROMOTION_PROGRAM_TYPE_NM VARCHAR(50),
	ADVERTISEMENT_TYPE_CD VARCHAR(50),
	ADVERTISEMENT_TYPE_DSC VARCHAR(250),
	ADVERTISEMENT_TYPE_SHORT_DSC VARCHAR(50),
	TRIGGER_ID VARCHAR(20),
	SAVINGS_VALUE_TXT VARCHAR(500),
	BRAND_INFO_TXT VARCHAR(500),
	DISCLAIMER_TXT VARCHAR(500),
	IMAGE_ID VARCHAR(500),
	DISPLAY_START_DT DATE,
	DISPLAY_END_DT DATE,
	OFFER_START_DT DATE,
	OFFER_END_DT DATE,
	TEST_START_DT DATE,
	TEST_END_DT DATE,
	SOURCE_SYSTEM_ID VARCHAR(50),
	APPLICATION_ID VARCHAR(50),
	UPDATED_APPLICATION_ID VARCHAR(50),
	OFFER_EFFECTIVE_DAY_MONDAY_IND VARCHAR(5),
	OFFER_EFFECTIVE_DAY_TUESDAY_IND VARCHAR(5),
	OFFER_EFFECTIVE_DAY_WEDNESDAY_IND VARCHAR(5),
	OFFER_EFFECTIVE_DAY_THURSDAY_IND VARCHAR(5),
	OFFER_EFFECTIVE_DAY_FRIDAY_IND VARCHAR(5),
	OFFER_EFFECTIVE_DAY_SATURDAY_IND VARCHAR(5),
	OFFER_EFFECTIVE_DAY_SUNDAY_IND VARCHAR(5),
	OFFER_EFFECTIVE_START_TM TIME(9),
	OFFER_EFFECTIVE_END_TM TIME(9),
	OFFER_EFFECTIVE_TIME_ZONE_CD VARCHAR(20),
	OFFER_ITEM_DSC VARCHAR(200),
	MANUFACTURER_TYPE_ID NUMBER(10,0),
	MANUFACTURER_TYPE_ID_TXT VARCHAR(20),
	MANUFACTURER_TYPE_SHORT_DSC VARCHAR(50),
	MANUFACTURER_TYPE_FILE_NM VARCHAR(100),
	MANUFACTURER_TYPE_CREATE_FILE_DT DATE,
	MANUFACTURER_TYPE_CREATE_FILE_TS TIMESTAMP_LTZ(9),
	MANUFACTURER_TYPE_FILE_SEQUENCE_NBR NUMBER(38,0),
	MANUFACTURER_TYPE_SOURCE_ID VARCHAR(20),
	MANUFACTURER_TYPE_DESTINATION_ID VARCHAR(20),
	MANUFACTURER_TYPE_DSC VARCHAR(100),
	OFFER_REQUEST_TYPE_CD VARCHAR(50),
	BUSINESS_JUSTIFICATION_TXT VARCHAR(20000),
	OFFER_REQUEST_COMMENT_TXT VARCHAR(1000),
	ADDITIONAL_DETAILS_TXT VARCHAR(20000),
	VERSION_QTY NUMBER(38,0),
	TIER_QTY NUMBER(38,0),
	PRODUCT_QTY NUMBER(38,0),
	STORE_GROUP_QTY NUMBER(38,0),
	DW_CREATE_TS TIMESTAMP_LTZ(9),
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9),
	DW_LOGICAL_DELETE_IND BOOLEAN,
	DW_SOURCE_CREATE_NM VARCHAR(255),
	DW_SOURCE_UPDATE_NM VARCHAR(255),
	DW_CURRENT_VERSION_IND BOOLEAN,
	OFFER_BANK_TYPE_CD VARCHAR(50),
	OFFER_BANK_ID VARCHAR(50),
	OFFER_BANK_NM VARCHAR(200),
	TEMPLATE_ID VARCHAR(50),
	TEMPLATE_NM VARCHAR(50),
	CHARGEBACK_DEPARTMENT_ID VARCHAR(16777216),
	ALLOCATION_TYPE_CD VARCHAR(50) COMMENT 'Allocation_Type_Cd',
	ALLOCATION_TYPE_DESC VARCHAR(250) COMMENT 'Allocation_Type_Desc',
	ALLOCATION_TYPE_SHORT_DESC VARCHAR(50) COMMENT 'Allocation_Type_Short_Desc',
	OFFER_TEMPLATE_STATUS_CD VARCHAR(16777216) COMMENT 'Template Code for the offer request',
	UPC_QTY_TXT VARCHAR(16777216) COMMENT 'Range of Upc',
	ECOMM_PROGRAM_TYPE_NM VARCHAR(16777216) COMMENT 'Name of a Ecomm promotion program type',
	ECOMM_PROGRAM_TYPE_CD VARCHAR(16777216) COMMENT 'Type/Sub type of a Ecomm promotion program',
	VALID_WITH_OTHER_OFFERS_IND BOOLEAN,
	VALID_FOR_FIRST_TIME_CUSTOMER_IND BOOLEAN,
	AUTO_APPLY_PROMO_IND BOOLEAN,
	OFFER_ELIGIBLE_ORDER_CNT VARCHAR(16777216),
	GAMING_VENDOR_NM VARCHAR(16777216),
	GAMING_LAND_NM VARCHAR(16777216),
	GAMING_LAND_SPACE_NM VARCHAR(16777216),
	GAMING_LAND_SPACE_SLOT_NM VARCHAR(16777216),
	PROMOTION_SUBPROGRAM_TYPE_CD VARCHAR(16777216),
	OFFER_QUALIFICATION_BEHAVIOR_CD VARCHAR(16777216),
	INITIAL_SUBSCRIPTION_OFFER_IND BOOLEAN,
	OFFER_TEMPLATE_STATUS_IND BOOLEAN,
	DYNAMIC_OFFER_IND BOOLEAN,
	DAYS_TO_REDEEM_OFFER_CNT NUMBER(38,0),
	constraint XPKOFFERREQUESTDATA primary key (OFFER_REQUEST_ID, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT)
);

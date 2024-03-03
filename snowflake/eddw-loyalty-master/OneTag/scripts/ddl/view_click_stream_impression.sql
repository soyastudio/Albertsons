--liquibase formatted sql
--changeset SYSTEM:CUSTOMER_SESSION_IMPRESSION runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database EDM_VIEWS_<<ENV>>;
use schema DW_VIEWS;

create or replace VIEW CUSTOMER_SESSION_IMPRESSION
(
	EVENT_ID COMMENT 'Unique ID generated based on AK for each event received from source',
    EVENT_TS COMMENT '',
    BASE_PRODUCT_NBR COMMENT 'Unique Identifier for product ',
    IMPRESSION_TYPE_CD COMMENT 'This contain type of impressions like Product Impression , Search Impressions',
	PRODUCT_FINDING_METHOD_DSC COMMENT 'Product Finder Method Impression. For example erums:cart:dnf#recommended-for-you#non-search#recs#R02#S00 [PAGE SUBSECTION]#{CAROUSEL]#[SEARCH]#[RECOMENDATION TYPE]#[ROW]#[SLOT]',
	ROW_LOCATION_CD COMMENT 'Row location of the product user seen on the page',
	SLOT_LOCATION_CD COMMENT 'Slot location of the product user seen on the page',
    MODEL_ID COMMENT 'impressionAdditionalDetail.[MODEL CONFIGURATION ID]#[MODEL ID]#[MODEL NAME]#[EXPERIMENT ID]#[EXPERIMENT GROUP]#[ELIGIBLE]',
    LIST_PRICE_AMT COMMENT 'listPrice of the product',
	CAROUSEL_NM COMMENT 'This is the title of dynamic carousel. For example "recommended-for-you"',
	PRODUCT_UNIT_CNT COMMENT 'number of units viewed, added to cart, increased quantity, decreased quantity or removed from cart. ',
    BASE_PRODUCT_NBR_VALID_IND COMMENT 'This is the title of dynamic carousel. For example "recommended-for-you"',
	DW_CREATE_TS COMMENT 'The timestamp the record was inserted',
	DW_LAST_UPDATE_TS COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM COMMENT 'Name of source system or user created the record',
	DW_SOURCE_UPDATE_NM COMMENT 'Name of source system or user updated the record',
	DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d',
    DW_Checksum_Value_Txt COMMENT 'Concatenated value of all the columns in the record used to capture SCD2 compare logic for updates.'
)COPY GRANTS COMMENT='View for CUSTOMER_SESSION_IMPRESSION'
 AS
SELECT
	EVENT_ID,
    EVENT_TS,
    BASE_PRODUCT_NBR,
    IMPRESSION_TYPE_CD,
	PRODUCT_FINDING_METHOD_DSC,
	ROW_LOCATION_CD,
	SLOT_LOCATION_CD,
    MODEL_ID,
    LIST_PRICE_AMT,
	CAROUSEL_NM,
	PRODUCT_UNIT_CNT,
    BASE_PRODUCT_NBR_VALID_IND,
	DW_CREATE_TS,
	DW_LAST_UPDATE_TS,
	DW_LOGICAL_DELETE_IND,
	DW_SOURCE_CREATE_NM,
	DW_SOURCE_UPDATE_NM,
	DW_CURRENT_VERSION_IND,
    DW_Checksum_Value_Txt
FROM EDM_CONFIRMED_<<ENV>>.DW_C_USER_ACTIVITY.CUSTOMER_SESSION_IMPRESSION;
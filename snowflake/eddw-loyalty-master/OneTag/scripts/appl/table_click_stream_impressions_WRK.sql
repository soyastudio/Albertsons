--liquibase formatted sql
--changeset SYSTEM:CUSTOMER_SESSION_IMPRESSION_WRK runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database EDM_CONFIRMED_<<ENV>>;
use schema DW_C_STAGE;
CREATE OR REPLACE TRANSIENT TABLE EDM_CONFIRMED_<<ENV>>.DW_C_STAGE.CUSTOMER_SESSION_IMPRESSION_WRK
(
	EVENT_ID VARCHAR NOT NULL  COMMENT 'Unique ID generated based on AK for each event received from source',
    EVENT_TS TIMESTAMP NOT NULL  COMMENT '',
    BASE_PRODUCT_NBR NUMBER(38,0) NOT NULL  COMMENT 'Unique Identifier for product ',
    IMPRESSION_TYPE_CD VARCHAR NULL  COMMENT 'This contain type of impressions like Product Impression , Search Impressions',
	PRODUCT_FINDING_METHOD_DSC VARCHAR NULL  COMMENT 'Product Finder Method Impression. For example erums:cart:dnf#recommended-for-you#non-search#recs#R02#S00 [PAGE SUBSECTION]#{CAROUSEL]#[SEARCH]#[RECOMENDATION TYPE]#[ROW]#[SLOT]',
	ROW_LOCATION_CD VARCHAR NULL  COMMENT 'Row location of the product user seen on the page',
	SLOT_LOCATION_CD VARCHAR NULL  COMMENT 'Slot location of the product user seen on the page',
    MODEL_ID VARCHAR NULL  COMMENT 'impressionAdditionalDetail.[MODEL CONFIGURATION ID]#[MODEL ID]#[MODEL NAME]#[EXPERIMENT ID]#[EXPERIMENT GROUP]#[ELIGIBLE]',
    LIST_PRICE_AMT NUMBER(16,5) NULL  COMMENT 'listPrice of the product',
	CAROUSEL_NM VARCHAR NULL  COMMENT 'This is the title of dynamic carousel. For example "recommended-for-you"',
	PRODUCT_UNIT_CNT NUMBER NULL  COMMENT 'number of units viewed, added to cart, increased quantity, decreased quantity or removed from cart. ',
    BASE_PRODUCT_NBR_VALID_IND BOOLEAN NULL  COMMENT 'This is the title of dynamic carousel. For example "recommended-for-you"',
	DW_CREATE_TS TIMESTAMP NULL  COMMENT 'The timestamp the record was inserted',
	DW_LOGICAL_DELETE_IND BOOLEAN NULL  COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_Checksum_Value_Txt VARCHAR(16777216) COMMENT 'Concatenated value of all the columns in the record used to capture SCD2 compare logic for updates.'	
);

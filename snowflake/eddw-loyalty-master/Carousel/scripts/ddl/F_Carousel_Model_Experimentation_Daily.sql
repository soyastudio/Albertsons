--liquibase formatted sql
--changeset SYSTEM:Tbl_ddl runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME_A>>;
use schema DW_REFERENCE;

create or replace TABLE F_CAROUSEL_MODEL_EXPERIMENTATION_DAILY (
	MODEL_CONFIG_ID VARCHAR(16777216) NOT NULL COMMENT 'Model Configuration ID',
	MODEL_NAME VARCHAR(16777216) NOT NULL COMMENT 'Model Name',
	EXPERIMENTATION_DAY_ID NUMBER(8,0) NOT NULL COMMENT 'Day Identifier;  Format: YYYYMMDD; e.g. 20210101',
	EXPERIMENTATION_DT DATE NOT NULL COMMENT 'Date when the order has been placed.',
	CAROUSEL_VISIT_CNT NUMBER(38,0) NOT NULL COMMENT 'Total number of visits to the carousel',
	HOUSEHOLD_CNT NUMBER(38,0) NOT NULL COMMENT 'Total number of distinct Household_ID',
	ITEMS_ADDED_TO_CART_CNT NUMBER(38,0) NOT NULL COMMENT 'Total quantity of items added to cart from carousel',
	TOTAL_ORDERS_PLACED_CNT NUMBER(38,0) NOT NULL COMMENT 'Quantity of total orders placed which has items from carousel',
	ITEMS_ORDERED_FROM_CAROUSEL_CNT NUMBER(38,0) NOT NULL COMMENT 'Count of Items ordered from carousel',
	GROSS_REVENUE_AMT NUMBER(13,2) NOT NULL COMMENT 'Total Sales value for the orders placed for items from carousel',
	DIRECT_REVENUE_AMT NUMBER(13,2) NOT NULL COMMENT 'Total Sales value for the orders placed for items from carousel',
	DW_CREATE_TS TIMESTAMP_LTZ(9) NOT NULL COMMENT 'timestamp when record was created.',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) NOT NULL COMMENT 'timestamp when record was updated.'
);

ALTER TABLE F_Carousel_Model_Experimentation_Daily
	ADD CONSTRAINT XPKF_Carousel_Model_Experiment PRIMARY KEY (MODEL_CONFIG_ID,Experimentation_Day_Id);

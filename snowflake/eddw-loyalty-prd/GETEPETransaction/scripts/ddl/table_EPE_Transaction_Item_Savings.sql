--liquibase formatted sql
--changeset SYSTEM:EPE_Transaction_Item_Savings runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_RETAILSALE;

create or replace TABLE EPE_TRANSACTION_ITEM_SAVINGS (
	TRANSACTION_INTEGRATION_ID NUMBER(38,0) NOT NULL COMMENT 'Generated Integration Identifier of each transaction',
	UPC_NBR NUMBER(38,0) NOT NULL COMMENT 'Universal Product Code of the item',
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT DATE NOT NULL COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	OFFER_ID NUMBER(38,0) NOT NULL COMMENT 'This is the Offer ID applied to the savings item',
	SAVINGS_CATEGORY_NM VARCHAR(16777216) COMMENT 'Savings Category will tell''s J4U Savings, Credit Card Savings, Club Card Savings and Reward Savings.',
	DISCOUNT_MESSAGE_TXT VARCHAR(16777216) COMMENT 'The message displayed for the discount. It’s the receipt text',
	SOURCE_SYSTEM_CD VARCHAR(16777216) COMMENT 'Source from where the offer has been created',
	DISCOUNT_TYPE_TXT VARCHAR(16777216) COMMENT 'Source from where the offer has been created',
	DISCOUNT_DSC VARCHAR(16777216) COMMENT 'Description of the discount being applied',
	DISCOUNT_LEVEL_TXT VARCHAR(16777216) COMMENT 'This describes if the level of the discount. Item Level/Basket Level/Dept Level',
	SAVINGS_CATEGORY_ID NUMBER(38,0) COMMENT 'Multiple category Id''s which is belongs to J4U, credit card savings etc.',
	MIN_PURCHASE_QTY NUMBER(9,2) COMMENT 'Minimum Purchase Quantity for the item',
	DISCOUNT_AMT NUMBER(38,5) COMMENT 'Discounted Amount of the transacted item',
	DISCOUNT_QTY NUMBER(9,2) COMMENT 'If the MF offer is applied, how much quantity the offer has been applied to',
	NON_DIGITAL_OFFER_IND BOOLEAN COMMENT 'Indicates whether the transaction level coupon is digital or non digital',
	DW_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND BOOLEAN COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM VARCHAR(255) COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM VARCHAR(255) COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND BOOLEAN COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d',
	CALCULATE_USAGE_IND BOOLEAN COMMENT 'Indicator to whether this offer need to keep track of it''s redemption',
	NET_PROMOTION_AMT NUMBER(38,5) COMMENT 'The final discount amount on any particular item',
	USAGE_CNT NUMBER(38,0) COMMENT 'How many times this offer has been redeemed before',
	PROGRAM_CD VARCHAR(16777216) COMMENT 'Program code of the offer being applied for the savings',
	START_DT DATE COMMENT 'Offer Start Date',
	END_DT DATE COMMENT 'Offer End Date',
	EXTERNAL_OFFER_ID VARCHAR(16777216) COMMENT 'To map External Offer ID from OMS offer BOD',
	primary key (TRANSACTION_INTEGRATION_ID, UPC_NBR, OFFER_ID, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT)
);

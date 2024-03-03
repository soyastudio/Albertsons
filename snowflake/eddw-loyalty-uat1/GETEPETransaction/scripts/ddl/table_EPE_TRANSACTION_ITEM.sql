--liquibase formatted sql
--changeset SYSTEM:EPE_TRANSACTION_ITEM runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_RETAILSALE;

create or replace TABLE EPE_TRANSACTION_ITEM (
	TRANSACTION_INTEGRATION_ID NUMBER(38,0) NOT NULL COMMENT 'Generated Integration Identifier of each transaction',
	UPC_NBR NUMBER(38,0) NOT NULL COMMENT 'Universal Product Code',
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT DATE NOT NULL COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	DEPARTMENT_NBR NUMBER(38,0) COMMENT 'Department Number of the transaction item',
	DISCOUNT_ALLOWED_IND BOOLEAN COMMENT 'Indicator to identify if the discount is allowed on the item',
	ITEM_SEQUENCE_ID NUMBER(38,0) COMMENT 'the sequence number for the items in the basket ',
	POINTS_APPLY_ITEM_IND BOOLEAN COMMENT 'Indicator to identify if the point  is applied on the item',
	ITEM_UOM_CD VARCHAR(16777216) COMMENT 'Quantity Type : Either weight or count',
	SELL_BY_WEIGHT_CD VARCHAR(50) COMMENT 'Sell By Weight Code',
	DEPARTMENT_GROUP_NBR NUMBER(38,0) COMMENT 'Department Group Number of the transaction item',
	LINK_PLU_NBR VARCHAR(16777216) COMMENT 'Link PLU Number of the Item',
	ITEM_PLU_NBR VARCHAR(16777216) COMMENT 'PLU Number of the item',
	CLIPPED_OFFER_START_TS TIMESTAMP_LTZ(9) COMMENT 'Clip Start Timestamp',
	CLIPPED_OFFER_END_TS TIMESTAMP_LTZ(9) COMMENT 'Clip End Timestamp',
	PRICE_PER_ITEM_AMT NUMBER(38,5) COMMENT 'Price per Item Amount',
	BASE_PRICE_AMT NUMBER(38,5) COMMENT 'Base Price of the item',
	ITEM_PRICE_AMT NUMBER(38,5) COMMENT 'Price Amount of the item',
	NET_PROMOTION_AMT NUMBER(38,5) COMMENT 'The net amount after promotion has been applied',
	UNIT_PRICE_AMT NUMBER(38,5) COMMENT 'Unit Price of the item',
	EXTENDED_PRICE_AMT NUMBER(38,5) COMMENT 'Unit price Amount*Quantity(Item Level) = Extended price Amount',
	BASE_PRICE_PER_AMT NUMBER(38,5) COMMENT 'Base price of the item',
	CLUB_CARD_SAVINGS_AMT NUMBER(38,5) COMMENT 'Club Card Saving Amount for the transaction item',
	AVERAGE_WEIGHT_QTY NUMBER(38,5) COMMENT 'Average Weight Quantity of the item',
	ITEM_UNIT_QTY NUMBER(38,5) COMMENT 'The unit of the quantity of the item',
	ITEM_QTY NUMBER(38,5) COMMENT 'Quantity of the item',
	DW_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND BOOLEAN COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM VARCHAR(255) COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM VARCHAR(255) COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND BOOLEAN COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d',
	primary key (TRANSACTION_INTEGRATION_ID, UPC_NBR, DW_LAST_EFFECTIVE_DT, DW_FIRST_EFFECTIVE_DT)
);


create or replace TABLE EPE_TRANSACTION_HEADER_SAVING_CLIPS (
	TRANSACTION_INTEGRATION_ID NUMBER(38,0) NOT NULL COMMENT 'Generated Integration Identifier of each transaction',
	OFFER_ID NUMBER(38,0) NOT NULL COMMENT 'This is the Offer ID applied to the savings item',
	CLIP_ID VARCHAR(250) NOT NULL COMMENT 'Identifier of Clip used for Header level savings',
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT DATE NOT NULL COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	DW_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'When a record is updated  this would be the current timestamp',
	DW_SOURCE_CREATE_NM VARCHAR(255) COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM VARCHAR(255) COMMENT 'The Bod (data source) name of this update or delete.',
	DW_LOGICAL_DELETE_IND BOOLEAN COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_CURRENT_VERSION_IND BOOLEAN COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d',
	primary key (TRANSACTION_INTEGRATION_ID, OFFER_ID, CLIP_ID, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT)
);

create or replace TABLE EPE_TRANSACTION_ITEM_SAVING_CLIPS (
	TRANSACTION_INTEGRATION_ID NUMBER(38,0) NOT NULL COMMENT 'Generated Integration Identifier of each transaction',
	UPC_NBR NUMBER(38,0) NOT NULL COMMENT 'Universal Product Code of the item',
	OFFER_ID NUMBER(38,0) NOT NULL COMMENT 'This is the Offer ID applied to the savings item',
	CLIP_ID VARCHAR(250) NOT NULL COMMENT 'Identifier of clip used for Item level Savings',
	ITEM_SEQUENCE_ID NUMBER(38,0) NOT NULL COMMENT 'the sequence number for the items in the basket ',
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT DATE NOT NULL COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	DW_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'When a record is updated  this would be the current timestamp',
	DW_SOURCE_CREATE_NM VARCHAR(255) COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM VARCHAR(255) COMMENT 'The Bod (data source) name of this update or delete.',
	DW_LOGICAL_DELETE_IND BOOLEAN COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_CURRENT_VERSION_IND BOOLEAN COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d',
	primary key (TRANSACTION_INTEGRATION_ID, UPC_NBR, OFFER_ID, CLIP_ID, ITEM_SEQUENCE_ID, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT)
);

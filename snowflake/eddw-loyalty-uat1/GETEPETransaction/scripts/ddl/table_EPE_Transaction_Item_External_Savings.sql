--liquibase formatted sql
--changeset SYSTEM:EPE_Transaction_Item_External_Savings runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_RETAILSALE;

create or replace TABLE EPE_TRANSACTION_ITEM_EXTERNAL_SAVINGS (
	TRANSACTION_INTEGRATION_ID NUMBER(38,0) NOT NULL COMMENT 'Generated Integration Identifier of each transaction',
	UPC_NBR NUMBER(38,0) NOT NULL COMMENT 'Universal Product Code of the Item',
	ADJUSTMENT_TYPE_CD NUMBER(38,0) NOT NULL COMMENT 'Is MF offer applied -1,  If other offers are applied 0 & 2',
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT DATE NOT NULL COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	ITEM_UOM_CD VARCHAR(16777216) COMMENT 'Quantity Type : Either weight or count.',
	PROMOTION_CD VARCHAR(16777216) COMMENT 'This is w.r.t the offer applied to the Item.',
	ITEM_UNIT_QTY NUMBER(38,0) COMMENT 'The unit of the quantity of the Item ',
	EXTERNAL_SAVING_AMT NUMBER(38,5) COMMENT 'External Saving(Saving applied by POS)',
	DW_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND BOOLEAN COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM VARCHAR(255) COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM VARCHAR(255) COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND BOOLEAN COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d',
	primary key (TRANSACTION_INTEGRATION_ID, UPC_NBR, ADJUSTMENT_TYPE_CD, DW_LAST_EFFECTIVE_DT, DW_FIRST_EFFECTIVE_DT)
);

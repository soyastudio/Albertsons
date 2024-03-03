--liquibase formatted sql
--changeset SYSTEM:EPE_Transaction_Header runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_RETAILSALE;

create or replace TABLE EPE_TRANSACTION_HEADER (
	TRANSACTION_INTEGRATION_ID NUMBER(38,0) NOT NULL COMMENT 'Generated Integration Identifier of each transaction',
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT DATE NOT NULL COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	TERMINAL_NBR VARCHAR(16777216) COMMENT 'The register at which the transaction took place.',
	TRANSACTION_ID NUMBER(38,0) COMMENT 'Unique Transaction Identifier for each transaction made',
	TRANSACTION_TS TIMESTAMP_TZ(9) COMMENT 'The real time of the transaction. Date and Time as printed on the receipt which is coimg from POS',
	ORDER_ID VARCHAR(16777216) COMMENT 'Order Identifier of the transaction',
	HOUSEHOLD_ID NUMBER(38,0) COMMENT 'Unique identifier of the Household. The ''aggregateId'' in CHMS service is the HHID unlike the other services where the ''aggegatedid'' we map to the CustomerId',
	STATUS_CD VARCHAR(16777216) COMMENT 'Status code for the transaction',
	CREATE_DT DATE COMMENT 'Transaction Created Date',
	SOURCE_SYSTEM_CD VARCHAR(16777216) COMMENT 'source system code \" Number\"',
	TOTAL_CARD_SAVINGS_AMT NUMBER(38,5) COMMENT 'Total Savings Amount for the transaction',
	TRANSACTION_TOTAL_AMT NUMBER(38,5) COMMENT 'Total Transaction Amount',
	DW_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND BOOLEAN COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM VARCHAR(255) COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM VARCHAR(255) COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND BOOLEAN COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d',
	STORE_NBR VARCHAR(16777216) COMMENT 'Store Number where the transaction took place',
	REGISTER_TRANSACTION_SEQUENCE_NBR NUMBER(38,0) COMMENT 'Transaction sequence number on the register',
	primary key (TRANSACTION_INTEGRATION_ID, DW_LAST_EFFECTIVE_DT, DW_FIRST_EFFECTIVE_DT),
	unique (TERMINAL_NBR, TRANSACTION_ID, TRANSACTION_TS),
	unique (TRANSACTION_ID, TRANSACTION_TS)
);

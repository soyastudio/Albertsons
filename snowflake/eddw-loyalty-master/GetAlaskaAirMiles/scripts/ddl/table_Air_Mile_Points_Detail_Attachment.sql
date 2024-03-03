--liquibase formatted sql
--changeset SYSTEM:Air_Mile_Points_Detail_Attachment runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_LOYALTY;

create or replace TABLE AIR_MILE_POINTS_DETAIL_ATTACHMENT (
	BATCH_ID VARCHAR(100) NOT NULL COMMENT 'The timestamp the record was inserted.',
	HOUSEHOLD_ID NUMBER(38,0) NOT NULL COMMENT 'Unique identifier of the Household. The aggregateId in CHMS service is the HHID unlike the other services where the aggegatedid we map to the CustomerId',
	TRANSACTION_ID VARCHAR(100) NOT NULL COMMENT 'Transaction Id for AirMiles.',
	TRANSACTION_TS TIMESTAMP_LTZ(9) NOT NULL COMMENT 'This is the timestamp when the transaction was updated.',
	FILE_NM VARCHAR(16777216) NOT NULL COMMENT 'Files assocoiated with AirMiles information.',
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT DATE NOT NULL COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	LINK_URL_TXT VARCHAR(16777216) COMMENT 'Link urls assocoiated with AirMiles information.',
	DW_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND BOOLEAN COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM VARCHAR(255) COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM VARCHAR(255) COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND BOOLEAN COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d',
	primary key (BATCH_ID, HOUSEHOLD_ID, TRANSACTION_ID, TRANSACTION_TS, FILE_NM, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT)
);

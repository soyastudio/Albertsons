--liquibase formatted sql
--changeset SYSTEM:Air_Mile_Points_Summary runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_LOYALTY;

create or replace TABLE AIR_MILE_POINTS_SUMMARY (
	BATCH_ID VARCHAR(100) NOT NULL COMMENT 'The timestamp the record was inserted.',
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT DATE NOT NULL COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	BATCH_START_DATE_TXT VARCHAR(10) COMMENT 'Start date of the batch file.',
	BATCH_END_DATE_TXT VARCHAR(10) COMMENT 'End date of the batch file.',
	TOTAL_AIR_MILE_POINTS_QTY NUMBER(38,0) COMMENT 'Total number of AirMiles in the batch file. Note that AirMiles are calculated from  customers rewards/points they have earned.',
	RECORD_CNT NUMBER(38,0) COMMENT 'Total number of records in the AirMiles batch file.',
	TOTAL_REJECTED_AIR_MILE_POINTS_QTY NUMBER(38,0) COMMENT 'Total number of rejected AirMiles.',
	REJECTED_RECORD_CNT NUMBER(38,0) COMMENT 'Total number of records rejected in the AirMiles batch file.',
	CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'Date and time when the record was created in the source system.',
	CREATE_USER_ID VARCHAR(50) COMMENT 'User Id of the record created in the source system record',
	UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'Last updated timestamp of the source sytem record.',
	UPDATE_USER_ID VARCHAR(50) COMMENT 'User Id of the last updated in the source system record',
	SOURCE_TYPE_CD VARCHAR(50) COMMENT 'Indicates if the AirMilePoints are SUMMARY or DETAIL payload.',
	DW_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND BOOLEAN COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM VARCHAR(255) COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM VARCHAR(255) COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND BOOLEAN COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d',
	primary key (BATCH_ID, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT)
);

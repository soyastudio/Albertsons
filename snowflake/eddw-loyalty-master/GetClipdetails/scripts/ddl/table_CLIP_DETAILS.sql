--liquibase formatted sql
--changeset SYSTEM:CLIP_DETAILS runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database EDM_CONFIRMED_PRD;
use schema DW_C_LOYALTY;

create or replace TABLE CLIP_DETAILS (
	CLIP_SEQUENCE_ID NUMBER(38,0) NOT NULL COMMENT 'System generated key to uniquely identify Clip details',
	CLIP_ID VARCHAR(250) NOT NULL COMMENT 'Unique generated string for every payload',
	EVENT_TS TIMESTAMP_LTZ(9) NOT NULL COMMENT 'Captures the timestamp when an offer has been clipped or unclipped',
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL COMMENT 'The timestamp the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT DATE NOT NULL COMMENT 'for the current record this is 12/31/9999 24.00.00.0000.',
	OFFER_ID NUMBER(38,0) COMMENT 'clip type Card(C) & List(L) has same J4U offer ID(When we do clip)',
	CLIP_SOURCE_APPLICATION_ID VARCHAR(50) COMMENT 'It stores Clip Source Application value',
	CLIP_TYPE_CD VARCHAR(50) COMMENT 'Clip Type code (C),(L) etc',
	CLIP_DT DATE COMMENT 'Date when clipping is done',
	CLIP_TM TIME(9) COMMENT 'Time when clipping is done',
	CLIP_SOURCE_CD VARCHAR(50) COMMENT 'Clip source code',
	VENDOR_BANNER_CD VARCHAR(50) COMMENT 'Vendor Banner Code',
	DW_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'When a record is created  this would be the current timestamp',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'When a record is created  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND BOOLEAN COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM VARCHAR(255) COMMENT 'The data source name of this insert',
	DW_SOURCE_UPDATE_NM VARCHAR(255) COMMENT 'The data source name of this update or delete',
	DW_CURRENT_VERSION_IND BOOLEAN COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 day',
	primary key (CLIP_SEQUENCE_ID, CLIP_ID, EVENT_TS, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT)
);
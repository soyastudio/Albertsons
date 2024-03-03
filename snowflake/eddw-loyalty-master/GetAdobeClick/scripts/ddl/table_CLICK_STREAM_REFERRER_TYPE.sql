--liquibase formatted sql
--changeset SYSTEM:CLICK_STREAM_REFERRER_TYPE runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database EDM_CONFIRMED_PRD;
use schema DW_C_USER_ACTIVITY;

create or replace TABLE CLICK_STREAM_REFERRER_TYPE (
	REFERRER_TYPE_NM VARCHAR(250) NOT NULL COMMENT 'Single-string identifier of the type of referral for the hit. Used in the Referrer type dimension.1: Inside your site,2: Other web sites,3: Search engines,4: Hard drive,5: USENET,6: Typed/Bookmarked (no referrer),7: Email,8: No JavaScript,9: Social Networks',
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL COMMENT 'The timestamp the record was inserted. For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT DATE NOT NULL COMMENT 'for the current record this is 12/31/9999 24.00.00.0000. For updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 micro second',
	REFERRER_TYPE_ID VARCHAR(50) COMMENT 'A numeric ID representing the type of referral for the hit. Used in the Referrer type dimension.1: Inside your site,2: Other web sites,3: Search engines,4: Hard drive,5: USENET,6: Typed/Bookmarked (no referrer),7: Email,8: No JavaScript,9: Social Networks',
	REFERRER_TYPE_DSC VARCHAR(250) COMMENT 'The type of referral for the hit. Used in the Referrer type dimension.1: Inside your site,2: Other web sites,3: Search engines,4: Hard drive,5: USENET,6: Typed/Bookmarked (no referrer),7: Email,8: No JavaScript,9: Social Networks',
	DW_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'When a record is created this would be the current timestamp',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'When a record is updated this would be the current timestamp',
	DW_LOGICAL_DELETE_IND BOOLEAN COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM VARCHAR(255) COMMENT 'The data source name of this insert',
	DW_SOURCE_UPDATE_NM VARCHAR(255) COMMENT 'The data source name of this update or delete',
	DW_CURRENT_VERSION_IND BOOLEAN COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 day',
	primary key (REFERRER_TYPE_NM, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT)
);
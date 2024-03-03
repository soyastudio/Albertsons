--liquibase formatted sql
--changeset SYSTEM:SURVEY_SENTIMENT runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_LOYALTY;

create or replace TABLE SURVEY_SENTIMENT (
	SURVEY_ID VARCHAR(16777216) NOT NULL COMMENT 'Medallia unique identifier',
	SURVEY_SENTIMENT_CD VARCHAR(16777216) COMMENT 'Comments received as a part of Survey',
	SURVEY_COMMENT_TXT VARCHAR(16777216) NOT NULL COMMENT 'Phrases out of comments received part of Survey ',
	SURVEY_TYPE_CATEGORY_DSC VARCHAR(16777216) COMMENT 'like or dislike or opinions',
	COMMENT_PHRASE_TXT VARCHAR(16777216) NOT NULL COMMENT 'Different Categories for Survey like Pharmacy/Delivery/DUG',
	SURVEY_SENTIMENT_TOPIC_TXT VARCHAR(16777216) COMMENT 'Topics for survey under different categories. ',
	SURVEY_QUESTION_SEQUENCE_NBR NUMBER(38,0) NOT NULL COMMENT 'Unique Identifier of a record. Generated value',
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL COMMENT 'The date that this division instance became effective.',
	DW_LAST_EFFECTIVE_DT DATE NOT NULL COMMENT 'The last date that this division instance was effective. Thid date for the current instance will be 9999/12/31.',
	DW_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND BOOLEAN COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM VARCHAR(255) COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM VARCHAR(255) COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND BOOLEAN COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d',
	primary key (SURVEY_ID, SURVEY_QUESTION_SEQUENCE_NBR, SURVEY_COMMENT_TXT, COMMENT_PHRASE_TXT, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT)
);

--liquibase formatted sql
--changeset SYSTEM:SURVEY_SENTIMENT runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view SURVEY_SENTIMENT(
	SURVEY_ID COMMENT 'This is the Medallia unique identifier. ',
	SURVEY_SENTIMENT_CD COMMENT 'Comments received as a part of Survey',
	SURVEY_COMMENT_TXT COMMENT 'Phrases out of comments received part of Survey ',
	SURVEY_TYPE_CATEGORY_DSC COMMENT 'like or dislike or opinions',
	COMMENT_PHRASE_TXT COMMENT 'Different Categories for Survey like Pharmacy/Delivery/DUG',
	SURVEY_SENTIMENT_TOPIC_TXT COMMENT 'Topics for survey under different categories. ',
	SURVEY_QUESTION_SEQUENCE_NBR COMMENT 'Unique Identifier of a record. Generated value',
	DW_FIRST_EFFECTIVE_DT COMMENT 'The date that this division instance became effective',
	DW_LAST_EFFECTIVE_DT COMMENT 'The last date that this division instance was effective. Thid date for the current instance will be 9999/12/31.',
	DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS COMMENT 'When a record is updated this would be the current timestamp',
	DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
) COMMENT='VIEW for SURVEY_SENTIMENT'
 as
select
Survey_Id,
SURVEY_SENTIMENT_CD,
SURVEY_COMMENT_TXT,
SURVEY_TYPE_CATEGORY_DSC,
COMMENT_PHRASE_TXT,
SURVEY_SENTIMENT_TOPIC_TXT,
SURVEY_QUESTION_SEQUENCE_NBR,
DW_First_Effective_Dt,
DW_Last_Effective_Dt,
DW_CREATE_TS,
DW_LAST_UPDATE_TS,
DW_LOGICAL_DELETE_IND,
DW_SOURCE_CREATE_NM,
DW_SOURCE_UPDATE_NM,
DW_CURRENT_VERSION_IND
from EDM_CONFIRMED_PRD.DW_C_LOYALTY.SURVEY_SENTIMENT;

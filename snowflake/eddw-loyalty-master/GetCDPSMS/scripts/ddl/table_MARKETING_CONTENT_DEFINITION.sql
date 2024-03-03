--liquibase formatted sql
--changeset SYSTEM:MARKETING_CONTENT_DEFINITION runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_LOYALTY;

create or replace TABLE MARKETING_CONTENT_DEFINITION (
	CAMPAIGN_ID VARCHAR(250) NOT NULL COMMENT 'This is the CampaignID used by Ops to track the campaign. ',
	CALENDAR_YEAR_NBR NUMBER(4,0) NOT NULL COMMENT 'This column represents the Year of the week that the content definition.',
	CALENDAR_WEEK_NBR NUMBER(2,0) NOT NULL COMMENT 'This column has the week of the content definition.',
	CHANNEL_CD VARCHAR(50) NOT NULL COMMENT 'This column is the Channel that corresponds to the Marketing Content Definition e.g. SMS',
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT DATE NOT NULL COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	BANNER_NM VARCHAR(100) COMMENT 'This is the Banner/Store name',
	THEME_NM VARCHAR(16777216) COMMENT 'This is an SMS campaign theme (examples could be \"Birthday\", July 4, Super Bowl, etc.)',
	MESSAGE_HEADER_TXT VARCHAR(16777216) COMMENT 'This will be the text that appears in the beginning of the text message. It will be a concatenation of Banner and \":\"',
	MESSAGE_CONTENT_TXT VARCHAR(16777216) COMMENT 'This is the text that will appear in the SMS. Example could be \"See your new offer here!\"',
	MESSAGE_URL_TXT VARCHAR(16777216) COMMENT 'This would be root URL that Ops will define.',
	MESSAGE_FOOTER_TXT VARCHAR(16777216) COMMENT ' This would be the text that appears at the end of a text message. Typically, this will be legally required language like \"Reply STOP to Stop, Help for Help. Msg & Data rates apply\" ',
	DW_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND BOOLEAN COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM VARCHAR(255) COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM VARCHAR(255) COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND BOOLEAN COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d',
	primary key (CAMPAIGN_ID, CALENDAR_YEAR_NBR, CALENDAR_WEEK_NBR, CHANNEL_CD, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT)
)COMMENT='This table has the CDP (Customer Data Platform) Marketing Content Definition data.'
;

--liquibase formatted sql
--changeset SYSTEM:MARKETING_CONTENT_DEFINITION runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view MARKETING_CONTENT_DEFINITION(
	CAMPAIGN_ID COMMENT 'This is the CampaignID used by Ops to track the campaign. ',
	DW_FIRST_EFFECTIVE_DT COMMENT 'The date the record was inserted. For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT COMMENT 'for the current record this is ''12/31/9999''. for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	CALENDAR_YEAR_NBR COMMENT 'This column represents the Year of the week that the content definition.',
	CALENDAR_WEEK_NBR COMMENT 'This column has the week of the content definition.',
	CHANNEL_CD COMMENT 'This column is the Channel that corresponds to the Marketing Content Definition e.g. SMS',
	BANNER_NM COMMENT 'This is the Banner/Store name',
	THEME_NM COMMENT 'This is an SMS campaign theme (examples could be \"Birthday\", July 4, Super Bowl, etc.)',
	MESSAGE_HEADER_TXT COMMENT 'This will be the text that appears in the beginning of the text message. It will be a concatenation of Banner and \":\"',
	MESSAGE_CONTENT_TXT COMMENT 'This is the text that will appear in the SMS. Example could be \"See your new offer here!\"',
	MESSAGE_URL_TXT COMMENT 'This would be root URL that Ops will define.',
	MESSAGE_FOOTER_TXT COMMENT 'This would be the text that appears at the end of a text message. Typically, this will be legally required language like \"Reply STOP to Stop, Help for Help. Msg & Data rates apply\"',
	DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS COMMENT 'When a record is updated this would be the current timestamp',
	DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
) COMMENT='VIEW for MARKETING_CONTENT_DEFINITION'
 as
select
Campaign_Id,
DW_First_Effective_Dt,
DW_Last_Effective_Dt,
Calendar_Year_Nbr,
Calendar_Week_Nbr,
Channel_Cd,
Banner_Nm,
Theme_Nm,
Message_Header_Txt,
Message_Content_Txt,
Message_URL_Txt,
Message_Footer_Txt,
DW_CREATE_TS,
DW_LAST_UPDATE_TS,
DW_LOGICAL_DELETE_IND,
DW_SOURCE_CREATE_NM,
DW_SOURCE_UPDATE_NM,
DW_CURRENT_VERSION_IND
from EDM_CONFIRMED_PRD.DW_C_LOYALTY.MARKETING_CONTENT_DEFINITION;

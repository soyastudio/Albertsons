--liquibase formatted sql
--changeset SYSTEM:EPISODIC_EVENT runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view EPISODIC_EVENT(
	PROGRAM_ID COMMENT 'Albertson''s program identifier.',
	EVENT_ID COMMENT 'Unique event identifier.',
	REQUEST_TIME_TS COMMENT 'Time of action.',
	SESSION_ID COMMENT 'Session.',
	PAGE_NM COMMENT 'Pagename.',
	CATEGORY_DSC COMMENT 'Category.',
	ACTION_CD COMMENT 'Action.',
	LABEL_DSC COMMENT 'Label.',
	LABEL_VALUE_NBR COMMENT 'Value.',
	EXTRACT_TS COMMENT 'The timestamp the record was queried.',
	EVENT_NBR COMMENT 'This field calls out the Number of urchin events.',
	EVENT_REFERENCE_ID COMMENT 'This field represents offer id, vpn id, receipe id depending on the urchin events.',
	DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d',
	DW_FIRST_EFFECTIVE_DT COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day'
) COMMENT='VIEW for Episodic_Event'
 as
select
Program_Id,
Event_Id,
Request_Time_Ts,
Session_Id,
Page_Nm,
Category_Dsc,
Action_Cd,
Label_Dsc,
Label_Value_Nbr,
Extract_Ts,
Event_Nbr,
Event_Reference_Id,
Dw_Create_Ts,
Dw_Last_Update_Ts,
Dw_Logical_Delete_Ind,
Dw_Source_Create_Nm,
Dw_Source_Update_Nm,
Dw_Current_Version_Ind,
Dw_First_Effective_Dt,
Dw_Last_Effective_Dt 
from EDM_CONFIRMED_PRD.DW_C_LOYALTY.Episodic_Event;

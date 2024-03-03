--liquibase formatted sql
--changeset SYSTEM:EPISODIC_OAMOE_EARN runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view EPISODIC_OAMOE_EARN(
	PROGRAM_ID COMMENT 'Albertson''s program identifier.',
	OAMOE_ID COMMENT 'Unique identifier.',
	HOUSEHOLD_ID COMMENT 'Household ID.',
	ACTION_CD COMMENT 'Action.',
	PLAY_EARNED_NBR COMMENT 'The Number of plays earned.',
	CREATED_TS COMMENT 'The Transaction time.',
	EXTRACT_TS COMMENT 'The timestamp the record was queried.',
	DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
	DW_FIRST_EFFECTIVE_DT COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	DW_LAST_UPDATE_TS COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
) COMMENT='VIEW for Episodic_Oamoe_Earn'
 as
select
Program_Id,
Oamoe_Id,
Household_Id,
Action_Cd,
Play_Earned_Nbr,
Created_Ts,
Extract_Ts,         
Dw_Create_Ts,
Dw_First_Effective_Dt,
Dw_Last_Effective_Dt,
Dw_Last_Update_Ts,
Dw_Logical_Delete_Ind,
Dw_Source_Create_Nm,
Dw_Source_Update_Nm,
Dw_Current_Version_Ind
from EDM_CONFIRMED_PRD.DW_C_LOYALTY.Episodic_Oamoe_Earn;

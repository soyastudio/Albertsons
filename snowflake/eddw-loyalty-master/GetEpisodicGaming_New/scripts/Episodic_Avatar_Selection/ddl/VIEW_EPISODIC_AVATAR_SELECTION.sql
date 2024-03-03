--liquibase formatted sql
--changeset SYSTEM:EPISODIC_AVATAR_SELECTION runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_DB_NAME_VIEW>>;
use schema <<EDM_DB_NAME_VIEW>>.DW_VIEWS;

create or replace view EPISODIC_AVATAR_SELECTION(
	Program_Id COMMENT 'Unique Identifier of the episodic program',
	Avatar_Selection_Id COMMENT 'Unique transaction id for each avatar selection',
	Dw_First_Effective_Dt COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	Dw_Last_Effective_Dt COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	Household_Id COMMENT 'Unique identifier of the Household. The ''aggregateId'' in CHMS service is the HHID unlike the other services where the ''aggegatedid'' we map to the CustomerId',
	Avatar_Nm COMMENT 'Name of the avatar selected',
	Times_Selected_Cnt COMMENT 'number of times avatar has been reselected',
	Created_Ts COMMENT 'Record created timestamp on source system',
	Last_Updated_Ts COMMENT 'record last updated timestamp on source system',
	Extract_Ts COMMENT 'record queried timestamp',
	Dw_Create_Ts COMMENT 'The timestamp the record was inserted.',
	Dw_Last_Update_Ts COMMENT 'When a record is updated  this would be the current timestamp',
	Dw_Logical_Delete_Ind COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	Dw_Source_Create_Nm COMMENT 'The Bod (data source) name of this insert.',
	Dw_Source_Update_Nm COMMENT 'The Bod (data source) name of this update or delete.',
	Dw_Current_Version_Ind COMMENT 'set to yes when the current record is deleted,Â  the Last Effective date on this record is still set to beÂ  current date -1 d'
) COMMENT='VIEW for EPISODIC_AVATAR_SELECTION' as
select
PROGRAM_ID,
AVATAR_SELECTION_ID,
DW_FIRST_EFFECTIVE_DT,
DW_LAST_EFFECTIVE_DT,
HOUSEHOLD_ID,
AVATAR_NM,
TIMES_SELECTED_CNT,
CREATED_TS,
LAST_UPDATED_TS,
EXTRACT_TS,
DW_CREATE_TS,
DW_LAST_UPDATE_TS,
DW_LOGICAL_DELETE_IND,
DW_SOURCE_CREATE_NM,
DW_SOURCE_UPDATE_NM,
DW_CURRENT_VERSION_IND
from <<EDM_DB_NAME>>.DW_C_LOYALTY.EPISODIC_AVATAR_SELECTION;

--liquibase formatted sql
--changeset SYSTEM:EPISODIC_PROFILE runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view EPISODIC_PROFILE(
	PROGRAM_ID COMMENT 'Albertson''s program identifier.',
	HOUSEHOLD_ID COMMENT 'Unique identifier of the Household. The ''aggregateId'' in CHMS service is the HHID unlike the other services where the ''aggegatedid'' we map to the CustomerId',
	PROFILE_ID COMMENT 'The Merkle internal id.',
	REGISTRATION_DT COMMENT 'The Time of registration.',
	RULES_ACCEPTED_TS COMMENT 'The Time of rules accepted.',
	LAST_UPDATED_TS COMMENT 'The timestamp the record was last updated.',
	EXTRACT_TS COMMENT 'The timestamp the record was queried.',
	DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d',
	DW_FIRST_EFFECTIVE_DT COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day'
) COMMENT='VIEW for EPISODIC_PROFILE'
 as
select
Program_Id,
Household_Id, 
Profile_Id,
Registration_Dt,
Rules_Accepted_Ts,
Last_Updated_Ts,
Extract_Ts,
DW_CREATE_TS,
DW_LAST_UPDATE_TS,
DW_LOGICAL_DELETE_IND,
DW_SOURCE_CREATE_NM,
DW_SOURCE_UPDATE_NM,
DW_CURRENT_VERSION_IND,
DW_First_Effective_Dt,
DW_Last_Effective_Dt
from EDM_CONFIRMED_PRD.DW_C_LOYALTY.EPISODIC_PROFILE;

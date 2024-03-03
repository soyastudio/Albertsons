--liquibase formatted sql
--changeset SYSTEM:views runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view PET_MEDICATION(
	HOUSEHOLD_ID,
	PET_ID COMMENT 'Unique Identifier for each pet profile created',
	MEDICATION_NM COMMENT 'Medication used by pet',
	DW_FIRST_EFFECTIVE_DT COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
) as
Select 
    HOUSEHOLD_ID,
	PET_ID,
	MEDICATION_NM,
	DW_FIRST_EFFECTIVE_DT,
	DW_LAST_EFFECTIVE_DT,
	DW_CREATE_TS,
	DW_LAST_UPDATE_TS,
	DW_LOGICAL_DELETE_IND,
	DW_SOURCE_CREATE_NM,
	DW_SOURCE_UPDATE_NM,
	DW_CURRENT_VERSION_IND
FROM <<EDM_DB_NAME>>.DW_C_LOYALTY.PET_MEDICATION;


create or replace view PET_MEDICAL_CONDITION(
	HOUSEHOLD_ID,
	PET_ID COMMENT 'Unique Identifier for each pet profile created',
	MEDICAL_CONDITION_NM COMMENT 'Medical Condition of the pet',
	DW_FIRST_EFFECTIVE_DT COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
) as
Select 
    HOUSEHOLD_ID,
	PET_ID,
	MEDICAL_CONDITION_NM,
	DW_FIRST_EFFECTIVE_DT,
	DW_LAST_EFFECTIVE_DT,
	DW_CREATE_TS,
	DW_LAST_UPDATE_TS,
	DW_LOGICAL_DELETE_IND,
	DW_SOURCE_CREATE_NM,
	DW_SOURCE_UPDATE_NM,
	DW_CURRENT_VERSION_IND
FROM <<EDM_DB_NAME>>.DW_C_LOYALTY.PET_MEDICAL_CONDITION;


create or replace view PET_BREED(
	HOUSEHOLD_ID,
	PET_ID COMMENT 'Unique Identifier for each pet profile created',
	BREED_NM COMMENT 'Breed of the Pet',
	DW_FIRST_EFFECTIVE_DT COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
) as
Select 
    HOUSEHOLD_ID,
	PET_ID,
	BREED_NM,
	DW_FIRST_EFFECTIVE_DT,
	DW_LAST_EFFECTIVE_DT,
	DW_CREATE_TS,
	DW_LAST_UPDATE_TS,
	DW_LOGICAL_DELETE_IND,
	DW_SOURCE_CREATE_NM,
	DW_SOURCE_UPDATE_NM,
	DW_CURRENT_VERSION_IND
FROM <<EDM_DB_NAME>>.DW_C_LOYALTY.PET_BREED;


create or replace view PET_ALLERGY(
	HOUSEHOLD_ID,
	PET_ID COMMENT 'Unique Identifier for each pet profile created',
	ALLERGY_NM COMMENT 'Allergy Name',
	DW_FIRST_EFFECTIVE_DT COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
) as
Select 
    HOUSEHOLD_ID,
	PET_ID COMMENT,
	ALLERGY_NM,
	DW_FIRST_EFFECTIVE_DT,
	DW_LAST_EFFECTIVE_DT,
	DW_CREATE_TS,
	DW_LAST_UPDATE_TS,
	DW_LOGICAL_DELETE_IND,
	DW_SOURCE_CREATE_NM,
	DW_SOURCE_UPDATE_NM,
	DW_CURRENT_VERSION_IND
FROM <<EDM_DB_NAME>>.DW_C_LOYALTY.PET_ALLERGY;


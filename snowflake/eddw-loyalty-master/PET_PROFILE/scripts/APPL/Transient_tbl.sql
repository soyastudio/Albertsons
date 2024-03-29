--liquibase formatted sql
--changeset SYSTEM:Transient_tbl runOnChange:true splitStatements:false OBJECT_TYPE:TABLE

use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_STAGE;

create or replace TRANSIENT TABLE PET_ALLERGY_WRK (
	HOUSEHOLD_ID NUMBER(38,0),
	PET_ID NUMBER(38,0),
	ALLERGY_NM VARCHAR(16777216),
	DW_LOGICAL_DELETE_IND BOOLEAN,
	FILENAME VARCHAR(16777216),
	DML_TYPE VARCHAR(1),
	SAMEDAY_CHG_IND NUMBER(1,0)
);


create or replace TRANSIENT TABLE PET_BREED_WRK (
	HOUSEHOLD_ID NUMBER(38,0),
	PET_ID NUMBER(38,0),
	BREED_NM VARCHAR(16777216),
	DW_LOGICAL_DELETE_IND BOOLEAN,
	FILENAME VARCHAR(16777216),
	DML_TYPE VARCHAR(1),
	SAMEDAY_CHG_IND NUMBER(1,0)
);


create or replace TRANSIENT TABLE PET_MEDICATION_WRK (
	HOUSEHOLD_ID NUMBER(38,0),
	PET_ID NUMBER(38,0),
	MEDICATION_NM VARCHAR(16777216),
	DW_LOGICAL_DELETE_IND BOOLEAN,
	FILENAME VARCHAR(16777216),
	DML_TYPE VARCHAR(1),
	SAMEDAY_CHG_IND NUMBER(1,0)
);

create or replace TRANSIENT TABLE PET_MEDICAL_CONDITION_WRK (
	HOUSEHOLD_ID NUMBER(38,0),
	PET_ID NUMBER(38,0),
	MEDICAL_CONDITION_NM VARCHAR(16777216),
	DW_LOGICAL_DELETE_IND BOOLEAN,
	FILENAME VARCHAR(16777216),
	DML_TYPE VARCHAR(1),
	SAMEDAY_CHG_IND NUMBER(1,0)
);

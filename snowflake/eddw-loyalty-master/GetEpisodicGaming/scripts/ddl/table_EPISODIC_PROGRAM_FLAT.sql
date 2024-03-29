--liquibase formatted sql
--changeset SYSTEM:EPISODIC_PROGRAM_FLAT runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME_R>>;
use schema <<EDM_DB_NAME_R>>.DW_R_LOYALTY;

create or replace TABLE EPISODIC_PROGRAM_FLAT (
	PROGRAM_ID VARCHAR(16777216),
	PROGRAM_NAME VARCHAR(16777216),
	PROGRAM_LABEL VARCHAR(16777216),
	MERKLE_PROMO_ID VARCHAR(16777216),
	START_DATE VARCHAR(16777216),
	END_DATE VARCHAR(16777216),
	EXTRACT_TS VARCHAR(16777216),
	DW_CREATE_TS VARCHAR(16777216),
	FILE_NAME VARCHAR(16777216)
);

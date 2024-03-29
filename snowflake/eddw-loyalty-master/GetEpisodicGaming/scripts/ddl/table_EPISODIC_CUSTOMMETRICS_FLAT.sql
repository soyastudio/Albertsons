--liquibase formatted sql
--changeset SYSTEM:EPISODIC_CUSTOMMETRICS_FLAT runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME_R>>;
use schema <<EDM_DB_NAME_R>>.DW_R_LOYALTY;

create or replace TABLE EPISODIC_CUSTOMMETRICS_FLAT (
	CUSTOM_METRICS_ID VARCHAR(16777216),
	HOUSEHOLD_ID VARCHAR(16777216),
	METRIC VARCHAR(16777216),
	VALUE VARCHAR(16777216),
	CREATE_TS VARCHAR(16777216),
	PROGRAM_ID VARCHAR(16777216),
	EXTRACT_TS VARCHAR(16777216),
	DW_CREATE_TS VARCHAR(16777216),
	FILE_NAME VARCHAR(16777216)
);

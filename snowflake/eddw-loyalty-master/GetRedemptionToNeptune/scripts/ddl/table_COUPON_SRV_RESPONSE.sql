--liquibase formatted sql
--changeset SYSTEM:COUPON_SRV_RESPONSE runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database EDM_CONFIRMED_OUT_PRD;
use schema DW_DCAT;

create or replace TABLE COUPON_SRV_RESPONSE (
	ID NUMBER(25,0) NOT NULL autoincrement,
	SRC_NM VARCHAR(5000),
	OFFSET VARCHAR(15),
	INPUT_MESSAGE VARCHAR(5000),
	STATUS VARCHAR(25),
	STATUS_MESSAGE VARCHAR(5000),
	CREATION_TS TIMESTAMP_NTZ(9),
	UPDATE_TS TIMESTAMP_NTZ(9),
	RETRY_COUNT NUMBER(25,0),
	UNIQUE_REFERENCE VARCHAR(500),
	KEY VARCHAR(500),
	primary key (ID)
);
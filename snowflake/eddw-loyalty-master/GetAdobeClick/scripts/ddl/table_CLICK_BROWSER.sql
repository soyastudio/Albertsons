--liquibase formatted sql
--changeset SYSTEM:CLICK_BROWSER runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database EDM_CONFIRMED_PRD;
use schema DW_C_USER_ACTIVITY;

create or replace TABLE CLICK_BROWSER (
	BROWSER_ID VARCHAR(16777216),
	BROWSER_NM VARCHAR(16777216),
	DW_CREATETS TIMESTAMP_LTZ(9)
)COMMENT='This table contains information about CLICK_BROWSER'
;
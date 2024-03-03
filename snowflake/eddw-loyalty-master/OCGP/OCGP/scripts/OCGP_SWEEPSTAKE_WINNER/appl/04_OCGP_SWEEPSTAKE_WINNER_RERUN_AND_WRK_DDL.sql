--liquibase formatted sql
--changeset SYSTEM:OCGP_SWEEPSTAKE_WINNER_FLAT_RERUN_AND_WRK runOnChange:true splitStatements:false OBJECT_TYPE:TABLE

USE DATABASE <<EDM_DB_NAME>>;
USE SCHEMA DW_C_STAGE;

create or replace TRANSIENT TABLE OCGP_SWEEPSTAKE_WINNER_FLAT_RERUN (
	PRIZE_ID VARCHAR(16777216),
	PROGRAM_CD VARCHAR(16777216),
	RETAIL_CUSTOMER_UUID VARCHAR(16777216),
	SWEEPSTAKE_DRAW_DT VARCHAR(16777216),
	DISPLAY_NOTIFICATION_IND VARCHAR(16777216),
	CLAIM_EXPIRATION_DT VARCHAR(16777216),
	WINNING_DETAIL_TXT VARCHAR(16777216),
	WIN_STATUS_CD VARCHAR(16777216),
	WINNER_EMAIL_ADDRESS_TXT VARCHAR(16777216),
	FILENAME VARCHAR(16777216),
	DW_CREATETS TIMESTAMP_TZ(9),
	METADATA$ACTION VARCHAR(10),
	METADATA$ISUPDATE BOOLEAN,
	METADATA$ROW_ID VARCHAR(40)
);


CREATE OR REPLACE TRANSIENT TABLE OCGP_SWEEPSTAKE_WINNER_WRK
(
	PRIZE_ID VARCHAR(16777216),
	PROGRAM_CD VARCHAR(16777216),
	RETAIL_CUSTOMER_UUID VARCHAR(16777216),
	SWEEPSTAKE_DRAW_DT DATE,
	DISPLAY_NOTIFICATION_IND BOOLEAN,
	CLAIM_EXPIRATION_DT DATE,
	WINNING_DETAIL_TXT VARCHAR(16777216),
	WIN_STATUS_CD VARCHAR(16777216),
	WINNER_EMAIL_ADDRESS_TXT VARCHAR(16777216),
	DW_CREATETS TIMESTAMP_TZ(9),
	FILENAME VARCHAR(16777216),
	DW_LOGICAL_DELETE_IND BOOLEAN,
	DML_TYPE VARCHAR(1)
);


CREATE OR REPLACE TRANSIENT TABLE OCGP_SWEEPSTAKE_WINNER_TMP_WRK
(
	PRIZE_ID VARCHAR(16777216),
	PROGRAM_CD VARCHAR(16777216),
	RETAIL_CUSTOMER_UUID VARCHAR(16777216),
	SWEEPSTAKE_DRAW_DT VARCHAR(16777216),
	DISPLAY_NOTIFICATION_IND VARCHAR(16777216),
	CLAIM_EXPIRATION_DT VARCHAR(16777216),
	WINNING_DETAIL_TXT VARCHAR(16777216),
	WIN_STATUS_CD VARCHAR(16777216),
	WINNER_EMAIL_ADDRESS_TXT VARCHAR(16777216),
	FILENAME VARCHAR(16777216),
	DW_CREATETS TIMESTAMP_TZ(9),
	METADATA$ACTION VARCHAR(10),
	METADATA$ISUPDATE BOOLEAN,
	METADATA$ROW_ID VARCHAR(40)
);



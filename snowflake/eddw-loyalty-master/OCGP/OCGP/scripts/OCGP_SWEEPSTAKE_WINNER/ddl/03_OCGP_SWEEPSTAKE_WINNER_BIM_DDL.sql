--liquibase formatted sql
--changeset SYSTEM:OCGP_SWEEPSTAKE_WINNER runOnChange:true splitStatements:false OBJECT_TYPE:TABLE

USE DATABASE <<EDM_DB_NAME>>;
USE SCHEMA DW_C_LOYALTY;

CREATE OR REPLACE TABLE OCGP_SWEEPSTAKE_WINNER
(
	PRIZE_ID VARCHAR(16777216) NOT NULL,
	PROGRAM_CD VARCHAR(16777216) NOT NULL,
	RETAIL_CUSTOMER_UUID VARCHAR(16777216) NOT NULL,
	SWEEPSTAKE_DRAW_DT DATE NOT NULL,
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL,
	DW_LAST_EFFECTIVE_DT DATE NOT NULL,
	DISPLAY_NOTIFICATION_IND BOOLEAN,
	CLAIM_EXPIRATION_DT DATE,
	WINNING_DETAIL_TXT VARCHAR(16777216),
	WIN_STATUS_CD VARCHAR(16777216),
	WINNER_EMAIL_ADDRESS_TXT VARCHAR(16777216),
	DW_CREATE_TS TIMESTAMP_TZ(9),
	DW_LAST_UPDATE_TS TIMESTAMP_TZ(9),
	DW_LOGICAL_DELETE_IND BOOLEAN,
	DW_SOURCE_CREATE_NM VARCHAR(16777216),
	DW_SOURCE_UPDATE_NM VARCHAR(16777216),
	DW_CURRENT_VERSION_IND BOOLEAN,
	PRIMARY KEY (PRIZE_ID, PROGRAM_CD, RETAIL_CUSTOMER_UUID, SWEEPSTAKE_DRAW_DT, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT)
);



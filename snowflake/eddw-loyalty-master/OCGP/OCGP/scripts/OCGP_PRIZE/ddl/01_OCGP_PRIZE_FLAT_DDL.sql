--liquibase formatted sql
--changeset SYSTEM:OCGP_PRIZE_FLAT runOnChange:true splitStatements:false OBJECT_TYPE:TABLE

USE DATABASE <<EDM_DB_NAME_R>>;
USE SCHEMA DW_R_LOYALTY;

CREATE OR REPLACE TABLE OCGP_PRIZE_FLAT
(
	PRIZE_ID VARCHAR(16777216),
	PROGRAM_CD VARCHAR(16777216),
	PRIZE_NM VARCHAR(16777216),
	PRIZE_DSC VARCHAR(16777216),
	PRIZE_TYPE_CD VARCHAR(16777216),
	VENDOR_NM VARCHAR(16777216),
	PRIZE_EXPIRATION_DT VARCHAR(16777216),
	EARN_POINTS_QTY VARCHAR(16777216),
	BURN_POINTS_QTY VARCHAR(16777216),
	PRIZE_RANKING_NBR VARCHAR(16777216),
	SWEEPSTAKE_DRAW_DT VARCHAR(16777216),
	DISCLAIMER_TXT  VARCHAR(16777216),
	DIGITAL_PRIZE_IND BOOLEAN,
	BURN_PROGRAM_NM VARCHAR(16777216),
	EARN_PROGRAM_NM VARCHAR(16777216),
	PRIZE_DETAIL_TXT VARCHAR(16777216),
	FILENAME VARCHAR(16777216),
	DW_CREATETS TIMESTAMP_TZ(9)
);


CREATE OR REPLACE TABLE OCGP_PRIZE_INVENTORY_STATUS_FLAT
(
	PRIZE_ID VARCHAR(16777216),
	INITIAL_STOCK_QTY VARCHAR(16777216),
	AVAILABLE_STOCK_QTY VARCHAR(16777216),
	FILENAME VARCHAR(16777216),
	DW_CREATETS TIMESTAMP_TZ(9)
);

ALTER TABLE OCGP_PRIZE_FLAT SET CHANGE_TRACKING = TRUE; 
ALTER TABLE OCGP_PRIZE_INVENTORY_STATUS_FLAT SET CHANGE_TRACKING = TRUE; 

--liquibase formatted sql
--changeset SYSTEM:OMS_Offer_Cashier_Message_Tier runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_PRODUCT;

create or replace TABLE OMS_OFFER_CASHIER_MESSAGE_TIER (
	OMS_OFFER_ID NUMBER(38,0) NOT NULL COMMENT 'Offer Id from OMS Offer',
	CASHIER_MESSAGE_LEVEL_NBR NUMBER(38,0) NOT NULL COMMENT 'Message level cashier number',
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL COMMENT 'Record first inserted date',
	DW_LAST_EFFECTIVE_DT DATE NOT NULL COMMENT 'Record last updated date',
	CASHIER_MESSAGE_BEEP_TYPE_TXT VARCHAR(16777216) COMMENT 'Type text of cashier message',
	CASHIER_MESSAGE_BEEP_DURATION_NBR NUMBER(38,0) COMMENT 'Duration number of cashier message',
	CASHIER_MESSAGE_LINE1_TXT VARCHAR(16777216) COMMENT 'line 1 text of cashier message',
	CASHIER_MESSAGE_LINE2_TXT VARCHAR(16777216) COMMENT 'line 2 text of cashier message',
	DW_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'Record inserted timestamp',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'Record updated timestamp',
	DW_SOURCE_CREATE_NM VARCHAR(255) COMMENT 'Source Filename',
	DW_LOGICAL_DELETE_IND BOOLEAN COMMENT 'Delete scenario indicator',
	DW_CURRENT_VERSION_IND BOOLEAN COMMENT 'To find the latest record',
	DW_SOURCE_UPDATE_NM VARCHAR(255) COMMENT 'Source filename based on SCD Types',
	primary key (OMS_OFFER_ID, CASHIER_MESSAGE_LEVEL_NBR, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT)
);

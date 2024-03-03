--liquibase formatted sql
--changeset SYSTEM:TXN_FACTS_DIGITAL_NEPTUNE_TABLE_NEW runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema DW_STAGE;

	create or replace TRANSIENT  TABLE KAFKA_OUT_QUEUE_REBATE_REDEMPTION_REPROCESS_WRK (
	KEY VARCHAR(16777216),
	PAYLOAD VARCHAR(16777216)
);

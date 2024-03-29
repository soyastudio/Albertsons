--liquibase formatted sql
--changeset SYSTEM:EPE_OMS_OFFER_JSON_RERUN runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME_OUT>>;
use schema <<EDM_DB_NAME_OUT>>.DW_STAGE;

create or replace TABLE EPE_OMS_OFFER_JSON_RERUN (
	TOPIC VARCHAR(16777216),
	KEY VARCHAR(16777216),
	PAYLOAD VARCHAR(16777216),
	CREATED_TS TIMESTAMP_TZ(9),
	METADATA$ACTION VARCHAR(6),
	METADATA$ISUPDATE BOOLEAN,
	METADATA$ROW_ID VARCHAR(40)
);

--liquibase formatted sql
--changeset SYSTEM:EPE_OFFER_JSON_AUDIT runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME_OUT>>;
use schema <<EDM_DB_NAME_OUT>>.DW_DCAT;

create or replace TABLE EPE_OFFER_JSON_AUDIT (
	TOPIC VARCHAR(16777216),
	KEY VARCHAR(16777216),
	FILENAME VARCHAR(16777216),
	DW_CREATE_TS TIMESTAMP_LTZ(9)
);

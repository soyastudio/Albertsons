--liquibase formatted sql
--changeset SYSTEM:EPE_OFFER_JSON runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME_OUT>>;
use schema <<EDM_DB_NAME_OUT>>.DW_DCAT;

create or replace TABLE EPE_OFFER_JSON (
	TOPIC VARCHAR(16777216),
	KEY VARCHAR(16777216),
	PAYLOAD VARCHAR(16777216)
);

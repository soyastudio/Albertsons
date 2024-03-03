--liquibase formatted sql
--changeset SYSTEM:EPE_OMS_OFFER_JSON runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME_OUT>>;
use schema <<EDM_DB_NAME_OUT>>.DW_DCAT;

create or replace TABLE EPE_OMS_OFFER_JSON (
	TOPIC VARCHAR(16777216) COMMENT 'To load Topic Name',
	KEY VARCHAR(16777216) COMMENT 'To load Key value which contains offer details',
	PAYLOAD VARCHAR(16777216) COMMENT 'Contains all columns and values details of offer',
	DW_CREATE_TS TIMESTAMP_TZ(9) COMMENT 'shows as what time offer created'
);

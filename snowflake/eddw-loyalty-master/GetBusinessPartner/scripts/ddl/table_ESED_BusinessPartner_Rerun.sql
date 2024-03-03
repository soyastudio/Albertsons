--liquibase formatted sql
--changeset SYSTEM:ESED_BusinessPartner_Rerun runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_R_STAGE;

create or replace TABLE ESED_BUSINESSPARTNER_RERUN (
	FILENAME VARCHAR(5000),
	SRC_XML VARIANT,
	CREATED_TS TIMESTAMP_LTZ(9),
	METADATA$ACTION VARCHAR(1),
	METADATA$ISUPDATE BOOLEAN,
	METADATA$ROW_ID VARCHAR(1)
);

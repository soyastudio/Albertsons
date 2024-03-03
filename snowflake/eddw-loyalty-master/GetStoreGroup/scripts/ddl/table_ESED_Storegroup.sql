--liquibase formatted sql
--changeset SYSTEM:ESED_Storegroup runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME_R>>;
use schema <<EDM_DB_NAME_R>>.DW_R_PRODUCT;

create or replace TABLE ESED_STOREGROUP (
	FILENAME VARCHAR(2000),
	SRC_JSON VARIANT,
	CREATED_TS TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP()
);

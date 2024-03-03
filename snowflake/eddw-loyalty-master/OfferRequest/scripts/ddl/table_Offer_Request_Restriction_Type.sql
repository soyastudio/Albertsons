--liquibase formatted sql
--changeset SYSTEM:Offer_Request_Restriction_Type runOnChange:true splitStatements:false OBJECT_TYPE:TABLE

use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_PURCHASING;

create or replace TABLE OFFER_REQUEST_RESTRICTION_TYPE (
	OFFER_REQUEST_ID NUMBER(38,0) NOT NULL,
	USAGE_LIMIT_TYPE_TXT VARCHAR(50) NOT NULL,
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL,
	DW_LAST_EFFECTIVE_DT DATE NOT NULL,
	LIMIT_QTY NUMBER(38,0),
	LIMIT_WT NUMBER(14,4),
	LIMIT_VOL NUMBER(14,4),
	UNIT_OF_MEASURE_CD VARCHAR(10),
	UNIT_OF_MEASURE_NM VARCHAR(20),
	LIMIT_AMT NUMBER(14,4),
	DW_CREATE_TS TIMESTAMP_LTZ(9),
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9),
	DW_LOGICAL_DELETE_IND BOOLEAN,
	DW_SOURCE_CREATE_NM VARCHAR(255),
	DW_SOURCE_UPDATE_NM VARCHAR(255),
	DW_CURRENT_VERSION_IND BOOLEAN,
	RESTRICTION_TYPE_CD VARCHAR(50),
	RESTRICTION_TYPE_DSC VARCHAR(250),
	RESTRICTION_TYPE_SHORT_DSC VARCHAR(50),
	USAGE_LIMIT_NBR NUMBER(38,0),
	USAGE_LIMIT_PERIOD_NBR NUMBER(38,0) COMMENT 'Describes the usage limit period time for an offer request.',
	constraint XPKOFFERRESTRICITIONTYPE primary key (OFFER_REQUEST_ID, USAGE_LIMIT_TYPE_TXT, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT)
);

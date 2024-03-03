--liquibase formatted sql
--changeset SYSTEM:OMS_Offer_Redemption_Store_Group runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_PRODUCT;

create or replace TABLE OMS_OFFER_REDEMPTION_STORE_GROUP (
	OMS_OFFER_ID NUMBER(38,0) NOT NULL COMMENT 'OMS Offer Id',
	STORE_GROUP_ID NUMBER(38,0) NOT NULL COMMENT 'Store Group Id',
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL COMMENT 'DW First Effective Dt',
	DW_LAST_EFFECTIVE_DT DATE NOT NULL COMMENT 'DW Last Effective Dt',
	DW_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'DW CREATE TS',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'DW LAST UPDATE TS',
	DW_SOURCE_CREATE_NM VARCHAR(255) COMMENT 'DW SOURCE CREATE NM',
	DW_LOGICAL_DELETE_IND BOOLEAN COMMENT 'DW LOGICAL DELETE IND',
	DW_CURRENT_VERSION_IND BOOLEAN COMMENT 'DW CURRENT VERSION IND',
	DW_SOURCE_UPDATE_NM VARCHAR(255) COMMENT 'DW SOURCE UPDATE NM',
	primary key (OMS_OFFER_ID, STORE_GROUP_ID, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT)
);

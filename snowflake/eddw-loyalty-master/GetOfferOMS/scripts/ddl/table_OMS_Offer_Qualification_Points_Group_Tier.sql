--liquibase formatted sql
--changeset SYSTEM:OMS_Offer_Qualification_Points_Group_Tier runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_PRODUCT;

create or replace TABLE OMS_OFFER_QUALIFICATION_POINTS_GROUP_TIER (
	OMS_OFFER_ID NUMBER(38,0) NOT NULL COMMENT 'OMS Offer ID',
	POINTS_GROUP_ID NUMBER(38,0) NOT NULL COMMENT 'Points Group ID',
	TIER_LEVEL_NBR NUMBER(38,0) NOT NULL COMMENT 'Tier Level Number',
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL COMMENT 'DW First Effective Date',
	DW_LAST_EFFECTIVE_DT DATE NOT NULL COMMENT 'DW Last Effective Date',
	TIER_QTY NUMBER(38,0) COMMENT 'Tier Quantity',
	DW_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'DW CREATE TS',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'DW LAST UPDATE TS',
	DW_SOURCE_CREATE_NM VARCHAR(255) COMMENT 'DW SOURCE CREATE NM',
	DW_LOGICAL_DELETE_IND BOOLEAN COMMENT 'DW LOGICAL DELETE INDICATOR',
	DW_CURRENT_VERSION_IND BOOLEAN COMMENT 'DW CURRENT VERSION INDICATOR',
	DW_SOURCE_UPDATE_NM VARCHAR(255) COMMENT 'DW SOURCE UPDATE NM',
	primary key (OMS_OFFER_ID, POINTS_GROUP_ID, TIER_LEVEL_NBR, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT)
);

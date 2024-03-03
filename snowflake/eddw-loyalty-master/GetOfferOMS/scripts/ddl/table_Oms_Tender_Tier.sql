--liquibase formatted sql
--changeset SYSTEM:Oms_Tender_Tier runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_PRODUCT;

create or replace TABLE OMS_TENDER_TIER (
	OMS_OFFER_ID NUMBER(38,0) NOT NULL COMMENT 'OMS Offer',
	TENDER_TYPE_DSC VARCHAR(16777216) NOT NULL COMMENT 'TenderType Is a Payment Method',
	TIER_LEVEL_NBR NUMBER(38,0) NOT NULL COMMENT 'Since it is coming under conditions in OMS we are saying it as level 1',
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT DATE NOT NULL COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	TIER_VALUE_NBR NUMBER(10,3) COMMENT 'value entered by the user. It is an integer',
	DW_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND BOOLEAN COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM VARCHAR(255) COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM VARCHAR(255) COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND BOOLEAN COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d',
	constraint XPKOMS_TENDE_TIERS primary key (OMS_OFFER_ID, TENDER_TYPE_DSC, TIER_LEVEL_NBR, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT)
);

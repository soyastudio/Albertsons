--liquibase formatted sql
--changeset SYSTEM:Oms_Offer_Fulfillment_Channel_Type runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_PRODUCT;

create or replace TABLE OMS_OFFER_FULFILLMENT_CHANNEL_TYPE (
	FULFILLMENT_CHANNEL_TYPE_CD VARCHAR(50) NOT NULL COMMENT 'It’s a Channel Thru which the offer will be fulfilled to customers. Eg .”delivery”, “dug”, “inStorePurchase',
	FULFILLMENT_CHANNEL_IND BOOLEAN COMMENT 'It’s a indicator for the Fulfillment channel',
	FULFILLMENT_CHANNEL_DSC VARCHAR(250) COMMENT 'It’s a description of Fulfillment Channel',
	OMS_OFFER_ID NUMBER(38,0) NOT NULL COMMENT 'Internal Ofer Id',
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT DATE NOT NULL COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	DW_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND BOOLEAN COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM VARCHAR(255) COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM VARCHAR(255) COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND BOOLEAN COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d',
	constraint XPKOMS_OFFER_FULFILLMENT_CHANNEL_TYPE primary key (FULFILLMENT_CHANNEL_TYPE_CD, OMS_OFFER_ID, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT)
);

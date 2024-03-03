--liquibase formatted sql
--changeset SYSTEM:Offer_Request_Fulfillment_Channel_Type runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database EDM_CONFIRMED_PRD;
use schema EDM_CONFIRMED_PRD.DW_C_PURCHASING;

create or replace TABLE OFFER_REQUEST_FULFILLMENT_CHANNEL_TYPE (
	create or replace TABLE OFFER_REQUEST_FULFILLMENT_CHANNEL_TYPE (
	OFFER_REQUEST_ID NUMBER(38,0) NOT NULL COMMENT 'Unique identifer for each offer request created in source system',
	FULFILLMENT_CHANNEL_TYPE_CD VARCHAR(16777216) NOT NULL COMMENT 'Type of the fulfillment Channel for the offer',
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT DATE NOT NULL COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	FULFILLMENT_CHANNEL_IND BOOLEAN COMMENT 'Indicator if the offer comes under a fulfillment',
	FULFILLMENT_CHANNEL_DSC VARCHAR(50) COMMENT 'Fulfillment Channel is the Shopping Channels - under Adv Options in OR. Some of the values are Delivery/DUG/In-Store Purch/WUG',
	DW_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'The timestamp the record was inserted',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND BOOLEAN COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_UPDATE_NM VARCHAR(255) COMMENT 'The Bod (data source) name of this update or delete',
	DW_CURRENT_VERSION_IND BOOLEAN COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d',
	DW_SOURCE_CREATE_NM VARCHAR(255) COMMENT 'The Bod (data source) name of this insert',
	constraint XPKOFFER_REQUEST_FULFILLMENT_CHANNEL_TYPE primary key (OFFER_REQUEST_ID, FULFILLMENT_CHANNEL_TYPE_CD, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT)
);

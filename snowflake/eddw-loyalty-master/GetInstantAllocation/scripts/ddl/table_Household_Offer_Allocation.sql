--liquibase formatted sql
--changeset SYSTEM:Household_Offer_Allocation runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_LOYALTY;
create or replace TRANSIENT TABLE HOUSEHOLD_OFFER_ALLOCATION (
	OMS_OFFER_ID NUMBER(38,0) NOT NULL COMMENT 'offer identified in j4u system',
	HOUSEHOLD_ID NUMBER(38,0) NOT NULL COMMENT 'Unique Identifier of Household',
	REGION_ID NUMBER(38,0) NOT NULL COMMENT 'loyalty Division Identifier',
	ALLOCATION_START_TS TIMESTAMP_LTZ(9) NOT NULL COMMENT 'effective allocation start timestamp to view the offer in gallery',
	ALLOCATION_END_TS TIMESTAMP_LTZ(9) NOT NULL COMMENT 'effective allocation end timestamp to view the offer in gallery',
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT DATE NOT NULL COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	ALLOCATION_CNT NUMBER(38,0) COMMENT 'Number of offers given or removed.',
	EVENT_NM VARCHAR(16777216) COMMENT 'Type of Allocation Event, Allocation or Deallocation',
	EVENT_TS TIMESTAMP_LTZ(9) COMMENT 'Event allocation or deallocation timestamp',
	DW_CREATE_TS TIMESTAMP_LTZ(9),
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9),
	DW_LOGICAL_DELETE_IND BOOLEAN,
	DW_SOURCE_CREATE_NM VARCHAR(255),
	DW_SOURCE_UPDATE_NM VARCHAR(255),
	DW_CURRENT_VERSION_IND BOOLEAN,
	EVENT_SOURCE_NM VARCHAR(16777216) COMMENT 'Column describes the source of Allocation',
	constraint XPKOFFER_INSTANT_ALLOCATION primary key (OMS_OFFER_ID, HOUSEHOLD_ID, REGION_ID, ALLOCATION_START_TS, ALLOCATION_END_TS, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT)
);

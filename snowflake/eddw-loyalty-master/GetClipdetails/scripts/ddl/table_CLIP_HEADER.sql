--liquibase formatted sql
--changeset SYSTEM:CLIP_HEADER runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database EDM_CONFIRMED_PRD;
use schema DW_C_LOYALTY;

create or replace TABLE CLIP_HEADER (
	CLIP_SEQUENCE_ID NUMBER(38,0) NOT NULL COMMENT 'System generated key to uniquely identify Clip details',
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL COMMENT 'The timestamp the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT DATE NOT NULL COMMENT 'for the current record this is 12/31/9999 24.00.00.0000.',
	CUSTOMER_GUID VARCHAR(16777216) COMMENT 'When Customer Registered, GUID will be created',
	CLUB_CARD_NBR NUMBER(38,0) COMMENT 'Club Card Nbr',
	FACILITY_INTEGRATION_ID NUMBER(38,0) COMMENT 'Facility Integration ID',
	RETAIL_STORE_ID VARCHAR(16777216) COMMENT 'Selected store Id where the clipping is done',
	HOUSEHOLD_ID NUMBER(38,0) COMMENT 'When a customer registered with Safeway HHID will be created',
	RETAIL_CUSTOMER_UUID VARCHAR(16777216) COMMENT 'Retail Customer UUID',
	BANNER_NM VARCHAR(50) COMMENT 'Banner code used by offer providers, required field for Manufacture Offer(MF)',
	POSTAL_CD VARCHAR(50) COMMENT 'Postal code used by offer providers',
	DW_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'When a record is created  this would be the current timestamp',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'When a record is created  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND BOOLEAN COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM VARCHAR(255) COMMENT 'The data source name of this insert',
	DW_SOURCE_UPDATE_NM VARCHAR(255) COMMENT 'The data source name of this update or delete',
	DW_CURRENT_VERSION_IND BOOLEAN COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 day',
	primary key (CLIP_SEQUENCE_ID, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT),
	unique (CUSTOMER_GUID, CLUB_CARD_NBR, FACILITY_INTEGRATION_ID, RETAIL_CUSTOMER_UUID)
);
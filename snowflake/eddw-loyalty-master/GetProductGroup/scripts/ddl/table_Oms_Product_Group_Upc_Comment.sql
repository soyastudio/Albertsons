--liquibase formatted sql
--changeset SYSTEM:Oms_Product_Group_Upc_Comment runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_PRODUCT;

create or replace TABLE OMS_PRODUCT_GROUP_UPC_COMMENT (
	UPC_CD VARCHAR(14) NOT NULL COMMENT 'UPC number e.g: 3520450097',
	PRODUCT_GROUP_ID NUMBER(38,0) NOT NULL COMMENT 'Product Group Primary Key',
	DW_LAST_EFFECTIVE_TS TIMESTAMP_LTZ(9) NOT NULL,
	DW_FIRST_EFFECTIVE_TS TIMESTAMP_LTZ(9) NOT NULL,
	COMMENT_TS TIMESTAMP_LTZ(9) NOT NULL COMMENT 'Comment date time, e.g: \"2021-08-01T03:48:40.712Z\"',
	COMMENT_DSC VARCHAR(16777216) COMMENT 'History of comments how the UPC made to the list',
	DW_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND BOOLEAN COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM VARCHAR(255) COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM VARCHAR(255) COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND BOOLEAN COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d',
	COMMENT_BY_USER_ID VARCHAR(16777216) COMMENT 'Commented by User Id',
	constraint XPKOMS_PRODUCT_GROUP_UPC_COMMENT primary key (PRODUCT_GROUP_ID, UPC_CD, COMMENT_TS, DW_LAST_EFFECTIVE_TS, DW_FIRST_EFFECTIVE_TS)
);

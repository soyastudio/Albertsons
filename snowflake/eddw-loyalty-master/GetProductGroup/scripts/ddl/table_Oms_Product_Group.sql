--liquibase formatted sql
--changeset SYSTEM:Oms_Product_Group runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_PRODUCT;

create or replace TABLE OMS_PRODUCT_GROUP (
	PRODUCT_GROUP_ID NUMBER(38,0) NOT NULL COMMENT 'Product Group Primary Key',
	DW_FIRST_EFFECTIVE_TS TIMESTAMP_LTZ(9) NOT NULL,
	DW_LAST_EFFECTIVE_TS TIMESTAMP_LTZ(9) NOT NULL,
	PRODUCT_GROUP_NM VARCHAR(16777216) COMMENT 'Name of the product group',
	PRODUCT_GROUP_DSC VARCHAR(16777216) COMMENT 'Product group description',
	CREATE_TS TIMESTAMP_LTZ(9),
	UPDATE_TS TIMESTAMP_LTZ(9),
	CREATE_USER_ID VARCHAR(16777216) COMMENT 'Overall product group created or updated user id, e.g: \"rkasi01\"',
	CREATE_FIRST_NM VARCHAR(16777216) COMMENT 'First name of the user who created or updated this product group',
	CREATE_LAST_NM VARCHAR(16777216) COMMENT 'Last name of the user who created or updated this product group',
	UPDATE_USER_ID VARCHAR(16777216),
	UPDATE_FIRST_NM VARCHAR(16777216),
	DW_CREATE_TS TIMESTAMP_LTZ(9),
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9),
	DW_LOGICAL_DELETE_IND BOOLEAN,
	DW_SOURCE_CREATE_NM VARCHAR(255),
	DW_SOURCE_UPDATE_NM VARCHAR(255),
	DW_CURRENT_VERSION_IND BOOLEAN,
	PRODUCT_GROUP_TYPE_DSC VARCHAR(16777216) COMMENT 'Type of product group BASE / NONBASE',
	MOB_ID VARCHAR(16777216) COMMENT 'Master Offer Bank Id or number, business users uses this id to uniquely identify product groups ',
	PRODUCT_GROUP_VERSION_NBR VARCHAR(16777216) COMMENT 'Product Group version number represents the message structure',
	primary key (PRODUCT_GROUP_ID, DW_FIRST_EFFECTIVE_TS, DW_LAST_EFFECTIVE_TS)
)COMMENT='This table contains information about OMS_PRODUCT_GROUP'
;

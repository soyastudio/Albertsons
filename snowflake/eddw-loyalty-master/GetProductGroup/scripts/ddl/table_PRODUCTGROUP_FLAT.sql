--liquibase formatted sql
--changeset SYSTEM:PRODUCTGROUP_FLAT runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_PRODUCT;

create or replace TABLE PRODUCTGROUP_FLAT (
	FILENAME VARCHAR(16777216),
	PAYLOADPART VARCHAR(16777216),
	PAGENUM VARCHAR(16777216),
	TOTALPAGES VARCHAR(16777216),
	ENTITYID VARCHAR(16777216),
	PAYLOADTYPE VARCHAR(16777216),
	ENTITYTYPE VARCHAR(16777216),
	SOURCEACTION VARCHAR(16777216),
	PAYLOAD_ID VARCHAR(16777216),
	PAYLOAD_NAME VARCHAR(16777216),
	PAYLOAD_DESCRIPTION VARCHAR(16777216),
	PAYLOAD_PRODUCTGROUPIDS_UPCIDS ARRAY,
	PAYLOAD_PRODUCTGROUPIDS_DEPARTMENTSECTIONIDS ARRAY,
	PAYLOAD_PRODUCTGROUPIDS_MANUFACTUREIDS ARRAY,
	PAYLOAD_CREATETS VARCHAR(16777216),
	PAYLOAD_UPDATETS VARCHAR(16777216),
	PAYLOAD_CREATEDUSER_USERID VARCHAR(16777216),
	PAYLOAD_CREATEDUSER_FIRSTNAME VARCHAR(16777216),
	PAYLOAD_CREATEDUSER_LASTNAME VARCHAR(16777216),
	PAYLOAD_UPDATEDUSER_USERID VARCHAR(16777216),
	PAYLOAD_UPDATEDUSER_FIRSTNAME VARCHAR(16777216),
	PAYLOAD_UPDATEDUSER_LASTNAME VARCHAR(16777216),
	LASTUPDATETS VARCHAR(16777216),
	DW_CREATE_TS TIMESTAMP_LTZ(9)
);
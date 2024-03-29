--liquibase formatted sql
--changeset SYSTEM:GETSHOPPINGLIST_FLAT runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME_R>>;
use schema <<EDM_DB_NAME_R>>.DW_R_LOYALTY;

create or replace TABLE GETSHOPPINGLIST_FLAT (
	FILENAME VARCHAR(16777216),
	BODNM VARCHAR(16777216),
	APIUID VARCHAR(16777216),
	BANNER VARCHAR(16777216),
	CARD VARCHAR(16777216),
	HHID VARCHAR(16777216),
	POSTALCD VARCHAR(16777216),
	STOREID VARCHAR(16777216),
	SWYAPIKEY VARCHAR(16777216),
	SWYVERSION VARCHAR(16777216),
	USERID VARCHAR(16777216),
	SWYLOGONID VARCHAR(16777216),
	CLIPID VARCHAR(16777216),
	CLIPSRC VARCHAR(16777216),
	CLIPTS VARCHAR(16777216),
	CLIPTYPE VARCHAR(16777216),
	EXTOFFERID VARCHAR(16777216),
	OFFERID VARCHAR(16777216),
	OFFERTYPE VARCHAR(16777216),
	PROGRAM VARCHAR(16777216),
	PROVIDER VARCHAR(16777216),
	SRCAPPID VARCHAR(16777216),
	VNDRBANNERCD VARCHAR(16777216),
	DW_CREATETS TIMESTAMP_LTZ(9),
	EVENTTS VARCHAR(16777216)
);

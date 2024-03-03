
--liquibase formatted sql
--changeset SYSTEM:RETAIL_CUSTOMER_HH_LOYALTY_PROGRAM_REPROCESS runOnChange:true splitStatements:false OBJECT_TYPE:TABLE

use database <<EDM_DB_NAME>>;
use schema DW_STAGE;
	
	
create or replace TRANSIENT TABLE CLIP_DETAILS_TMP_DIGITAL cluster by (HOUSEHOLD_ID,OFFER_ID)(
	DW_CURRENT_VERSION_IND BOOLEAN,
	CLIP_SEQUENCE_ID NUMBER(38,0),
	HOUSEHOLD_ID NUMBER(38,0),
	CUSTOMER_GUID VARCHAR(16777216),
	RETAIL_STORE_ID VARCHAR(16777216),
	CLIP_ID VARCHAR(250),
	CLIP_TS TIMESTAMP_LTZ(9),
	OFFER_ID NUMBER(38,0),
	CLUB_CARD_NBR NUMBER(38,0),
	BANNER_NM VARCHAR(16777216)
);


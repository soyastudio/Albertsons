--liquibase formatted sql
--changeset SYSTEM:ONE_TAG_CAROUSEL_FLAT_RERUN runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database EDM_REFINED_<<ENV>>;
use schema DW_R_STAGE;

CREATE OR REPLACE TRANSIENT TABLE ONE_TAG_CAROUSEL_FLAT_RERUN
(	event_hr   NUMBER
,   event_id	VARCHAR
, 	event_subevent	VARCHAR
,   eventtime	timestamp
, 	carousel_section	VARCHAR
, 	carousel_detail	VARCHAR
, 	carousel_lp	NUMBER(16,5)
, 	carousel_pfm	VARCHAR
, 	carousel_pid	VARCHAR
, 	carousel_units	VARCHAR
,	METADATA$ACTION VARCHAR(6)
,	METADATA$ISUPDATE BOOLEAN
,	METADATA$ROW_ID VARCHAR(40)
 	);

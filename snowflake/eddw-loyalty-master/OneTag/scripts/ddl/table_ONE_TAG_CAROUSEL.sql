--liquibase formatted sql
--changeset SYSTEM:ONE_TAG_CAROUSEL runOnChange:true splitStatements:false OBJECT_TYPE:TABLE

use database <<EDM_DB_R_NAME>>;
use schema <<EDM_SCHEMA_R_TARGET>>;

CREATE OR REPLACE TABLE ONE_TAG_CAROUSEL
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
 	) Change_Tracking = TRUE;

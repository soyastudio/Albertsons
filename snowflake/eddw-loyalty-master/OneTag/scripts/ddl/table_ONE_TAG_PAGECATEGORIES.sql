--liquibase formatted sql
--changeset SYSTEM:ONE_TAG_PAGECATEGORIES runOnChange:true splitStatements:false OBJECT_TYPE:TABLE

use database <<EDM_DB_R_NAME>>;
use schema <<EDM_SCHEMA_R_TARGET>>;

CREATE OR REPLACE TABLE ONE_TAG_PAGECATEGORIES
(	event_hr   NUMBER
,	event_id	VARCHAR
, 	event_subevent	VARCHAR
,   eventtime	timestamp
, 	search_pagecategories	VARCHAR
 	);

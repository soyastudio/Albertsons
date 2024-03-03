--liquibase formatted sql
--changeset SYSTEM:ONE_TAG_CARTINFO runOnChange:true splitStatements:false OBJECT_TYPE:TABLE

use database <<EDM_DB_R_NAME>>;
use schema <<EDM_SCHEMA_R_TARGET>>;

CREATE OR REPLACE TABLE ONE_TAG_CARTINFO
(	event_hr   NUMBER
, 	event_id	VARCHAR
, 	event_subevent	VARCHAR
,   eventtime	timestamp
, 	cart_cartinfo_mpcart_cart_id	VARCHAR
, 	cart_cartinfo_mpcart_cart_type	VARCHAR
, 	cart_cartinfo_mpcart_cart_skus	VARCHAR
, 	cart_cartinfo_mpcart_cart_total	VARCHAR
, 	cart_cartinfo_mpcart_vendor	VARCHAR
 	);

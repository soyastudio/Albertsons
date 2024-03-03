--liquibase formatted sql
--changeset SYSTEM:ONE_TAG_CART_TRANSACTION runOnChange:true splitStatements:false OBJECT_TYPE:TABLE

use database <<EDM_DB_R_NAME>>;
use schema <<EDM_SCHEMA_R_TARGET>>;

CREATE OR REPLACE TABLE ONE_TAG_CART_TRANSACTION
(	event_hr   NUMBER
,	event_id	VARCHAR
, 	event_subevent	VARCHAR
,   eventtime	timestamp
, 	cart_transaction_subttlmpcarts_sellerid	VARCHAR
, 	cart_transaction_subttlmpcarts_subtotal	NUMBER
 	);

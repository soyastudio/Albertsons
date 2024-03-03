--liquibase formatted sql
--changeset SYSTEM:ONE_TAG_CARTITEM runOnChange:true splitStatements:false OBJECT_TYPE:TABLE

use database <<EDM_DB_R_NAME>>;
use schema <<EDM_SCHEMA_R_TARGET>>;

CREATE OR REPLACE TABLE ONE_TAG_CARTITEM
(	event_hr   NUMBER
,	event_id	VARCHAR
, 	event_subevent	VARCHAR
,   eventtime	timestamp
, 	cart_item_prdid	VARCHAR
, 	cart_item_prdnam	VARCHAR
, 	cart_item_units	VARCHAR
, 	cart_item_bp	VARCHAR
, 	cart_item_lp	VARCHAR
, 	cart_item_itemp	VARCHAR
, 	cart_item_totp	VARCHAR
, 	cart_item_inv	VARCHAR
, 	cart_item_cpnflg	VARCHAR
, 	cart_item_cpnclpd	VARCHAR
, 	cart_item_cpnapld	VARCHAR
, 	cart_item_cpnid	VARCHAR
, 	cart_item_ccsav	VARCHAR
, 	cart_item_j4usav	VARCHAR
, 	cart_item_rsav	VARCHAR
, 	cart_item_esav	VARCHAR
, 	cart_item_pcsav	VARCHAR
, 	cart_item_catinvalidid	VARCHAR
, 	cart_item_catinvalidname	VARCHAR
, 	cart_item_catinvalidnone	VARCHAR
, 	cart_item_tsav	VARCHAR
, 	cart_item_ppunit	VARCHAR
, 	cart_item_utyp	VARCHAR
, 	cart_item_otyp	VARCHAR
, 	cart_item_otext	VARCHAR
, 	cart_item_isfound	boolean
 	);

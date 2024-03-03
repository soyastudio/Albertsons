--liquibase formatted sql
--changeset SYSTEM:ONE_TAG_CARTINFO runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_DB_NAME_VIEWS>>;
use schema <<EDM_SCHEMA_VIEWS>>;

CREATE OR REPLACE VIEW  ONE_TAG_CARTINFO
(	
    event_id
, 	event_subevent
,	event_time
, 	cart_cartinfo_mpcart_cart_id
, 	cart_cartinfo_mpcart_cart_type
, 	cart_cartinfo_mpcart_cart_skus
, 	cart_cartinfo_mpcart_cart_total
, 	cart_cartinfo_mpcart_vendor
) COPY GRANTS comment = 'VIEW FOR ONE_TAG_CARTINFO' 
AS
SELECT
 	event_id
, 	event_subevent
, 	eventtime
, 	cart_cartinfo_mpcart_cart_id
, 	cart_cartinfo_mpcart_cart_type
, 	cart_cartinfo_mpcart_cart_skus
, 	cart_cartinfo_mpcart_cart_total
, 	cart_cartinfo_mpcart_vendor
FROM <<EDM_DB_R_NAME>>.<<EDM_SCHEMA_R_TARGET>>."ONE_TAG_CARTINFO";

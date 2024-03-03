--liquibase formatted sql
--changeset SYSTEM:ONE_TAG_CARTITEM runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_DB_NAME_VIEWS>>;
use schema <<EDM_SCHEMA_VIEWS>>;

CREATE OR REPLACE VIEW  ONE_TAG_CARTITEM
(	
    event_id
, 	event_subevent
,	event_time
, 	cart_item_prdid
, 	cart_item_prdnam
, 	cart_item_units
, 	cart_item_bp
, 	cart_item_lp
, 	cart_item_itemp
, 	cart_item_totp
, 	cart_item_inv
, 	cart_item_cpnflg
, 	cart_item_cpnclpd
, 	cart_item_cpnapld
, 	cart_item_cpnid
, 	cart_item_ccsav
, 	cart_item_j4usav
, 	cart_item_rsav
, 	cart_item_esav
, 	cart_item_pcsav
, 	cart_item_catinvalidid
, 	cart_item_catinvalidname	
, 	cart_item_catinvalidnone
, 	cart_item_tsav
, 	cart_item_ppunit
, 	cart_item_utyp
, 	cart_item_otyp
, 	cart_item_otext
, 	cart_item_isfound
) COPY GRANTS comment = 'VIEW FOR ONE_TAG_CARTITEM' 
AS
SELECT
 	event_id
, 	event_subevent
,	eventtime
, 	cart_item_prdid
, 	cart_item_prdnam
, 	cart_item_units
, 	cart_item_bp
, 	cart_item_lp
, 	cart_item_itemp
, 	cart_item_totp
, 	cart_item_inv
, 	cart_item_cpnflg
, 	cart_item_cpnclpd
, 	cart_item_cpnapld
, 	cart_item_cpnid
, 	cart_item_ccsav
, 	cart_item_j4usav
, 	cart_item_rsav
, 	cart_item_esav
, 	cart_item_pcsav
, 	cart_item_catinvalidid
, 	cart_item_catinvalidname	
, 	cart_item_catinvalidnone
, 	cart_item_tsav
, 	cart_item_ppunit
, 	cart_item_utyp
, 	cart_item_otyp
, 	cart_item_otext
, 	cart_item_isfound
FROM <<EDM_DB_R_NAME>>.<<EDM_SCHEMA_R_TARGET>>."ONE_TAG_CARTITEM";

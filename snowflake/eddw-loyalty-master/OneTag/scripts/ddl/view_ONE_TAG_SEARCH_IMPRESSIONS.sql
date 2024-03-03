--liquibase formatted sql
--changeset SYSTEM:ONE_TAG_SEARCH_IMPRESSIONS runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_DB_NAME_VIEWS>>;
use schema <<EDM_SCHEMA_VIEWS>>;

CREATE OR REPLACE VIEW  ONE_TAG_SEARCH_IMPRESSIONS
(	
    event_id
, 	event_subevent
, 	event_time
, 	search_impressions_pid
, 	search_impressions_pname
, 	search_impressions_fname
, 	search_impressions_units
, 	search_impressions_utyp
, 	search_impressions_ppunit
, 	search_impressions_lp
, 	search_impressions_carouselsec
, 	search_impressions_bp
, 	search_impressions_ptype
, 	search_impressions_cpnflg
, 	search_impressions_cpnclpd
, 	search_impressions_cpnid
, 	search_impressions_cpnapld
, 	search_impressions_aid
, 	search_impressions_upc
, 	search_impressions_pfmdtl
, 	search_impressions_ics
, 	search_impressions_mto
, 	search_impressions_deptnam
, 	search_impressions_lbs
, 	search_impressions_each
, 	search_impressions_aisle
, 	search_impressions_shelf
, 	search_impressions_offtyp
, 	search_impressions_offtext
, 	search_impressions_carouselsz
, 	search_impressions_isrcmnd
, 	search_impressions_iqn
, 	search_impressions_iv
, 	search_impressions_sellerid
, 	search_impressions_sellernam
) COPY GRANTS comment = 'VIEW FOR ONE_TAG_SEARCH_IMPRESSIONS' 
AS
SELECT
 	event_id
, 	event_subevent
,	eventtime
, 	search_impressions_pid
, 	search_impressions_pname
, 	search_impressions_fname
, 	search_impressions_units
, 	search_impressions_utyp
, 	search_impressions_ppunit
, 	search_impressions_lp
, 	search_impressions_carouselsec
, 	search_impressions_bp
, 	search_impressions_ptype
, 	search_impressions_cpnflg
, 	search_impressions_cpnclpd
, 	search_impressions_cpnid
, 	search_impressions_cpnapld
, 	search_impressions_aid
, 	search_impressions_upc
, 	search_impressions_pfmdtl
, 	search_impressions_ics
, 	search_impressions_mto
, 	search_impressions_deptnam
, 	search_impressions_lbs
, 	search_impressions_each
, 	search_impressions_aisle
, 	search_impressions_shelf
, 	search_impressions_offtyp
, 	search_impressions_offtext
, 	search_impressions_carouselsz
, 	search_impressions_isrcmnd
, 	search_impressions_iqn
, 	search_impressions_iv
, 	search_impressions_sellerid
, 	search_impressions_sellernam
FROM <<EDM_DB_R_NAME>>.<<EDM_SCHEMA_R_TARGET>>."ONE_TAG_SEARCH_IMPRESSIONS";

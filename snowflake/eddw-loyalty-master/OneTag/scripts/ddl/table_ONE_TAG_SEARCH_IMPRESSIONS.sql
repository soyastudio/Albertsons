--liquibase formatted sql
--changeset SYSTEM:ONE_TAG_SEARCH_IMPRESSIONS runOnChange:true splitStatements:false OBJECT_TYPE:TABLE

use database <<EDM_DB_R_NAME>>;
use schema <<EDM_SCHEMA_R_TARGET>>;

CREATE OR REPLACE TABLE ONE_TAG_SEARCH_IMPRESSIONS
(	event_hr   NUMBER
,	event_id	VARCHAR
, 	event_subevent	VARCHAR
,   eventtime	timestamp
, 	search_impressions_pid	VARCHAR
, 	search_impressions_pname	VARCHAR
, 	search_impressions_fname	VARCHAR
, 	search_impressions_units	NUMBER
, 	search_impressions_utyp	VARCHAR
, 	search_impressions_ppunit	VARCHAR
, 	search_impressions_lp	VARCHAR
, 	search_impressions_carouselsec	VARCHAR
, 	search_impressions_bp	VARCHAR
, 	search_impressions_ptype	VARCHAR
, 	search_impressions_cpnflg	VARCHAR
, 	search_impressions_cpnclpd	VARCHAR
, 	search_impressions_cpnid	VARCHAR
, 	search_impressions_cpnapld	VARCHAR
, 	search_impressions_aid	VARCHAR
, 	search_impressions_upc	VARCHAR
, 	search_impressions_pfmdtl	VARCHAR
, 	search_impressions_ics	boolean
, 	search_impressions_mto	VARCHAR
, 	search_impressions_deptnam	VARCHAR
, 	search_impressions_lbs	VARCHAR
, 	search_impressions_each	VARCHAR
, 	search_impressions_aisle	VARCHAR
, 	search_impressions_shelf	VARCHAR
, 	search_impressions_offtyp	VARCHAR
, 	search_impressions_offtext	VARCHAR
, 	search_impressions_carouselsz	NUMBER
, 	search_impressions_isrcmnd	VARCHAR
, 	search_impressions_iqn	VARCHAR
, 	search_impressions_iv	VARCHAR
, 	search_impressions_sellerid	VARCHAR
, 	search_impressions_sellernam	VARCHAR
 	);

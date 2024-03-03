--liquibase formatted sql
--changeset SYSTEM:ONE_TAG_CAROUSEL runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_DB_NAME_VIEWS>>;
use schema <<EDM_SCHEMA_VIEWS>>;

CREATE OR REPLACE VIEW  ONE_TAG_CAROUSEL
(	
    event_id
, 	event_subevent
,   event_time
, 	carousel_section
, 	carousel_detail
, 	carousel_lp
, 	carousel_pfm
, 	carousel_pid
, 	carousel_units
) COPY GRANTS comment = 'VIEW FOR ONE_TAG_CAROUSEL' 
AS
SELECT
 	event_id
, 	event_subevent
,	eventtime
, 	carousel_section
, 	carousel_detail
, 	carousel_lp
, 	carousel_pfm
, 	carousel_pid
, 	carousel_units
FROM <<EDM_DB_R_NAME>>.<<EDM_SCHEMA_R_TARGET>>."ONE_TAG_CAROUSEL";

--liquibase formatted sql
--changeset SYSTEM:ONE_TAG_GLOBAL runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_DB_NAME_VIEWS>>;
use schema <<EDM_SCHEMA_VIEWS>>;

CREATE OR REPLACE VIEW  ONE_TAG_GLOBAL
(	
    event_id
, 	event_subevent
,   event_time
, 	global_expids
) COPY GRANTS comment = 'VIEW FOR ONE_TAG_GLOBAL' 
AS
SELECT
 	event_id
, 	event_subevent
,   eventtime
, 	global_expids
FROM <<EDM_DB_R_NAME>>.<<EDM_SCHEMA_R_TARGET>>."ONE_TAG_GLOBAL";

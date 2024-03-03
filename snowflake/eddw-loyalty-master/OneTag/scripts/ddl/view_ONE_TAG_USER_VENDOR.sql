--liquibase formatted sql
--changeset SYSTEM:ONE_TAG_USER_VENDOR runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_DB_NAME_VIEWS>>;
use schema <<EDM_SCHEMA_VIEWS>>;

CREATE OR REPLACE VIEW  ONE_TAG_USER_VENDOR
(	
    event_id
, 	event_subevent
,	event_time
, 	user_vendor_name
, 	user_vendor_vars_key
,   user_vendor_vars_value
) COPY GRANTS comment = 'VIEW FOR ONE_TAG_USER_VENDOR' 
AS
SELECT
 	event_id
, 	event_subevent
,   eventtime
, 	user_vendor_name
, 	user_vendor_vars_key
,   user_vendor_vars_value
FROM <<EDM_DB_R_NAME>>.<<EDM_SCHEMA_R_TARGET>>."ONE_TAG_USER_VENDOR";

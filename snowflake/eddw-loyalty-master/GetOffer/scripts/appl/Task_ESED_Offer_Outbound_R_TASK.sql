--liquibase formatted sql
--changeset SYSTEM:ESED_Offer_Outbound_R_TASK runOnChange:true splitStatements:false OBJECT_TYPE:TASK
use database <<EDM_DB_NAME_R>>;
use schema <<EDM_DB_NAME_R>>.DW_R_PRODUCT;

CREATE OR REPLACE TASK ESED_OFFER_OUTBOUND_R_TASK
WAREHOUSE='PROD_INGESTION_SMALL_WH'
SCHEDULE='1 minutes'
WHEN SYSTEM$STREAM_HAS_DATA('ESED_Offer_Outbound_R_STREAM')
AS call sp_getoffer_to_outbound_flat_load();

ALTER TASK ESED_OFFER_OUTBOUND_R_TASK RESUME;

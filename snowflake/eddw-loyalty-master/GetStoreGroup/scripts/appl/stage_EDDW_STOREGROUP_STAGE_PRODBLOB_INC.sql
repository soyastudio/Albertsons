--liquibase formatted sql
--changeset SYSTEM:EDDW_STOREGROUP_STAGE_PRODBLOB_INC runOnChange:true splitStatements:false OBJECT_TYPE:STAGE
use database <<EDM_DB_NAME_R>>;
use schema <<EDM_DB_NAME_R>>.DW_R_PRODUCT;

Create or replace stage EDDW_STOREGROUP_STAGE_PRODBLOB_INC
stage_integration = STORAGE_ABSITDSPRODWUSSEDDW001
url = azure://absitdsprodwusseddw001.blob.core.windows.net/itds-prod-kafka-topics

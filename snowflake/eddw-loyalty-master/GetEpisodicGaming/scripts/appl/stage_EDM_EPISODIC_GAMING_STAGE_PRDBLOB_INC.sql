--liquibase formatted sql
--changeset SYSTEM:EDM_EPISODIC_GAMING_STAGE_PRDBLOB_INC runOnChange:true splitStatements:false OBJECT_TYPE:STAGE
use database <<EDM_DB_NAME_R>>;
use schema <<EDM_DB_NAME_R>>.DW_APPL;

Create or replace stage EDM_EPISODIC_GAMING_STAGE_PRDBLOB_INC
stage_integration = STORAGE_ABSITDSPRODWUSSEDDW001
url = azure://absitdsprodwusseddw001.blob.core.windows.net/itds-prod-direct-feeds

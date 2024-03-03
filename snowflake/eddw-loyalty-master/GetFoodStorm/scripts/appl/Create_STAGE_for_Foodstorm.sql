--liquibase formatted sql
--changeset SYSTEM:Create_STAGE_for_Foodstorm runOnChange:true splitStatements:false OBJECT_TYPE:STAGE
USE DATABASE <<EDM_DB_NAME_R>>;
USE SCHEMA <<EDM_DB_NAME_R>>.DW_APPL;

CREATE OR REPLACE STAGE EDM_FOOD_STORM_STAGE_<<ENV>>BLOB_INC
	url = 'azure://absitds<<env>>wusseddw001.blob.core.windows.net/itds-<<env>>-direct-feeds' 
	STORAGE_INTEGRATION = STORAGE_ABSITDS<<ENVI>>WUSSEDDW001;

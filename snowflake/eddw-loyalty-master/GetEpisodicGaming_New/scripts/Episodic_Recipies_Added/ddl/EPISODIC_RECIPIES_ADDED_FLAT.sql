--liquibase formatted sql
--changeset SYSTEM:EPISODIC_RECIPIES_ADDED_FLAT runOnChange:true splitStatements:false OBJECT_TYPE:TABLE

USE DATABASE <<EDM_DB_NAME_R>>;
USE SCHEMA <<EDM_DB_NAME_R>>.DW_R_LOYALTY;

CREATE OR REPLACE TABLE EPISODIC_RECIPIES_ADDED_FLAT(
PROGRAM_ID VARCHAR(16777216),
RECIPE_ADDED_ID VARCHAR(16777216),
HOUSEHOLD_ID VARCHAR(16777216),
RECIPE_ID VARCHAR(16777216),
STORE_ID VARCHAR(16777216),
BANNER VARCHAR(16777216),
DIVISION VARCHAR(16777216),
CHANNEL VARCHAR(16777216),
ACCESS_TYPE VARCHAR(16777216),
APP_USER VARCHAR(16777216),
CREATED_TS VARCHAR(16777216),
EXTRACT_TS VARCHAR(16777216),
DW_CREATE_TS VARCHAR(16777216),
FILE_NAME VARCHAR(16777216)
);

ALTER TABLE EPISODIC_RECIPIES_ADDED_FLAT SET CHANGE_TRACKING = TRUE;

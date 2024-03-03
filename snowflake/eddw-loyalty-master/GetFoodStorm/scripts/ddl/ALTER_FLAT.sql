--liquibase formatted sql
--changeset SYSTEM:ALTER_FLAT runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
USE DATABASE <<EDM_DB_NAME_R>>;
USE SCHEMA <<EDM_DB_NAME_R>>.DW_R_LOYALTY;

alter table GETFOODSTORM_FLAT
rename COLUMN file_name to FILENAME;

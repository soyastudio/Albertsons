--liquibase formatted sql
--changeset SYSTEM:Transient_tbl runOnChange:true splitStatements:false OBJECT_TYPE:TABLE

USE DATABaSE <<EDM_DB_NAME>>;
USE SCHEMA DW_C_STAGE;

CREATE OR REPLACE TABLE UBER_ORDER_INFO_Flat_Rerun AS 
SELECT * FROM <<EDM_DB_NAME_R>>.DW_APPL.UBER_ORDER_INFO_FLAT_R_STREAM;

create or replace table UBER_ORDER_INFO_Flat_Main_WRK DATA_RETENTION_TIME_IN_DAYS = 0 as 
								SELECT * FROM <<EDM_DB_NAME_R>>.DW_APPL.UBER_ORDER_INFO_FLAT_R_STREAM 
								UNION ALL 
								SELECT * FROM <<EDM_DB_NAME>>.DW_C_STAGE.UBER_ORDER_INFO_Flat_Rerun;

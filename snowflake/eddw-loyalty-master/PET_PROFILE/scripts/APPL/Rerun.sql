USE DATABaSE EDM_REFINED_PRD;
USE SCHEMA DW_R_STAGE;

create or replace table GETPETPROFILE_ADF_FLAT_rerun 
AS
select * from EDM_REFINED_PRD.dw_appl.GETPETPROFILE_ADF_FLAT_R_STREAM where 1=2;

alter table GETPETPROFILE_ADF_FLAT_rerun ALTER COLUMN METADATA$ACTION VARCHAR, METADATA$ROW_ID VARCHAR;
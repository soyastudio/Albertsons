--liquibase formatted sql
--changeset SYSTEM:Temp_Tables runOnChange:true splitStatements:false OBJECT_TYPE:TABLE

			
USE DATABASE <<EDM_DB_NAME_OUT>>;
USE SCHEMA DW_stage;
 
  Create or replace transient table  <<EDM_DB_NAME_OUT>>.dw_stage.EPE_OMS_OFFER_JSON_TEMP
as Select * from  <<EDM_DB_NAME_OUT>>.dw_dcat.epe_oms_offer_json where 1=2;

Create or replace transient table  <<EDM_DB_NAME_OUT>>.dw_stage.EPE_OMS_OFFER_JSON_EXCEPTION
as Select *,'' as Reason from  <<EDM_DB_NAME_OUT>>.dw_dcat.epe_oms_offer_json where 1=2;

Create or Replace transient table  <<EDM_DB_NAME_OUT>>.dw_stage.EPE_OFFEROMS_O_WRK
as select * from <<EDM_DB_NAME>>.dw_appl.GetOfferOMS_Flat_C_Stream where 1=2;

Create or Replace transient table  <<EDM_DB_NAME_OUT>>.dw_stage.EPE_OFFEROMS_O_RERUN
as select * from <<EDM_DB_NAME>>.dw_appl.GetOfferOMS_Flat_C_Stream where 1=2;

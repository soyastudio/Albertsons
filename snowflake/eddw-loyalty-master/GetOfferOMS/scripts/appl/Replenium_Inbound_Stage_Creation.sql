
--liquibase formatted sql
--changeset SYSTEM:Replenium_Inbound_Stage_Creation runOnChange:true splitStatements:false OBJECT_TYPE:STAGE

USE DATABASE <<EDM_DB_NAME_R>>;
USE SCHEMA DW_APPL;
CREATE OR REPLACE stage EDDW_REPLENIUM_STAGE_INBOUND
storage_integration = <<STORAGE_ACC1>>
url='<<URL1>>';

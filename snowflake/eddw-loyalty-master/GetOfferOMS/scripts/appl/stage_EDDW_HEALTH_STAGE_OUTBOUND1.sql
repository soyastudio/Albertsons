--liquibase formatted sql
--changeset SYSTEM:EDDW_HEALTH_STAGE_OUTBOUND1 runOnChange:true splitStatements:false OBJECT_TYPE:STAGE


USE DATABASE <<EDM_DB_NAME_OUT>>;
USE SCHEMA DW_APPL;
CREATE OR REPLACE stage EDDW_HEALTH_STAGE_OUTBOUND1
storage_integration = <<STORAGE_ACC1>>
URL= <<URL1>>;

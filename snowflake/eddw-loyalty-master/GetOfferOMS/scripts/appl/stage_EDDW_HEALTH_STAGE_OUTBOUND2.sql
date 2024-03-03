--liquibase formatted sql
--changeset SYSTEM:EDDW_HEALTH_STAGE_OUTBOUND2 runOnChange:true splitStatements:false OBJECT_TYPE:STAGE

USE DATABASE <<EDM_DB_NAME_OUT>>;
USE SCHEMA DW_APPL;
CREATE OR REPLACE stage EDDW_HEALTH_STAGE_OUTBOUND2
storage_integration = <<STORAGE_ACC2>>
URL= <<URL2>>;

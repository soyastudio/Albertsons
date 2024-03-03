--liquibase formatted sql
--changeset SYSTEM:GETOFFER_FLAT_O_STREAM runOnChange:true splitStatements:false OBJECT_TYPE:STREAM
use database <<EDM_DB_NAME_OUT>>;
use schema <<EDM_DB_NAME_OUT>>.DW_DCAT;

Create or replace stream DW_DCAT.GETOFFER_FLAT_O_STREAM ON TABLE <<EDM_DB_NAME>>.DW_C_PRODUCT.GETOFFER_FLAT

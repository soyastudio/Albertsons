--liquibase formatted sql
--changeset SYSTEM:EPE_OFFER_JSON_O_STREAM runOnChange:true splitStatements:false OBJECT_TYPE:STREAM
use database <<EDM_DB_NAME_OUT>>;
use schema <<EDM_DB_NAME_OUT>>.DW_APPL;

Create or replace stream DW_APPL.EPE_OFFER_JSON_O_STREAM ON TABLE <<EDM_DB_NAME_OUT>>.DW_DCAT.EPE_OFFER_JSON

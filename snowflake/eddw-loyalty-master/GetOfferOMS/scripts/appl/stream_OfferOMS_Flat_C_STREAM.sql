--liquibase formatted sql
--changeset SYSTEM:OfferOMS_Flat_C_STREAM runOnChange:true splitStatements:false OBJECT_TYPE:STREAM
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

Create or replace stream DW_APPL.OFFEROMS_FLAT_C_STREAM ON TABLE <<EDM_DB_NAME>>.DW_C_PRODUCT.OFFEROMS_FLAT

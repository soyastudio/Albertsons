--liquibase formatted sql
--changeset SYSTEM:GetOfferOMS_Flat_C_Stream runOnChange:true splitStatements:false OBJECT_TYPE:STREAM
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

Create or replace stream DW_APPL.GETOFFEROMS_FLAT_C_STREAM ON TABLE <<EDM_DB_NAME>>.DW_C_PRODUCT.OFFEROMS_FLAT

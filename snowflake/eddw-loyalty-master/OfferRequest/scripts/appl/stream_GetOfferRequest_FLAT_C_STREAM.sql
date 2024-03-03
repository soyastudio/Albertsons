--liquibase formatted sql
--changeset SYSTEM:GetOfferRequest_FLAT_C_STREAM runOnChange:true splitStatements:false OBJECT_TYPE:STREAM
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_PRODUCT;

Create or replace stream DW_C_PRODUCT.GETOFFERREQUEST_FLAT_C_STREAM ON TABLE <<EDM_DB_NAME>>.DW_C_PRODUCT.GETOFFERREQUEST_FLAT

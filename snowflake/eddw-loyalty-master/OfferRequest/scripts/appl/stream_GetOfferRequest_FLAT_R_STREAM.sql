--liquibase formatted sql
--changeset SYSTEM:GetOfferRequest_FLAT_R_STREAM runOnChange:true splitStatements:false OBJECT_TYPE:STREAM
use database <<EDM_DB_NAME_R>>;
use schema <<EDM_DB_NAME_R>>.DW_R_PRODUCT;

Create or replace stream DW_R_PRODUCT.GETOFFERREQUEST_FLAT_R_STREAM ON TABLE <<EDM_DB_NAME_R>>.DW_R_PRODUCT.GETOFFERREQUEST_FLAT

--liquibase formatted sql
--changeset SYSTEM:PRODUCTGROUP_FLAT_R_STREAM runOnChange:true splitStatements:false OBJECT_TYPE:STREAM
use database <<EDM_DB_NAME_R>>;
use schema <<EDM_DB_NAME_R>>.DW_R_PRODUCT;

Create or replace stream PRODUCTGROUP_FLAT_R_STREAM ON TABLE <<EDM_DB_NAME_R>>.DW_R_PRODUCT.PRODUCTGROUP_FLAT

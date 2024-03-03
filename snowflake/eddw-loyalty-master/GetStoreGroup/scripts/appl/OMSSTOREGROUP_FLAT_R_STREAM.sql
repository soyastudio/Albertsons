--liquibase formatted sql
--changeset SYSTEM:OMSSTOREGROUP_FLAT_R_STREAM runOnChange:true splitStatements:false OBJECT_TYPE:STREAM
use database <<EDM_DB_NAME_R>>;
use schema <<EDM_DB_NAME_R>>.DW_APPL;

create or replace stream OMSSTOREGROUP_FLAT_R_STREAM on table <<EDM_DB_NAME_R>>.DW_R_PRODUCT.STOREGROUP_FLAT;

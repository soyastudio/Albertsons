--liquibase formatted sql
--changeset SYSTEM:GetBusinessPartner_Flat_R_STREAM runOnChange:true splitStatements:false OBJECT_TYPE:STREAM
use database <<EDM_DB_NAME_R>>;
use schema <<EDM_DB_NAME_R>>.DW_APPL;

Create or replace stream DW_APPL.GETBUSINESSPARTNER_FLAT_R_STREAM ON TABLE <<EDM_DB_NAME_R>>.DW_R_LOYALTY.GETBUSINESSPARTNER_FLAT

--liquibase formatted sql
--changeset SYSTEM:GETNPSUMASURVEY_FLAT_R_STREAM runOnChange:true splitStatements:false OBJECT_TYPE:STREAM
use database <<EDM_DB_NAME_R>>;
use schema <<EDM_DB_NAME_R>>.DW_APPL;

Create or replace stream DW_APPL.GETNPSUMASURVEY_FLAT_R_STREAM ON TABLE EDM_REFINED_PRD.DW_R_LOYALTY.GETNPSUMASURVEY_FLAT

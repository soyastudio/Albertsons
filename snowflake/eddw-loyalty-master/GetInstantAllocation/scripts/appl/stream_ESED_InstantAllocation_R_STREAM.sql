--liquibase formatted sql
--changeset SYSTEM:ESED_InstantAllocation_R_STREAM runOnChange:true splitStatements:false OBJECT_TYPE:STREAM
use database EDM_REFINED_PRD;
use schema EDM_REFINED_PRD.DW_APPL;

Create or replace stream DW_APPL.ESED_INSTANTALLOCATION_R_STREAM ON TABLE EDM_REFINED_PRD.DW_R_LOYALTY.ESED_INSTANTALLOCATION

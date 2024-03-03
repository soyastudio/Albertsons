--liquibase formatted sql
--changeset SYSTEM:TRANSACTION_HEADER_REBATE_NEPTUNE_STREAM runOnChange:true splitStatements:false OBJECT_TYPE:STREAM
use database EDM_CONFIRMED_OUT_PRD;
use schema DW_APPL;

Create or replace stream DW_APPL.TRANSACTION_HEADER_REBATE_NEPTUNE_STREAM ON TABLE EDM_CONFIRMED_PRD.DW_C_RETAILSALE.EPE_TRANSACTION_HEADER;
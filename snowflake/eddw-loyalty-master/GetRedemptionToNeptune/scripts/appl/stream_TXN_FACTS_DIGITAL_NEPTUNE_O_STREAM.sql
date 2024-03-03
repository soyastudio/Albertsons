--liquibase formatted sql
--changeset SYSTEM:TXN_FACTS_DIGITAL_NEPTUNE_O_STREAM runOnChange:true splitStatements:false OBJECT_TYPE:STREAM
use database EDM_CONFIRMED_OUT_PRD;
use schema DW_APPL;

Create or replace stream DW_APPL.TXN_FACTS_DIGITAL_NEPTUNE_O_STREAM ON TABLE No privilege or table dropped;
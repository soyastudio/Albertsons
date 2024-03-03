--liquibase formatted sql
--changeset SYSTEM:TXN_FACTS_DIGITAL_NEPTUNE_STREAM runOnChange:true splitStatements:false OBJECT_TYPE:STREAM
use database <<EDM_DB_NAME>>;
use schema DW_APPL;

Create or replace stream DW_APPL.TXN_FACTS_DIGITAL_NEPTUNE_STREAM ON TABLE <<EDW_DB_SHARE>>.DW_TRANSACTION.TXN_FACTS;

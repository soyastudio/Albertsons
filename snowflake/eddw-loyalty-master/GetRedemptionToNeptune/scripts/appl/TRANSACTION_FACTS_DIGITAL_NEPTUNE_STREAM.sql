--liquibase formatted sql
--changeset SYSTEM:TXN_FACTS_DIGITAL_NEPTUNE_STREAM_NEW runOnChange:true splitStatements:false OBJECT_TYPE:STREAM
use database <<EDM_DB_NAME>>;
use schema DW_APPL;

Create or replace stream DW_APPL.TRANSACTION_FACTS_DIGITAL_NEPTUNE_STREAM ON TABLE <<EDM_QA_Analytics>>.DW_RETAIL_OPS.TRANSACTION_FACTS;

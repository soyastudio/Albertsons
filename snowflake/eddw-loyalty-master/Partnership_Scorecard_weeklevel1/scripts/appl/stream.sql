--liquibase formatted sql
--changeset SYSTEM:stream runOnChange:true splitStatements:false OBJECT_TYPE:STREAM
use database <<EDM_DB_NAME_C>>;
use schema <<EDM_DB_NAME_C>>.DW_APPL;

CREATE OR REPLACE STREAM GW_REG99_TXNS_2020_STREAM_LOYALTY_2 ON VIEW
<<EDM_VIEW_NAME>>.DW_BIZOPS_VIEWS.GW_REG99_TXNS_2020;



 

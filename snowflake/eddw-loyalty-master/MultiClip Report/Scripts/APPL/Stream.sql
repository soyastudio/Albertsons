--liquibase formatted sql
--changeset SYSTEM:Stream runOnChange:true splitStatements:false OBJECT_TYPE:SP

use database EDM_CONFIRMED_PRD;
use schema EDM_CONFIRMED_PRD.DW_APPL;

DROP STREAM IF EXISTS FACT_MULTICLIP_REPORT_Stream;

create or replace stream FACT_MULTICLIP_REPORT_Stream on table EDM_CONFIRMED_PRD.dw_c_loyalty.CLIP_DETAILS;

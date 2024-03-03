--liquibase formatted sql
--changeset SYSTEM:ONE_TAG_OTHER_SEQ runOnChange:true splitStatements:false OBJECT_TYPE:SEQUENCE

use database EDM_CONFIRMED_<<ENV>>;
use schema DW_APPL;

create or replace sequence ONETAG_OTHER_SEQ start with 1001 increment by 1 order;
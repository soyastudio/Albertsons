--liquibase formatted sql
--changeset SYSTEM:ONETAG_OTHER_FLAT_R_STREAM runOnChange:true splitStatements:false OBJECT_TYPE:STREAM
use database EDM_REFINED_<<ENV>>;
use schema DW_APPL;

Create or replace stream ONETAG_OTHER_FLAT_R_STREAM ON TABLE EDM_REFINED_<<ENV>>.DW_R_USER_ACTIVITY.ONE_TAG_OTHER;

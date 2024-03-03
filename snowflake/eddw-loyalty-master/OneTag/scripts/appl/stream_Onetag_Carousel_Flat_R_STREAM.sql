--liquibase formatted sql
--changeset SYSTEM:ONETAG_CAROUSEL_FLAT_R_STREAM runOnChange:true splitStatements:false OBJECT_TYPE:STREAM
use database EDM_REFINED_<<ENV>>;
use schema DW_APPL;

create or replace STREAM ONETAG_CAROUSEL_FLAT_R_STREAM
	ON TABLE EDM_REFINED_<<ENV>>.DW_R_USER_ACTIVITY.ONE_TAG_CAROUSEL;

--liquibase formatted sql
--changeset SYSTEM:CLICK_STREAM_OPERATING_SYSTEM runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database EDM_CONFIRMED_<<ENV>>;
use schema DW_C_USER_ACTIVITY;

INSERT INTO CLICK_STREAM_OPERATING_SYSTEM VALUES (-1,NULL,NULL,NULL,getdate(),getdate(),FALSE,'OneTag','OneTag',TRUE);

--liquibase formatted sql
--changeset SYSTEM:CLICK_STREAM_PAGE_DETAIL runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database EDM_CONFIRMED_<<ENV>>;
use schema DW_C_USER_ACTIVITY;

INSERT INTO CLICK_STREAM_PAGE_DETAIL VALUES (-1,NULL,NULL,NULL,NULL,NULL,NULL,getdate(),getdate(),FALSE,'OneTag','OneTag',TRUE);

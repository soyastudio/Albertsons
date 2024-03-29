--liquibase formatted sql
--changeset SYSTEM:CUSTOMER_SESSION runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database EDM_CONFIRMED_<<ENV>>;
use schema DW_C_STAGE;

ALTER TABLE CLICK_STREAM_PAGE_DETAIL_WRK RENAME TO CUSTOMER_SESSION_PAGE_WRK;
ALTER TABLE ONETAG_CLICK_STREAM_OPERATING_SYSTEM_DETAIL_WRK RENAME TO CUSTOMER_SESSION_OPERATING_SYSTEM_WRK;
ALTER TABLE CLICK_STREAM_VISITOR_WRK RENAME TO CUSTOMER_SESSION_VISITOR_WRK;
ALTER TABLE CLICK_STREAM_IMPRESSIONS_WRK RENAME TO CUSTOMER_SESSION_IMPRESSION_WRK;
ALTER TABLE CLICK_STREAM_EVENT_MASTER_WRK RENAME TO CUSTOMER_SESSION_EVENT_MASTER_WRK;
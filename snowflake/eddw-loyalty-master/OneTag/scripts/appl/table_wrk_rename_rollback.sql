--liquibase formatted sql
--changeset SYSTEM:CUSTOMER_SESSION runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database EDM_CONFIRMED_<<ENV>>;
use schema DW_C_STAGE;

ALTER TABLE CUSTOMER_SESSION_PAGE_WRK RENAME TO CLICK_STREAM_PAGE_DETAIL_WRK;
ALTER TABLE CUSTOMER_SESSION_OPERATING_SYSTEM_WRK RENAME TO ONETAG_CLICK_STREAM_OPERATING_SYSTEM_DETAIL_WRK;
ALTER TABLE CUSTOMER_SESSION_USER_WRK RENAME TO CLICK_STREAM_VISITOR_WRK;
ALTER TABLE CUSTOMER_SESSION_IMPRESSION_WRK RENAME TO CLICK_STREAM_IMPRESSIONS_WRK;
ALTER TABLE CUSTOMER_SESSION_EVENT_MASTER_WRK RENAME TO CLICK_STREAM_EVENT_MASTER_WRK;
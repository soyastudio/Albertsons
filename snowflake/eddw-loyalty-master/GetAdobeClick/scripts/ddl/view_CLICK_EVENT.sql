--liquibase formatted sql
--changeset SYSTEM:CLICK_EVENT runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database EDM_VIEWS_PRD;
use schema DW_VIEWS;

CREATE OR REPLACE VIEW click_event COPY GRANTS AS
SELECT
 event_ID
,event_nm
,DW_CREATETS
from EDM_CONFIRMED_PRD.DW_C_USER_ACTIVITY.click_event;
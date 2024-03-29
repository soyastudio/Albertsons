--liquibase formatted sql
--changeset SYSTEM:CLICK_CONNECTIONTYPE runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database EDM_VIEWS_PRD;
use schema DW_VIEWS;

CREATE OR REPLACE VIEW CLICK_CONNECTIONTYPE COPY GRANTS AS
SELECT
 connectiontype_ID
,connectiontype_nm
,DW_CREATETS
from EDM_CONFIRMED_PRD.DW_C_USER_ACTIVITY.CLICK_CONNECTIONTYPE;
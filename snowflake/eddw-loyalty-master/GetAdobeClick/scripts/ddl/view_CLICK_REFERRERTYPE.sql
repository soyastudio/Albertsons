--liquibase formatted sql
--changeset SYSTEM:CLICK_REFERRERTYPE runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database EDM_VIEWS_PRD;
use schema DW_VIEWS;

CREATE OR REPLACE VIEW click_referrertype COPY GRANTS AS
 SELECT
 referrertype_ID
,referrertype_dsc
,referrertype_nm
,DW_CREATETS
from EDM_CONFIRMED_PRD.DW_C_USER_ACTIVITY.click_referrertype;
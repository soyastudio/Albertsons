--liquibase formatted sql
--changeset SYSTEM:CLICK_JAVASCRIPTVERSION runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database EDM_VIEWS_PRD;
use schema DW_VIEWS;

CREATE OR REPLACE VIEW click_javascriptversion COPY GRANTS AS
SELECT
 javascriptversion_ID
,javascriptversion_nm
,DW_CREATETS
from EDM_CONFIRMED_PRD.DW_C_USER_ACTIVITY.click_javascriptversion;
--liquibase formatted sql
--changeset SYSTEM:CLICK_LANGUAGE runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database EDM_VIEWS_PRD;
use schema DW_VIEWS;

CREATE OR REPLACE VIEW click_language COPY GRANTS AS
SELECT
 language_ID
,language
,DW_CREATETS
from EDM_CONFIRMED_PRD.DW_C_USER_ACTIVITY.click_language;
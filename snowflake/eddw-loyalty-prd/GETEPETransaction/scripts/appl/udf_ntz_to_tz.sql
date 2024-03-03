--liquibase formatted sql
--changeset SYSTEM:Create_Function_udf_ntz_to_tz runOnChange:true splitStatements:false OBJECT_TYPE:FUNCTION
use database EDM_CONFIRMED_PRD;
use schema EDM_CONFIRMED_PRD.DW_APPL;

CREATE OR REPLACE FUNCTION udf_ntz_to_tz(dt TIMESTAMP_NTZ, timezone VARCHAR)
RETURNS TIMESTAMP_TZ
AS
'
TIMESTAMP_TZ_FROM_PARTS( year(dt), month(dt),day(dt), hour(dt), minute(dt), second(dt) , date_part(nanosecond, dt), timezone )
';

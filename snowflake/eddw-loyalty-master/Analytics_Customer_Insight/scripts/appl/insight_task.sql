--liquibase formatted sql
--changeset SYSTEM:insight_Task runOnChange:true splitStatements:false OBJECT_TYPE:SP

USE DATABASE <<EDM_DB_NAME_A>>;
USE SCHEMA DW_APPL;

create or replace task SP_F_Partner_Customer_Insight_TASK
  warehouse = '<<WH>>'

  schedule = 'USING CRON 00 19 * * * Asia/Kolkata'
  as call SP_F_Partner_Customer_Insight();


  alter task SP_F_Partner_Customer_Insight_TASK resume;


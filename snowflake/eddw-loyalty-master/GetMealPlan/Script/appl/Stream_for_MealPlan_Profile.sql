--liquibase formatted sql
--changeset SYSTEM:Stream_for_MealPlan_Profile runOnChange:true splitStatements:false OBJECT_TYPE:STREAM
USE DATABASE <<EDM_DB_NAME_R>>;
USE SCHEMA <<EDM_DB_NAME_R>>.DW_APPL;

CREATE OR REPLACE STREAM GetRetail_Customer_MealPlan_Profile_Flat_R_STREAM 

ON TABLE <<EDM_DB_NAME_R>>.DW_R_LOYALTY.GetRetail_Customer_MealPlan_Profile_Flat;   

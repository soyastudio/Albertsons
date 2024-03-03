--liquibase formatted sql
--changeset SYSTEM:Flat_DDl_MealPlan_Profile runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
USE DATABASE <<EDM_DB_NAME_R>>;
USE SCHEMA <<EDM_DB_NAME_R>>.DW_R_LOYALTY;

CREATE OR REPLACE TABLE GetRetail_Customer_MealPlan_Profile_Flat
(
  id                 VARCHAR NULL ,
  userId             VARCHAR NULL ,
  servingCount       VARCHAR NULL ,
  recipeTypeId       VARCHAR NULL ,
  recipeTypename     VARCHAR NULL ,
  profilename        VARCHAR NULL ,
  DW_CreateTs       TIMESTAMP NULL ,
  File_Name          VARCHAR NULL
)
CHANGE_TRACKING = TRUE;

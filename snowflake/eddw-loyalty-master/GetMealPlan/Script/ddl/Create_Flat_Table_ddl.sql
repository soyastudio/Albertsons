--liquibase formatted sql
--changeset SYSTEM:Create_Flat_Table_ddl runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
USE DATABASE <<EDM_DB_NAME_R>>;
USE SCHEMA <<EDM_DB_NAME_R>>.DW_R_LOYALTY;

CREATE OR REPLACE TABLE GetMeal_Plan_Recipe_Flat
(
  recipeId             VARCHAR NULL ,
  variantId            VARCHAR NULL ,
  name                 VARCHAR NULL ,
  groupLabel           VARCHAR NULL ,
  thumbnailImageUrl    VARCHAR NULL ,
  presentationImageUrl VARCHAR NULL ,
  servingCount         VARCHAR NULL ,
  cookingMinutes       VARCHAR NULL ,
  calories             VARCHAR NULL ,
  DW_CreateTs         TIMESTAMP NULL ,
  File_Name            VARCHAR NULL
)
CHANGE_TRACKING = TRUE;  

CREATE OR REPLACE TABLE GetMeal_Plan_Cuisine_Tag_Flat
(
  Id            VARCHAR NULL ,
  name          VARCHAR NULL ,
  DW_CreateTs  TIMESTAMP NULL ,
  File_Name     VARCHAR NULL
)
CHANGE_TRACKING = TRUE;

CREATE OR REPLACE TABLE GetRetail_Customer_Dislike_Flat
(
 id            VARCHAR NULL ,
 name          VARCHAR NULL ,
 DW_CreateTs  TIMESTAMP NULL ,
 File_Name     VARCHAR NULL 
)  
CHANGE_TRACKING = TRUE;

CREATE OR REPLACE TABLE GetRetail_Customer_Dislike_Ingredients_Flat
(
 dislikeId     VARCHAR NULL ,
 ingredientId  VARCHAR NULL ,
 Dw_CreateTs  TIMESTAMP NULL ,
 File_Name     VARCHAR NULL 
)  
CHANGE_TRACKING = TRUE;

CREATE OR REPLACE TABLE GetRetail_Customer_Favorite_Flat
(
  favorite_id          VARCHAR NULL,
  createdDate            VARCHAR NULL,
  updatedDate           VARCHAR NULL,
  recipeId             VARCHAR NULL,
  userId               VARCHAR NULL,
  DW_CreateTs         TIMESTAMP NULL,
  File_Name            VARCHAR NULL
)
CHANGE_TRACKING = TRUE;

CREATE OR REPLACE TABLE GetMeal_Plan_Customer_Flag_Flat
(
  flag                 VARCHAR NULL,
  userId               VARCHAR NULL,
  DW_CreateTs         TIMESTAMP NULL,
  File_Name            VARCHAR NULL
)
CHANGE_TRACKING = TRUE;

CREATE OR REPLACE TABLE GetMeal_Plan_Ingredient_Restriction_Flat
(  
  restrictionId     VARCHAR NULL ,
  ingredientId      VARCHAR NULL ,
  isWarningOnly     VARCHAR NULL,
  DW_CreateTs      TIMESTAMP NULL ,
  File_Name         VARCHAR NULL
)
CHANGE_TRACKING = TRUE;

CREATE OR REPLACE TABLE GetMeal_Plan_Ingredient_Flat
(  
  Id            VARCHAR NULL ,
  name          VARCHAR NULL ,
  DW_CreateTs  TIMESTAMP NULL ,
  File_Name     VARCHAR NULL
)
CHANGE_TRACKING = TRUE;

CREATE OR REPLACE TABLE GetRetail_Customer_Meal_Plan_Flat
(
  id                   VARCHAR NULL,
  createdDate          VARCHAR NULL,
  userId               VARCHAR NULL,
  variantId            VARCHAR NULL,
  cookedDate             VARCHAR NULL,
  DW_CreateTs         TIMESTAMP NULL,
  File_Name            VARCHAR NULL
)
CHANGE_TRACKING = TRUE;

CREATE OR REPLACE TABLE GetRetail_Customer_Pending_Meal_Flat
(
  id            VARCHAR NULL ,
  createdDate     VARCHAR NULL ,
  userId        VARCHAR NULL ,
  variantId     VARCHAR NULL ,
  DW_CreateTs  TIMESTAMP NULL ,
  File_Name     VARCHAR NULL 
 )
CHANGE_TRACKING = TRUE;

CREATE OR REPLACE TABLE GetRetail_Customer_MealPlan_Profile_Flat
(
  id                 VARCHAR NULL ,
  userId             VARCHAR NULL ,
  servingCount       VARCHAR NULL ,
  profilename        VARCHAR NULL ,
  recipeTypeId       VARCHAR NULL ,
  recipeTypename     VARCHAR NULL ,
  DW_CreateTs       TIMESTAMP NULL ,
  File_Name          VARCHAR NULL
)
CHANGE_TRACKING = TRUE;

CREATE OR REPLACE TABLE GetMeal_Plan_Restriction_Flat
(
  id                   VARCHAR NULL,
  name                 VARCHAR NULL,
  DW_CreateTs         TIMESTAMP NULL,
  File_Name            VARCHAR NULL
)
CHANGE_TRACKING = TRUE;

CREATE OR REPLACE TABLE GetMeal_Plan_Variety_Tag_Flat
(
 id            VARCHAR NULL ,
 name          VARCHAR NULL ,
 DW_CreateTs  TIMESTAMP NULL ,
 File_Name     VARCHAR NULL 
)  
CHANGE_TRACKING = TRUE;

CREATE OR REPLACE TABLE GetRetail_Customer_Profile_To_Restriction_Flat
(  
  profileId      VARCHAR NULL ,
  restrictionId  VARCHAR NULL,
  DW_CreateTs   TIMESTAMP NULL ,
  File_Name      VARCHAR NULL
)
CHANGE_TRACKING = TRUE;

CREATE OR REPLACE TABLE GetRetail_Customer_Dislike_To_Profile_Flat
(
  dislikeId          VARCHAR NULL ,
  profileId          VARCHAR NULL ,
  DW_CreateTs        TIMESTAMP NULL ,
  File_Name          VARCHAR NULL
)
CHANGE_TRACKING = TRUE;

CREATE OR REPLACE TABLE GetMeal_Plan_App_Feedback_Flat
(
 id             VARCHAR NULL ,
 body           VARCHAR NULL ,
 starRating     VARCHAR NULL ,
 createdDate    VARCHAR NULL ,
 DW_CreateTs    TIMESTAMP NULL ,
 File_Name      VARCHAR NULL 
)
CHANGE_TRACKING = TRUE;

CREATE OR REPLACE TABLE GetMeal_Plan_Customer_Flat
(
  id                   VARCHAR NULL,
  createdDate          VARCHAR NULL,
  updatedDate          VARCHAR NULL,
  role                 VARCHAR NULL,
  DW_CreateTs          TIMESTAMP NULL,
  File_Name            VARCHAR NULL
)
CHANGE_TRACKING = TRUE;

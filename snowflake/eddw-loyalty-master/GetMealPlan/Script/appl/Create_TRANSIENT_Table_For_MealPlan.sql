--liquibase formatted sql
--changeset SYSTEM:Create_TRANSIENT_Table_For_MealPlan runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
USE DATABASE <<EDM_DB_NAME>>;
USE SCHEMA <<EDM_DB_NAME>>.DW_C_STAGE;

CREATE OR REPLACE TRANSIENT TABLE Meal_Plan_Recipe_tmp_WRK
(
	  recipeId             VARCHAR(16777216) ,
	  variantId            VARCHAR(16777216) ,
	  name                 VARCHAR(16777216) ,
	  groupLabel           VARCHAR(16777216) ,
	  thumbnailImageUrl    VARCHAR(16777216) ,
	  presentationImageUrl VARCHAR(16777216) ,
	  servingCount         VARCHAR(16777216) ,
	  cookingMinutes       VARCHAR(16777216) ,
	  calories             VARCHAR(16777216) ,
	  DW_CREATETS          TIMESTAMP_LTZ(9) ,
	  File_Name            VARCHAR(16777216) ,
	  METADATA$ACTION      VARCHAR(10),
	  METADATA$ISUPDATE    BOOLEAN ,
	  METADATA$ROW_ID      VARCHAR(40)
);

Create or replace transient table Meal_Plan_Cuisine_Tag_Tmp_WRK
(
      Id                   VARCHAR(16777216) ,
      name                 VARCHAR(16777216) ,
	  DW_CREATETS          TIMESTAMP_LTZ(9) ,
	  File_Name            VARCHAR(16777216) ,
	  METADATA$ACTION      VARCHAR(10),
	  METADATA$ISUPDATE    BOOLEAN ,
	  METADATA$ROW_ID      VARCHAR(40)
);

Create or replace transient table Retail_Customer_Dislike_Tmp_WRK
(
      Id                   VARCHAR(16777216) ,
      name                 VARCHAR(16777216) ,
	  DW_CREATETS         TIMESTAMP_LTZ(9) ,
	  File_Name            VARCHAR(16777216) ,
	  METADATA$ACTION      VARCHAR(10),
	  METADATA$ISUPDATE    BOOLEAN ,
	  METADATA$ROW_ID      VARCHAR(40)
);

Create or Replace Transient Table Retail_Customer_Dislike_Ingredients_Tmp_WRK
(
      dislikeId            VARCHAR(16777216) ,
      ingredientId         VARCHAR(16777216) ,
	  DW_CREATETS         TIMESTAMP_LTZ(9) ,
	  File_Name            VARCHAR(16777216) ,
	  METADATA$ACTION      VARCHAR(10),
	  METADATA$ISUPDATE    BOOLEAN ,
	  METADATA$ROW_ID      VARCHAR(40)
);

Create or replace TRANSIENT TABLE RETAIL_CUSTOMER_FAVORITE_TMP_WRK 
(
	FAVORITE_ID VARCHAR(16777216),
	CREATEDDATE VARCHAR(16777216),
	UPDATEDDATE VARCHAR(16777216),
	RECIPEID VARCHAR(16777216),
	USERID VARCHAR(16777216),
	DW_CREATETS TIMESTAMP_LTZ(9),
	FILE_NAME VARCHAR(16777216),
	METADATA$ACTION VARCHAR(10),
	METADATA$ISUPDATE BOOLEAN,
	METADATA$ROW_ID VARCHAR(40)
);

CREATE OR REPLACE TRANSIENT TABLE Meal_Plan_Customer_Flag_tmp_WRK
(
      flag                 VARCHAR(16777216) ,
      userId               VARCHAR(16777216) ,
      DW_CREATETS         TIMESTAMP_LTZ(9) ,
      File_Name            VARCHAR(16777216) ,
      METADATA$ACTION      VARCHAR(10),
      METADATA$ISUPDATE    BOOLEAN ,
      METADATA$ROW_ID      VARCHAR(40)
);

Create or replace transient table Meal_Plan_Ingredient_Restriction_Tmp_WRK
(
      restrictionId        VARCHAR(16777216) ,
      ingredientId         VARCHAR(16777216) ,
	  isWarningOnly        VARCHAR(16777216) ,
	  DW_CREATETS         TIMESTAMP_LTZ(9) ,
	  File_Name            VARCHAR(16777216) ,
	  METADATA$ACTION      VARCHAR(10),
	  METADATA$ISUPDATE    BOOLEAN ,
	  METADATA$ROW_ID      VARCHAR(40)
);

Create or replace transient table Meal_Plan_Ingredient_Tmp_WRK
(
      Id                   VARCHAR(16777216) ,
      name                 VARCHAR(16777216) ,
	  DW_CREATETS         TIMESTAMP_LTZ(9) ,
	  File_Name            VARCHAR(16777216) ,
	  METADATA$ACTION      VARCHAR(10),
	  METADATA$ISUPDATE    BOOLEAN ,
	  METADATA$ROW_ID      VARCHAR(40)
);

CREATE OR REPLACE TRANSIENT TABLE Retail_Customer_Meal_Plan_tmp_WRK
(
      id                   VARCHAR(16777216),
      CREATEDDATE            VARCHAR(16777216) ,
      userId               VARCHAR(16777216),
      variantId            VARCHAR(16777216) ,
      CookedDate             VARCHAR(16777216) ,
      DW_CREATETS         TIMESTAMP ,
      File_Name            VARCHAR(16777216) ,
      METADATA$ACTION      VARCHAR(10) ,
      METADATA$ISUPDATE    BOOLEAN ,
      METADATA$ROW_ID      VARCHAR(40)
);

CREATE OR REPLACE TRANSIENT TABLE Retail_Customer_Pending_Meal_tmp_WRK
(
      id                   VARCHAR(16777216) ,
      CREATEDDATE            VARCHAR(16777216) ,
      userId               VARCHAR(16777216) ,
      variantId            VARCHAR(16777216) ,
      DW_CREATETS         TIMESTAMP_LTZ(9) ,
      File_Name            VARCHAR(16777216) ,
      METADATA$ACTION      VARCHAR(10),
      METADATA$ISUPDATE    BOOLEAN ,
      METADATA$ROW_ID      VARCHAR(40)
);

CREATE OR REPLACE TRANSIENT TABLE Retail_Customer_MealPlan_Profile_tmp_WRK
(
      id                    VARCHAR(16777216) ,
      userId                VARCHAR(16777216) ,
      servingCount          VARCHAR(16777216) ,
      recipeTypeId          VARCHAR(16777216) ,
      recipeTypename        VARCHAR(16777216) ,
      profilename           VARCHAR(16777216) ,
      DW_CREATETS          TIMESTAMP_LTZ(9) ,
      File_Name             VARCHAR(16777216) ,
      METADATA$ACTION       VARCHAR(10),
      METADATA$ISUPDATE     BOOLEAN ,
      METADATA$ROW_ID       VARCHAR(40)
);

Create or replace transient table Meal_Plan_Restriction_Tmp_WRK
(
      Id                   VARCHAR(16777216) ,
      name                 VARCHAR(16777216) ,
	  DW_CREATETS         TIMESTAMP_LTZ(9) ,
	  File_Name            VARCHAR(16777216) ,
	  METADATA$ACTION      VARCHAR(10),
	  METADATA$ISUPDATE    BOOLEAN ,
	  METADATA$ROW_ID      VARCHAR(40)
);

Create or replace transient table Meal_Plan_Variety_Tag_Tmp_WRK
(
      Id                   VARCHAR(16777216) ,
      name                 VARCHAR(16777216) ,
	  DW_CREATETS         TIMESTAMP_LTZ(9) ,
	  File_Name            VARCHAR(16777216) ,
	  METADATA$ACTION      VARCHAR(10),
	  METADATA$ISUPDATE    BOOLEAN ,
	  METADATA$ROW_ID      VARCHAR(40)
);

Create or replace transient table Retail_Customer_Dislike_To_Profile_Tmp_WRK
(
      dislikeId            VARCHAR(16777216) ,
	  profileId            VARCHAR(16777216) ,
	  DW_CREATETS         TIMESTAMP_LTZ(9) ,
	  File_Name            VARCHAR(16777216) ,
	  METADATA$ACTION      VARCHAR(10),
	  METADATA$ISUPDATE    BOOLEAN ,
	  METADATA$ROW_ID      VARCHAR(40)
);


Create or replace transient table Retail_Customer_Profile_To_Restriction_Tmp_WRK
(
      profileId            VARCHAR(16777216) ,
	  restrictionId        VARCHAR(16777216) ,
	  DW_CREATETS          TIMESTAMP_LTZ(9) ,
	  File_Name            VARCHAR(16777216) ,
	  METADATA$ACTION      VARCHAR(10),
	  METADATA$ISUPDATE    BOOLEAN ,
	  METADATA$ROW_ID      VARCHAR(40)
);



CREATE OR REPLACE TRANSIENT TABLE Meal_Plan_App_Feedback_tmp_WRK
(
      id                   VARCHAR(16777216) ,
      body                 VARCHAR(16777216) ,
      starRating           VARCHAR(16777216) ,
      CREATEDDATE            VARCHAR(16777216) ,
      DW_CREATETS         TIMESTAMP_LTZ(9) ,
      File_Name            VARCHAR(16777216) ,
      METADATA$ACTION      VARCHAR(16777216),
      METADATA$ISUPDATE    BOOLEAN ,
      METADATA$ROW_ID      VARCHAR(40)
);

Create or replace TRANSIENT TABLE Meal_Plan_Customer_TMP_WRK 
(
	id VARCHAR(16777216),
	CREATEDDATE VARCHAR(16777216),
	UPDATEDDATE VARCHAR(16777216),
	role VARCHAR(16777216),
	DW_CREATETS TIMESTAMP_LTZ(9),
	FILE_NAME VARCHAR(16777216),
	METADATA$ACTION VARCHAR(10),
	METADATA$ISUPDATE BOOLEAN,
	METADATA$ROW_ID VARCHAR(40)
);

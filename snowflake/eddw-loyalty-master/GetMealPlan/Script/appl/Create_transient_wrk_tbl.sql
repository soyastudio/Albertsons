--liquibase formatted sql
--changeset SYSTEM:Create_transient_wrk_tbl runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
USE DATABASE <<EDM_DB_NAME>>;
USE SCHEMA <<EDM_DB_NAME>>.DW_C_STAGE;


Create or replace transient table Retail_Customer_Dislike_To_Profile_WRK
(
      Meal_Plan_Profile_Id        VARCHAR(16777216) ,
      Meal_Plan_Dislike_Id        VARCHAR(16777216) ,
	  Filename                    VARCHAR(16777216) ,
	  DW_LOGICAL_DELETE_IND       BOOLEAN 
	
);


Create or replace transient table Retail_Customer_Profile_To_Restriction_WRK
(
      Meal_Plan_Profile_Id        VARCHAR(16777216) ,
	  Meal_Plan_Restriction_Id    VARCHAR(16777216) ,
	  Filename                    VARCHAR(16777216) ,
	  DW_LOGICAL_DELETE_IND       BOOLEAN 
);


CREATE OR REPLACE TRANSIENT TABLE Meal_Plan_Recipe_WRK
(
	 Recipe_Varient_Id                VARCHAR(16777216) ,
	 Recipe_Variant_Nm                VARCHAR(16777216) ,
	 Meal_Plan_Recipe_Id              VARCHAR(16777216) ,
	 Recipe_Group_Label_Nm            VARCHAR(16777216) ,
	 Recipe_Thumbnail_Image_Url_Txt   VARCHAR(16777216) ,
	 Recipe_Presentation_Img_Url_Txt  VARCHAR(16777216) ,
	 Recipe_Serving_Cnt               VARCHAR(16777216) ,
	 Recipe_Cooking_Minutes_Cnt       VARCHAR(16777216) ,
	 Recipe_Calories_Cnt              VARCHAR(16777216) ,
	 Filename                         VARCHAR(16777216) ,
	 DW_LOGICAL_DELETE_IND            BOOLEAN 
);

Create or replace transient table Meal_Plan_Cuisine_Tag_WRK
(
      Meal_Plan_Cuisine_Id             VARCHAR(16777216) ,
      Meal_Plan_Cuisine_Nm             VARCHAR(16777216) ,
	  Filename                         VARCHAR(16777216) ,
	  DW_LOGICAL_DELETE_IND            BOOLEAN 
);

Create or replace transient table Retail_Customer_Dislike_WRK
(
      Meal_Plan_Dislike_Id            VARCHAR(16777216) ,
      Meal_Plan_Dislike_Nm            VARCHAR(16777216) ,
	  Filename                        VARCHAR(16777216) ,
	  DW_LOGICAL_DELETE_IND           BOOLEAN 
);

Create or replace transient table Retail_Customer_Dislike_Ingredients_WRK
(
      Meal_Plan_Dislike_Ingredient_Id  VARCHAR(16777216) ,
      Meal_Plan_Dislike_Id             VARCHAR(16777216) ,
	  Filename                         VARCHAR(16777216) ,
	  DW_LOGICAL_DELETE_IND            BOOLEAN 
);

create or replace TRANSIENT TABLE RETAIL_CUSTOMER_FAVORITE_WRK 
(
    RETAIL_CUSTOMER_UUID VARCHAR(16777216),
	RECIPE_FAVORITE_ID VARCHAR(16777216),
	RECIPE_VARIANT_ID VARCHAR(16777216),
	SOURCE_CREATE_TS VARCHAR(16777216),
	SOURCE_UPDATE_TS VARCHAR(16777216),
	Filename VARCHAR(16777216),
	DW_LOGICAL_DELETE_IND BOOLEAN
);

CREATE OR REPLACE TRANSIENT TABLE Meal_Plan_Customer_Flag_WRK
(
     Retail_Customer_UUID         VARCHAR(16777216) ,
     Retail_Customer_Flag_Txt     VARCHAR(16777216) ,
     Filename                     VARCHAR(16777216) ,
     DW_LOGICAL_DELETE_IND        BOOLEAN 
);

Create or replace transient table Meal_Plan_Ingredient_Restriction_WRK
(
      Meal_Plan_Restriction_Id             VARCHAR(16777216) ,
      Meal_Plan_Restricted_Ingredient_Id   VARCHAR(16777216) ,
	  Meal_Plan_Is_Warning_Only_Ind        VARCHAR(16777216) ,
	  Filename                             VARCHAR(16777216) ,
	  DW_LOGICAL_DELETE_IND                BOOLEAN
);

Create or replace transient table Meal_Plan_Ingredient_WRK
(
      Meal_Plan_Ingredient_Id          VARCHAR(16777216) ,
      Meal_Plan_Ingredient_Nm          VARCHAR(16777216) ,
	  Filename                         VARCHAR(16777216) ,
	  DW_LOGICAL_DELETE_IND            BOOLEAN 
);	  

CREATE OR REPLACE TRANSIENT TABLE Retail_Customer_Meal_Plan_WRK
(
     Retail_Customer_UUID             VARCHAR(16777216) ,
     MEAL_PLAN_MEAL_ID                VARCHAR(16777216) ,
     RECIPE_VARIENT_ID                VARCHAR(16777216) ,
     SOURCE_CREATE_TS                 VARCHAR(16777216) ,
     Recipe_Cooked_Dt                 VARCHAR(16777216) ,
     Filename                         VARCHAR(16777216) ,
     DW_LOGICAL_DELETE_IND            BOOLEAN
);

CREATE OR REPLACE TRANSIENT TABLE Retail_Customer_Pending_Meal_WRK
(
     Retail_Customer_UUID          VARCHAR(16777216) ,
     Pending_Meal_Id               VARCHAR(16777216) ,
     Recipe_Varient_Id             VARCHAR(16777216) ,
     Source_Create_Ts              VARCHAR(16777216) ,
     Filename                      VARCHAR(16777216) ,
     DW_LOGICAL_DELETE_IND         BOOLEAN 
);

CREATE OR REPLACE TRANSIENT TABLE Retail_Customer_MealPlan_Profile_WRK
(
     Retail_Customer_UUID          VARCHAR(16777216) ,
     Meal_Plan_Profile_Id          VARCHAR(16777216) ,
     Meal_Plan_Profile_Nm          VARCHAR(16777216) ,
     Recipe_Serving_Cnt            VARCHAR(16777216) ,
     Meal_Plan_Diet_Type_Nm        VARCHAR(16777216) ,
     Meal_Plan_Diet_Type_Cd        VARCHAR(16777216) ,
     Filename                      VARCHAR(16777216) ,
     DW_LOGICAL_DELETE_IND         BOOLEAN 
);

Create or replace transient table Meal_Plan_Restriction_WRK
(
      Meal_Plan_Restriction_Id        VARCHAR(16777216) ,
      Meal_Plan_Restriction_Nm        VARCHAR(16777216) ,
	  Filename                        VARCHAR(16777216) ,
	  DW_LOGICAL_DELETE_IND           BOOLEAN 
);

Create or replace transient table Meal_Plan_Variety_Tag_WRK
(
      Recipe_Variety_Id               VARCHAR(16777216) ,
      Recipe_Variety_Type_Nm          VARCHAR(16777216) ,
	  Filename                        VARCHAR(16777216) ,
	  DW_LOGICAL_DELETE_IND           BOOLEAN 
);

CREATE OR REPLACE TRANSIENT TABLE Meal_Plan_App_Feedback_WRK
(
     Meal_Plan_App_Feedback_Id        VARCHAR(16777216) ,
     Meal_Plan_User_Feedback_Txt      VARCHAR(16777216) ,
     Meal_Plan_Star_Rating_Cnt        VARCHAR(16777216) ,
     Source_Create_Ts                 VARCHAR(16777216) ,
     Filename                         VARCHAR(16777216) ,
     DW_LOGICAL_DELETE_IND            BOOLEAN 
);    

create or replace TRANSIENT TABLE Meal_Plan_Customer_WRK 
(
    RETAIL_CUSTOMER_UUID VARCHAR(16777216),
	Meal_Plan_Role_Nm VARCHAR(16777216),
	Source_Create_Ts VARCHAR(16777216),
	Source_Last_Update_Ts VARCHAR(16777216),
	Filename VARCHAR(16777216),
	DW_LOGICAL_DELETE_IND BOOLEAN
);

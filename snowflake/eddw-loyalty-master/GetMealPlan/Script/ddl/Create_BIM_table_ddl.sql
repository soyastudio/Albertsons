--liquibase formatted sql
--changeset SYSTEM:Create_BIM_table_ddl runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
USE DATABASE <<EDM_DB_NAME>>;
USE SCHEMA DW_C_LOYALTY;

CREATE OR REPLACE TABLE Meal_Plan_Recipe
(
 Recipe_Varient_Id                NUMBER   NOT NULL ,
 DW_First_Effective_Dt            DATE     NOT NULL ,
 DW_Last_Effective_Dt             DATE     NOT NULL ,
 Recipe_Variant_Nm                VARCHAR  NOT NULL ,
 Meal_Plan_Recipe_Id              NUMBER ,
 Recipe_Group_Label_Nm            VARCHAR ,
 Recipe_Thumbnail_Image_Url_Txt   VARCHAR ,
 Recipe_Presentation_Img_Url_Txt  VARCHAR ,
 Recipe_Serving_Cnt               NUMBER ,
 Recipe_Cooking_Minutes_Cnt       NUMBER ,
 Recipe_Calories_Cnt              NUMBER(38,2) ,
 DW_CREATE_TS                     TIMESTAMP ,
 DW_LAST_UPDATE_TS                TIMESTAMP ,
 DW_LOGICAL_DELETE_IND            BOOLEAN ,
 DW_SOURCE_CREATE_NM              VARCHAR(255) ,
 DW_SOURCE_UPDATE_NM              VARCHAR(255) ,
 DW_CURRENT_VERSION_IND           BOOLEAN  
);

COMMENT ON TABLE Meal_Plan_Recipe IS 'Minimal recipe data to display as a thumbnail';

COMMENT ON COLUMN Meal_Plan_Recipe.Recipe_Varient_Id IS 'Primary key of recipe variant, integer';

COMMENT ON COLUMN Meal_Plan_Recipe.Meal_Plan_Recipe_Id IS 'Foreign key of recipe variant in meal plan, integer';

COMMENT ON COLUMN Meal_Plan_Recipe.Recipe_Group_Label_Nm IS 'E.g. Dairy-free vegetarian';

COMMENT ON COLUMN Meal_Plan_Recipe.Recipe_Thumbnail_Image_Url_Txt IS 'URL to image CDN (small)';

COMMENT ON COLUMN Meal_Plan_Recipe.Recipe_Presentation_Img_Url_Txt IS 'URL to image CDN (large)';

COMMENT ON COLUMN Meal_Plan_Recipe.Recipe_Serving_Cnt IS 'Number of servings';

COMMENT ON COLUMN Meal_Plan_Recipe.Recipe_Cooking_Minutes_Cnt IS 'Time to cook in minutes';

COMMENT ON COLUMN Meal_Plan_Recipe.Recipe_Calories_Cnt IS 'Number of calories';

COMMENT ON COLUMN Meal_Plan_Recipe.DW_CREATE_TS IS 'The timestamp the record was inserted.';

COMMENT ON COLUMN Meal_Plan_Recipe.DW_LAST_UPDATE_TS IS 'When a record is updated  this would be the current timestamp';

COMMENT ON COLUMN Meal_Plan_Recipe.DW_LOGICAL_DELETE_IND IS 'Set to True when we receive a delete record for the primary key, else False';

COMMENT ON COLUMN Meal_Plan_Recipe.DW_SOURCE_CREATE_NM IS 'The Bod (data source) name of this insert.';

COMMENT ON COLUMN Meal_Plan_Recipe.DW_SOURCE_UPDATE_NM IS 'The Bod (data source) name of this update or delete.';

COMMENT ON COLUMN Meal_Plan_Recipe.DW_CURRENT_VERSION_IND IS 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d';

COMMENT ON COLUMN Meal_Plan_Recipe.DW_First_Effective_Dt IS 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key';

COMMENT ON COLUMN Meal_Plan_Recipe.DW_Last_Effective_Dt IS 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day';

COMMENT ON COLUMN Meal_Plan_Recipe.Recipe_Variant_Nm IS 'Name of Varient of a recipe. 
Recipe can be Soup and the two varients can be 1> Soup with meal ball and 2>another Soup with Pasta';


ALTER TABLE Meal_Plan_Recipe
ADD PRIMARY KEY (Recipe_Varient_Id, DW_First_Effective_Dt, DW_Last_Effective_Dt);

CREATE OR REPLACE TABLE Meal_Plan_Cuisine_Tag
(
 Meal_Plan_Cuisine_Id  NUMBER  NOT NULL ,
 DW_First_Effective_Dt  DATE  NOT NULL ,
 DW_Last_Effective_Dt  DATE  NOT NULL ,
 Meal_Plan_Cuisine_Nm  VARCHAR  NOT NULL ,
 DW_CREATE_TS          TIMESTAMP  ,
 DW_LAST_UPDATE_TS     TIMESTAMP  ,
 DW_LOGICAL_DELETE_IND  BOOLEAN  ,
 DW_SOURCE_CREATE_NM   VARCHAR(255)  ,
 DW_SOURCE_UPDATE_NM   VARCHAR(255)  ,
 DW_CURRENT_VERSION_IND  BOOLEAN  
);

COMMENT ON TABLE Meal_Plan_Cuisine_Tag IS 'A recipe category indicator, specific to cuisines (Asian, Indian, American, etc.)';

COMMENT ON COLUMN Meal_Plan_Cuisine_Tag.Meal_Plan_Cuisine_Id IS 'Primary key of recipe cuisine tag, integer';

COMMENT ON COLUMN Meal_Plan_Cuisine_Tag.Meal_Plan_Cuisine_Nm IS 'Name of cuisine type. Tagged on recipe variants.';

COMMENT ON COLUMN Meal_Plan_Cuisine_Tag.DW_CREATE_TS IS 'The timestamp the record was inserted.';

COMMENT ON COLUMN Meal_Plan_Cuisine_Tag.DW_LAST_UPDATE_TS IS 'When a record is updated  this would be the current timestamp';

COMMENT ON COLUMN Meal_Plan_Cuisine_Tag.DW_LOGICAL_DELETE_IND IS 'Set to True when we receive a delete record for the primary key, else False';

COMMENT ON COLUMN Meal_Plan_Cuisine_Tag.DW_SOURCE_CREATE_NM IS 'The Bod (data source) name of this insert.';

COMMENT ON COLUMN Meal_Plan_Cuisine_Tag.DW_SOURCE_UPDATE_NM IS 'The Bod (data source) name of this update or delete.';

COMMENT ON COLUMN Meal_Plan_Cuisine_Tag.DW_CURRENT_VERSION_IND IS 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d';

COMMENT ON COLUMN Meal_Plan_Cuisine_Tag.DW_First_Effective_Dt IS 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key';

COMMENT ON COLUMN Meal_Plan_Cuisine_Tag.DW_Last_Effective_Dt IS 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day';

ALTER TABLE Meal_Plan_Cuisine_Tag
 ADD PRIMARY KEY (Meal_Plan_Cuisine_Id, DW_First_Effective_Dt, DW_Last_Effective_Dt);

CREATE OR REPLACE TABLE Retail_Customer_Dislike
(
 Meal_Plan_Dislike_Id  NUMBER  NOT NULL IDENTITY(1,1),
 DW_First_Effective_Dt  DATE  NOT NULL ,
 DW_Last_Effective_Dt  DATE  NOT NULL ,
 Meal_Plan_Dislike_Nm  VARCHAR  NOT NULL ,
 DW_CREATE_TS          TIMESTAMP  ,
 DW_LAST_UPDATE_TS     TIMESTAMP  ,
 DW_LOGICAL_DELETE_IND  BOOLEAN  ,
 DW_SOURCE_CREATE_NM   VARCHAR(255)  ,
 DW_SOURCE_UPDATE_NM   VARCHAR(255)  ,
 DW_CURRENT_VERSION_IND  BOOLEAN  
);

COMMENT ON TABLE Retail_Customer_Dislike IS 'A category of ingredients that a user can mark as a dislike';

COMMENT ON COLUMN Retail_Customer_Dislike.Meal_Plan_Dislike_Id IS 'Dislike category';

COMMENT ON COLUMN Retail_Customer_Dislike.Meal_Plan_Dislike_Nm IS 'Name of dislike category (e.g. Pork)';

COMMENT ON COLUMN Retail_Customer_Dislike.DW_CREATE_TS IS 'The timestamp the record was inserted.';

COMMENT ON COLUMN Retail_Customer_Dislike.DW_LOGICAL_DELETE_IND IS 'Set to True when we receive a delete record for the primary key, else False';

COMMENT ON COLUMN Retail_Customer_Dislike.DW_SOURCE_CREATE_NM IS 'The Bod (data source) name of this insert.';

COMMENT ON COLUMN Retail_Customer_Dislike.DW_SOURCE_UPDATE_NM IS 'The Bod (data source) name of this update or delete.';

COMMENT ON COLUMN Retail_Customer_Dislike.DW_CURRENT_VERSION_IND IS 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d';

COMMENT ON COLUMN Retail_Customer_Dislike.DW_First_Effective_Dt IS 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key';

COMMENT ON COLUMN Retail_Customer_Dislike.DW_Last_Effective_Dt IS 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day';

COMMENT ON COLUMN Retail_Customer_Dislike.DW_LAST_UPDATE_TS IS 'When a record is updated  this would be the current timestamp';

ALTER TABLE Retail_Customer_Dislike
 ADD PRIMARY KEY (Meal_Plan_Dislike_Id, DW_First_Effective_Dt, DW_Last_Effective_Dt);

CREATE OR REPLACE TABLE Retail_Customer_Dislike_Ingredients
(
 Meal_Plan_Dislike_Ingredient_Id  NUMBER  NOT NULL ,
 Meal_Plan_Dislike_Id  NUMBER  NOT NULL ,
 DW_First_Effective_Dt  DATE  NOT NULL ,
 DW_Last_Effective_Dt  DATE  NOT NULL ,
 DW_CREATE_TS          TIMESTAMP  ,
 DW_LAST_UPDATE_TS     TIMESTAMP  ,
 DW_SOURCE_CREATE_NM   VARCHAR(255)  ,
 DW_SOURCE_UPDATE_NM   VARCHAR(255)  ,
 DW_LOGICAL_DELETE_IND  BOOLEAN  ,
 DW_CURRENT_VERSION_IND  BOOLEAN  
);

COMMENT ON COLUMN Retail_Customer_Dislike_Ingredients.Meal_Plan_Dislike_Ingredient_Id IS 'Ingredient id included in this dislike';

COMMENT ON COLUMN Retail_Customer_Dislike_Ingredients.Meal_Plan_Dislike_Id IS 'Dislike category';

COMMENT ON COLUMN Retail_Customer_Dislike_Ingredients.DW_LAST_UPDATE_TS IS 'When a record is updated  this would be the current timestamp';

COMMENT ON COLUMN Retail_Customer_Dislike_Ingredients.DW_LOGICAL_DELETE_IND IS 'Set to True when we receive a delete record for the primary key, else False';

COMMENT ON COLUMN Retail_Customer_Dislike_Ingredients.DW_SOURCE_CREATE_NM IS 'The Bod (data source) name of this insert.';

COMMENT ON COLUMN Retail_Customer_Dislike_Ingredients.DW_SOURCE_UPDATE_NM IS 'The Bod (data source) name of this update or delete.';

COMMENT ON COLUMN Retail_Customer_Dislike_Ingredients.DW_CURRENT_VERSION_IND IS 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d';

COMMENT ON COLUMN Retail_Customer_Dislike_Ingredients.DW_First_Effective_Dt IS 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key';

COMMENT ON COLUMN Retail_Customer_Dislike_Ingredients.DW_Last_Effective_Dt IS 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day';

COMMENT ON COLUMN Retail_Customer_Dislike_Ingredients.DW_CREATE_TS IS 'The timestamp the record was inserted.';

ALTER TABLE Retail_Customer_Dislike_Ingredients
 ADD PRIMARY KEY (Meal_Plan_Dislike_Ingredient_Id, Meal_Plan_Dislike_Id, DW_First_Effective_Dt, DW_Last_Effective_Dt);

CREATE OR REPLACE TABLE Retail_Customer_Favorite
(
 Retail_Customer_UUID   VARCHAR NOT NULL ,
 Recipe_Favorite_Id     NUMBER NOT NULL IDENTITY (1,1),
 DW_First_Effective_Dt  DATE NOT NULL ,
 DW_Last_Effective_Dt   DATE NOT NULL ,
 Recipe_Variant_Id      NUMBER NOT NULL ,
 Source_Create_Ts       TIMESTAMP ,
 Source_Update_Ts       TIMESTAMP ,
 DW_CREATE_TS           TIMESTAMP ,
 DW_LAST_UPDATE_TS      TIMESTAMP ,
 DW_LOGICAL_DELETE_IND  BOOLEAN ,
 DW_SOURCE_CREATE_NM    VARCHAR(255) ,
 DW_SOURCE_UPDATE_NM    VARCHAR(255) ,
 DW_CURRENT_VERSION_IND  BOOLEAN  
);

COMMENT ON TABLE Retail_Customer_Favorite IS 'Recipes that a user has marked as a favorite';

COMMENT ON COLUMN Retail_Customer_Favorite.DW_CREATE_TS IS 'The timestamp the record was inserted.';

COMMENT ON COLUMN Retail_Customer_Favorite.DW_LAST_UPDATE_TS IS 'When a record is updated  this would be the current timestamp';

COMMENT ON COLUMN Retail_Customer_Favorite.DW_LOGICAL_DELETE_IND IS 'Set to True when we receive a delete record for the primary key, else False';

COMMENT ON COLUMN Retail_Customer_Favorite.DW_SOURCE_CREATE_NM IS 'The Bod (data source) name of this insert.';

COMMENT ON COLUMN Retail_Customer_Favorite.DW_SOURCE_UPDATE_NM IS 'The Bod (data source) name of this update or delete.';

COMMENT ON COLUMN Retail_Customer_Favorite.DW_CURRENT_VERSION_IND IS 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d';

COMMENT ON COLUMN Retail_Customer_Favorite.DW_First_Effective_Dt IS 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key';

COMMENT ON COLUMN Retail_Customer_Favorite.DW_Last_Effective_Dt IS 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day';

COMMENT ON COLUMN Retail_Customer_Favorite.Retail_Customer_UUID IS 'This ID represents an Universal Unique Identifier for a Retail Customer';

ALTER TABLE Retail_Customer_Favorite
 ADD PRIMARY KEY (Retail_Customer_UUID, Recipe_Favorite_Id, DW_First_Effective_Dt, DW_Last_Effective_Dt);

CREATE OR REPLACE TABLE Meal_Plan_Ingredient_Restriction
(
 Meal_Plan_Restriction_Id  NUMBER  NOT NULL ,
 Meal_Plan_Restricted_Ingredient_Id  NUMBER  NOT NULL ,
 DW_First_Effective_Dt  DATE  NOT NULL ,
 DW_Last_Effective_Dt  DATE  NOT NULL ,
 Meal_Plan_Is_Warning_Only_Ind  BOOLEAN  NOT NULL ,
 DW_CREATE_TS          TIMESTAMP  ,
 DW_LAST_UPDATE_TS     TIMESTAMP  ,
 DW_LOGICAL_DELETE_IND  BOOLEAN  ,
 DW_SOURCE_CREATE_NM   VARCHAR(255)  ,
 DW_SOURCE_UPDATE_NM   VARCHAR(255)  ,
 DW_CURRENT_VERSION_IND  BOOLEAN  
);

COMMENT ON COLUMN Meal_Plan_Ingredient_Restriction.Meal_Plan_Restriction_Id IS 'Primary key of restriction, integer';

COMMENT ON COLUMN Meal_Plan_Ingredient_Restriction.Meal_Plan_Is_Warning_Only_Ind IS 'Indicates that the restriction may include an allergen, but not necessarily';

COMMENT ON COLUMN Meal_Plan_Ingredient_Restriction.Meal_Plan_Restricted_Ingredient_Id IS 'Ingredient included in the restriction type';

COMMENT ON COLUMN Meal_Plan_Ingredient_Restriction.DW_CREATE_TS IS 'The timestamp the record was inserted.';

COMMENT ON COLUMN Meal_Plan_Ingredient_Restriction.DW_LAST_UPDATE_TS IS 'When a record is updated  this would be the current timestamp';

COMMENT ON COLUMN Meal_Plan_Ingredient_Restriction.DW_LOGICAL_DELETE_IND IS 'Set to True when we receive a delete record for the primary key, else False';

COMMENT ON COLUMN Meal_Plan_Ingredient_Restriction.DW_SOURCE_CREATE_NM IS 'The Bod (data source) name of this insert.';

COMMENT ON COLUMN Meal_Plan_Ingredient_Restriction.DW_SOURCE_UPDATE_NM IS 'The Bod (data source) name of this update or delete.';

COMMENT ON COLUMN Meal_Plan_Ingredient_Restriction.DW_CURRENT_VERSION_IND IS 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d';

COMMENT ON COLUMN Meal_Plan_Ingredient_Restriction.DW_First_Effective_Dt IS 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key';

COMMENT ON COLUMN Meal_Plan_Ingredient_Restriction.DW_Last_Effective_Dt IS 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day';

ALTER TABLE Meal_Plan_Ingredient_Restriction
 ADD PRIMARY KEY (Meal_Plan_Restriction_Id, Meal_Plan_Restricted_Ingredient_Id, DW_First_Effective_Dt, DW_Last_Effective_Dt);

CREATE OR REPLACE TABLE Meal_Plan_Ingredient
(
 Meal_Plan_Ingredient_Id  NUMBER  NOT NULL IDENTITY(1,1),
 DW_First_Effective_Dt  DATE  NOT NULL ,
 DW_Last_Effective_Dt  DATE  NOT NULL ,
 Meal_Plan_Ingredient_Nm  VARCHAR  NOT NULL ,
 DW_CREATE_TS          TIMESTAMP  ,
 DW_LAST_UPDATE_TS     TIMESTAMP  ,
 DW_LOGICAL_DELETE_IND  BOOLEAN  ,
 DW_SOURCE_CREATE_NM   VARCHAR(255)  ,
 DW_SOURCE_UPDATE_NM   VARCHAR(255)  ,
 DW_CURRENT_VERSION_IND  BOOLEAN  
);

COMMENT ON TABLE Meal_Plan_Ingredient IS 'An ingredient that forms a part of a recipe';

COMMENT ON COLUMN Meal_Plan_Ingredient.Meal_Plan_Ingredient_Id IS 'Primary key of recipe ingredient, integer';

COMMENT ON COLUMN Meal_Plan_Ingredient.Meal_Plan_Ingredient_Nm IS 'Name of ingredient (e.g. Asparagus)';

COMMENT ON COLUMN Meal_Plan_Ingredient.DW_CREATE_TS IS 'The timestamp the record was inserted.';

COMMENT ON COLUMN Meal_Plan_Ingredient.DW_LAST_UPDATE_TS IS 'When a record is updated  this would be the current timestamp';

COMMENT ON COLUMN Meal_Plan_Ingredient.DW_LOGICAL_DELETE_IND IS 'Set to True when we receive a delete record for the primary key, else False';

COMMENT ON COLUMN Meal_Plan_Ingredient.DW_SOURCE_CREATE_NM IS 'The Bod (data source) name of this insert.';

COMMENT ON COLUMN Meal_Plan_Ingredient.DW_SOURCE_UPDATE_NM IS 'The Bod (data source) name of this update or delete.';

COMMENT ON COLUMN Meal_Plan_Ingredient.DW_CURRENT_VERSION_IND IS 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d';

COMMENT ON COLUMN Meal_Plan_Ingredient.DW_First_Effective_Dt IS 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key';

COMMENT ON COLUMN Meal_Plan_Ingredient.DW_Last_Effective_Dt IS 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day';

ALTER TABLE Meal_Plan_Ingredient
 ADD PRIMARY KEY (Meal_Plan_Ingredient_Id, DW_First_Effective_Dt, DW_Last_Effective_Dt);

CREATE OR REPLACE TABLE Retail_Customer_Meal_Plan
(
 Retail_Customer_UUID    VARCHAR NOT NULL ,
 Meal_Plan_Meal_Id       VARCHAR NOT NULL ,
 DW_First_Effective_Dt   DATE NOT NULL ,
 DW_Last_Effective_Dt    DATE NOT NULL ,
 Recipe_Varient_Id       NUMBER NOT NULL ,
 Source_Create_Ts        TIMESTAMP ,
 Recipe_Cooked_Dt        DATE ,
 DW_CREATE_TS            TIMESTAMP ,
 DW_LAST_UPDATE_TS       TIMESTAMP ,
 DW_LOGICAL_DELETE_IND   BOOLEAN ,
 DW_SOURCE_CREATE_NM     VARCHAR(255) ,
 DW_SOURCE_UPDATE_NM     VARCHAR(255) ,
 DW_CURRENT_VERSION_IND  BOOLEAN  
);

COMMENT ON TABLE Retail_Customer_Meal_Plan IS 'Recipes that a user has both selected and completed shopping';

COMMENT ON COLUMN Retail_Customer_Meal_Plan.Meal_Plan_Meal_Id IS 'Meal/ Recipe orderd at specific time.mealid is a uuid, like 028e155e-05f8-4343-a300-91e226316e7c';

COMMENT ON COLUMN Retail_Customer_Meal_Plan.Recipe_Cooked_Dt IS 'Indicates if the planned meal was cooked';

COMMENT ON COLUMN Retail_Customer_Meal_Plan.DW_CREATE_TS IS 'The timestamp the record was inserted.';

COMMENT ON COLUMN Retail_Customer_Meal_Plan.DW_LAST_UPDATE_TS IS 'When a record is updated  this would be the current timestamp';

COMMENT ON COLUMN Retail_Customer_Meal_Plan.DW_LOGICAL_DELETE_IND IS 'Set to True when we receive a delete record for the primary key, else False';

COMMENT ON COLUMN Retail_Customer_Meal_Plan.DW_SOURCE_CREATE_NM IS 'The Bod (data source) name of this insert.';

COMMENT ON COLUMN Retail_Customer_Meal_Plan.DW_SOURCE_UPDATE_NM IS 'The Bod (data source) name of this update or delete.';

COMMENT ON COLUMN Retail_Customer_Meal_Plan.DW_CURRENT_VERSION_IND IS 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d';

COMMENT ON COLUMN Retail_Customer_Meal_Plan.DW_First_Effective_Dt IS 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key';

COMMENT ON COLUMN Retail_Customer_Meal_Plan.DW_Last_Effective_Dt IS 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day';

COMMENT ON COLUMN Retail_Customer_Meal_Plan.Retail_Customer_UUID IS 'This ID represents an Universal Unique Identifier for a Retail Customer';

ALTER TABLE Retail_Customer_Meal_Plan
 ADD PRIMARY KEY (Retail_Customer_UUID, Meal_Plan_Meal_Id, DW_First_Effective_Dt, DW_Last_Effective_Dt);
 
CREATE OR REPLACE TABLE Retail_Customer_Pending_Meal
(
 Retail_Customer_UUID  VARCHAR  NOT NULL ,
 Pending_Meal_Id       VARCHAR  NOT NULL ,
 DW_First_Effective_Dt  DATE  NOT NULL ,
 DW_Last_Effective_Dt  DATE  NOT NULL ,
 Recipe_Varient_Id     NUMBER  ,
 Source_Create_Ts      TIMESTAMP  ,
 DW_LAST_UPDATE_TS     TIMESTAMP  ,
 DW_LOGICAL_DELETE_IND  BOOLEAN  ,
 DW_CREATE_TS          TIMESTAMP  ,
 DW_SOURCE_CREATE_NM   VARCHAR(255)  ,
 DW_SOURCE_UPDATE_NM   VARCHAR(255)  ,
 DW_CURRENT_VERSION_IND  BOOLEAN  
);

COMMENT ON TABLE Retail_Customer_Pending_Meal IS 'Recipes that a user has selected to shop for, but has not yet completed shopping';

COMMENT ON COLUMN Retail_Customer_Pending_Meal.Pending_Meal_id IS 'Primary key of pending meal plan, UUID';

COMMENT ON COLUMN Retail_Customer_Pending_Meal.DW_CREATE_TS IS 'The timestamp the record was inserted.';

COMMENT ON COLUMN Retail_Customer_Pending_Meal.DW_LAST_UPDATE_TS IS 'When a record is updated  this would be the current timestamp';

COMMENT ON COLUMN Retail_Customer_Pending_Meal.DW_LOGICAL_DELETE_IND IS 'Set to True when we receive a delete record for the primary key, else False';

COMMENT ON COLUMN Retail_Customer_Pending_Meal.DW_SOURCE_CREATE_NM IS 'The Bod (data source) name of this insert.';

COMMENT ON COLUMN Retail_Customer_Pending_Meal.DW_SOURCE_UPDATE_NM IS 'The Bod (data source) name of this update or delete.';

COMMENT ON COLUMN Retail_Customer_Pending_Meal.DW_CURRENT_VERSION_IND IS 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d';

COMMENT ON COLUMN Retail_Customer_Pending_Meal.DW_First_Effective_Dt IS 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key';

COMMENT ON COLUMN Retail_Customer_Pending_Meal.DW_Last_Effective_Dt IS 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day';

COMMENT ON COLUMN Retail_Customer_Pending_Meal.Retail_Customer_UUID IS 'This ID represents an Universal Unique Identifier for a Retail Customer';

ALTER TABLE Retail_Customer_Pending_Meal
 ADD PRIMARY KEY (Retail_Customer_UUID, Pending_Meal_Id, DW_First_Effective_Dt, DW_Last_Effective_Dt);

CREATE OR REPLACE TABLE Retail_Customer_MealPlan_Profile
(
 Retail_Customer_UUID  VARCHAR  NOT NULL ,
 Meal_Plan_Profile_Id  NUMBER  NOT NULL ,
 DW_First_Effective_Dt  DATE  NOT NULL ,
 DW_Last_Effective_Dt  DATE  NOT NULL ,
 Meal_Plan_Profile_Nm  VARCHAR  ,
 Recipe_Serving_Cnt    NUMBER  ,
 Meal_Plan_Diet_Type_Nm  VARCHAR  ,
 Meal_Plan_Diet_Type_Cd  NUMBER  ,
 DW_CREATE_TS          TIMESTAMP  ,
 DW_LAST_UPDATE_TS     TIMESTAMP  ,
 DW_LOGICAL_DELETE_IND  BOOLEAN  ,
 DW_SOURCE_CREATE_NM   VARCHAR(255)  ,
 DW_SOURCE_UPDATE_NM   VARCHAR(255)  ,
 DW_CURRENT_VERSION_IND  BOOLEAN  
);

COMMENT ON TABLE Retail_Customer_MealPlan_Profile IS 'Defines a user''s dietary and recipe preferences';

COMMENT ON COLUMN Retail_Customer_MealPlan_Profile.Meal_Plan_Profile_id Is 'Primary key of user profile, auto-incrementing integer';

COMMENT ON COLUMN Retail_Customer_MealPlan_Profile.Recipe_Serving_Cnt IS 'Default recipe serving size to show user';

COMMENT ON COLUMN Retail_Customer_MealPlan_Profile.Meal_Plan_Diet_Type_Nm IS 'Which type of meals to show the user (e.g. Vegetarian)';

COMMENT ON COLUMN Retail_Customer_MealPlan_Profile.Meal_Plan_Diet_Type_Cd IS 'Name of diet type, string.';

COMMENT ON COLUMN Retail_Customer_MealPlan_Profile.DW_CREATE_TS IS 'The timestamp the record was inserted.';

COMMENT ON COLUMN Retail_Customer_MealPlan_Profile.DW_LAST_UPDATE_TS IS 'When a record is updated  this would be the current timestamp';

COMMENT ON COLUMN Retail_Customer_MealPlan_Profile.DW_LOGICAL_DELETE_IND IS 'Set to True when we receive a delete record for the primary key, else False';

COMMENT ON COLUMN Retail_Customer_MealPlan_Profile.DW_SOURCE_CREATE_NM IS 'The Bod (data source) name of this insert.';

COMMENT ON COLUMN Retail_Customer_MealPlan_Profile.DW_SOURCE_UPDATE_NM IS 'The Bod (data source) name of this update or delete.';

COMMENT ON COLUMN Retail_Customer_MealPlan_Profile.DW_CURRENT_VERSION_IND IS 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d';

COMMENT ON COLUMN Retail_Customer_MealPlan_Profile.DW_First_Effective_Dt IS 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key';

COMMENT ON COLUMN Retail_Customer_MealPlan_Profile.DW_Last_Effective_Dt IS 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day';

COMMENT ON COLUMN Retail_Customer_MealPlan_Profile.Retail_Customer_UUID IS 'This ID represents an Universal Unique Identifier for a Retail Customer';

ALTER TABLE Retail_Customer_MealPlan_Profile
 ADD PRIMARY KEY (Retail_Customer_UUID, Meal_Plan_Profile_Id, DW_First_Effective_Dt, DW_Last_Effective_Dt);


CREATE OR REPLACE TABLE Meal_Plan_Restriction
(
 Meal_Plan_Restriction_Id  NUMBER  NOT NULL IDENTITY ( 1,1 ),
 DW_First_Effective_Dt  DATE  NOT NULL ,
 DW_Last_Effective_Dt  DATE  NOT NULL ,
 Meal_Plan_Restriction_Nm  VARCHAR  NOT NULL ,
 DW_CREATE_TS          TIMESTAMP  ,
 DW_LAST_UPDATE_TS     TIMESTAMP  ,
 DW_LOGICAL_DELETE_IND  BOOLEAN  ,
 DW_SOURCE_CREATE_NM   VARCHAR(255)  ,
 DW_SOURCE_UPDATE_NM   VARCHAR(255)  ,
 DW_CURRENT_VERSION_IND  BOOLEAN  
);

COMMENT ON TABLE Meal_Plan_Restriction IS 'A category of ingredients that a user can mark as an allergy/dietary restriction';

COMMENT ON COLUMN Meal_Plan_Restriction.Meal_Plan_Restriction_Id IS 'Primary key of restriction, integer';

COMMENT ON COLUMN Meal_Plan_Restriction.Meal_Plan_Restriction_Nm IS 'Name of the restriction category (e.g. Dairy)';

COMMENT ON COLUMN Meal_Plan_Restriction.DW_CREATE_TS IS 'The timestamp the record was inserted.';

COMMENT ON COLUMN Meal_Plan_Restriction.DW_LAST_UPDATE_TS IS 'When a record is updated  this would be the current timestamp';

COMMENT ON COLUMN Meal_Plan_Restriction.DW_LOGICAL_DELETE_IND IS 'Set to True when we receive a delete record for the primary key, else False';

COMMENT ON COLUMN Meal_Plan_Restriction.DW_SOURCE_CREATE_NM IS 'The Bod (data source) name of this insert.';

COMMENT ON COLUMN Meal_Plan_Restriction.DW_SOURCE_UPDATE_NM IS 'The Bod (data source) name of this update or delete.';

COMMENT ON COLUMN Meal_Plan_Restriction.DW_CURRENT_VERSION_IND IS 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d';

COMMENT ON COLUMN Meal_Plan_Restriction.DW_First_Effective_Dt IS 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key';

COMMENT ON COLUMN Meal_Plan_Restriction.DW_Last_Effective_Dt IS 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day';

ALTER TABLE Meal_Plan_Restriction
 ADD PRIMARY KEY (Meal_Plan_Restriction_Id, DW_First_Effective_Dt, DW_Last_Effective_Dt);

CREATE OR REPLACE TABLE Meal_Plan_Variety_Tag
(
 Recipe_Variety_Id       NUMBER    NOT NULL ,
 DW_First_Effective_Dt   DATE      NOT NULL ,
 DW_Last_Effective_Dt    DATE      NOT NULL ,
 Recipe_Variety_Type_Nm  VARCHAR   NOT NULL ,
 DW_CREATE_TS            TIMESTAMP ,
 DW_LAST_UPDATE_TS       TIMESTAMP ,
 DW_LOGICAL_DELETE_IND   BOOLEAN ,
 DW_SOURCE_CREATE_NM     VARCHAR(255) ,
 DW_SOURCE_UPDATE_NM     VARCHAR(255) ,
 DW_CURRENT_VERSION_IND  BOOLEAN  
);

COMMENT ON TABLE Meal_Plan_Variety_Tag IS 'A recipe category indicator';

COMMENT ON COLUMN Meal_Plan_Variety_Tag.Recipe_Variety_Id IS 'recipe variety tag, integer';

COMMENT ON COLUMN Meal_Plan_Variety_Tag.Recipe_Variety_Type_Nm IS 'Variety type (e.g. Salad). Tagged on recipes.';

COMMENT ON COLUMN Meal_Plan_Variety_Tag.DW_CREATE_TS IS 'The timestamp the record was inserted.';

COMMENT ON COLUMN Meal_Plan_Variety_Tag.DW_LAST_UPDATE_TS IS 'When a record is updated  this would be the current timestamp';

COMMENT ON COLUMN Meal_Plan_Variety_Tag.DW_LOGICAL_DELETE_IND IS 'Set to True when we receive a delete record for the primary key, else False';

COMMENT ON COLUMN Meal_Plan_Variety_Tag.DW_SOURCE_CREATE_NM IS 'The Bod (data source) name of this insert.';

COMMENT ON COLUMN Meal_Plan_Variety_Tag.DW_SOURCE_UPDATE_NM IS 'The Bod (data source) name of this update or delete.';

COMMENT ON COLUMN Meal_Plan_Variety_Tag.DW_CURRENT_VERSION_IND IS 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d';

COMMENT ON COLUMN Meal_Plan_Variety_Tag.DW_First_Effective_Dt IS 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key';

COMMENT ON COLUMN Meal_Plan_Variety_Tag.DW_Last_Effective_Dt IS 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day';

ALTER TABLE Meal_Plan_Variety_Tag
 ADD PRIMARY KEY (Recipe_Variety_Id, DW_First_Effective_Dt, DW_Last_Effective_Dt);

CREATE OR REPLACE TABLE Meal_Plan_App_Feedback
(
 Meal_Plan_App_Feedback_Id  NUMBER  NOT NULL IDENTITY (1,1),
 DW_First_Effective_Dt  DATE  NOT NULL ,
 DW_Last_Effective_Dt  DATE  NOT NULL ,
 Meal_Plan_User_Feedback_Txt  VARCHAR  ,
 Meal_Plan_Star_Rating_Cnt  NUMBER  ,
 Source_Create_Ts      TIMESTAMP  ,
 DW_CREATE_TS          TIMESTAMP  ,
 DW_LAST_UPDATE_TS     TIMESTAMP  ,
 DW_LOGICAL_DELETE_IND  BOOLEAN  ,
 DW_SOURCE_CREATE_NM   VARCHAR(255)  ,
 DW_SOURCE_UPDATE_NM   VARCHAR(255)  ,
 DW_CURRENT_VERSION_IND  BOOLEAN  
);

COMMENT ON TABLE Meal_Plan_App_Feedback IS 'User-submitted feedback within the meal plan app section';

COMMENT ON COLUMN Meal_Plan_App_Feedback.Meal_Plan_App_Feedback_id IS 'Primary key of app feedback, auto-incrementing integer';

COMMENT ON COLUMN Meal_Plan_App_Feedback.Meal_Plan_User_Feedback_Txt IS 'Text body of user-submitted feedback';

COMMENT ON COLUMN Meal_Plan_App_Feedback.Meal_Plan_Star_Rating_Cnt IS 'star rating feedback for Meal Plans';

COMMENT ON COLUMN Meal_Plan_App_Feedback.Source_Create_Ts IS 'Timestamp of feedback';

COMMENT ON COLUMN Meal_Plan_App_Feedback.DW_CREATE_TS IS 'The timestamp the record was inserted.';

COMMENT ON COLUMN Meal_Plan_App_Feedback.DW_LAST_UPDATE_TS IS 'When a record is updated  this would be the current timestamp';

COMMENT ON COLUMN Meal_Plan_App_Feedback.DW_LOGICAL_DELETE_IND IS 'Set to True when we receive a delete record for the primary key, else False';

COMMENT ON COLUMN Meal_Plan_App_Feedback.DW_SOURCE_CREATE_NM IS 'The Bod (data source) name of this insert.';

COMMENT ON COLUMN Meal_Plan_App_Feedback.DW_SOURCE_UPDATE_NM IS 'The Bod (data source) name of this update or delete.';

COMMENT ON COLUMN Meal_Plan_App_Feedback.DW_CURRENT_VERSION_IND IS 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d';

COMMENT ON COLUMN Meal_Plan_App_Feedback.DW_First_Effective_Dt IS 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key';

COMMENT ON COLUMN Meal_Plan_App_Feedback.DW_Last_Effective_Dt IS 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day';

ALTER TABLE Meal_Plan_App_Feedback
 ADD PRIMARY KEY (Meal_Plan_App_Feedback_Id, DW_First_Effective_Dt, DW_Last_Effective_Dt);

CREATE OR REPLACE TABLE Meal_Plan_Customer_Flag
(
 Retail_Customer_UUID  VARCHAR  NOT NULL ,
 Retail_Customer_Flag_Txt  VARCHAR  NOT NULL ,
 DW_First_Effective_Dt  DATE  NOT NULL ,
 DW_Last_Effective_Dt  DATE  NOT NULL ,
 DW_CREATE_TS          TIMESTAMP  ,
 DW_LAST_UPDATE_TS     TIMESTAMP  ,
 DW_LOGICAL_DELETE_IND  BOOLEAN  ,
 DW_SOURCE_CREATE_NM   VARCHAR(255)  ,
 DW_SOURCE_UPDATE_NM   VARCHAR(255)  ,
 DW_CURRENT_VERSION_IND  BOOLEAN  
);

COMMENT ON TABLE Meal_Plan_Customer_Flag IS 'User flags indicate various user properties specific to the meal planning app';

COMMENT ON COLUMN Meal_Plan_Customer_Flag.Retail_Customer_Flag_Txt IS 'Value of the flag example, onboarding complete, optin for notifications';

COMMENT ON COLUMN Meal_Plan_Customer_Flag.DW_CREATE_TS IS 'The timestamp the record was inserted.';

COMMENT ON COLUMN Meal_Plan_Customer_Flag.DW_LAST_UPDATE_TS IS 'When a record is updated  this would be the current timestamp';

COMMENT ON COLUMN Meal_Plan_Customer_Flag.DW_LOGICAL_DELETE_IND IS 'Set to True when we receive a delete record for the primary key, else False';

COMMENT ON COLUMN Meal_Plan_Customer_Flag.DW_SOURCE_CREATE_NM IS 'The Bod (data source) name of this insert.';

COMMENT ON COLUMN Meal_Plan_Customer_Flag.DW_SOURCE_UPDATE_NM IS 'The Bod (data source) name of this update or delete.';

COMMENT ON COLUMN Meal_Plan_Customer_Flag.DW_CURRENT_VERSION_IND IS 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d';

COMMENT ON COLUMN Meal_Plan_Customer_Flag.DW_First_Effective_Dt IS 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key';

COMMENT ON COLUMN Meal_Plan_Customer_Flag.DW_Last_Effective_Dt IS 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day';

COMMENT ON COLUMN Meal_Plan_Customer_Flag.Retail_Customer_UUID IS 'This ID represents an Universal Unique Identifier for a Retail Customer';

ALTER TABLE Meal_Plan_Customer_Flag
 ADD PRIMARY KEY (Retail_Customer_UUID, Retail_Customer_Flag_Txt, DW_First_Effective_Dt, DW_Last_Effective_Dt);


CREATE OR REPLACE TABLE Meal_Plan_Customer
(
 Retail_Customer_UUID   VARCHAR  NOT NULL ,
 DW_First_Effective_Dt  DATE  NOT NULL ,
 DW_Last_Effective_Dt   DATE  NOT NULL ,
 Meal_Plan_Role_Nm      VARCHAR  NOT NULL ,
 Source_Create_Ts       TIMESTAMP  ,
 Source_Last_Update_Ts  TIMESTAMP  , 
 DW_CREATE_TS           TIMESTAMP  ,
 DW_LAST_UPDATE_TS      TIMESTAMP  ,
 DW_LOGICAL_DELETE_IND  BOOLEAN  ,
 DW_SOURCE_CREATE_NM    VARCHAR(255)  ,
 DW_SOURCE_UPDATE_NM    VARCHAR(255)  ,
 DW_CURRENT_VERSION_IND  BOOLEAN  
);

COMMENT ON TABLE Meal_Plan_Customer IS 'A person who has identified themselves to us as a customer, genearlly identified by a club card.';

COMMENT ON COLUMN Meal_Plan_Customer.Retail_Customer_UUID IS 'This ID represents an Universal Unique Identifier for a Retail Customer';

COMMENT ON COLUMN Meal_Plan_Customer.Source_Create_Ts IS 'Timestamp of Meal plan Customer';

COMMENT ON COLUMN Meal_Plan_Customer.Source_Last_Update_Ts IS 'The last update Meal plan Customer timestamp';

COMMENT ON COLUMN Meal_Plan_Customer.DW_LAST_UPDATE_TS IS 'The last update timestamp of the record in BIM Snowflake database';

COMMENT ON COLUMN Meal_Plan_Customer.DW_LOGICAL_DELETE_IND IS 'The logical delete indicator indicates if the record is a candidate for logical deletion in of the BIM Snowflake database';

COMMENT ON COLUMN Meal_Plan_Customer.DW_SOURCE_UPDATE_NM IS 'The Bod (data source) name of update or delete';

COMMENT ON COLUMN Meal_Plan_Customer.DW_CURRENT_VERSION_IND IS 'Set to yes when the current record is deleted,  the Last Effective Timestamp on this record is still set to be  current timestamp -1 second';

COMMENT ON COLUMN Meal_Plan_Customer.DW_CREATE_TS IS 'The create timestamp of the record in BIM Snowflake database';

COMMENT ON COLUMN Meal_Plan_Customer.Meal_Plan_Role_Nm IS 'to control the MealPlan site contant based upon Role , Example Admin or user. In future as admin they will be able to load their own MealPlan.';

ALTER TABLE Meal_Plan_Customer
 ADD PRIMARY KEY (Retail_Customer_UUID, DW_First_Effective_Dt, DW_Last_Effective_Dt);
 
 
CREATE OR REPLACE TABLE Retail_Customer_Dislike_To_Profile
(
 Meal_Plan_Profile_Id  NUMBER  NOT NULL ,
 Meal_Plan_Dislike_Id  NUMBER  NOT NULL ,
 DW_First_Effective_Dt  DATE  NOT NULL ,
 DW_Last_Effective_Dt  DATE  NOT NULL ,
 DW_CREATE_TS          TIMESTAMP  ,
 DW_LAST_UPDATE_TS     TIMESTAMP  ,
 DW_SOURCE_CREATE_NM   VARCHAR(255)  ,
 DW_SOURCE_UPDATE_NM   VARCHAR(255)  ,
 DW_LOGICAL_DELETE_IND  BOOLEAN  ,
 DW_CURRENT_VERSION_IND  BOOLEAN  
);

COMMENT ON TABLE Retail_Customer_Dislike_To_Profile IS 'Join table of profiles to dislikes';

COMMENT ON COLUMN Retail_Customer_Dislike_To_Profile.Meal_Plan_Dislike_Id IS 'Dislike category';

COMMENT ON COLUMN Retail_Customer_Dislike_To_Profile.Meal_Plan_Profile_id Is 'Primary key of user profile, auto-incrementing integer';

COMMENT ON COLUMN Retail_Customer_Dislike_To_Profile.DW_LAST_UPDATE_TS IS 'When a record is updated  this would be the current timestamp';

COMMENT ON COLUMN Retail_Customer_Dislike_To_Profile.DW_LOGICAL_DELETE_IND IS 'Set to True when we receive a delete record for the primary key, else False';

COMMENT ON COLUMN Retail_Customer_Dislike_To_Profile.DW_SOURCE_CREATE_NM IS 'The Bod (data source) name of this insert.';

COMMENT ON COLUMN Retail_Customer_Dislike_To_Profile.DW_SOURCE_UPDATE_NM IS 'The Bod (data source) name of this update or delete.';

COMMENT ON COLUMN Retail_Customer_Dislike_To_Profile.DW_CURRENT_VERSION_IND IS 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d';

COMMENT ON COLUMN Retail_Customer_Dislike_To_Profile.DW_First_Effective_Dt IS 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key';

COMMENT ON COLUMN Retail_Customer_Dislike_To_Profile.DW_Last_Effective_Dt IS 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day';

COMMENT ON COLUMN Retail_Customer_Dislike_To_Profile.DW_CREATE_TS IS 'The timestamp the record was inserted.';

ALTER TABLE Retail_Customer_Dislike_To_Profile
 ADD PRIMARY KEY (Meal_Plan_Profile_Id, Meal_Plan_Dislike_Id, DW_First_Effective_Dt, DW_Last_Effective_Dt);

CREATE OR REPLACE TABLE Retail_Customer_Profile_To_Restriction
(
 Meal_Plan_Profile_Id  NUMBER  NOT NULL ,
 Meal_Plan_Restriction_Id  NUMBER  NOT NULL ,
 DW_First_Effective_Dt  DATE  NOT NULL ,
 DW_Last_Effective_Dt  DATE  NOT NULL ,
 DW_CREATE_TS          TIMESTAMP  ,
 DW_LAST_UPDATE_TS     TIMESTAMP  ,
 DW_SOURCE_CREATE_NM   VARCHAR(255)  ,
 DW_SOURCE_UPDATE_NM   VARCHAR(255)  ,
 DW_LOGICAL_DELETE_IND  BOOLEAN  ,
 DW_CURRENT_VERSION_IND  BOOLEAN  
);

COMMENT ON COLUMN Retail_Customer_Profile_To_Restriction.Meal_Plan_Profile_id Is 'Primary key of user profile, auto-incrementing integer';

COMMENT ON COLUMN Retail_Customer_Profile_To_Restriction.Meal_Plan_Restriction_Id IS 'Primary key of restriction, integer';

COMMENT ON COLUMN Retail_Customer_Profile_To_Restriction.DW_LAST_UPDATE_TS IS 'When a record is updated  this would be the current timestamp';

COMMENT ON COLUMN Retail_Customer_Profile_To_Restriction.DW_LOGICAL_DELETE_IND IS 'Set to True when we receive a delete record for the primary key, else False';

COMMENT ON COLUMN Retail_Customer_Profile_To_Restriction.DW_SOURCE_CREATE_NM IS 'The Bod (data source) name of this insert.';

COMMENT ON COLUMN Retail_Customer_Profile_To_Restriction.DW_SOURCE_UPDATE_NM IS 'The Bod (data source) name of this update or delete.';

COMMENT ON COLUMN Retail_Customer_Profile_To_Restriction.DW_CURRENT_VERSION_IND IS 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d';

COMMENT ON COLUMN Retail_Customer_Profile_To_Restriction.DW_First_Effective_Dt IS 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key';

COMMENT ON COLUMN Retail_Customer_Profile_To_Restriction.DW_Last_Effective_Dt IS 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day';

COMMENT ON COLUMN Retail_Customer_Profile_To_Restriction.DW_CREATE_TS IS 'The timestamp the record was inserted.';

ALTER TABLE Retail_Customer_Profile_To_Restriction
 ADD PRIMARY KEY (Meal_Plan_Profile_Id, Meal_Plan_Restriction_Id, DW_First_Effective_Dt, DW_Last_Effective_Dt);

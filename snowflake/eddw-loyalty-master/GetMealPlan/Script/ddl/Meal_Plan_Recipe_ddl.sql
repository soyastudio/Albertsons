--liquibase formatted sql
--changeset SYSTEM:Meal_Plan_Recipe_ddl runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
USE DATABASE <<EDM_DB_NAME>>;
USE SCHEMA <<EDM_DB_NAME>>.DW_C_LOYALTY;

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

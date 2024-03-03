--liquibase formatted sql
--changeset SYSTEM:Create_VIEWS_for_MealPlan runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
USE DATABASE <<EDM_VIEW_NAME>>;
USE SCHEMA <<EDM_VIEW_NAME>>.DW_VIEWS;

CREATE OR REPLACE view Meal_Plan_Recipe
(
 Recipe_Varient_Id Comment 'Primary key of recipe variant, integer ', 
 DW_First_Effective_Dt Comment 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
 DW_Last_Effective_Dt Comment 'for the current record this is ''12/31/9999''. for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
 Recipe_Variant_Nm Comment 'Name of Varient of a recipe. Recipe can be Soup and th etwo varients can be 1> Soup with meal ball and 2>another Soup with Pasta' ,
 Meal_Plan_Recipe_Id Comment 'Foreign key of recipe variant in meal plan, integer' ,
 Recipe_Group_Label_Nm Comment 'E.g. Dairy-free vegetarian',
 Recipe_Thumbnail_Image_Url_Txt Comment 'URL to image CDN (small)',
 Recipe_Presentation_Img_Url_Txt Comment 'URL to image CDN (large)',
 Recipe_Serving_Cnt Comment 'Number of servings',
 Recipe_Cooking_Minutes_Cnt Comment 'Time to cook in minutes',
 Recipe_Calories_Cnt Comment 'Number of calories',
 DW_CREATE_TS Comment 'The timestamp the record was inserted.',
 DW_LAST_UPDATE_TS Comment 'When a record is updated  this would be the current timestamp',
 DW_LOGICAL_DELETE_IND Comment 'Set to True when we receive a delete record for the primary key, else False',
 DW_SOURCE_CREATE_NM  Comment 'The Bod (data source) name of this insert.',
 DW_SOURCE_UPDATE_NM Comment 'The Bod (data source) name of this update or delete.',
 DW_CURRENT_VERSION_IND Comment 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d' 
)
COPY GRANTS
comment = 'VIEW for Meal_Plan_Recipe'
As 
SELECT
Recipe_Varient_Id ,              
DW_First_Effective_Dt ,           
DW_Last_Effective_Dt ,            
Recipe_Variant_Nm ,              
Meal_Plan_Recipe_Id ,  
Recipe_Group_Label_Nm ,          
Recipe_Thumbnail_Image_Url_Txt ,  
Recipe_Presentation_Img_Url_Txt , 
Recipe_Serving_Cnt ,              
Recipe_Cooking_Minutes_Cnt ,      
Recipe_Calories_Cnt ,             
DW_CREATE_TS ,                    
DW_LAST_UPDATE_TS ,               
DW_LOGICAL_DELETE_IND ,           
DW_SOURCE_CREATE_NM ,            
DW_SOURCE_UPDATE_NM ,    
DW_CURRENT_VERSION_IND  
FROM <<EDM_DB_NAME>>.DW_C_LOYALTY.Meal_Plan_Recipe;

CREATE OR REPLACE VIEW MEAL_PLAN_APP_FEEDBACK
(
MEAL_PLAN_APP_FEEDBACK_ID COMMENT 'Primary key of app feedback, auto-incrementing integer' ,
DW_FIRST_EFFECTIVE_DT COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
DW_LAST_EFFECTIVE_DT COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
MEAL_PLAN_USER_FEEDBACK_TXT COMMENT 'Text body of user-submitted feedback',
MEAL_PLAN_STAR_RATING_CNT COMMENT 'star rating feedback for Meal Plans',
SOURCE_CREATE_TS COMMENT 'Timestamp for Feedback',
DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
DW_LAST_UPDATE_TS COMMENT 'When a record is updated  this would be the current timestamp',
DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',
DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
) 
COPY GRANTS
COMMENT='VIEW for Meal_Plan_App_Feedback'
as 
SELECT
Meal_Plan_App_Feedback_Id,
DW_First_Effective_Dt ,           
DW_Last_Effective_Dt ,
Meal_Plan_User_Feedback_Txt,
Meal_Plan_Star_Rating_Cnt,
Source_Create_Ts,
DW_CREATE_TS ,                    
DW_LAST_UPDATE_TS ,               
DW_LOGICAL_DELETE_IND ,           
DW_SOURCE_CREATE_NM ,            
DW_SOURCE_UPDATE_NM ,    
DW_CURRENT_VERSION_IND  
FROM <<EDM_DB_NAME>>.DW_C_LOYALTY.Meal_Plan_App_Feedback;

CREATE OR REPLACE view Meal_Plan_Cuisine_Tag
(
 Meal_Plan_Cuisine_Id comment 'Primary key of recipe cuisine tag, integer',  
 DW_First_Effective_Dt comment 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
 DW_Last_Effective_Dt comment 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',  
 Meal_Plan_Cuisine_Nm comment 'Name of cuisine type. Tagged on recipe variants.',
 DW_CREATE_TS comment 'The timestamp the record was inserted.',
 DW_LAST_UPDATE_TS comment 'When a record is updated  this would be the current timestamp',
 DW_LOGICAL_DELETE_IND comment 'Set to True when we receive a delete record for the primary key, else False',
 DW_SOURCE_CREATE_NM comment 'The Bod (data source) name of this insert.',  
 DW_SOURCE_UPDATE_NM comment 'The Bod (data source) name of this update or delete.',
 DW_CURRENT_VERSION_IND comment 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
)
COPY GRANTS
comment = 'VIEW for Meal_Plan_Cuisine_Tag'
As 
SELECT
Meal_Plan_Cuisine_Id,
DW_First_Effective_Dt,           
DW_Last_Effective_Dt,
Meal_Plan_Cuisine_Nm,
DW_CREATE_TS ,                    
DW_LAST_UPDATE_TS ,               
DW_LOGICAL_DELETE_IND ,           
DW_SOURCE_CREATE_NM ,            
DW_SOURCE_UPDATE_NM ,    
DW_CURRENT_VERSION_IND  
FROM <<EDM_DB_NAME>>.DW_C_LOYALTY.Meal_Plan_Cuisine_Tag;

CREATE OR REPLACE view Meal_Plan_Ingredient
(
 Meal_Plan_Ingredient_Id comment 'Primary key of recipe ingredient, integer',  
 DW_First_Effective_Dt comment 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
 DW_Last_Effective_Dt comment 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',  
 Meal_Plan_Ingredient_Nm comment 'Name of ingredient (e.g. Asparagus)',
 DW_CREATE_TS comment 'The timestamp the record was inserted.',
 DW_LAST_UPDATE_TS comment 'When a record is updated  this would be the current timestamp',
 DW_LOGICAL_DELETE_IND comment 'Set to True when we receive a delete record for the primary key, else False',
 DW_SOURCE_CREATE_NM comment 'The Bod (data source) name of this insert.',  
 DW_SOURCE_UPDATE_NM comment 'The Bod (data source) name of this update or delete.',
 DW_CURRENT_VERSION_IND comment 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
)
COPY GRANTS
comment = 'VIEW for Meal_Plan_Ingredient'
As 
SELECT
Meal_Plan_Ingredient_Id,
DW_First_Effective_Dt,           
DW_Last_Effective_Dt,
Meal_Plan_Ingredient_Nm,
DW_CREATE_TS ,                    
DW_LAST_UPDATE_TS ,               
DW_LOGICAL_DELETE_IND ,           
DW_SOURCE_CREATE_NM ,            
DW_SOURCE_UPDATE_NM ,    
DW_CURRENT_VERSION_IND  
FROM <<EDM_DB_NAME>>.DW_C_LOYALTY.Meal_Plan_Ingredient;

CREATE OR REPLACE view Meal_Plan_Ingredient_Restriction
(
 Meal_Plan_Restriction_Id comment 'Primary key of restriction, integer', 
 Meal_Plan_Restricted_Ingredient_Id comment 'Ingredient included in the restriction type',
 DW_First_Effective_Dt comment 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
 DW_Last_Effective_Dt comment 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',  
 Meal_Plan_Is_Warning_Only_Ind comment 'Indicates that the restriction may include an allergen, but not necessarily',
 DW_CREATE_TS comment 'The timestamp the record was inserted.',
 DW_LAST_UPDATE_TS comment 'When a record is updated  this would be the current timestamp',
 DW_LOGICAL_DELETE_IND comment 'Set to True when we receive a delete record for the primary key, else False',
 DW_SOURCE_CREATE_NM comment 'The Bod (data source) name of this insert.',  
 DW_SOURCE_UPDATE_NM comment 'The Bod (data source) name of this update or delete.',
 DW_CURRENT_VERSION_IND comment 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
)
COPY GRANTS
comment = 'VIEW for Meal_Plan_Ingredient_Restriction'
As 
SELECT
Meal_Plan_Restriction_Id,
Meal_Plan_Restricted_Ingredient_Id,
DW_First_Effective_Dt,           
DW_Last_Effective_Dt,
Meal_Plan_Is_Warning_Only_Ind,
DW_CREATE_TS ,                    
DW_LAST_UPDATE_TS ,               
DW_LOGICAL_DELETE_IND ,           
DW_SOURCE_CREATE_NM ,            
DW_SOURCE_UPDATE_NM ,    
DW_CURRENT_VERSION_IND  
FROM <<EDM_DB_NAME>>.DW_C_LOYALTY.Meal_Plan_Ingredient_Restriction;

CREATE OR REPLACE view Meal_Plan_Variety_Tag
(
 Recipe_Variety_Id comment 'recipe variety tag, integer', 
 DW_First_Effective_Dt comment 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
 DW_Last_Effective_Dt comment 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',  
 Recipe_Variety_Type_Nm comment 'Variety type (e.g. Salad). Tagged on recipes.',
 DW_CREATE_TS comment 'The timestamp the record was inserted.',
 DW_LAST_UPDATE_TS comment 'When a record is updated  this would be the current timestamp',
 DW_LOGICAL_DELETE_IND comment 'Set to True when we receive a delete record for the primary key, else False',
 DW_SOURCE_CREATE_NM comment 'The Bod (data source) name of this insert.',  
 DW_SOURCE_UPDATE_NM comment 'The Bod (data source) name of this update or delete.',
 DW_CURRENT_VERSION_IND comment 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
)
COPY GRANTS
comment = 'VIEW for Meal_Plan_Variety_Tag'
As 
SELECT
Recipe_Variety_Id,
DW_First_Effective_Dt,           
DW_Last_Effective_Dt,
Recipe_Variety_Type_Nm,
DW_CREATE_TS ,                    
DW_LAST_UPDATE_TS ,               
DW_LOGICAL_DELETE_IND ,           
DW_SOURCE_CREATE_NM ,            
DW_SOURCE_UPDATE_NM ,    
DW_CURRENT_VERSION_IND  
FROM <<EDM_DB_NAME>>.DW_C_LOYALTY.Meal_Plan_Variety_Tag;

CREATE OR REPLACE view Retail_Customer_Dislike
(
 Meal_Plan_Dislike_Id comment 'Dislike category', 
 DW_First_Effective_Dt comment 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
 DW_Last_Effective_Dt comment 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',  
 Meal_Plan_Dislike_Nm comment 'Name of dislike category (e.g. Pork)',
 DW_CREATE_TS comment 'The timestamp the record was inserted.',
 DW_LAST_UPDATE_TS comment 'When a record is updated  this would be the current timestamp',
 DW_LOGICAL_DELETE_IND comment 'Set to True when we receive a delete record for the primary key, else False',
 DW_SOURCE_CREATE_NM comment 'The Bod (data source) name of this insert.',  
 DW_SOURCE_UPDATE_NM comment 'The Bod (data source) name of this update or delete.',
 DW_CURRENT_VERSION_IND comment 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
)
COPY GRANTS
comment = 'VIEW for Retail_Customer_Dislike'
As 
SELECT
Meal_Plan_Dislike_Id,
DW_First_Effective_Dt,           
DW_Last_Effective_Dt,
Meal_Plan_Dislike_Nm,
DW_CREATE_TS ,                    
DW_LAST_UPDATE_TS ,               
DW_LOGICAL_DELETE_IND ,           
DW_SOURCE_CREATE_NM ,            
DW_SOURCE_UPDATE_NM ,    
DW_CURRENT_VERSION_IND  
FROM <<EDM_DB_NAME>>.DW_C_LOYALTY.Retail_Customer_Dislike;

CREATE OR REPLACE view Retail_Customer_Dislike_Ingredients
(
 Meal_Plan_Dislike_Ingredient_Id comment 'Ingredient id included in this dislike',
 Meal_Plan_Dislike_Id comment 'Dislike category', 
 DW_First_Effective_Dt comment 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
 DW_Last_Effective_Dt comment 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',  
 DW_CREATE_TS comment 'The timestamp the record was inserted.',
 DW_LAST_UPDATE_TS comment 'When a record is updated  this would be the current timestamp',
 DW_LOGICAL_DELETE_IND comment 'Set to True when we receive a delete record for the primary key, else False',
 DW_SOURCE_CREATE_NM comment 'The Bod (data source) name of this insert.',  
 DW_SOURCE_UPDATE_NM comment 'The Bod (data source) name of this update or delete.',
 DW_CURRENT_VERSION_IND comment 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
)
COPY GRANTS
comment = 'VIEW for Retail_Customer_Dislike_Ingredients'
As 
SELECT
Meal_Plan_Dislike_Ingredient_Id,
Meal_Plan_Dislike_Id,
DW_First_Effective_Dt,           
DW_Last_Effective_Dt,
DW_CREATE_TS ,                    
DW_LAST_UPDATE_TS ,               
DW_LOGICAL_DELETE_IND ,           
DW_SOURCE_CREATE_NM ,            
DW_SOURCE_UPDATE_NM ,    
DW_CURRENT_VERSION_IND  
FROM <<EDM_DB_NAME>>.DW_C_LOYALTY.Retail_Customer_Dislike_Ingredients;

CREATE OR REPLACE view Meal_Plan_Restriction
(
 Meal_Plan_Restriction_Id comment 'Primary key of restriction, integer', 
 DW_First_Effective_Dt comment 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
 DW_Last_Effective_Dt comment 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',  
 Meal_Plan_Restriction_Nm comment 'Name of the restriction category (e.g. Dairy)',
 DW_CREATE_TS comment 'The timestamp the record was inserted.',
 DW_LAST_UPDATE_TS comment 'When a record is updated  this would be the current timestamp',
 DW_LOGICAL_DELETE_IND comment 'Set to True when we receive a delete record for the primary key, else False',
 DW_SOURCE_CREATE_NM comment 'The Bod (data source) name of this insert.',  
 DW_SOURCE_UPDATE_NM comment 'The Bod (data source) name of this update or delete.',
 DW_CURRENT_VERSION_IND comment 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
)
COPY GRANTS
comment = 'VIEW for Meal_Plan_Restriction'
As 
SELECT
Meal_Plan_Restriction_Id,
DW_First_Effective_Dt,           
DW_Last_Effective_Dt,
Meal_Plan_Restriction_Nm,
DW_CREATE_TS ,                    
DW_LAST_UPDATE_TS ,               
DW_LOGICAL_DELETE_IND ,           
DW_SOURCE_CREATE_NM ,            
DW_SOURCE_UPDATE_NM ,    
DW_CURRENT_VERSION_IND  
FROM <<EDM_DB_NAME>>.DW_C_LOYALTY.Meal_Plan_Restriction;

CREATE OR REPLACE view Retail_Customer_Dislike_To_Profile
(
Meal_Plan_Profile_Id comment 'Primary key of user profile, auto-incrementing integer',
Meal_Plan_Dislike_Id comment 'Dislike category',
DW_First_Effective_Dt comment 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
DW_Last_Effective_Dt comment 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',  
DW_CREATE_TS comment 'The timestamp the record was inserted.',
DW_LAST_UPDATE_TS comment 'When a record is updated  this would be the current timestamp',
DW_LOGICAL_DELETE_IND comment 'Set to True when we receive a delete record for the primary key, else False',
DW_SOURCE_CREATE_NM comment 'The Bod (data source) name of this insert.',  
DW_SOURCE_UPDATE_NM comment 'The Bod (data source) name of this update or delete.',
DW_CURRENT_VERSION_IND comment 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
)
COPY GRANTS
comment = 'VIEW for Retail_Customer_Dislike_To_Profile'
As 
SELECT
Meal_Plan_Profile_Id,
Meal_Plan_Dislike_Id,
DW_First_Effective_Dt,           
DW_Last_Effective_Dt,
DW_CREATE_TS ,                    
DW_LAST_UPDATE_TS ,               
DW_LOGICAL_DELETE_IND ,           
DW_SOURCE_CREATE_NM ,            
DW_SOURCE_UPDATE_NM ,    
DW_CURRENT_VERSION_IND  
FROM <<EDM_DB_NAME>>.DW_C_LOYALTY.Retail_Customer_Dislike_To_Profile;

CREATE OR REPLACE view Retail_Customer_Profile_To_Restriction
(
Meal_Plan_Profile_Id comment 'Primary key of user profile, auto-incrementing integer',
Meal_Plan_Restriction_Id comment 'Primary key of restriction, integer',
DW_First_Effective_Dt comment 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
DW_Last_Effective_Dt comment 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',  
DW_CREATE_TS comment 'The timestamp the record was inserted.',
DW_LAST_UPDATE_TS comment 'When a record is updated  this would be the current timestamp',
DW_LOGICAL_DELETE_IND comment 'Set to True when we receive a delete record for the primary key, else False',
DW_SOURCE_CREATE_NM comment 'The Bod (data source) name of this insert.',  
DW_SOURCE_UPDATE_NM comment 'The Bod (data source) name of this update or delete.',
DW_CURRENT_VERSION_IND comment 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
)
COPY GRANTS
comment = 'VIEW for Retail_Customer_Profile_To_Restriction'
As 
SELECT
Meal_Plan_Profile_Id,
Meal_Plan_Restriction_Id,
DW_First_Effective_Dt,           
DW_Last_Effective_Dt,
DW_CREATE_TS ,                    
DW_LAST_UPDATE_TS ,               
DW_LOGICAL_DELETE_IND ,           
DW_SOURCE_CREATE_NM ,            
DW_SOURCE_UPDATE_NM ,    
DW_CURRENT_VERSION_IND  
FROM <<EDM_DB_NAME>>.DW_C_LOYALTY.Retail_Customer_Profile_To_Restriction;

create or replace view RETAIL_CUSTOMER_FAVORITE
(
RETAIL_CUSTOMER_UUID COMMENT 'This ID represents an Universal Unique Identifier for a Retail Customer',
RECIPE_FAVORITE_ID COMMENT 'Primary key of recipe favorite record, auto-incrementing integer ',
DW_FIRST_EFFECTIVE_DT COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
DW_LAST_EFFECTIVE_DT COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
RECIPE_VARIANT_ID COMMENT 'Primary key of recipe variant, integer ' ,
SOURCE_CREATE_TS COMMENT 'Timestamp for Favorite',
SOURCE_UPDATE_TS COMMENT 'The timestamp the record was Updated.',
DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
DW_LAST_UPDATE_TS COMMENT 'When a record is updated  this would be the current timestamp',
DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',
DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
) 
COPY GRANTS
COMMENT='VIEW for Retail_Customer_Favorite'
 as 
SELECT
Retail_Customer_UUID ,
Recipe_Favorite_Id,
DW_First_Effective_Dt ,           
DW_Last_Effective_Dt ,
Recipe_Variant_Id,
Source_Create_Ts,
Source_Update_Ts,
DW_CREATE_TS ,                    
DW_LAST_UPDATE_TS ,               
DW_LOGICAL_DELETE_IND ,           
DW_SOURCE_CREATE_NM ,            
DW_SOURCE_UPDATE_NM ,    
DW_CURRENT_VERSION_IND  
FROM <<EDM_DB_NAME>>.DW_C_LOYALTY.Retail_Customer_Favorite;

CREATE OR REPLACE view Meal_Plan_Customer_Flag
(
Retail_Customer_UUID comment 'This ID represents an Universal Unique Identifier for a Retail Customer',
Retail_Customer_Flag_Txt comment 'Value of the flag example, onboarding complete, optin for notifications', 
DW_First_Effective_Dt comment 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
DW_Last_Effective_Dt comment 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day', 
DW_CREATE_TS comment 'The timestamp the record was inserted.',
DW_LAST_UPDATE_TS comment 'When a record is updated  this would be the current timestamp',
DW_LOGICAL_DELETE_IND comment 'Set to True when we receive a delete record for the primary key, else False',
DW_SOURCE_CREATE_NM comment 'The Bod (data source) name of this insert.',  
DW_SOURCE_UPDATE_NM comment 'The Bod (data source) name of this update or delete.',
DW_CURRENT_VERSION_IND comment 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
)
COPY GRANTS
comment = 'VIEW for Meal_Plan_Customer_Flag'
As 
SELECT
Retail_Customer_UUID ,
Retail_Customer_Flag_Txt,
DW_First_Effective_Dt ,           
DW_Last_Effective_Dt ,           
DW_CREATE_TS ,                    
DW_LAST_UPDATE_TS ,               
DW_LOGICAL_DELETE_IND ,           
DW_SOURCE_CREATE_NM ,            
DW_SOURCE_UPDATE_NM ,    
DW_CURRENT_VERSION_IND  
FROM <<EDM_DB_NAME>>.DW_C_LOYALTY.Meal_Plan_Customer_Flag;

create or replace view RETAIL_CUSTOMER_MEAL_PLAN
(
RETAIL_CUSTOMER_UUID COMMENT 'This ID represents an Universal Unique Identifier for a Retail Customer',
MEAL_PLAN_MEAL_ID COMMENT 'Meal/ Recipe orderd at specific time.mealid is a uuid, like 028e155e-05f8-4343-a300-91e226316e7c',
DW_FIRST_EFFECTIVE_DT COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
DW_LAST_EFFECTIVE_DT COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
RECIPE_VARIENT_ID COMMENT 'Primary key of recipe variant, integer',
SOURCE_CREATE_TS COMMENT 'Timestamp for Meal Plan.',
RECIPE_COOKED_DT COMMENT 'Indicates if the planned meal was cooked',
DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
DW_LAST_UPDATE_TS COMMENT 'When a record is updated  this would be the current timestamp',
DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',
DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
) 
COPY GRANTS
COMMENT='VIEW for Retail_Customer_Meal_Plan'
 as 
SELECT
Retail_Customer_UUID ,
Meal_Plan_Meal_Id,
DW_First_Effective_Dt ,           
DW_Last_Effective_Dt ,
Recipe_Varient_Id,
Source_Create_Ts,
Recipe_Cooked_Dt,
DW_CREATE_TS ,                    
DW_LAST_UPDATE_TS ,               
DW_LOGICAL_DELETE_IND ,           
DW_SOURCE_CREATE_NM ,            
DW_SOURCE_UPDATE_NM ,    
DW_CURRENT_VERSION_IND  
FROM <<EDM_DB_NAME>>.DW_C_LOYALTY.Retail_Customer_Meal_Plan;

create or replace view RETAIL_CUSTOMER_PENDING_MEAL
(
RETAIL_CUSTOMER_UUID COMMENT 'This ID represents an Universal Unique Identifier for a Retail Customer',
PENDING_MEAL_ID COMMENT 'Primary key of pending meal plan, UUID ' ,
DW_FIRST_EFFECTIVE_DT COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
DW_LAST_EFFECTIVE_DT COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
RECIPE_VARIENT_ID COMMENT 'Primary key of recipe variant, integer ' ,
SOURCE_CREATE_TS COMMENT 'Timestamp for Pending Meal.',
DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
DW_LAST_UPDATE_TS COMMENT 'When a record is updated  this would be the current timestamp',
DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',
DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
) 
COPY GRANTS
COMMENT='VIEW for Retail_Customer_Pending_Meal'
as 
SELECT
Retail_Customer_UUID ,
Pending_Meal_Id,
DW_First_Effective_Dt ,           
DW_Last_Effective_Dt ,
Recipe_Varient_Id,
Source_Create_Ts,
DW_CREATE_TS ,                    
DW_LAST_UPDATE_TS ,               
DW_LOGICAL_DELETE_IND ,           
DW_SOURCE_CREATE_NM ,            
DW_SOURCE_UPDATE_NM ,    
DW_CURRENT_VERSION_IND  
FROM <<EDM_DB_NAME>>.DW_C_LOYALTY.Retail_Customer_Pending_Meal;

create or replace view RETAIL_CUSTOMER_MEALPLAN_PROFILE
(
RETAIL_CUSTOMER_UUID COMMENT 'This ID represents an Universal Unique Identifier for a Retail Customer',
MEAL_PLAN_PROFILE_ID COMMENT 'Primary key of user profile, auto-incrementing integer',
DW_FIRST_EFFECTIVE_DT COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
DW_LAST_EFFECTIVE_DT COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
MEAL_PLAN_PROFILE_NM COMMENT 'Profile name of Meal Plan ' ,
RECIPE_SERVING_CNT COMMENT 'Default recipe serving size to show user',
MEAL_PLAN_DIET_TYPE_NM COMMENT 'Which type of meals to show the user (e.g. Vegetarian)',
MEAL_PLAN_DIET_TYPE_CD COMMENT 'Name of diet type, string ' ,
DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
DW_LAST_UPDATE_TS COMMENT 'When a record is updated  this would be the current timestamp',
DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',
DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
) 
COPY GRANTS
COMMENT='VIEW for Retail_Customer_MealPlan_Profile'
as 
SELECT
Retail_Customer_UUID ,
Meal_Plan_Profile_Id,
DW_First_Effective_Dt ,           
DW_Last_Effective_Dt ,
Meal_Plan_Profile_Nm,
Recipe_Serving_Cnt,
Meal_Plan_Diet_Type_Nm,
Meal_Plan_Diet_Type_Cd,
DW_CREATE_TS ,                    
DW_LAST_UPDATE_TS ,               
DW_LOGICAL_DELETE_IND ,           
DW_SOURCE_CREATE_NM ,            
DW_SOURCE_UPDATE_NM ,    
DW_CURRENT_VERSION_IND  
FROM <<EDM_DB_NAME>>.DW_C_LOYALTY.Retail_Customer_MealPlan_Profile;

Create or Replace view Meal_Plan_Customer
(
Retail_Customer_UUID COMMENT 'This ID represents an Universal Unique Identifier for a Retail Customer',
DW_First_Effective_Dt COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
DW_Last_Effective_Dt COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
Meal_Plan_Role_Nm COMMENT 'to control the MealPlan site contant based upon Role , Example Admin or user. In future as admin they will be able to load their own MealPlan.',
Source_Create_Ts COMMENT 'Timestamp of Meal plan Customer',
Source_Last_Update_Ts COMMENT 'The last update Meal plan Customer timestamp', 
DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
DW_LAST_UPDATE_TS COMMENT 'When a record is updated  this would be the current timestamp',
DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',  
DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
 )
COPY GRANTS
COMMENT = 'VIEW for Meal_Plan_Customer'
As 
SELECT
Retail_Customer_UUID,
DW_First_Effective_Dt,
DW_Last_Effective_Dt,
Meal_Plan_Role_Nm,
Source_Create_Ts,
Source_Last_Update_Ts, 
DW_CREATE_TS,
DW_LAST_UPDATE_TS,
DW_LOGICAL_DELETE_IND,
DW_SOURCE_CREATE_NM,
DW_SOURCE_UPDATE_NM,
DW_CURRENT_VERSION_IND    
FROM <<EDM_DB_NAME>>.DW_C_LOYALTY.Meal_Plan_Customer;

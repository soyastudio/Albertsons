--liquibase formatted sql
--changeset SYSTEM:Create_STREAM_for_MealPlan runOnChange:true splitStatements:false OBJECT_TYPE:STREAM
USE DATABASE <<EDM_DB_NAME_R>>;
USE SCHEMA <<EDM_DB_NAME_R>>.DW_APPL;

CREATE OR REPLACE STREAM GetMeal_Plan_Recipe_Flat_R_STREAM 

ON TABLE <<EDM_DB_NAME_R>>.DW_R_LOYALTY.GetMeal_Plan_Recipe_Flat;

CREATE OR REPLACE STREAM GetMeal_Plan_Cuisine_Tag_Flat_R_STREAM 

ON TABLE <<EDM_DB_NAME_R>>.DW_R_LOYALTY.GetMeal_Plan_Cuisine_Tag_Flat;      
		
CREATE OR REPLACE STREAM GetRetail_Customer_Dislike_Flat_R_STREAM 

ON TABLE <<EDM_DB_NAME_R>>.DW_R_LOYALTY.GetRetail_Customer_Dislike_Flat;
  
CREATE OR REPLACE STREAM GetRetail_Customer_Dislike_Ingredients_Flat_R_STREAM 

ON TABLE <<EDM_DB_NAME_R>>.DW_R_LOYALTY.GetRetail_Customer_Dislike_Ingredients_Flat;

CREATE OR REPLACE STREAM GetRetail_Customer_Favorite_Flat_R_STREAM 

ON TABLE <<EDM_DB_NAME_R>>.DW_R_LOYALTY.GetRetail_Customer_Favorite_Flat;
	
CREATE OR REPLACE STREAM GetMeal_Plan_Customer_Flag_Flat_R_STREAM 

ON TABLE <<EDM_DB_NAME_R>>.DW_R_LOYALTY.GetMeal_Plan_Customer_Flag_Flat;

CREATE OR REPLACE STREAM GetMeal_Plan_Ingredient_Restriction_Flat_R_STREAM 

ON TABLE <<EDM_DB_NAME_R>>.DW_R_LOYALTY.GetMeal_Plan_Ingredient_Restriction_Flat;

CREATE OR REPLACE STREAM GetMeal_Plan_Ingredient_Flat_R_STREAM 

ON TABLE <<EDM_DB_NAME_R>>.DW_R_LOYALTY.GetMeal_Plan_Ingredient_Flat;

CREATE OR REPLACE STREAM GetRetail_Customer_Meal_Plan_Flat_R_STREAM 

ON TABLE <<EDM_DB_NAME_R>>.DW_R_LOYALTY.GetRetail_Customer_Meal_Plan_Flat;

CREATE OR REPLACE STREAM GetRetail_Customer_Pending_Meal_Flat_R_STREAM

ON TABLE <<EDM_DB_NAME_R>>.DW_R_LOYALTY.GetRetail_Customer_Pending_Meal_Flat;
	   
CREATE OR REPLACE STREAM GetRetail_Customer_MealPlan_Profile_Flat_R_STREAM 

ON TABLE <<EDM_DB_NAME_R>>.DW_R_LOYALTY.GetRetail_Customer_MealPlan_Profile_Flat;    
	
CREATE OR REPLACE STREAM GetMeal_Plan_Restriction_Flat_R_STREAM 

ON TABLE <<EDM_DB_NAME_R>>.DW_R_LOYALTY.GetMeal_Plan_Restriction_Flat;

CREATE OR REPLACE STREAM GetMeal_Plan_Variety_Tag_Flat_R_STREAM 

ON TABLE <<EDM_DB_NAME_R>>.DW_R_LOYALTY.GetMeal_Plan_Variety_Tag_Flat;

CREATE OR REPLACE STREAM GetRetail_Customer_Profile_To_Restriction_Flat_R_STREAM 

ON TABLE <<EDM_DB_NAME_R>>.DW_R_LOYALTY.GetRetail_Customer_Profile_To_Restriction_Flat;
 
CREATE OR REPLACE STREAM GetMeal_Plan_App_Feedback_Flat_R_STREAM

ON TABLE <<EDM_DB_NAME_R>>.DW_R_LOYALTY.GetMeal_Plan_App_Feedback_Flat;

CREATE OR REPLACE STREAM GetRetail_Customer_Dislike_To_Profile_Flat_R_STREAM 

ON TABLE <<EDM_DB_NAME_R>>.DW_R_LOYALTY.GetRetail_Customer_Dislike_To_Profile_Flat;
	
CREATE OR REPLACE STREAM GetMeal_Plan_Customer_Flat_R_STREAM 

ON TABLE <<EDM_DB_NAME_R>>.DW_R_LOYALTY.GetMeal_Plan_Customer_Flat;	

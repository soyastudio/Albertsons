--liquibase formatted sql
--changeset SYSTEM:Create_PIPES_for_Meal_Plan runOnChange:true splitStatements:false OBJECT_TYPE:PIPE
USE DATABASE <<EDM_DB_NAME_R>>;
USE SCHEMA <<EDM_DB_NAME_R>>.DW_APPL; 

CREATE OR REPLACE PIPE EDM_Meal_Plan_Recipe_PIPE_<<ENV>>BLOB_INC 
auto_ingest = true
integration = '<<EDM_PIPE_INTEGRATION>>'
as 
COPY INTO <<EDM_DB_NAME_R>>.DW_R_LOYALTY.GetMeal_Plan_Recipe_Flat
FROM
( select $1 ,$2 ,$3 ,$4 ,$5 ,$6 ,$7 ,$8 ,$9 ,current_timestamp ,metadata$filename
  from @EDM_MEAL_PLAN_STAGE_<<ENV>>BLOB_INC/Meal_Plan/ 
)
file_format = 'CSV_MEAL_PLAN'
pattern = '.*Meal_Plan_Recipe.*[.]*'
on_error = 'SKIP_FILE';

alter pipe <<EDM_DB_NAME_R>>.DW_APPL.EDM_Meal_Plan_Recipe_PIPE_<<ENV>>BLOB_INC SET TAG <<EDM_PIPE_DB>>.OBJECTTAG.APPCODE = 'DIRM', <<EDM_PIPE_DB>>.OBJECTTAG.TECHNICAL_CONTACT = 'LOYALTY';


CREATE OR REPLACE PIPE EDM_Meal_Plan_Cuisine_Tag_PIPE_<<ENV>>BLOB_INC 
auto_ingest = true
integration = '<<EDM_PIPE_INTEGRATION>>'
as 
COPY INTO <<EDM_DB_NAME_R>>.DW_R_LOYALTY.GetMeal_Plan_Cuisine_Tag_Flat
FROM
( select $1 ,$2 ,current_timestamp ,metadata$filename
   from @EDM_MEAL_PLAN_STAGE_<<ENV>>BLOB_INC/Meal_Plan/ 
)
file_format = 'CSV_MEAL_PLAN'
pattern = '.*Meal_Plan_Cuisine_Tag.*[.]*'
on_error = 'SKIP_FILE';

alter pipe <<EDM_DB_NAME_R>>.DW_APPL.EDM_Meal_Plan_Cuisine_Tag_PIPE_<<ENV>>BLOB_INC  SET TAG <<EDM_PIPE_DB>>.OBJECTTAG.APPCODE = 'DIRM', <<EDM_PIPE_DB>>.OBJECTTAG.TECHNICAL_CONTACT = 'LOYALTY';


CREATE OR REPLACE PIPE EDM_Retail_Customer_Dislike_PIPE_<<ENV>>BLOB_INC
auto_ingest = true
integration = '<<EDM_PIPE_INTEGRATION>>'
as
COPY INTO <<EDM_DB_NAME_R>>.DW_R_LOYALTY.GetRetail_Customer_Dislike_Flat
FROM
(select $1 ,$2 ,current_timestamp ,metadata$filename
 from @EDM_MEAL_PLAN_STAGE_<<ENV>>BLOB_INC/Meal_Plan/ 
)
file_format = 'CSV_MEAL_PLAN'
pattern = '.*Retail_Customer_Dislike*[.].*'
on_error = 'SKIP_FILE';

alter pipe <<EDM_DB_NAME_R>>.DW_APPL.EDM_Retail_Customer_Dislike_PIPE_<<ENV>>BLOB_INC  SET TAG <<EDM_PIPE_DB>>.OBJECTTAG.APPCODE = 'DIRM', <<EDM_PIPE_DB>>.OBJECTTAG.TECHNICAL_CONTACT = 'LOYALTY';


CREATE OR REPLACE PIPE EDM_Retail_Customer_Dislike_Ingredients_PIPE_<<ENV>>BLOB_INC
auto_ingest = true
integration = '<<EDM_PIPE_INTEGRATION>>'
as
COPY INTO <<EDM_DB_NAME_R>>.DW_R_LOYALTY.GetRetail_Customer_Dislike_Ingredients_Flat
FROM
( select $1 ,$2 ,current_timestamp ,metadata$filename
  from @EDM_MEAL_PLAN_STAGE_<<ENV>>BLOB_INC/Meal_Plan/ 
)
file_format = 'CSV_MEAL_PLAN'
pattern = '.*Retail_Customer_Dislike_Ingredients.*[.]*'
on_error = 'SKIP_FILE';

alter pipe <<EDM_DB_NAME_R>>.DW_APPL.EDM_Retail_Customer_Dislike_Ingredients_PIPE_<<ENV>>BLOB_INC  SET TAG <<EDM_PIPE_DB>>.OBJECTTAG.APPCODE = 'DIRM', <<EDM_PIPE_DB>>.OBJECTTAG.TECHNICAL_CONTACT = 'LOYALTY';


CREATE OR REPLACE PIPE EDM_Retail_Customer_Favorite_PIPE_<<ENV>>BLOB_INC 
auto_ingest = true
integration = '<<EDM_PIPE_INTEGRATION>>'
as 
COPY INTO <<EDM_DB_NAME_R>>.DW_R_LOYALTY.GetRetail_Customer_Favorite_Flat
FROM
(select $1 ,$2 ,$3 ,$4 ,$5 ,current_timestamp ,metadata$filename
from @EDM_MEAL_PLAN_STAGE_<<ENV>>BLOB_INC/Meal_Plan/
)
file_format = 'CSV_MEAL_PLAN'
pattern = '.*Retail_Customer_Favorite.*[.]*'
on_error = 'SKIP_FILE';

alter pipe <<EDM_DB_NAME_R>>.DW_APPL.EDM_Retail_Customer_Favorite_PIPE_<<ENV>>BLOB_INC  SET TAG <<EDM_PIPE_DB>>.OBJECTTAG.APPCODE = 'DIRM', <<EDM_PIPE_DB>>.OBJECTTAG.TECHNICAL_CONTACT = 'LOYALTY';


CREATE OR REPLACE PIPE EDM_Meal_Plan_Customer_Flag_PIPE_<<ENV>>BLOB_INC 
auto_ingest = true
integration = '<<EDM_PIPE_INTEGRATION>>'
as 
COPY INTO <<EDM_DB_NAME_R>>.DW_R_LOYALTY.GetMeal_Plan_Customer_Flag_Flat
FROM
( select $1 ,$2 ,current_timestamp ,metadata$filename
from @EDM_MEAL_PLAN_STAGE_<<ENV>>BLOB_INC/Meal_Plan/
)
file_format = 'CSV_MEAL_PLAN'
pattern = '.*Meal_Plan_Customer_Flag.*[.]*'
on_error = 'SKIP_FILE';

alter pipe <<EDM_DB_NAME_R>>.DW_APPL.EDM_Meal_Plan_Customer_Flag_PIPE_<<ENV>>BLOB_INC  SET TAG <<EDM_PIPE_DB>>.OBJECTTAG.APPCODE = 'DIRM', <<EDM_PIPE_DB>>.OBJECTTAG.TECHNICAL_CONTACT = 'LOYALTY';


CREATE OR REPLACE PIPE EDM_Meal_Plan_Ingredient_Restriction_PIPE_<<ENV>>BLOB_INC 
auto_ingest = true
integration = '<<EDM_PIPE_INTEGRATION>>'
as 
COPY INTO <<EDM_DB_NAME_R>>.DW_R_LOYALTY.GetMeal_Plan_Ingredient_Restriction_Flat
FROM
( select $1 ,$2 ,$3 ,current_timestamp ,metadata$filename
   from @EDM_MEAL_PLAN_STAGE_<<ENV>>BLOB_INC/Meal_Plan/ 
)
file_format = 'CSV_MEAL_PLAN'
pattern = '.*Meal_Plan_Ingredient_Restriction.*[.]*'
on_error = 'SKIP_FILE';

alter pipe <<EDM_DB_NAME_R>>.DW_APPL.EDM_Meal_Plan_Ingredient_Restriction_PIPE_<<ENV>>BLOB_INC  SET TAG <<EDM_PIPE_DB>>.OBJECTTAG.APPCODE = 'DIRM', <<EDM_PIPE_DB>>.OBJECTTAG.TECHNICAL_CONTACT = 'LOYALTY';


CREATE OR REPLACE PIPE EDM_Meal_Plan_Ingredient_PIPE_<<ENV>>BLOB_INC 
auto_ingest = true
integration = '<<EDM_PIPE_INTEGRATION>>'
as 
COPY INTO <<EDM_DB_NAME_R>>.DW_R_LOYALTY.GetMeal_Plan_Ingredient_Flat
FROM
( select $1 ,$2 ,current_timestamp ,metadata$filename
   from @EDM_MEAL_PLAN_STAGE_<<ENV>>BLOB_INC/Meal_Plan/ 
)
file_format = 'CSV_MEAL_PLAN'
pattern = '.*Meal_Plan_Ingredient*[.].*' 
on_error = 'SKIP_FILE';

alter pipe <<EDM_DB_NAME_R>>.DW_APPL.EDM_Meal_Plan_Ingredient_PIPE_<<ENV>>BLOB_INC  SET TAG <<EDM_PIPE_DB>>.OBJECTTAG.APPCODE = 'DIRM', <<EDM_PIPE_DB>>.OBJECTTAG.TECHNICAL_CONTACT = 'LOYALTY';


CREATE OR REPLACE PIPE EDM_Retail_Customer_Meal_Plan_PIPE_<<ENV>>BLOB_INC 
auto_ingest = true
integration = '<<EDM_PIPE_INTEGRATION>>'
as 
COPY INTO <<EDM_DB_NAME_R>>.DW_R_LOYALTY.GetRetail_Customer_Meal_Plan_Flat
FROM
(select $1,$2,$3,$4,$5,current_timestamp,metadata$filename
 from @EDM_MEAL_PLAN_STAGE_<<ENV>>BLOB_INC/Meal_Plan/
)
file_format = 'CSV_MEAL_PLAN'
pattern = '.*Retail_Customer_Meal_Plan.*[.]*'
on_error = 'SKIP_FILE';

alter pipe <<EDM_DB_NAME_R>>.DW_APPL.EDM_Retail_Customer_Meal_Plan_PIPE_<<ENV>>BLOB_INC  SET TAG <<EDM_PIPE_DB>>.OBJECTTAG.APPCODE = 'DIRM', <<EDM_PIPE_DB>>.OBJECTTAG.TECHNICAL_CONTACT = 'LOYALTY';


CREATE OR REPLACE PIPE EDM_Retail_Customer_Pending_Meal_PIPE_<<ENV>>BLOB_INC
auto_ingest = true
integration = '<<EDM_PIPE_INTEGRATION>>'
as
COPY INTO <<EDM_DB_NAME_R>>.DW_R_LOYALTY.GetRetail_Customer_Pending_Meal_Flat
FROM
( select $1 ,$2 ,$3 ,$4 ,current_timestamp ,metadata$filename
 from @EDM_MEAL_PLAN_STAGE_<<ENV>>BLOB_INC/Meal_Plan/ 
)
file_format = 'CSV_MEAL_PLAN'
pattern = '.*Retail_Customer_Pending_Meal.*[.]*'
on_error = 'SKIP_FILE';

alter pipe <<EDM_DB_NAME_R>>.DW_APPL.EDM_Retail_Customer_Pending_Meal_PIPE_<<ENV>>BLOB_INC  SET TAG <<EDM_PIPE_DB>>.OBJECTTAG.APPCODE = 'DIRM', <<EDM_PIPE_DB>>.OBJECTTAG.TECHNICAL_CONTACT = 'LOYALTY';


CREATE OR REPLACE PIPE EDM_Retail_Customer_MealPlan_Profile_PIPE_<<ENV>>BLOB_INC 
auto_ingest = true
integration = '<<EDM_PIPE_INTEGRATION>>'
as 
COPY INTO <<EDM_DB_NAME_R>>.DW_R_LOYALTY.GetRetail_Customer_MealPlan_Profile_Flat
FROM
( select $1 ,$2 ,$3 ,$4 ,$5 ,$6 ,current_timestamp ,metadata$filename
 from @EDM_MEAL_PLAN_STAGE_<<ENV>>BLOB_INC/Meal_Plan/ 
)
file_format = 'CSV_MEAL_PLAN'
pattern = '.*Retail_Customer_MealPlan_Profile.*[.]*'
on_error = 'SKIP_FILE';

alter pipe <<EDM_DB_NAME_R>>.DW_APPL.EDM_Retail_Customer_MealPlan_Profile_PIPE_<<ENV>>BLOB_INC  SET TAG <<EDM_PIPE_DB>>.OBJECTTAG.APPCODE = 'DIRM', <<EDM_PIPE_DB>>.OBJECTTAG.TECHNICAL_CONTACT = 'LOYALTY';


CREATE OR REPLACE PIPE EDM_Meal_Plan_Restriction_PIPE_<<ENV>>BLOB_INC 
auto_ingest = true
integration = '<<EDM_PIPE_INTEGRATION>>'
as 
COPY INTO <<EDM_DB_NAME_R>>.DW_R_LOYALTY.GetMeal_Plan_Restriction_Flat
FROM
( select $1 ,$2 ,current_timestamp ,metadata$filename
 from @EDM_MEAL_PLAN_STAGE_<<ENV>>BLOB_INC/Meal_Plan/
)
file_format = 'CSV_MEAL_PLAN'
pattern = '.*Meal_Plan_Restriction.*[.]*'
on_error = 'SKIP_FILE';

alter pipe <<EDM_DB_NAME_R>>.DW_APPL.EDM_Meal_Plan_Restriction_PIPE_<<ENV>>BLOB_INC  SET TAG <<EDM_PIPE_DB>>.OBJECTTAG.APPCODE = 'DIRM', <<EDM_PIPE_DB>>.OBJECTTAG.TECHNICAL_CONTACT = 'LOYALTY';


CREATE OR REPLACE PIPE EDM_Meal_Plan_Variety_Tag_PIPE_<<ENV>>BLOB_INC
auto_ingest = true
integration = '<<EDM_PIPE_INTEGRATION>>'
as
COPY INTO <<EDM_DB_NAME_R>>.DW_R_LOYALTY.GetMeal_Plan_Variety_Tag_Flat
FROM
( select $1 ,$2 ,current_timestamp ,metadata$filename
 from @EDM_MEAL_PLAN_STAGE_<<ENV>>BLOB_INC/Meal_Plan/
)
file_format = 'CSV_MEAL_PLAN'
pattern = '.*Meal_Plan_Variety_Tag.*[.]*'
on_error = 'SKIP_FILE';

alter pipe <<EDM_DB_NAME_R>>.DW_APPL.EDM_Meal_Plan_Variety_Tag_PIPE_<<ENV>>BLOB_INC  SET TAG <<EDM_PIPE_DB>>.OBJECTTAG.APPCODE = 'DIRM', <<EDM_PIPE_DB>>.OBJECTTAG.TECHNICAL_CONTACT = 'LOYALTY';


CREATE OR REPLACE PIPE EDM_Retail_Customer_Profile_To_Restriction_PIPE_<<ENV>>BLOB_INC
auto_ingest = true
integration = '<<EDM_PIPE_INTEGRATION>>'
as
COPY INTO <<EDM_DB_NAME_R>>.DW_R_LOYALTY.GetRetail_Customer_Profile_To_Restriction_Flat
FROM
(select $1 ,$2 ,current_timestamp ,metadata$filename
from @EDM_MEAL_PLAN_STAGE_<<ENV>>BLOB_INC/Meal_Plan/ 
)
file_format = 'CSV_MEAL_PLAN'
pattern = '.*Retail_Customer_Profile_To_Restriction.*[.]*'
on_error = 'SKIP_FILE';

alter pipe <<EDM_DB_NAME_R>>.DW_APPL.EDM_Retail_Customer_Profile_To_Restriction_PIPE_<<ENV>>BLOB_INC  SET TAG <<EDM_PIPE_DB>>.OBJECTTAG.APPCODE = 'DIRM', <<EDM_PIPE_DB>>.OBJECTTAG.TECHNICAL_CONTACT = 'LOYALTY';


CREATE OR REPLACE PIPE EDM_Meal_Plan_App_Feedback_PIPE_<<ENV>>BLOB_INC
auto_ingest = true
integration = '<<EDM_PIPE_INTEGRATION>>'
as
COPY INTO <<EDM_DB_NAME_R>>.DW_R_LOYALTY.GetMeal_Plan_App_Feedback_Flat
FROM
( select $1 ,$2 ,$3 ,$4 ,current_timestamp ,metadata$filename
from @EDM_MEAL_PLAN_STAGE_<<ENV>>BLOB_INC/Meal_Plan/ 
)
file_format = 'CSV_MEAL_PLAN'
pattern = '.*Meal_Plan_App_Feedback.*[.]*'
on_error = 'SKIP_FILE';

alter pipe <<EDM_DB_NAME_R>>.DW_APPL.EDM_Meal_Plan_App_Feedback_PIPE_<<ENV>>BLOB_INC  SET TAG <<EDM_PIPE_DB>>.OBJECTTAG.APPCODE = 'DIRM', <<EDM_PIPE_DB>>.OBJECTTAG.TECHNICAL_CONTACT = 'LOYALTY';


CREATE OR REPLACE PIPE EDM_Retail_Customer_Dislike_To_Profile_PIPE_<<ENV>>BLOB_INC 
auto_ingest = true
integration = '<<EDM_PIPE_INTEGRATION>>'
as 
COPY INTO <<EDM_DB_NAME_R>>.DW_R_LOYALTY.GetRetail_Customer_Dislike_To_Profile_Flat
FROM
( select $1 ,$2 ,current_timestamp ,metadata$filename
   from @EDM_MEAL_PLAN_STAGE_<<ENV>>BLOB_INC/Meal_Plan/ 
)
file_format = 'CSV_MEAL_PLAN'
pattern = '.*Retail_Customer_Dislike_To_Profile.*[.]*'
on_error = 'SKIP_FILE';

alter pipe <<EDM_DB_NAME_R>>.DW_APPL.EDM_Retail_Customer_Dislike_To_Profile_PIPE_<<ENV>>BLOB_INC  SET TAG <<EDM_PIPE_DB>>.OBJECTTAG.APPCODE = 'DIRM', <<EDM_PIPE_DB>>.OBJECTTAG.TECHNICAL_CONTACT = 'LOYALTY';


CREATE OR REPLACE PIPE EDM_Meal_Plan_Customer_PIPE_<<ENV>>BLOB_INC 
auto_ingest = true
integration = '<<EDM_PIPE_INTEGRATION>>'
as 
COPY INTO <<EDM_DB_NAME_R>>.DW_R_LOYALTY.GetMeal_Plan_Customer_Flat
FROM
(select $1 ,$2 ,$3 ,$4 ,current_timestamp ,metadata$filename
from @EDM_MEAL_PLAN_STAGE_<<ENV>>BLOB_INC/Meal_Plan/
)
file_format = 'CSV_MEAL_PLAN'
pattern = '.*Meal_Plan_Customer*[.].*'
on_error = 'SKIP_FILE';

alter pipe <<EDM_DB_NAME_R>>.DW_APPL.EDM_Meal_Plan_Customer_PIPE_<<ENV>>BLOB_INC  SET TAG <<EDM_PIPE_DB>>.OBJECTTAG.APPCODE = 'DIRM', <<EDM_PIPE_DB>>.OBJECTTAG.TECHNICAL_CONTACT = 'LOYALTY';

--liquibase formatted sql
--changeset SYSTEM:Create_FILEFORMAT_for_Meal_Plan runOnChange:true splitStatements:false OBJECT_TYPE:FILE_FORMAT
USE DATABASE <<EDM_DB_NAME_R>>;
USE SCHEMA <<EDM_DB_NAME_R>>.DW_APPL; 

CREATE OR REPLACE FILE FORMAT CSV_MEAL_PLAN  
							TYPE = CSV 
							COMPRESSION = 'AUTO'
                            FIELD_DELIMITER = ',' 
							RECORD_DELIMITER = '\\n' 
							SKIP_HEADER = 1 
							FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
							TRIM_SPACE = FALSE 
							ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE 
                            ESCAPE = '\\' 
							ESCAPE_UNENCLOSED_FIELD = '\\134'
				            DATE_FORMAT = 'AUTO' 
							TIMESTAMP_FORMAT = 'AUTO' 
							NULL_IF = ('\\N');

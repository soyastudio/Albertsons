--liquibase formatted sql
--changeset SYSTEM:SP_GetRetail_Customer_Profile_To_Restriction_TO_BIM_LOAD_Retail_Customer_Profile_To_Restriction runOnChange:true splitStatements:false OBJECT_TYPE:SP
Use database <<EDM_DB_NAME>>;
Use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE SP_GetRetail_Customer_Profile_To_Restriction_TO_BIM_LOAD_Retail_Customer_Profile_To_Restriction(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_LOYAL VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS 
$$
// **************LOAD for Retail_Customer_Profile_To_Restriction table BEGINS*****************
		var src_wrk_tbl = SRC_WRK_TBL;
		var temp_wrk_tbl = `${CNF_DB}.${C_STAGE}.Retail_Customer_Profile_To_Restriction_Tmp_WRK`;
		var cnf_schema = C_LOYAL;
		var tgt_wrk_tbl = `${CNF_DB}.${C_STAGE}.Retail_Customer_Profile_To_Restriction_WRK`;
		var tgt_tbl = `${CNF_DB}.${cnf_schema}.Retail_Customer_Profile_To_Restriction`;
		var src_rerun_tbl = `${CNF_DB}.${C_STAGE}.Retail_Customer_Profile_To_Restriction_Flat_RERUN`;

// Empty the rerun queue table
var sql_empty_rerun_tbl = `TRUNCATE TABLE ${src_rerun_tbl}`;

//query to load rerun queue table when encountered a failure
var sql_ins_rerun_tbl = `INSERT INTO ${src_rerun_tbl} SELECT * FROM ${temp_wrk_tbl}`;

// ***************** Truncate and Reload the Temp work table ********************
 var truncate_temp_wrk_table = `Truncate Table ${temp_wrk_tbl}`;
try {
snowflake.execute ({ sqlText: truncate_temp_wrk_table });
	}
    catch (err) {
        return `Truncation of work table ${temp_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
				}

// persist stream data in work table for the current transaction, includes data from previous failed run

var sql_crt_src_wrk_tbl = `INSERT INTO `+ temp_wrk_tbl +` 
SELECT * FROM `+ src_wrk_tbl +`
UNION ALL
SELECT * FROM `+ src_rerun_tbl +``;

try {
snowflake.execute (
{sqlText: sql_crt_src_wrk_tbl }
);
}
catch (err) {
throw "Inserting of Source Work table "+ temp_wrk_tbl +" Failed with error: " + err; // Return a error message.
}

		
// ************************************ Truncate and Reload the work table ****************************************
 var truncate_tgt_wrk_table = `Truncate Table ${tgt_wrk_tbl}`;
try {
snowflake.execute ({ sqlText: truncate_tgt_wrk_table });
	}
    catch (err) {
        return `Deletion of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
				}
	
// *********************** _DEV for Retail_Customer_Profile_To_Restriction table BEGIN *****************
// identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
var create_tgt_wrk_table = `INSERT INTO ${tgt_wrk_tbl}
							SELECT DISTINCT 
							Meal_Plan_Profile_Id ,
							Meal_Plan_Restriction_Id ,
						    Filename ,
							DW_LOGICAL_DELETE_IND 
							
                            FROM ( SELECT
									Meal_Plan_Profile_Id ,  
									Meal_Plan_Restriction_Id ,
									Filename ,
									DW_LOGICAL_DELETE_IND
								FROM ( SELECT 
											Meal_Plan_Profile_Id , 
											Meal_Plan_Restriction_Id ,
											Filename ,
											FALSE AS DW_LOGICAL_DELETE_IND,
										  	Row_number() OVER (
											PARTITION BY Meal_Plan_Profile_Id,Meal_Plan_Restriction_Id
											order by DW_CreateTs DESC) as rn 
										    FROM(
											        SELECT
													Meal_Plan_Profile_Id ,
													Meal_Plan_Restriction_Id ,
                                                    DW_CreateTs ,
													Filename 
													FROM
													  (
													  SELECT
													  profileId as Meal_Plan_Profile_Id  ,  
													  restrictionId as Meal_Plan_Restriction_Id ,
													  DW_CreateTs ,         
													  File_Name as Filename 
													  FROM 
													   ${temp_wrk_tbl} S
													  )
                                                    )
											    )  where rn=1
									    ) `;
											
try {
snowflake.execute ({ sqlText: create_tgt_wrk_table });
	}
    catch (err) {
	
snowflake.execute ({ sqlText: sql_ins_rerun_tbl});

  return `Inserting of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
				} 
// Transaction for Updates, Insert begins           
    var sql_begin = "BEGIN"
	
var sql_Delete_Check = `UPDATE ${tgt_tbl} as tgt
					SET 
					DW_Last_Effective_dt = CURRENT_DATE - 1,
					DW_CURRENT_VERSION_IND = TRUE,
					DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
					DW_LOGICAL_DELETE_IND = TRUE 
                     from ( 
					       SELECT 
						   Meal_Plan_Profile_Id as profileId ,
						   Meal_Plan_Restriction_Id as restrictionId 
						   from ${tgt_tbl} as a
						   left join ${temp_wrk_tbl} as b
						   on a.Meal_Plan_Profile_Id = b.profileId
						   and a.Meal_Plan_Restriction_Id = b.restrictionId
						   and a.DW_CURRENT_VERSION_IND = TRUE 
						   and a.DW_LOGICAL_DELETE_IND = FALSE
						   where b.profileId is null and b.restrictionId is null
						   )del
				      where tgt.Meal_Plan_Profile_Id = del.profileId 
					  AND tgt.Meal_Plan_Restriction_Id = del.restrictionId
                      AND ( select count(*) from ${temp_wrk_tbl} ) > 0                        
					  AND tgt.DW_CURRENT_VERSION_IND = TRUE`; 	
	
var sql_delete = `delete from ${tgt_tbl} 
                 where (Meal_Plan_Profile_Id,Meal_Plan_Restriction_Id) in 
				 ( select Meal_Plan_Profile_Id,Meal_Plan_Restriction_Id from ${tgt_wrk_tbl})`;
				 
				 
// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl} 
								    (
									Meal_Plan_Profile_Id ,
									Meal_Plan_Restriction_Id ,
								    DW_First_Effective_Dt ,           
							     	DW_Last_Effective_Dt ,   						                     
									DW_CREATE_TS ,    
                                    DW_SOURCE_CREATE_NM ,									
									DW_LOGICAL_DELETE_IND ,           
									DW_CURRENT_VERSION_IND           
									)
									SELECT 
									Meal_Plan_Profile_Id ,
									Meal_Plan_Restriction_Id ,
									CURRENT_DATE ,           
									'31-DEC-9999' ,                                  
									CURRENT_TIMESTAMP ,  
									Filename ,
									DW_LOGICAL_DELETE_IND ,           
									TRUE
									FROM ${tgt_wrk_tbl}
									WHERE
								    Meal_Plan_Profile_Id is Not Null
									and Meal_Plan_Restriction_Id is Not Null
									`;
    
var sql_commit = "COMMIT";
var sql_rollback = "ROLLBACK";
    
try {
        snowflake.execute ({ sqlText: sql_begin });
		snowflake.execute ({ sqlText: sql_empty_rerun_tbl });
		snowflake.execute ({ sqlText: sql_Delete_Check });
        snowflake.execute ({ sqlText: sql_delete });
        snowflake.execute ({ sqlText: sql_inserts });
        snowflake.execute ({ sqlText: sql_commit });
	} catch (err) {
        snowflake.execute ({ sqlText: sql_rollback });
		snowflake.execute ({ sqlText: sql_ins_rerun_tbl});
	   return `_DEVing of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
				  }	
                               
                // **************LOAD for Retail_Customer_Profile_To_Restriction Table ENDs *****************
				
$$;									

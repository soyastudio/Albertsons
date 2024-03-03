--liquibase formatted sql
--changeset SYSTEM:SP_GetCustomerRewardBalance_To_BIM_Customer_Program_Scorecard runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETCUSTOMERREWARDBALANCE_TO_BIM_CUSTOMER_PROGRAM_SCORECARD(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_LOYAL VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
   
   

var cnf_db = CNF_DB ;
var wrk_schema = C_STAGE ; 
var cnf_schema = C_LOYAL;   
var src_wrk_tbl = SRC_WRK_TBL;

var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.Customer_Program_Scorecard_wrk`;
var tgt_tbl = `${cnf_db}.${cnf_schema}.Customer_Program_Scorecard`;

// ************** Load for Customer_Program_Scorecard table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.

var sql_command = `CREATE OR REPLACE TABLE ${tgt_wrk_tbl} as
                            WITH src_wrk_tbl_recs as
                            (SELECT DISTINCT 
                             Balance_Update_Ts
							,Household_Id
							,Program_Type_Cd
							,Program_Dsc
							,Program_Value_Qty
							,Program_Validity_End_Dt
							,Program_Modify_Ts
							,FileName
							,ActionTypeCd
							,creationdt
							,Row_number() OVER ( partition BY TO_TIMESTAMP_LTZ(Balance_Update_Ts), Household_Id, Program_Type_Cd,Program_Dsc ORDER BY To_timestamp_ntz(creationdt) DESC) AS rn
                            from
                            (
							SELECT DISTINCT 
                             BalanceUpdateTs as Balance_Update_Ts
                           , HouseholdId as Household_Id
						   ,ProgramType as Program_Type_Cd
						   ,ProgramDsc as Program_Dsc
						   ,ProgramValueQty as Program_Value_Qty
						   ,ProgramValidityEndTs as Program_Validity_End_Dt
						   ,ProgramModifyTs as Program_Modify_Ts
						   ,FileName
						   ,ActionTypeCd
						   ,creationdt
						   FROM ${src_wrk_tbl}
						  )
                          )
                          SELECT
                           src.Balance_Update_Ts
						  ,src.Household_Id
						  ,src.Program_Type_Cd
						  ,src.Program_Dsc
						  ,src.Program_Value_Qty
						  ,src.Program_Validity_End_Dt
						  ,src.Program_Modify_Ts
						  ,src.FileName
						  ,src.ActionTypeCd
						  ,src.creationdt
						  ,src.DW_LOGICAL_DELETE_IND                                                                                                                                                                            
						  from
                          (SELECT
                           Balance_Update_Ts
						  ,Household_Id
						  ,Program_Type_Cd
						  ,Program_Dsc
						  ,Program_Value_Qty
						  ,Program_Validity_End_Dt
						  ,Program_Modify_Ts
						  ,FileName
						  ,ActionTypeCd
						  ,creationdt
						  ,False as DW_LOGICAL_DELETE_IND 						   
						  FROM src_wrk_tbl_recs 
                          WHERE rn = 1 
                          And Balance_Update_Ts is not null
						  And Household_Id is not null
						  And Program_Type_Cd is not null
						  And Program_Dsc is not null
                          ) src 
                          LEFT JOIN 
                          (SELECT  DISTINCT
                           tgt.Balance_Update_Ts
						  ,tgt.Household_Id
						  ,tgt.Program_Type_Cd
						  ,tgt.Program_Dsc
						  ,tgt.Program_Value_Qty
						  ,tgt.Program_Validity_End_Dt
						  ,tgt.Program_Modify_Ts						  
						  ,tgt.dw_logical_delete_ind
                          ,tgt.dw_first_effective_dt                                                                                                                                                                                                                                                               
                          FROM ${tgt_tbl} tgt 
                          WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
                          ) tgt 
                          ON tgt.Balance_Update_Ts = src.Balance_Update_Ts
						  and tgt.Household_Id = src.Household_Id
						  and tgt.Program_Type_Cd = src.Program_Type_Cd
						  and tgt.Program_Dsc = src.Program_Dsc
                          WHERE  (tgt.Balance_Update_Ts is null and tgt.Household_Id is null and tgt.Program_Type_Cd is null And tgt.Program_Dsc is null)  
                          or(
                           NVL(src.Program_Value_Qty,'-1') <> NVL(tgt.Program_Value_Qty,'-1')     
						  or NVL(src.Program_Validity_End_Dt,'9999-12-31') <> NVL(tgt.Program_Validity_End_Dt,'9999-12-31') 
						  or NVL(src.Program_Modify_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Program_Modify_Ts,'9999-12-31 00:00:00.000')     		
						  OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
                          )`;

try {
        snowflake.execute (
            {sqlText: sql_command  }
            );
        }
    catch (err)  {
        return "Creation of Customer_Program_Scorecard work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }


var sql_begin = "BEGIN"

  var sql_updates = `// Processing Updates of Type 2 SCD
                UPDATE ${tgt_tbl} as tgt
                SET  DW_Last_Effective_Dt = current_timestamp 
               ,DW_CURRENT_VERSION_IND = FALSE
               ,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
               ,DW_SOURCE_UPDATE_NM = filename
                FROM ( SELECT Balance_Update_Ts
				   ,Household_Id
				   ,Program_Type_Cd
				   ,Program_Dsc
				   ,Filename	
                        FROM ${tgt_wrk_tbl}
                       -- WHERE DML_Type = 'U'
                        
					) src
				WHERE 
				tgt.Balance_Update_Ts = src.Balance_Update_Ts AND
				tgt.Household_Id = src.Household_Id AND
				tgt.Program_Type_Cd = src.Program_Type_Cd AND
				tgt.Program_Dsc = src.Program_Dsc
		                AND  tgt.DW_CURRENT_VERSION_IND = TRUE`;
                             

// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl}
                   (
				    Balance_Update_Ts
				   ,Household_Id
				   ,Program_Type_Cd
				   ,Program_Dsc				   
				   ,DW_First_Effective_Dt
				   ,DW_Last_Effective_Dt
				   ,Program_Value_Qty
				   ,Program_Validity_End_Dt
				   ,Program_Modify_Ts
				   ,DW_CREATE_TS                                                                                             
                   ,DW_LOGICAL_DELETE_IND                                                                                           
                   ,DW_SOURCE_CREATE_NM                                                                                         
                   ,DW_CURRENT_VERSION_IND
				   )
				   
                   SELECT distinct
					 Balance_Update_Ts
					,Household_Id
					,Program_Type_Cd
					,Program_Dsc					
					,CURRENT_DATE
					,'31-DEC-9999'
					,Program_Value_Qty
					,Program_Validity_End_Dt
					,Program_Modify_Ts
					,CURRENT_TIMESTAMP
                    ,DW_Logical_delete_ind
                    ,FileName
                    ,TRUE                                                                                                      
				FROM ${tgt_wrk_tbl}
                where Balance_Update_Ts is not null
				  And Household_Id is not null
				  And Program_Type_Cd is not null
				  And Program_Dsc is not null`;
				

var sql_commit = "COMMIT"
var sql_rollback = "ROLLBACK"
try {
        snowflake.execute (
            {sqlText: sql_begin  }
            );
         snowflake.execute (
            {sqlText: sql_updates }
            );
		snowflake.execute (
            {sqlText: sql_inserts  }
            );
        snowflake.execute (
            {sqlText: sql_commit  }
            ); 

                             }             
    catch (err)  {
        snowflake.execute (
            {sqlText: sql_rollback  }
            );

        return `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
        }

// ************** Load for Customer_Program_Scorecard table ENDs *****************


$$;

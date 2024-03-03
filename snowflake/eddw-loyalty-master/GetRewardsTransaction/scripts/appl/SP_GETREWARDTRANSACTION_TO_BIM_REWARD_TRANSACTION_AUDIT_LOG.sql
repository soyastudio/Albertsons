--liquibase formatted sql
--changeset SYSTEM:SP_GETREWARDTRANSACTION_TO_BIM_REWARD_TRANSACTION_AUDIT_LOG runOnChange:true splitStatements:false OBJECT_TYPE:SP

use database <<EDM_DB_NAME>>;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE SP_GETREWARDTRANSACTION_TO_BIM_REWARD_TRANSACTION_AUDIT_LOG
("SRC_WRK_TBL" VARCHAR(16777216), "CNF_DB" VARCHAR(16777216), "C_LOYAL" VARCHAR(16777216), "C_STAGE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS 
$$

var cnf_db = CNF_DB ;
var wrk_schema = C_STAGE ;
var cnf_schema = C_LOYAL;
var src_wrk_tbl = SRC_WRK_TBL;


var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.Reward_Transaction_Audit_Log_wrk`;
var tgt_tbl = `${cnf_db}.${cnf_schema}.Reward_Transaction_Audit_Log`;


// ************** Load for Reward_Transaction_Audit_Log table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.


var sql_command = `CREATE OR REPLACE TABLE ${tgt_wrk_tbl} as
				WITH src_wrk_tbl_recs as
					(
					SELECT DISTINCT 					
					Household_Id,
					Transaction_Id,
					Loyalty_Program_Cd,
					Transaction_Type_Cd,	
					Create_Ts,
					Update_Ts,
					Before_Snapshot,
					After_Snapshot,
					Status_Cd,
					Reward_Dollar_End_Ts,
					FileName,													
					creationdt,
					Row_number() OVER ( partition BY Household_Id, 
					Transaction_Id, Loyalty_Program_Cd,Status_Cd,
					Reward_Dollar_End_Ts,
					Transaction_Type_Cd, Update_Ts  ORDER BY Create_Ts DESC) AS rn
					from
					(
					SELECT DISTINCT 
					PRT.HouseholdId AS Household_Id,
					PRT.TransactionId AS  Transaction_Id,
					PRT.LoyaltyProgramCd As Loyalty_Program_Cd,
					PRT.TransactionTypeCd_Code  As Transaction_Type_Cd,	
					TRY_To_timestamp(PRT.CreateTs) AS Create_Ts,
					TRY_To_timestamp(PRT.UpdateTs) AS Update_Ts,
					PARSE_JSON(TO_CHAR(PRT.BeforeTransactionSnapshot)::variant) AS Before_Snapshot,
					PARSE_JSON(TO_CHAR(PRT.AfterTransactionSnapshot)::variant) AS After_Snapshot,
					PRT.StatusCd AS Status_Cd ,
					TRY_To_timestamp(PRT.RewardDollarEndTs) As Reward_Dollar_End_Ts   ,
					PRT.FileName,													
					PRT.creationdt													
									
					FROM ${src_wrk_tbl} PRT
					)
					)
				SELECT
				src.Household_Id,
				src.Transaction_Id,
				src.Loyalty_Program_Cd,
				src.Transaction_Type_Cd,	
				src.Create_Ts,
				src.Update_Ts,
				src.Before_Snapshot,
				src.After_Snapshot,
				src.Status_Cd,
				src.Reward_Dollar_End_Ts,
				src.DW_LOGICAL_DELETE_IND,
				src.FileName,				
				src.creationdt,				
				CASE WHEN tgt.Household_Id IS NULL AND tgt.Transaction_Id IS NULL
				AND tgt.Loyalty_Program_Cd IS NULL 
				AND tgt.Transaction_Type_Cd IS NULL AND tgt.Update_Ts IS NULL
				and tgt.Status_Cd is NULL AND tgt.Reward_Dollar_End_Ts is NULL
				THEN 'I' ELSE 'U' END AS DML_Type,
				CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1  ELSE 0 END AS Sameday_chg_ind 
				from
				(SELECT
				Household_Id,
				Transaction_Id,
				Loyalty_Program_Cd,
				Transaction_Type_Cd,	
				Create_Ts,
				Update_Ts,
				Before_Snapshot,
				After_Snapshot,
				Status_Cd,
				Reward_Dollar_End_Ts,
				FileName,													
				creationdt,
				False as DW_LOGICAL_DELETE_IND																	
				FROM src_wrk_tbl_recs 
				WHERE rn = 1 
				AND Household_Id IS NOT NULL 
				and  Transaction_Id IS NOT NULL
				and  Loyalty_Program_Cd IS NOT NULL 
				and Transaction_Type_Cd is not null
				and Update_Ts is not null
				and Status_Cd is not NULL
				and Reward_Dollar_End_Ts is not null
				) src 
				
				LEFT JOIN 
				(SELECT  DISTINCT													
					tgt.Household_Id,
					tgt.Transaction_Id,
					tgt.Loyalty_Program_Cd,
					tgt.Transaction_Type_Cd,	
					tgt.Create_Ts,
					tgt.Update_Ts,
					tgt.Before_Snapshot,
					tgt.After_Snapshot,
					tgt.status_cd,
					tgt.Reward_Dollar_End_Ts,
					tgt.dw_logical_delete_ind,
					tgt.dw_first_effective_dt				
					FROM ${tgt_tbl} tgt 
					WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
					) tgt 
					ON tgt.Household_Id = src.Household_Id 
					and  tgt.Transaction_Id = src.Transaction_Id
					and tgt.Loyalty_Program_Cd = src.Loyalty_Program_Cd
					and tgt.Transaction_Type_Cd = src.Transaction_Type_Cd
					and tgt.Update_Ts = src.Update_Ts
					and tgt.Status_Cd = src.Status_Cd
				    and tgt.Reward_Dollar_End_Ts = src.Reward_Dollar_End_Ts
					WHERE  (tgt.Household_Id is null and tgt.Transaction_Id  is null and  tgt.Loyalty_Program_Cd is null 
					and tgt.Transaction_Type_Cd is null and tgt.Update_Ts is null and
					tgt.Status_Cd is null and tgt.Reward_Dollar_End_Ts is null)  
					or(							
					 NVL(src.Create_Ts ,'9999-12-31 00:00:00') <> NVL(tgt.Create_Ts,'9999-12-31 00:00:00')
					 OR NVL(src.Before_Snapshot,'-1') <> NVL(tgt.Before_Snapshot,'-1')
					 OR NVL(src.After_Snapshot, '-1') <> NVL(tgt.After_Snapshot,'-1')
					 OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND)`;


try {
		snowflake.execute (
			{sqlText: sql_command  }
			);
		}
	catch (err)  {
		return `Creation of Reward_Transaction_Audit_Log work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
		}



//SCD Type2 transaction begins
var sql_begin = "BEGIN"


// SCD Type2 - Processing Different day updates
              var sql_updates = `UPDATE ${tgt_tbl} as tgt
              SET 
                             DW_Last_Effective_dt = CURRENT_DATE - 1,
							 --Dw_Last_Effective_Ts = dateadd(minute,-1,current_timestamp),
                             DW_CURRENT_VERSION_IND = FALSE,
                             DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
                             DW_SOURCE_UPDATE_NM = filename
              FROM ( 
                             SELECT 
                                           Household_Id, 
										   Transaction_Id,
										   Loyalty_Program_Cd,
										   Transaction_Type_Cd,
										   Update_Ts,
										   status_cd,
										   Reward_Dollar_End_Ts,
                                           filename					   
                             FROM ${tgt_wrk_tbl}
                             WHERE DML_Type = 'U' 
                             AND Sameday_chg_ind = 0                                      
                             AND Household_Id is not NULL                              
							 AND Transaction_Id is not null							 
							 AND Loyalty_Program_Cd is not null							 
							 AND Transaction_Type_Cd is not null							 
							 AND Update_Ts is not null
							 AND STATUS_CD IS NOT NULL
							 AND Reward_Dollar_End_Ts IS NOT NULL
										   
                             ) src
                             WHERE tgt.Household_Id = src.Household_Id
							 AND tgt.Transaction_Id = src.Transaction_Id
							 AND tgt.Loyalty_Program_Cd = src.Loyalty_Program_Cd
							 AND tgt.Transaction_Type_Cd = src.Transaction_Type_Cd
							 AND TGT.Update_Ts = SRC.Update_Ts
							 AND tgt.Status_Cd = src.Status_Cd
							 AND tgt.Reward_Dollar_End_Ts = src.Reward_Dollar_End_Ts
							 AND tgt.DW_CURRENT_VERSION_IND = TRUE 
							 AND tgt.DW_LOGICAL_DELETE_IND = FALSE`;
							 

// Processing Sameday updates
var sql_sameday = ` UPDATE ${tgt_tbl} as tgt
					SET  Create_Ts                    = src.Create_Ts
						,Before_Snapshot              = src.Before_Snapshot
						,After_Snapshot               = src.After_Snapshot
						,DW_LAST_UPDATE_TS			  =CURRENT_TIMESTAMP
						,DW_SOURCE_UPDATE_NM 		  = FileName											
						,DW_LOGICAL_DELETE_IND   	  =src.DW_LOGICAL_DELETE_IND
						
						FROM ( SELECT 
								     Household_Id,
									 Transaction_Id,
									 Loyalty_Program_Cd,
									 Transaction_Type_Cd,	
									 Create_Ts,
									 Update_Ts,
									 Before_Snapshot,
									 After_Snapshot,
									 STATUS_CD,
									 Reward_Dollar_End_Ts,
									 DW_LOGICAL_DELETE_IND,
									 filename																	
							   FROM ${tgt_wrk_tbl}
							   WHERE DML_Type = 'U'
							   AND Sameday_chg_ind = 1
							   AND Household_Id is not NULL                              
							   AND Transaction_Id is not null							 
							   AND Loyalty_Program_Cd is not null							 
							   AND Transaction_Type_Cd is not null
							   AND Update_Ts is not null
							   AND STATUS_CD IS NOT NULL 
							   AND Reward_Dollar_End_Ts is not NULL
							) src
							WHERE tgt.Household_Id = src.Household_Id							
							AND tgt.Transaction_Id = src.Transaction_Id
							AND tgt.Loyalty_Program_Cd = src.Loyalty_Program_Cd
							AND tgt.Transaction_Type_Cd = src.Transaction_Type_Cd
							AND tgt.Update_Ts = src.Update_Ts
							AND tgt.STATUS_CD = src.STATUS_CD
							AND tgt.Reward_Dollar_End_Ts = src.Reward_Dollar_End_Ts
							AND tgt.DW_CURRENT_VERSION_IND = TRUE`;


// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl} 
                   ( Household_Id,				   
					 Transaction_Id ,
					 Loyalty_Program_Cd,
					 Transaction_Type_Cd,
					 Update_Ts,
					 DW_First_Effective_Dt,
					 DW_Last_Effective_Dt,
					 
					 Create_Ts,
					 Before_Snapshot,
					 After_Snapshot,
					 status_cd,
					 Reward_Dollar_End_Ts,
					 DW_CREATE_TS,
					 DW_LOGICAL_DELETE_IND,
					 DW_SOURCE_CREATE_NM,
					 DW_CURRENT_VERSION_IND					
					 )
					SELECT distinct
					Household_Id         ,
					Transaction_Id         ,
					Loyalty_Program_Cd   ,
					Transaction_Type_Cd,
					Update_Ts,
					CURRENT_DATE,
					'31-DEC-9999',					
					Create_Ts,
					Before_Snapshot,
					After_Snapshot,
					status_cd,
					Reward_Dollar_End_Ts,
					CURRENT_TIMESTAMP,
					DW_Logical_delete_ind,
				    FileName,
					TRUE					
					FROM ${tgt_wrk_tbl}
					WHERE Household_Id IS NOT NULL
					and Transaction_Id IS NOT NULL
					and Loyalty_Program_Cd IS NOT NULL
					and Transaction_Type_Cd is not null
					and Update_Ts is not null
					AND status_cd is not null
					and Reward_Dollar_End_Ts is not null
					and Sameday_chg_ind = 0`;



var sql_commit = "COMMIT"
var sql_rollback = "ROLLBACK"
try {
        snowflake.execute (
            {sqlText: sql_begin  }
            );		
		snowflake.execute (
            {sqlText: sql_updates  }
            );
        snowflake.execute (
            {sqlText: sql_sameday  }
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
 
 

// ************** Load for Reward_Transaction_Audit_Log table ENDs *****************

$$;

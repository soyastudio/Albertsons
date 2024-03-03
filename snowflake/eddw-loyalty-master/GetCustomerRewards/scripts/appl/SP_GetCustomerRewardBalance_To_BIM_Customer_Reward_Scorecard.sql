--liquibase formatted sql
--changeset SYSTEM:SP_GetCustomerRewardBalance_To_BIM_Customer_Reward_Scorecard runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETCUSTOMERREWARDBALANCE_TO_BIM_CUSTOMER_REWARD_SCORECARD(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_LOYAL VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
   
   

var cnf_db = CNF_DB ;
var wrk_schema = C_STAGE ;
var cnf_schema = C_LOYAL;
var src_wrk_tbl = SRC_WRK_TBL;

var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.Customer_Reward_Scorecard_wrk`;
var tgt_tbl = `${cnf_db}.${cnf_schema}.Customer_Reward_Scorecard`;

// ************** Load for customer_reward_scorecard table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.
 
var sql_command = `CREATE OR REPLACE TABLE ${tgt_wrk_tbl} as
								WITH src_wrk_tbl_recs as
								(SELECT DISTINCT 
								Balance_Update_Ts,
								Household_Id,
								Reward_Bucket_Type_Cd,
								Reward_Bucket_Type_Dsc,
								Reward_Bucket_Type_Short_Dsc,
								Reward_Value_Qty,
								Reward_Validity_Start_Dt,
								Reward_Validity_End_Dt,								
								FileName,	
								ActionTypeCd,
								creationdt,																				
								Row_number() OVER ( partition BY TO_TIMESTAMP_LTZ(Balance_Update_Ts), Household_Id, Reward_Bucket_Type_Cd ORDER BY To_timestamp_ntz(creationdt) DESC) AS rn
								from
								(
								SELECT DISTINCT 
								BalanceUpdateTs As Balance_Update_Ts,
								HouseholdId AS Household_Id,
								RewardTypeCd_Code AS Reward_Bucket_Type_Cd,
								RewardTypeCd_Description AS Reward_Bucket_Type_Dsc,
								RewardTypeCd_ShortDescription AS Reward_Bucket_Type_Short_Dsc,
								RewardValueQty AS Reward_Value_Qty,	
								RewardValidityEndTs AS Reward_Validity_Start_Dt,
								RewardValidityEndTs AS Reward_Validity_End_Dt,																
								FileName,
								ActionTypeCd,
								creationdt																				
								FROM ${src_wrk_tbl}
								)
								)
								SELECT
								src.Balance_Update_Ts,
								src.Household_Id,
								src.Reward_Bucket_Type_Cd,
								src.Reward_Bucket_Type_Dsc,								
								src.Reward_Bucket_Type_Short_Dsc,
								src.Reward_Value_Qty,
								src.Reward_Validity_Start_Dt,
								src.Reward_Validity_End_Dt,								
								src.DW_LOGICAL_DELETE_IND,												
								src.FileName,
								src.ActionTypeCd,
								src.creationdt
								from
								(SELECT
								s.Balance_Update_Ts,
								s.Household_Id,
								s.Reward_Bucket_Type_Cd,
								s.Reward_Bucket_Type_Dsc,								
								s.Reward_Bucket_Type_Short_Dsc,
								s.Reward_Value_Qty,
								case when (s.Reward_Bucket_Type_Cd= 'Points' or s.Reward_Bucket_Type_Cd='Rwd_exp') then dateadd(month,-1,date_trunc('month',dateadd('day',1,s.Reward_Validity_End_DT)))
								when (s.Reward_Bucket_Type_Cd ='Rwd') then dateadd(month,-2,date_trunc('month',dateadd('day',1,s.Reward_Validity_End_DT)))
								when (s.Reward_Bucket_Type_Cd ='Rwd_nexp' or s.Reward_Bucket_Type_Cd ='Rwd_Nexp') then dateadd(day, 0, date_trunc('month',current_timestamp()))
								else NULL END as Reward_Validity_Start_DT, 
								
							       case when (s.Reward_Bucket_Type_Cd = 'Rwd_nexp' or s.Reward_Bucket_Type_Cd = 'Rwd_Nexp') then '2099-12-31' 
								else s.Reward_Validity_End_Dt END as Reward_Validity_End_Dt,
								s.DW_LOGICAL_DELETE_IND,
								s.FileName,
								s.ActionTypeCd,
								s.creationdt
								from
								(SELECT
								Balance_Update_Ts,
								Household_Id,
								Reward_Bucket_Type_Cd,
								Reward_Bucket_Type_Dsc,								
								Reward_Bucket_Type_Short_Dsc,
								Reward_Value_Qty,
								Reward_Validity_Start_Dt,
								Reward_Validity_End_Dt,
								False as DW_LOGICAL_DELETE_IND,
								FileName,
								ActionTypeCd,
								creationdt
								FROM src_wrk_tbl_recs 
								WHERE rn = 1 
								AND Balance_Update_Ts IS NOT NULL
								and Household_Id IS NOT NULL
								and Reward_Bucket_Type_Cd IS NOT NULL
								)s) src 

								LEFT JOIN 
								(SELECT  DISTINCT
								tgt.Balance_Update_Ts,
								tgt.Household_Id,
								tgt.Reward_Bucket_Type_Cd,
								tgt.Reward_Bucket_Type_Dsc,								
								tgt.Reward_Bucket_Type_Short_Dsc,
								tgt.Reward_Value_Qty,
								tgt.Reward_Validity_Start_Dt,
								tgt.Reward_Validity_End_Dt,
								tgt.dw_logical_delete_ind,
								tgt.dw_first_effective_dt
								FROM ${tgt_tbl} tgt 
								WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
								) tgt 
								ON tgt.Balance_Update_Ts = src.Balance_Update_Ts
								and tgt.Household_Id = src.Household_Id 
								and tgt.Reward_Bucket_Type_Cd = src.Reward_Bucket_Type_Cd
								WHERE  (tgt.Balance_Update_Ts is null and tgt.Household_Id IS NULL and tgt.Reward_Bucket_Type_Cd is null)  
								or(
								NVL(src.Reward_Bucket_Type_Dsc,'-1') <> NVL(tgt.Reward_Bucket_Type_Dsc,'-1')								
								OR NVL(src.Reward_Bucket_Type_Short_Dsc,'-1') <> NVL(tgt.Reward_Bucket_Type_Short_Dsc,'-1')							
								OR NVL(src.Reward_Value_Qty,'-1') <> NVL(tgt.Reward_Value_Qty,'-1')	
								OR NVL(src.Reward_Validity_Start_Dt,'9999-12-31 00:00:00.000') <> NVL(tgt.Reward_Validity_Start_Dt,'9999-12-31 00:00:00.000')
								OR NVL(src.Reward_Validity_End_Dt,'9999-12-31 00:00:00.000') <> NVL(tgt.Reward_Validity_End_Dt,'9999-12-31 00:00:00.000')								
								OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
								)`;

try {
        snowflake.execute (
            {sqlText: sql_command  }
            );
        }
    catch (err)  {
        return "Creation of customer_reward_scorecard work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }

							
//SCD Type2 transaction begins

var sql_updates = `// Processing Updates of Type 2 SCD
                UPDATE ${tgt_tbl} as tgt
                SET  DW_Last_Effective_Dt = current_timestamp 
               ,DW_CURRENT_VERSION_IND = FALSE
               ,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
               ,DW_SOURCE_UPDATE_NM = filename
                FROM ( SELECT Balance_Update_Ts
				   ,Household_Id
				   ,Reward_Bucket_Type_Cd
				   ,Filename	
                        FROM ${tgt_wrk_tbl}
                                           
					) src
				WHERE 
				tgt.Balance_Update_Ts = src.Balance_Update_Ts AND
				tgt.Household_Id = src.Household_Id AND
				tgt.Reward_Bucket_Type_Cd = src.Reward_Bucket_Type_Cd 
				AND  tgt.DW_CURRENT_VERSION_IND = TRUE`;
                             

var sql_begin = "BEGIN"

// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl}
                   (Balance_Update_Ts,
				    Household_Id,
					Reward_Bucket_Type_Cd,
				    DW_First_Effective_Dt,
					 DW_Last_Effective_Dt  ,
					 Reward_Bucket_Type_Dsc,					
					Reward_Bucket_Type_Short_Dsc,
					Reward_Value_Qty,										
					DW_CREATE_TS,
					DW_LOGICAL_DELETE_IND,
					DW_SOURCE_CREATE_NM,
					DW_CURRENT_VERSION_IND,
                                        Reward_Validity_Start_Dt,
					Reward_Validity_End_Dt
					)
					SELECT distinct
					Balance_Update_Ts,
				    Household_Id,
					Reward_Bucket_Type_Cd,				    
					CURRENT_DATE,
					'31-DEC-9999',
			        Reward_Bucket_Type_Dsc,					
					Reward_Bucket_Type_Short_Dsc,
					Reward_Value_Qty,					
					CURRENT_TIMESTAMP,
					DW_Logical_delete_ind,
				    FileName,
					TRUE,
                                        Reward_Validity_Start_Dt,
					Reward_Validity_End_Dt
					FROM ${tgt_wrk_tbl}
					WHERE Balance_Update_Ts IS NOT NULL
					and Household_Id IS NOT NULL
					and Reward_Bucket_Type_Cd IS NOT NULL`;

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

// ************** Load for customer_reward_scorecard table ENDs *****************

// ************** Load for customer_reward_scorecard table ENDs *****************


$$;

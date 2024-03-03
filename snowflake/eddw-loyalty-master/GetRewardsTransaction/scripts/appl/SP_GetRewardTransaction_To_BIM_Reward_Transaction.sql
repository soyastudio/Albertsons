--liquibase formatted sql

--changeset SYSTEM:SP_GetRewardTransaction_To_BIM_Reward_Transaction runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema DW_APPL;


CREATE OR REPLACE PROCEDURE SP_GETREWARDTRANSACTION_TO_BIM_REWARD_TRANSACTION(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_LOYAL VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

var cnf_db = CNF_DB ;
var wrk_schema = C_STAGE ;
var cnf_schema = C_LOYAL;
var src_wrk_tbl = SRC_WRK_TBL;


var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.Reward_Transaction_wrk`;
var tgt_tbl = `${cnf_db}.${cnf_schema}.Reward_Transaction`;


// ************** Load for Reward_Transaction table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.


var sql_command = `CREATE OR REPLACE TABLE ${tgt_wrk_tbl} as
				WITH src_wrk_tbl_recs as
													(SELECT DISTINCT 
													Household_Id			,										
													Transaction_Id         ,
													Loyalty_Program_Cd  ,
													Transaction_Detail_Txt   ,
													Transaction_Ts         ,
													Reference_Nbr           ,
													Loyalty_Program_Dsc    ,
													Transaction_Type_Cd    ,
													Transaction_Type_Dsc   ,
													Transaction_Type_Short_Dsc   ,
													Reward_Dollar_Start_Ts ,
													Reward_Dollar_End_Ts  ,
													Reward_Dollar_Points_Qty   ,
													Reward_Origin_Cd      ,
													Reward_Origin_Dsc      ,
													Reward_Origin_Ts        ,
													Status_Cd               ,
													Status_Dsc              ,
													Status_Effective_Ts     ,
													Status_Type_Cd         ,
													Customer_Tier_Cd      ,
													Customer_Tier_Dsc       ,
													Customer_Tier_Short_Dsc ,
													Create_Ts             ,
													Create_User_Id          ,
													Update_Ts             ,
													Update_User_Id          ,
													FileName,
													ActionTypeCd,
													creationdt,	
													Alt_Transaction_Id,													
													Row_number() OVER ( partition BY 
													Household_Id,Transaction_Id,Loyalty_Program_Cd,
													Transaction_Type_Cd,Status_Cd, Reward_Dollar_End_Ts  
													ORDER BY To_timestamp_ntz(Create_Ts) DESC) AS rn
													from
													(
													SELECT DISTINCT 
													PRT.HouseholdId AS Household_Id          ,
													PRT.TransactionId AS  Transaction_Id,
													PRT.LoyaltyProgramCd As Loyalty_Program_Cd              ,
													PRT.TransactionDetailTxt AS Transaction_Detail_Txt ,
													try_to_timestamp('') As Transaction_Ts,
													'' AS Reference_Nbr,
													'' As Loyalty_Program_Dsc            ,
													PRT.TransactionTypeCd_Code  As Transaction_Type_Cd           ,	
													'' As Transaction_Type_Dsc         ,
													''  AS Transaction_Type_Short_Dsc    ,
													PRT.RewardDollarStartTs AS Reward_Dollar_Start_Ts   ,
													PRT.RewardDollarEndTs As Reward_Dollar_End_Ts   ,
													PRT.RewardDollarPointsQty AS Reward_Dollar_Points_Qty   ,
													PRT.RewardOriginCd AS  Reward_Origin_Cd        ,
													'' AS Reward_Origin_Dsc       ,
													PRT.REwardOriginTs AS Reward_Origin_Ts       ,
													PRT.StatusCd AS Status_Cd              ,
													'' AS Status_Dsc              ,
													try_to_timestamp('') AS Status_Effective_Ts    ,
													'' AS Status_Type_Cd        ,
													PRT.CustomerTierCd_Code AS Customer_Tier_Cd       ,
													'' AS Customer_Tier_Dsc    ,
													'' AS Customer_Tier_Short_Dsc   ,
													PRT.CreateTs AS Create_Ts              ,
													PRT.CreateUserId As Create_User_Id          ,
													PRT.UpdateTs AS Update_Ts              ,
													PRT.UpdateUserId AS Update_User_Id,
													PRT.FileName,
													PRT.ActionTypeCd,
													PRT.creationdt,
													PRT.AltTransactionId as Alt_Transaction_Id													
																	
													FROM ${src_wrk_tbl} PRT
													)
													)
													SELECT
													src.Household_Id        ,
													src.Transaction_Id         ,
													src.Loyalty_Program_Cd  ,
													src.Transaction_Detail_Txt   ,
													src.Transaction_Ts         ,
													src.Reference_Nbr           ,
													src.Loyalty_Program_Dsc    ,
													src.Transaction_Type_Cd    ,
													src.Transaction_Type_Dsc   ,
													src.Transaction_Type_Short_Dsc   ,
													src.Reward_Dollar_Start_Ts ,
													src.Reward_Dollar_End_Ts  ,
													src.Reward_Dollar_Points_Qty   ,
													src.Reward_Origin_Cd      ,
													src.Reward_Origin_Dsc      ,
													src.Reward_Origin_Ts        ,
													src.Status_Cd               ,
													src.Status_Dsc              ,
													src.Status_Effective_Ts     ,
													src.Status_Type_Cd         ,
													src.Customer_Tier_Cd      ,
													src.Customer_Tier_Dsc       ,
													src.Customer_Tier_Short_Dsc ,
													src.Create_Ts             ,
													src.Create_User_Id          ,
													src.Update_Ts             ,
													src.Update_User_Id          ,
													src.DW_LOGICAL_DELETE_IND,
													src.FileName,
													src.ActionTypeCd,
													src.creationdt,
													src.Alt_Transaction_Id,
													CASE WHEN tgt.Household_Id IS NULL AND tgt.Transaction_Id IS NULL AND tgt.Loyalty_Program_Cd IS NULL AND tgt.Transaction_Type_Cd IS NULL 
													              AND TGT.Status_Cd IS NULL AND TGT.Reward_Dollar_End_Ts IS NULL
													THEN 'I' ELSE 'U' END AS DML_Type,
													CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1  ELSE 0 END AS Sameday_chg_ind 
													from
													(SELECT
													Household_Id			,										
													Transaction_Id         ,
													Loyalty_Program_Cd  ,
													Transaction_Detail_Txt   ,
													Transaction_Ts         ,
													Reference_Nbr           ,
													Loyalty_Program_Dsc    ,
													Transaction_Type_Cd    ,
													Transaction_Type_Dsc   ,
													Transaction_Type_Short_Dsc   ,
													Reward_Dollar_Start_Ts ,
													Reward_Dollar_End_Ts  ,
													Reward_Dollar_Points_Qty   ,
													Reward_Origin_Cd      ,
													Reward_Origin_Dsc      ,
													Reward_Origin_Ts        ,
													Status_Cd               ,
													Status_Dsc              ,
													Status_Effective_Ts     ,
													Status_Type_Cd         ,
													Customer_Tier_Cd      ,
													Customer_Tier_Dsc       ,
													Customer_Tier_Short_Dsc ,
													Create_Ts             ,
													Create_User_Id          ,
													Update_Ts             ,
													Update_User_Id          ,
													False as DW_LOGICAL_DELETE_IND,
													FileName,
													ActionTypeCd,
													creationdt,
													Alt_Transaction_Id													
													FROM src_wrk_tbl_recs 
													WHERE rn = 1 
													AND Household_Id IS NOT NULL 
													and  Transaction_Id IS NOT NULL
													and  Loyalty_Program_Cd IS NOT NULL 
													and Transaction_Type_Cd is not null
													AND Status_Cd IS NOT NULL 
													AND Reward_Dollar_End_Ts IS NOT NULL
													) src 
													
													LEFT JOIN 
													(SELECT  DISTINCT													
														tgt.Household_Id			,											
														tgt.Transaction_Id         ,
														tgt.Loyalty_Program_Cd  ,
														tgt.Transaction_Detail_Txt   ,
														tgt.Transaction_Ts         ,
														tgt.Reference_Nbr           ,
														tgt.Loyalty_Program_Dsc    ,
														tgt.Transaction_Type_Cd    ,
														tgt.Transaction_Type_Dsc   ,
														tgt.Transaction_Type_Short_Dsc   ,
														tgt.Reward_Dollar_Start_Ts ,
														tgt. Reward_Dollar_End_Ts  ,
														tgt.Reward_Dollar_Points_Qty   ,
														tgt.Reward_Origin_Cd      ,
														tgt. Reward_Origin_Dsc      ,
														tgt.Reward_Origin_Ts        ,
														tgt.Status_Cd               ,
														tgt.Status_Dsc              ,
														tgt.Status_Effective_Ts     ,
														tgt.Status_Type_Cd         ,
														tgt.Customer_Tier_Cd      ,
														tgt.Customer_Tier_Dsc       ,
														tgt.Customer_Tier_Short_Dsc ,
														tgt.Create_Ts             ,
														tgt. Create_User_Id          ,
														tgt.Update_Ts             ,
														tgt.Update_User_Id          ,
														tgt.dw_logical_delete_ind,
														tgt.dw_first_effective_dt,
								                        tgt.Alt_Transaction_Id
														FROM ${tgt_tbl} tgt 
														WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
														) tgt 
														ON tgt.Household_Id = src.Household_Id 
														and  tgt.Transaction_Id = src.Transaction_Id
														and tgt.Loyalty_Program_Cd = src.Loyalty_Program_Cd
														and tgt.Transaction_Type_Cd = src.Transaction_Type_Cd
                                                        and tgt.Status_Cd = src.Status_Cd
														and tgt.Reward_Dollar_End_Ts = src.Reward_Dollar_End_Ts
														
														WHERE  (tgt.Household_Id is null and tgt.Transaction_Id  is null and 
														tgt.Loyalty_Program_Cd is null and tgt.Transaction_Type_Cd is null AND
														 tgt.Status_Cd is null and tgt.Reward_Dollar_End_Ts is null)  
														or(							
														 NVL(src.Transaction_Detail_Txt ,'-1') <> NVL(tgt.Transaction_Detail_Txt,'-1')
														 OR NVL(src.Transaction_Ts ,'9999-12-31 00:00:00') <> NVL(tgt.Transaction_Ts,'9999-12-31 00:00:00')
														 OR NVL(src.Reference_Nbr,'-1') <> NVL(tgt.Reference_Nbr,'-1')
														 OR NVL(src.Loyalty_Program_Dsc, '-1') <> NVL(tgt.Loyalty_Program_Dsc,'-1')
														 OR NVL(src.Transaction_Type_Dsc,'-1') <> NVL (tgt.Transaction_Type_Dsc,'-1')
														 OR NVL(src.Transaction_Type_Short_Dsc ,'-1') <> NVL (tgt.Transaction_Type_Short_Dsc,'-1')
														 OR NVL(src.Reward_Dollar_Start_Ts , '9999-12-31 00:00:00') <> NVL (tgt.Reward_Dollar_Start_Ts,'9999-12-31 00:00:00')
														 
														 OR NVL(src.Reward_Dollar_Points_Qty,'-1')<> NVL(tgt.Reward_dollar_Points_Qty,'-1')
														 OR NVL(src.Reward_Origin_Cd ,'-1') <> NVL (tgt.Reward_Origin_Cd,'-1')
														 OR NVL(src.Reward_Origin_Dsc  ,'-1') <>NVL (tgt.Reward_Origin_Dsc,'-1')
														 OR NVL(src.Reward_Origin_Ts  ,'9999-12-31 00:00:00') <> NVL (tgt.Reward_origin_Ts,'9999-12-31 00:00:00')
														 
														 OR NVL(src.Status_Dsc ,'-1') <> NVL (tgt.Status_DSC,'-1')
														 OR NVL(src.Status_Effective_Ts ,'9999-12-31 00:00:00')<> NVL(tgt.Status_Effective_Ts,'9999-12-31 00:00:00')
														 OR NVL(src.Status_Type_Cd  ,'-1')<> NVL(tgt.Status_Type_Cd,'-1')
														 OR NVL(src.Customer_Tier_Cd  ,'-1') <> NVL (tgt.Customer_Tier_Cd,'-1')
														 OR NVL(src.Customer_Tier_Dsc ,'-1')<> NVL (tgt.Customer_Tier_Dsc,'-1')
														 OR NVL(src.Customer_Tier_Short_Dsc ,'-1') <>NVL(tgt.Customer_Tier_Short_Dsc,'-1')
														 OR NVL(src.Create_Ts ,'9999-12-31 00:00:00')<>NVL(tgt.Create_Ts,'9999-12-31 00:00:00') 
														 OR NVL(src.Create_User_Id  ,'-1') <>NVL (tgt.Create_User_Id,'-1')
														 OR NVL(src.Update_Ts ,'9999-12-31 00:00:00') <> NVL(tgt.Update_Ts,'9999-12-31 00:00:00')
														 OR NVL(src.Update_User_Id  ,'-1') <> NVL (tgt.Update_User_Id,'-1')
														 OR NVL(src.Alt_Transaction_Id  ,'-1') <> NVL (tgt.Alt_Transaction_Id,'-1')
														 OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND)`;


							try {
									snowflake.execute (
										{sqlText: sql_command  }
										);
									}
								catch (err)  {
									return `Creation of Reward_Transaction work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
									}



//SCD Type2 transaction begins
var sql_begin = "BEGIN"


// SCD Type2 - Processing Different day updates
              var sql_updates = `UPDATE ${tgt_tbl} as tgt
              SET 
                             DW_Last_Effective_dt = CURRENT_DATE - 1,
                             DW_CURRENT_VERSION_IND = FALSE,
                             DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
                             DW_SOURCE_UPDATE_NM = filename
              FROM ( 
                             SELECT 
                                           Household_Id, 
										   Transaction_Id,
										   Loyalty_Program_Cd,
										   Transaction_Type_Cd,
										   Status_Cd,
										   Reward_Dollar_End_Ts,
                                           filename					   
                             FROM ${tgt_wrk_tbl}
                             WHERE DML_Type = 'U' 
                             AND Sameday_chg_ind = 0                                      
                             AND Household_Id is not NULL                              
							 AND Transaction_Id is not null							 
							 AND Loyalty_Program_Cd is not null							 
							 AND Transaction_Type_Cd is not null
							 AND Status_Cd is not null
							 AND Reward_Dollar_End_Ts is not null
                             ) src
                             WHERE tgt.Household_Id = src.Household_Id
							 AND tgt.Transaction_Id = src.Transaction_Id
							 AND tgt.Loyalty_Program_Cd = src.Loyalty_Program_Cd
							 AND tgt.Transaction_Type_Cd = src.Transaction_Type_Cd
							 AND tgt.Status_Cd = src.Status_Cd
							 AND tgt.Reward_Dollar_End_Ts = src.Reward_Dollar_End_Ts
							 AND tgt.DW_CURRENT_VERSION_IND = TRUE 
							 AND tgt.DW_LOGICAL_DELETE_IND = FALSE`;
							 

// Processing Sameday updates
var sql_sameday = ` UPDATE ${tgt_tbl} as tgt
					SET  Household_Id                 =src.Household_Id  
					    ,Transaction_Id               =src.Transaction_Id 
					    ,Loyalty_Program_Cd           =src.Loyalty_Program_Cd
					    ,Transaction_Detail_Txt       =src.Transaction_Detail_Txt   
					    ,Transaction_Ts               =src.Transaction_Ts         
					    ,Reference_Nbr                =src.Reference_Nbr           
					    ,Loyalty_Program_Dsc          =src.Loyalty_Program_Dsc    
					    ,Transaction_Type_Cd          =src.Transaction_Type_Cd    
					    ,Transaction_Type_Dsc         =src.Transaction_Type_Dsc   
					    ,Transaction_Type_Short_Dsc   =src.Transaction_Type_Short_Dsc   
					    ,Reward_Dollar_Start_Ts       =src.Reward_Dollar_Start_Ts 
					    ,Reward_Dollar_End_Ts         =src.Reward_Dollar_End_Ts  
					    ,Reward_Dollar_Points_Qty     =src.Reward_Dollar_Points_Qty   
						,Reward_Origin_Cd             =src.Reward_Origin_Cd      
						,Reward_Origin_Dsc            =src.Reward_Origin_Dsc      
						,Reward_Origin_Ts             =src.Reward_Origin_Ts        
						,Status_Cd                    =src.Status_Cd               
						,Status_Dsc                   =src.Status_Dsc              
						,Status_Effective_Ts          =src.Status_Effective_Ts     
						,Status_Type_Cd               =src.Status_Type_Cd         
						,Customer_Tier_Cd             =src.Customer_Tier_Cd      
						,Customer_Tier_Dsc            =src.Customer_Tier_Dsc       
						,Customer_Tier_Short_Dsc      =src.Customer_Tier_Short_Dsc 
						,Create_Ts                    =src.Create_Ts					
						,Create_User_Id               =src.Create_User_Id          
						,Update_Ts                    =src.Update_Ts 
						,DW_LAST_UPDATE_TS			  =CURRENT_TIMESTAMP
						,DW_SOURCE_UPDATE_NM 		  = FileName					
						,Update_User_Id               =src.Update_User_Id          
						,DW_LOGICAL_DELETE_IND   	  =src.DW_LOGICAL_DELETE_IND
						,Alt_Transaction_Id      	  =src.Alt_Transaction_Id
						FROM ( SELECT 
								     Household_Id               
									,Transaction_Id             
									,Loyalty_Program_Cd         									  
									,Transaction_Detail_Txt     
									,Transaction_Ts             
									,Reference_Nbr              
									,Loyalty_Program_Dsc        
									,Transaction_Type_Cd        
									,Transaction_Type_Dsc       
									,Transaction_Type_Short_Dsc 
									,Reward_Dollar_Start_Ts     
									,Reward_Dollar_End_Ts       
									,Reward_Dollar_Points_Qty   
									,Reward_Origin_Cd           
									,Reward_Origin_Dsc          
									,Reward_Origin_Ts           
									,Status_Cd                  
									,Status_Dsc                 
									,Status_Effective_Ts        
									,Status_Type_Cd             
									,Customer_Tier_Cd           
									,Customer_Tier_Dsc          
									,Customer_Tier_Short_Dsc    
									,Create_Ts                  
									,Create_User_Id             
									,Update_Ts                  
									,Update_User_Id             									               
									,DW_LOGICAL_DELETE_IND
									,filename									
									,Alt_Transaction_Id      	
							   FROM ${tgt_wrk_tbl}
							   WHERE DML_Type = 'U'
							   AND Sameday_chg_ind = 1
							   AND Household_Id is not NULL                              
							   AND Transaction_Id is not null							 
							   AND Loyalty_Program_Cd is not null							 
							   AND Transaction_Type_Cd is not null
							   AND STATUS_CD IS NOT NULL 
							   AND Reward_Dollar_End_Ts IS NOT NULL
							) src
							WHERE tgt.Household_Id = src.Household_Id							
							AND tgt.Transaction_Id = src.Transaction_Id
							AND tgt.Loyalty_Program_Cd = src.Loyalty_Program_Cd
							AND tgt.Transaction_Type_Cd = src.Transaction_Type_Cd
							AND tgt.STATUS_CD = src.STATUS_CD
							AND tgt.Reward_Dollar_End_Ts = src.Reward_Dollar_End_Ts
							AND tgt.DW_CURRENT_VERSION_IND = TRUE`;


// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl} 
                   ( Household_Id,				   
					 Transaction_Id ,
					 Loyalty_Program_Cd,
					 DW_First_Effective_Dt,
					 DW_Last_Effective_Dt,
					 Transaction_Detail_Txt   ,
					 Transaction_Ts         ,
					 Reference_Nbr           ,
					 Loyalty_Program_Dsc    ,
					 Transaction_Type_Cd    ,
					 Transaction_Type_Dsc   ,
					 Transaction_Type_Short_Dsc   ,
					 Reward_Dollar_Start_Ts ,
					 Reward_Dollar_End_Ts  ,
					 Reward_Dollar_Points_Qty   ,
					 Reward_Origin_Cd      ,
					 Reward_Origin_Dsc      ,
					 Reward_Origin_Ts        ,
					 Status_Cd               ,
					 Status_Dsc              ,
					 Status_Effective_Ts     ,
					 Status_Type_Cd         ,
					 Customer_Tier_Cd      ,
					 Customer_Tier_Dsc       ,
					 Customer_Tier_Short_Dsc ,
					 Create_Ts             ,
					 Create_User_Id          ,
					 Update_Ts             ,
					 Update_User_Id          ,
					 DW_CREATE_TS,
					DW_LOGICAL_DELETE_IND,
					DW_SOURCE_CREATE_NM,
					DW_CURRENT_VERSION_IND,
					Alt_Transaction_Id
					 )
					SELECT distinct
					Household_Id         ,
					Transaction_Id         ,
					Loyalty_Program_Cd   ,
					CURRENT_DATE,
					'31-DEC-9999',
					Transaction_Detail_Txt   ,
					Transaction_Ts         ,
					Reference_Nbr           ,
					Loyalty_Program_Dsc    ,
					Transaction_Type_Cd    ,
					Transaction_Type_Dsc   ,
					Transaction_Type_Short_Dsc   ,
					Reward_Dollar_Start_Ts   ,
					Reward_Dollar_End_Ts   ,
					Reward_Dollar_Points_Qty   ,
					Reward_Origin_Cd        ,
					Reward_Origin_Dsc    ,
					Reward_Origin_Ts       ,
					Status_Cd               ,
					Status_Dsc              ,
					Status_Effective_Ts    ,
					Status_Type_Cd        ,
					Customer_Tier_Cd        ,
					Customer_Tier_Dsc       ,
					Customer_Tier_Short_Dsc   ,
					Create_Ts               ,
					Create_User_Id         ,
					Update_Ts             ,
					Update_User_Id          ,
					CURRENT_TIMESTAMP,
					DW_Logical_delete_ind,
				    FileName,
					TRUE,
					Alt_Transaction_Id					
					FROM ${tgt_wrk_tbl}
					WHERE Household_Id IS NOT NULL
					and Transaction_Id IS NOT NULL
					and Loyalty_Program_Cd IS NOT NULL
					and Transaction_Type_Cd is not null
					AND STATUS_CD IS NOT NULL
					AND Reward_Dollar_End_Ts IS NOT NULL
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
 
 

// ************** Load for Reward_Transaction table ENDs *****************

$$;

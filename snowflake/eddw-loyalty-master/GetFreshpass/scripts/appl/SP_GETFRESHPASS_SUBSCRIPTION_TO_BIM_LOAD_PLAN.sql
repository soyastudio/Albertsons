--liquibase formatted sql
--changeset SYSTEM:SP_GETFRESHPASS_SUBSCRIPTION_TO_BIM_LOAD_PLAN runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETFRESHPASS_SUBSCRIPTION_TO_BIM_LOAD_PLAN(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_LOYAL VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

		var src_wrk_tbl = SRC_WRK_TBL;
		var cnf_schema = 'DW_C_LOYALTY';
		var tgt_wrk_tbl = `${CNF_DB}.${C_STAGE}.Freshpass_Subscription_Plan_WRK`;
		var tgt_tbl = `${CNF_DB}.${cnf_schema}.Freshpass_Subscription_Plan`;
		
		// **************        Truncate and Reload the work table *****************

    var truncate_tgt_wrk_table = `DELETE from ${tgt_wrk_tbl}`;
try {
snowflake.execute ({ sqlText: truncate_tgt_wrk_table });
	}
    catch (err) {
        return `Deletion of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
				}

                       
    // **************        Load for Freshpass_Subscription_Plan table BEGIN *****************
    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
	var create_tgt_wrk_table = `INSERT INTO ${tgt_wrk_tbl}
								SELECT DISTINCT
								src.Subscription_Plan_Type_Nm
								,src.Subscription_Plan_Cd
								,src.Subscription_Type_Nm
								,src.Cancellation_Grace_Period_Nbr
								,src.Discount_Type_Dsc
								,src.Currency_Cd
								,src.Discount_Duration_Days_Nbr
								,src.Discount_End_Dt
								,src.Discount_Plan_End_Dt
								,src.Discount_Plan_Start_Dt
								,src.Discounted_Price_Ind
								,src.Extended_Trial_Duration_Ind		
								,src.Regular_Plan_Price_Amt
								,src.Subscription_Price_Amt
								,src.Trial_Duration_Days_Nbr
								,src.filename
								,src.DW_LOGICAL_DELETE_IND								
								,CASE 
								    WHEN (
											 tgt.Subscription_Plan_Type_Nm IS NULL
										and	 tgt.Subscription_Plan_Cd IS NULL 
										and  tgt.Subscription_Type_Nm IS NULL 
										and	 tgt.Discount_Type_Dsc IS NULL 
										 ) 
									THEN 'I' 
									ELSE 'U' 
								END AS DML_Type
								,CASE   
									WHEN tgt.DW_First_Effective_dt = CURRENT_DATE 
									THEN 1 
									Else 0 
								END as Sameday_chg_ind
								FROM (  
								        SELECT
											Subscription_Plan_Type_Nm
											,Subscription_Plan_Cd
											,Subscription_Type_Nm
											,Cancellation_Grace_Period_Nbr
											,Discount_Type_Dsc
											,Currency_Cd
											,Discount_Duration_Days_Nbr
											,Discount_End_Dt
											,Discount_Plan_End_Dt
											,Discount_Plan_Start_Dt
											,Discounted_Price_Ind
											,Extended_Trial_Duration_Ind
											,Regular_Plan_Price_Amt
											,Subscription_Price_Amt
											,Trial_Duration_Days_Nbr
											,filename
											,DW_CREATETS
											,DW_LOGICAL_DELETE_IND											
											
										FROM ( 
											   SELECT
													
													 Subscription_Plan_Type_Nm
													,Subscription_Plan_Cd
													,Subscription_Type_Nm
													,Cancellation_Grace_Period_Nbr
													,Discount_Type_Dsc
													,Currency_Cd
													,try_to_numeric(Discount_Duration_Days_Nbr) as Discount_Duration_Days_Nbr
													,Discount_End_Dt
													,Discount_Plan_End_Dt
													,Discount_Plan_Start_Dt
													,Discounted_Price_Ind
													,Extended_Trial_Duration_Ind
													,try_to_numeric(Regular_Plan_Price_Amt, 12 ,2) as Regular_Plan_Price_Amt
													,try_to_numeric(Subscription_Price_Amt, 12 ,2) as Subscription_Price_Amt
													,try_to_numeric(Trial_Duration_Days_Nbr) as Trial_Duration_Days_Nbr
													,filename
													,DW_CREATETS										   
											        ,false as  DW_LOGICAL_DELETE_IND
											        ,Row_number() OVER (
											        PARTITION BY Subscription_Plan_Type_Nm, Subscription_Plan_Cd, Subscription_Type_Nm,Discount_Type_Dsc
											        ORDER BY(event_ts) DESC) as rn
											  
													  FROM													   
														(
													    SELECT
														Subscription_plan_type as Subscription_Plan_Type_Nm
														,subscriptioncode AS Subscription_Plan_Cd
														,subscriptiontype AS Subscription_Type_Nm
														,discounttype AS Discount_Type_Dsc
														,CancellationGracePeriod AS Cancellation_Grace_Period_Nbr
														,currency AS Currency_Cd
														,discountduration AS Discount_Duration_Days_Nbr
														,to_date(discountenddate) AS Discount_End_Dt 
														,to_date(campaignenddate) AS Discount_Plan_End_Dt
														,to_date(campaignstartdate) AS Discount_Plan_Start_Dt
														,isdiscountedprice AS Discounted_Price_Ind
														,isextendedtrialduration AS Extended_Trial_Duration_Ind
														,regularplanprice AS Regular_Plan_Price_Amt
														,fee AS Subscription_Price_Amt
														,trialduration AS Trial_Duration_Days_Nbr
														,filename
														,DW_CREATETS
                                                        ,EVENTTIME as event_ts
													   FROM  ${src_wrk_tbl} S
													  )                         
												) WHERE rn=1																
											) src  
																						  
									LEFT JOIN
									( 
									SELECT  DISTINCT
										Subscription_Plan_Type_Nm
										,Subscription_Plan_Cd
										,Subscription_Type_Nm
										,Cancellation_Grace_Period_Nbr
										,Discount_Type_Dsc
										,Currency_Cd
										,Discount_Duration_Days_Nbr
										,Discount_End_Dt
										,Discount_Plan_End_Dt
										,Discount_Plan_Start_Dt
										,Discounted_Price_Ind
										,Extended_Trial_Duration_Ind
										,Regular_Plan_Price_Amt
										,Subscription_Price_Amt
										,Trial_Duration_Days_Nbr
										,DW_LOGICAL_DELETE_IND
										,DW_First_Effective_dt
									FROM ${tgt_tbl} tgt
									WHERE DW_CURRENT_VERSION_IND = TRUE
									)as tgt 
									ON
									nvl(src.Subscription_Plan_Type_Nm,'-1') = nvl(tgt.Subscription_Plan_Type_Nm,'-1')
									AND nvl(src.Subscription_Plan_Cd,'-1') = nvl(tgt.Subscription_Plan_Cd,'-1')
									AND nvl(src.Subscription_Type_Nm,'-1') =  nvl(tgt.Subscription_Type_Nm,'-1')
									AND nvl(src.Discount_Type_Dsc,'-1') =  nvl(tgt.Discount_Type_Dsc,'-1')
									
									WHERE  
									(
									tgt.Subscription_Plan_Type_Nm is NULL 
									AND tgt.Subscription_Plan_Cd is NULL   
									AND tgt.Subscription_Type_Nm is  NULL							
									AND tgt.Discount_Type_Dsc is NULL
									 )
									OR
									(  
									    NVL(src.Currency_Cd,'-1') <> NVL(tgt.Currency_Cd,'-1')
									 OR NVL(src.Discount_Duration_Days_Nbr,'-1') <>NVL(tgt.Discount_Duration_Days_Nbr,'-1')
									 OR NVL(src.Regular_Plan_Price_Amt,'-1') <> NVL(tgt.Regular_Plan_Price_Amt,'-1')
									 OR NVL(src.Subscription_Price_Amt,'-1') <> NVL(tgt.Subscription_Price_Amt,'-1')
									 OR NVL(to_date(src.Discount_End_Dt),'9999-12-31') <> NVL(tgt.Discount_End_Dt,'9999-12-31')
									 OR NVL(src.Trial_Duration_Days_Nbr,'-1') <> NVL(tgt.Trial_Duration_Days_Nbr,'-1')
									 OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND 
									 OR NVL(src.Cancellation_Grace_Period_Nbr,'-1')<>NVL(tgt.Cancellation_Grace_Period_Nbr,'-1')
                                     OR NVL(to_date(src.Discount_Plan_End_Dt),'9999-12-31') <> NVL(tgt.Discount_Plan_End_Dt,'9999-12-31')
                                     OR NVL(to_date(src.Discount_Plan_Start_Dt),'9999-12-31') <> NVL(tgt.Discount_Plan_Start_Dt,'9999-12-31')
								     OR NVL(to_boolean(src.Discounted_Price_Ind),-1) <> NVL(tgt.Discounted_Price_Ind,-1)
									 OR NVL(to_boolean(src.Extended_Trial_Duration_Ind),-1) <> NVL(tgt.Extended_Trial_Duration_Ind,-1) 
									 )`;   

try {
snowflake.execute ({ sqlText: create_tgt_wrk_table });
	}
    catch (err) {
        return `Creation of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
				}          
            
 
// Transaction for Updates, Insert begins           
    var sql_begin = "BEGIN"
// SCD Type2 - Processing Different day updates
var sql_updates = `UPDATE ${tgt_tbl} as tgt
					SET 
					DW_Last_Effective_dt = CURRENT_DATE - 1,
					DW_CURRENT_VERSION_IND = FALSE,
					//DW_LOGICAL_DELETE_IND=TRUE,
					DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
					DW_SOURCE_UPDATE_NM = filename
				
					FROM ( 
							SELECT 
								    Subscription_Plan_Type_Nm
									,Subscription_Plan_Cd
									,Subscription_Type_Nm
									,Discount_Type_Dsc
									,Currency_Cd
									,filename
							FROM ${tgt_wrk_tbl}
							WHERE 
							DML_Type = 'U' 
							AND Sameday_chg_ind = 0
					) src
					WHERE
					    NVL(src.Subscription_Plan_Type_Nm,'-1') = NVL(tgt.Subscription_Plan_Type_Nm,'-1')
						AND NVL(src.Subscription_Plan_Cd,'-1') = NVL(tgt.Subscription_Plan_Cd,'-1')				
					    AND NVL(src.Subscription_Type_Nm,'-1') = NVL(tgt.Subscription_Type_Nm,'-1')
						AND NVL(src.Discount_Type_Dsc,'-1') = NVL(tgt.Discount_Type_Dsc,'-1')
						AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
                   
 // SCD Type1 - Processing Sameday updates
var sql_sameday = `UPDATE ${tgt_tbl} as tgt
					SET 
					Subscription_Plan_Type_Nm = src.Subscription_Plan_Type_Nm,
					Subscription_Plan_Cd = src.Subscription_Plan_Cd,
					Subscription_Type_Nm=src.Subscription_Type_Nm,
					Discount_Type_Dsc = src.Discount_Type_Dsc,
					Currency_Cd = src.Currency_Cd,
					Discount_Duration_Days_Nbr = src.Discount_Duration_Days_Nbr,
					Discount_End_Dt = src.Discount_End_Dt,
					Regular_Plan_Price_Amt = src.Regular_Plan_Price_Amt,
					Subscription_Price_Amt = src.Subscription_Price_Amt,
					Trial_Duration_Days_Nbr = src.Trial_Duration_Days_Nbr,
					Discount_Plan_End_Dt=src.Discount_Plan_End_Dt,
					Discount_Plan_Start_Dt=src.Discount_Plan_Start_Dt,
					Discounted_Price_Ind=src.Discounted_Price_Ind,
					Extended_Trial_Duration_Ind=src.Extended_Trial_Duration_Ind,
					Cancellation_Grace_Period_Nbr=src.Cancellation_Grace_Period_Nbr,
					DW_LOGICAL_DELETE_IND  =  src.DW_LOGICAL_DELETE_IND,  
					DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
					DW_SOURCE_UPDATE_NM = filename
					FROM ( 
							SELECT
								    Subscription_Plan_Type_Nm
									,Subscription_Plan_Cd
									,Subscription_Type_Nm
									,Discount_Type_Dsc
									,Currency_Cd
									,Discount_Duration_Days_Nbr
									,Discount_End_Dt
									,Regular_Plan_Price_Amt
									,Subscription_Price_Amt
									,Trial_Duration_Days_Nbr
									,Cancellation_Grace_Period_Nbr
									,Discount_Plan_End_Dt
									,Discount_Plan_Start_Dt
									,Discounted_Price_Ind
									,Extended_Trial_Duration_Ind
									,filename
									,DW_LOGICAL_DELETE_IND
							FROM ${tgt_wrk_tbl}
							WHERE 
							DML_Type = 'U' 
							AND Sameday_chg_ind = 1
						) src WHERE
							NVL(src.Subscription_Plan_Type_Nm,'-1') = NVL(tgt.Subscription_Plan_Type_Nm,'-1') 
							AND NVL(src.Subscription_Plan_Cd,'-1') = NVL(tgt.Subscription_Plan_Cd,'-1')
							AND NVL(src.Subscription_Type_Nm,'-1') = NVL(tgt.Subscription_Type_Nm,'-1')
							AND NVL(src.Discount_Type_Dsc,'-1') = NVL(tgt.Discount_Type_Dsc,'-1')
					        AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
					
// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl} 
					(
					Subscription_Plan_Integration_Id
					,Dw_First_Effective_Dt
					,Dw_Last_Effective_Dt
					,Subscription_Plan_Type_Nm
					,Subscription_Plan_Cd
					,Subscription_Type_Nm
					,Discount_Type_Dsc
					,Currency_Cd
					,Discount_Duration_Days_Nbr
					,Discount_End_Dt
					,Regular_Plan_Price_Amt
					,Subscription_Price_Amt
					,Trial_Duration_Days_Nbr
					,Cancellation_Grace_Period_Nbr
					,Discount_Plan_End_Dt
					,Discount_Plan_Start_Dt
					,Discounted_Price_Ind
					,Extended_Trial_Duration_Ind
					,DW_CREATE_TS
					,DW_LOGICAL_DELETE_IND
					,DW_SOURCE_CREATE_NM 
					,DW_CURRENT_VERSION_IND
					)
					SELECT
					(SELECT nvl(MAX(Subscription_Plan_Integration_Id),0) FROM ${tgt_tbl}) +
					ROW_NUMBER() OVER (ORDER BY Subscription_Plan_Type_Nm,Subscription_Plan_Cd,Subscription_Type_Nm,
												Discount_Type_Dsc ASC) AS Subscription_Plan_Integration_Id
							,CURRENT_DATE
							,'31-DEC-9999'
							,Subscription_Plan_Type_Nm
							,Subscription_Plan_Cd
							,Subscription_Type_Nm
							,Discount_Type_Dsc
							,Currency_Cd
							,Discount_Duration_Days_Nbr
							,Discount_End_Dt
							,Regular_Plan_Price_Amt
							,Subscription_Price_Amt
							,Trial_Duration_Days_Nbr
							,Cancellation_Grace_Period_Nbr
							,Discount_Plan_End_Dt
							,Discount_Plan_Start_Dt
							,Discounted_Price_Ind
							,Extended_Trial_Duration_Ind
							,CURRENT_TIMESTAMP
							,DW_LOGICAL_DELETE_IND
							,filename 
							,TRUE
							FROM ${tgt_wrk_tbl}
							WHERE 
							Sameday_chg_ind = 0`;

    
var sql_commit = "COMMIT";
var sql_rollback = "ROLLBACK";
    
try {
        snowflake.execute ({ sqlText: sql_begin });
        snowflake.execute ({ sqlText: sql_updates });
        snowflake.execute ({ sqlText: sql_sameday });
        snowflake.execute ({ sqlText: sql_inserts });
        snowflake.execute ({ sqlText: sql_commit });
	} catch (err) {
        snowflake.execute ({ sqlText: sql_rollback });
		return `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
					}		
                               
                // **************        Load for Freshpass_Subscription_Plan Table ENDs *****************
			

$$;

--liquibase formatted sql
--changeset SYSTEM:SP_GETFRESHPASS_SUBSCRIPTION_TO_BIM_LOAD_BENEFIT runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETFRESHPASS_SUBSCRIPTION_TO_BIM_LOAD_BENEFIT(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_LOYAL VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

		var src_wrk_tbl = SRC_WRK_TBL;
		var cnf_schema = C_LOYAL;
		var tgt_wrk_tbl = `${CNF_DB}.${C_STAGE}.FRESHPASS_SUBSCRIPTION_BENEFIT_WRK`;
		var tgt_tbl = `${CNF_DB}.${cnf_schema}.FRESHPASS_SUBSCRIPTION_BENEFIT`;
		
		// **************        Truncate and Reload the work table *****************

    var truncate_tgt_wrk_table = `DELETE from ${tgt_wrk_tbl}`;
try {
snowflake.execute ({ sqlText: truncate_tgt_wrk_table });
	}
    catch (err) {
        return `Deletion of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
				}

                       
    // **************        Load for FRESHPASS_SUBSCRIPTION_BENEFIT table BEGIN *****************
    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
	var create_tgt_wrk_table = `INSERT INTO ${tgt_wrk_tbl}
								SELECT DISTINCT
								src.Retail_Customer_Uuid
								,src.Event_Id 
								,src.Subscription_Plan_Integration_Id
								,src.Benefit_Type_Dsc
								,src.filename
								,src.DW_LOGICAL_DELETE_IND
                                ,CASE 
								    WHEN (
										     tgt.Retail_Customer_Uuid IS NULL 
										AND  tgt.Event_Id is NULL 
										--AND  tgt.Subscription_Plan_Integration_Id is NULL 
										AND  tgt.Benefit_Type_Dsc is NULL
								         ) 
									THEN 'I' 
									ELSE 'U' 
								END AS DML_Type
								,CASE   
									WHEN tgt.DW_First_Effective_dt = CURRENT_DATE 
									THEN 1 
									Else 0 
								END as Sameday_chg_ind
								
										FROM 
									 (
										SELECT 
										 Retail_Customer_Uuid
										,Event_Id
										,Subscription_Plan_Integration_Id
										,Benefit_Type_Dsc
										,filename
										,DW_CREATETS
										,DW_LOGICAL_DELETE_IND
										FROM ( 
											   SELECT
												 Retail_Customer_Uuid 
												,Event_Id 
												,Subscription_Plan_Integration_Id
												,Benefit_Type_Dsc
												,filename
												,DW_CREATETS
												,false as  DW_LOGICAL_DELETE_IND
												,Row_number() OVER (
											    PARTITION BY Retail_Customer_Uuid,Event_Id,Subscription_Plan_Integration_Id,Benefit_Type_Dsc
											    ORDER BY(event_ts) DESC) as rn
											  FROM(
                                                    SELECT
													E.Subscription_Plan_Integration_Id
													,S.Retail_Customer_Uuid 
													,S.Event_Id 
													,Benefit_Type_Dsc
													,filename
													,DW_CREATETS
													,event_ts
													FROM
													 (
													 (
													 SELECT  
													 Customer_UUID as Retail_Customer_Uuid
													 ,eventId  as Event_Id
													 ,benefits_values as Benefit_Type_Dsc
													 ,subscriptioncode
													 ,subscriptiontype
													 ,discounttype
													 ,Subscription_plan_type
													 ,filename
													 ,DW_CREATETS
													 ,EVENTTIME as event_ts
													 FROM ${src_wrk_tbl} 
													 where benefits_values is not null
													 and Customer_UUID is not null
													 and eventId is not null
													 ) S
													 LEFT JOIN 
													 (
													 SELECT DISTINCT Subscription_Plan_Integration_Id,Retail_Customer_Uuid,Event_Id
													 FROM ${CNF_DB}.${cnf_schema}.Freshpass_Subscription_Event WHERE DW_CURRENT_VERSION_IND=TRUE AND DW_LOGICAL_DELETE_IND=FALSE
													 ) E
													 ON NVL(S.Retail_Customer_Uuid,'-1')=NVL(E.Retail_Customer_Uuid,'-1')
													 AND NVL(S.Event_Id,'-1')=NVL(E.Event_Id,'-1')
													 )
													 )
												) where rn=1	 
											) src
									 
									LEFT JOIN
									( 
									SELECT  DISTINCT
											 Retail_Customer_Uuid 
											,Event_Id 
											,Subscription_Plan_Integration_Id
											,Benefit_Type_Dsc
											,DW_First_Effective_dt
											,DW_LOGICAL_DELETE_IND
									FROM  ${tgt_tbl} tgt
									WHERE DW_CURRENT_VERSION_IND = TRUE
									)as tgt 
									ON
									    NVL(src.Retail_Customer_Uuid ,'-1') = NVL(tgt.Retail_Customer_Uuid ,'-1')
									AND NVL(src.Event_Id,'-1') = NVL(tgt.Event_Id ,'-1')
									--AND NVL(src.Subscription_Plan_Integration_Id,'-1') = NVL(tgt.Subscription_Plan_Integration_Id,'-1')
									AND  NVL(src.Benefit_Type_Dsc,'-1') = NVL(tgt.Benefit_Type_Dsc,'-1')
									WHERE  (
									tgt.Retail_Customer_Uuid IS  NULL
									AND tgt.Event_Id is  NULL
									--AND tgt.Subscription_Plan_Integration_Id is NULL
									AND tgt.Benefit_Type_Dsc is NULL
									 )
									OR
									(
									 NVL(src.Subscription_Plan_Integration_Id,'-1') <> NVL(tgt.Subscription_Plan_Integration_Id,'-1')
									 
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
								 Retail_Customer_Uuid
								,Event_Id
								,Subscription_Plan_Integration_Id 
								,Benefit_Type_Dsc
								,filename
							FROM ${tgt_wrk_tbl}
							WHERE 
							DML_Type = 'U' 
							// AND Sameday_chg_ind = 0
					) src
					WHERE
					--NVL(src.Subscription_Plan_Integration_Id,'-1')= NVL(tgt.Subscription_Plan_Integration_Id,'-1') AND
					NVL(src.Retail_Customer_Uuid,'-1')= NVL(tgt.Retail_Customer_Uuid,'-1')
					AND NVL(src.Event_Id,'-1')= NVL(tgt.Event_Id,'-1')	
					AND  NVL(src.Benefit_Type_Dsc,'-1') = NVL(tgt.Benefit_Type_Dsc,'-1')					
					AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
                   
 /* // SCD Type1 - Processing Sameday updates
var sql_sameday = `UPDATE ${tgt_tbl} as tgt
					SET Retail_Customer_Uuid = src.Retail_Customer_Uuid,
					Event_Id = src.Event_Id,
					Subscription_Plan_Integration_Id = src.Subscription_Plan_Integration_Id,
					Benefit_Type_Dsc=src.Benefit_Type_Dsc,
					DW_Logical_delete_ind = src.DW_Logical_delete_ind,
					DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
					DW_SOURCE_UPDATE_NM = filename
					FROM ( 
							SELECT
								Retail_Customer_Uuid
								,Event_Id
								,Subscription_Plan_Integration_Id 
								,Benefit_Type_Dsc
								,filename
								,DW_Logical_delete_ind
							FROM ${tgt_wrk_tbl}
							WHERE 
							DML_Type = 'U' 
							AND Sameday_chg_ind = 1
						) src 
						WHERE
						    NVL(src.Subscription_Plan_Integration_Id,'-1')= NVL(tgt.Subscription_Plan_Integration_Id,'-1') AND
							NVL(src.Retail_Customer_Uuid,'-1')= NVL(tgt.Retail_Customer_Uuid,'-1')
							AND NVL(src.Event_Id,'-1')= NVL(tgt.Event_Id,'-1')
							AND  NVL(src.Benefit_Type_Dsc,'-1') = NVL(tgt.Benefit_Type_Dsc,'-1')
							AND tgt.DW_CURRENT_VERSION_IND = TRUE`;  */
// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl} 
					(
					Subscription_Plan_Integration_Id
					,Retail_Customer_Uuid
					,Event_Id 
					,Benefit_Type_Dsc
					,DW_First_Effective_Dt
					,DW_Last_Effective_Dt
					,DW_CREATE_TS
					,DW_LOGICAL_DELETE_IND
					,DW_SOURCE_CREATE_NM
					,DW_CURRENT_VERSION_IND
					)
					SELECT
					Subscription_Plan_Integration_Id
					,Retail_Customer_Uuid 
					,Event_Id 
					,Benefit_Type_Dsc
					,CURRENT_DATE
					,'31-DEC-9999'
					,CURRENT_TIMESTAMP
					,DW_LOGICAL_DELETE_IND
					,filename
					,TRUE 
					FROM ${tgt_wrk_tbl}
					WHERE 
					// Sameday_chg_ind = 0  and
					 Subscription_Plan_Integration_Id is not null`;

    
var sql_commit = "COMMIT";
var sql_rollback = "ROLLBACK";
    
try {
        snowflake.execute ({ sqlText: sql_begin });
        snowflake.execute ({ sqlText: sql_updates });
       // snowflake.execute ({ sqlText: sql_sameday });
        snowflake.execute ({ sqlText: sql_inserts });
        snowflake.execute ({ sqlText: sql_commit });
	} catch (err) {
        snowflake.execute ({ sqlText: sql_rollback });
		return `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
					}		
                               
                // **************        Load for FRESHPASS_SUBSCRIPTION_BENEFIT Table ENDs *****************
				

$$;

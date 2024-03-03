--liquibase formatted sql
--changeset SYSTEM:SP_GETEPISODIC_TO_BIM_LOAD_EPISODIC_PRIZE runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETEPISODIC_TO_BIM_LOAD_EPISODIC_PRIZE(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_LOYAL VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$


var src_wrk_tbl = SRC_WRK_TBL;
var temp_wrk_tbl = `${CNF_DB}.${C_STAGE}.EPISODIC_PRIZE_tmp_WRK`;
var cnf_schema = C_LOYAL;
var tgt_wrk_tbl = `${CNF_DB}.${C_STAGE}.EPISODIC_PRIZE_WRK`;
var tgt_tbl = `${CNF_DB}.${cnf_schema}.EPISODIC_PRIZE`;
var src_rerun_tbl = `${CNF_DB}.${C_STAGE}.EPISODIC_PRIZE_FLAT_RERUN `;

// Empty the rerun queue table
var sql_empty_rerun_tbl = `TRUNCATE TABLE ${src_rerun_tbl}`;


//query to load rerun queue table when encountered a failure
var sql_ins_rerun_tbl = `INSERT INTO ${src_rerun_tbl} SELECT * FROM ${temp_wrk_tbl}`;


// persist stream data in work table for the current transaction, includes data from previous failed run

var sql_empty_tmp_wrk_tbl = `TRUNCATE TABLE `+ temp_wrk_tbl +``;

var sql_crt_src_wrk_tbl = `INSERT INTO `+ temp_wrk_tbl +` 
SELECT * FROM `+ src_wrk_tbl +`
UNION ALL
SELECT * FROM `+ src_rerun_tbl+``;

try {
snowflake.execute ({sqlText: sql_empty_tmp_wrk_tbl});
snowflake.execute ({sqlText: sql_crt_src_wrk_tbl });
}
catch (err) {
throw "Creation of Source Work table "+ temp_wrk_tbl +" Failed with error: " + err; // Return a error message.
}
		


                       
    // **************        Load for EPISODIC_PRIZE table BEGIN *****************
    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
	
	var empty_tgt_wrk_table = `TRUNCATE TABLE ${tgt_wrk_tbl}`;
	
	var create_tgt_wrk_table = `INSERT INTO ${tgt_wrk_tbl}
										SELECT DISTINCT 
										src.PRIZE_UUID
										,src.PRIZE_DSC
										,src.RETAIL_VALUE_AMT
										,src.TOTAL_QTY
										,src.DELIVERY_TYPE_CD
										,src.LAST_UPDATED_TS
										,src.PROGRAM_ID
										,src.EXTRACT_TS
										,src.DW_CREATE_TS
										,src.FILENAME
										,src.DW_LOGICAL_DELETE_IND
										,CASE 
										 WHEN (
													 tgt.PRIZE_UUID IS NULL
         										 AND tgt.PROGRAM_ID IS NULL
												 ) 										
										THEN 'I' 
										ELSE 'U' 
										END AS DML_Type
										,CASE   
										WHEN tgt.DW_First_Effective_dt = CURRENT_DATE 
										THEN 1 
										Else 0 
										END as Sameday_chg_ind
										From
										(
										  SELECT
											 PRIZE_UUID
											,PRIZE_DSC
											,RETAIL_VALUE_AMT
											,TOTAL_QTY
											,DELIVERY_TYPE_CD
											,LAST_UPDATED_TS
											,PROGRAM_ID
											,EXTRACT_TS
											,DW_CREATE_TS
											,FILENAME
											,DW_LOGICAL_DELETE_IND
										    FROM 
										        (
													SELECT
														PRIZE_UUID
														,PRIZE_DSC
														,RETAIL_VALUE_AMT
														,TOTAL_QTY
														,DELIVERY_TYPE_CD
														,LAST_UPDATED_TS
														,PROGRAM_ID
														,EXTRACT_TS
														,DW_CREATE_TS
														,FILENAME
													  ,false as  DW_LOGICAL_DELETE_IND
													  ,Row_number() OVER (PARTITION BY PRIZE_UUID,PROGRAM_ID ORDER BY to_timestamp_ntz(Extract_Ts) DESC) as rn
													From 
														(
															  SELECT
																 PRIZE_UUID
																,PRIZE_DSC
																,RETAIL_VALUE_AMT
																,TOTAL_QTY
																,DELIVERY_TYPE_CD
																,LAST_UPDATED_TS
																,PROGRAM_ID
																,EXTRACT_TS
																,DW_CREATE_TS
																,FILENAME
															 FROM
														         (
																	SELECT 
																	   PRIZE_UUID
																	  ,PRIZE_DESCRIPTION as PRIZE_DSC
																	  ,RETAIL_VALUE as RETAIL_VALUE_AMT
																	  ,TOTAL_QTY
																	  ,DELIVERY_TYPE as DELIVERY_TYPE_CD
																	  ,to_timestamp_ntz(LAST_UPDATED) as LAST_UPDATED_TS
																	  ,PROGRAM_ID
																	  ,to_timestamp_ntz(Extract_Ts) as EXTRACT_TS
																	  ,DW_CREATE_TS
																	  ,FILE_NAME as FILENAME
																	FROM 
																	 ${temp_wrk_tbl} S
																  )
											            )
										        ) 
										  WHERE rn=1
										  AND PRIZE_UUID IS NOT NULL
										  AND PROGRAM_ID IS NOT NULL
										) src
										 
									 LEFT JOIN
										(
											   SELECT
												 PRIZE_UUID
												,EXTRACT_TS
												,DW_CREATE_TS
												,DW_LOGICAL_DELETE_IND
												,DW_FIRST_EFFECTIVE_DT
												,PROGRAM_ID
												,PRIZE_DSC
												,RETAIL_VALUE_AMT
												,TOTAL_QTY
												,DELIVERY_TYPE_CD
												,LAST_UPDATED_TS
											  From ${tgt_tbl} tgt
											  WHERE  DW_CURRENT_VERSION_IND = TRUE

										) as tgt
										ON 
										  nvl(src.PRIZE_UUID, '-1') = nvl(tgt.PRIZE_UUID, '-1')
										  AND  nvl(src.PROGRAM_ID, '-1') = nvl (tgt.PROGRAM_ID, '-1')
										WHERE
										     (
												tgt.PRIZE_UUID IS  NULL
												AND tgt.PROGRAM_ID IS NULL
											 )
										OR
										   (
												 NVL(src.PRIZE_UUID,'-1') <> NVL(tgt.PRIZE_UUID,'-1')
												OR NVL(src.PRIZE_DSC,'-1') <> NVL(tgt.PRIZE_DSC,'-1')
												OR NVL(src.RETAIL_VALUE_AMT,'-1') <> NVL(tgt.RETAIL_VALUE_AMT,'-1')
												OR NVL(src.TOTAL_QTY,'-1') <> NVL(tgt.TOTAL_QTY,'-1')
												OR NVL(src.DELIVERY_TYPE_CD,'-1') <> NVL(tgt.DELIVERY_TYPE_CD,'-1')
												OR NVL(src.PROGRAM_ID,'-1') <> NVL(tgt.PROGRAM_ID,'-1')
												OR NVL(src.LAST_UPDATED_TS,'9999-12-31 00:00:00.000') <> NVL(tgt.LAST_UPDATED_TS,'9999-12-31 00:00:00.000')
												OR NVL(src.EXTRACT_TS,'9999-12-31 00:00:00.000') <> NVL(tgt.EXTRACT_TS,'9999-12-31 00:00:00.000')
											)`;
                                         
                                                                                        
try {
snowflake.execute ({sqlText: empty_tgt_wrk_table});                                       
snowflake.execute ({ sqlText: create_tgt_wrk_table });}
    catch (err) {
	
snowflake.execute ({ sqlText: sql_ins_rerun_tbl});

        throw `Creation of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
				}      
				
// Transaction for Updates, Insert begins           
  var sql_begin = "BEGIN"
// SCD Type2 - Processing Different day updates

var sql_updates = `UPDATE ${tgt_tbl} as tgt
					SET 
					DW_Last_Effective_dt = CURRENT_DATE - 1,
					DW_CURRENT_VERSION_IND = FALSE,
					DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
					DW_SOURCE_UPDATE_NM = filename
				
					FROM ( SELECT 
                            PRIZE_UUID
                            ,PROGRAM_ID
                            ,filename
							FROM ${tgt_wrk_tbl}
							WHERE 
							DML_Type = 'U'
							AND Sameday_chg_ind = 0
                      )src
					WHERE 
                    nvl(src.PRIZE_UUID, '-1') = nvl(tgt.PRIZE_UUID, '-1')
					AND nvl(src.PROGRAM_ID, '-1') = nvl(tgt.PROGRAM_ID, '-1')
                    AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
                    
                    
  // SCD Type1 - Processing Sameday updates
var sql_sameday = `UPDATE ${tgt_tbl} as tgt
                                          SET 
                                           PRIZE_UUID = src.PRIZE_UUID
                                          ,PRIZE_DSC = src.PRIZE_DSC
                                          ,RETAIL_VALUE_AMT = src.RETAIL_VALUE_AMT
                                          ,TOTAL_QTY = src.TOTAL_QTY
                                          ,DELIVERY_TYPE_CD = src.DELIVERY_TYPE_CD
                                          ,PROGRAM_ID = src.PROGRAM_ID
                                          ,EXTRACT_TS = src.EXTRACT_TS
                                          ,LAST_UPDATED_TS = src.LAST_UPDATED_TS
                                          ,DW_Logical_delete_ind = src.DW_Logical_delete_ind
					                      ,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
				                          ,DW_SOURCE_UPDATE_NM = filename
                                          

                                          FROM ( 
							                 SELECT 
                                             PRIZE_UUID
                                            ,PRIZE_DSC
                                            ,RETAIL_VALUE_AMT
                                            ,TOTAL_QTY
                                            ,DELIVERY_TYPE_CD
                                            ,PROGRAM_ID
                                            ,EXTRACT_TS
                                            ,LAST_UPDATED_TS
                                            ,FILENAME
                                            ,DW_LOGICAL_DELETE_IND
                                            FROM ${tgt_wrk_tbl} 
							                WHERE 
							                DML_Type = 'U' 
							                AND Sameday_chg_ind = 1
                                            )src
                                            WHERE 
                                             nvl(src.PRIZE_UUID, '-1') = nvl(tgt.PRIZE_UUID, '-1')
											AND nvl(src.PROGRAM_ID, '-1') = nvl(tgt.PROGRAM_ID, '-1')
                                            AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
                                            
  // Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl} 
					(
                     PRIZE_UUID
                    ,EXTRACT_TS
                    ,DW_CREATE_TS
					,DW_LOGICAL_DELETE_IND
					,DW_SOURCE_CREATE_NM
					,DW_CURRENT_VERSION_IND
                    ,DW_FIRST_EFFECTIVE_DT
                    ,DW_LAST_EFFECTIVE_DT
                    ,PROGRAM_ID
                    ,PRIZE_DSC
                    ,RETAIL_VALUE_AMT
                    ,TOTAL_QTY
                    ,DELIVERY_TYPE_CD
                    ,LAST_UPDATED_TS 
                     )
                     SELECT
                     PRIZE_UUID
                    ,EXTRACT_TS
                    ,CURRENT_TIMESTAMP
                    ,DW_LOGICAL_DELETE_IND
                    ,filename
                    ,TRUE 
                    ,CURRENT_DATE
                    ,'31-DEC-9999'
                    ,PROGRAM_ID
                    ,PRIZE_DSC
                    ,RETAIL_VALUE_AMT
                    ,TOTAL_QTY
                    ,DELIVERY_TYPE_CD
                    ,LAST_UPDATED_TS 
                    FROM ${tgt_wrk_tbl}
					WHERE 
					Sameday_chg_ind = 0`;
 
 
 
 
var sql_commit = "COMMIT";
var sql_rollback = "ROLLBACK";
    
try {
        snowflake.execute ({ sqlText: sql_begin });
		snowflake.execute({ sqlText: sql_empty_rerun_tbl });
        snowflake.execute ({ sqlText: sql_updates });
        snowflake.execute ({ sqlText: sql_sameday });
        snowflake.execute ({ sqlText: sql_inserts });
        snowflake.execute ({ sqlText: sql_commit });
	} catch (err) {
        snowflake.execute ({ sqlText: sql_rollback });
		
		snowflake.execute ({ sqlText: sql_ins_rerun_tbl});
		
		
		throw `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
					}		
                               
                // **************        Load for EPISODIC_PRIZE Table ENDs *****************

 
$$;

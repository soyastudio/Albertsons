--liquibase formatted sql
--changeset SYSTEM:SP_OCGP_SWEEPSTAKE_WINNER_TO_BIM_LOAD runOnChange:true splitStatements:false OBJECT_TYPE:SP

Use database <<EDM_DB_NAME>>;
Use schema DW_APPL;

CREATE OR REPLACE PROCEDURE SP_OCGP_SWEEPSTAKE_WINNER_TO_BIM_LOAD(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_LOYAL VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS 
$$
// ************** Load for OCGP_SWEEPSTAKE_WINNER table BEGIN *****************
	var src_wrk_tbl = SRC_WRK_TBL;
	var temp_wrk_tbl = `${CNF_DB}.${C_STAGE}.OCGP_SWEEPSTAKE_WINNER_TMP_WRK`;
	var cnf_schema = C_LOYAL;
	var tgt_wrk_tbl = `${CNF_DB}.${C_STAGE}.OCGP_SWEEPSTAKE_WINNER_WRK`;
	var tgt_tbl = `${CNF_DB}.${cnf_schema}.OCGP_SWEEPSTAKE_WINNER`;
	var src_rerun_tbl = `${CNF_DB}.${C_STAGE}.OCGP_SWEEPSTAKE_WINNER_FLAT_RERUN`;

//query to empty the rerun queue table
	var sql_empty_rerun_tbl = `TRUNCATE TABLE ${src_rerun_tbl}`;

//query to load rerun queue table when encountered a failure
	var sql_ins_rerun_tbl = `INSERT INTO ${src_rerun_tbl} SELECT * FROM ${temp_wrk_tbl}`;

// ***************** Truncate the temp work table ********************
	var truncate_temp_wrk_table = `Truncate Table ${temp_wrk_tbl}`;
	try {
		snowflake.execute ({ sqlText: truncate_temp_wrk_table });
	}
	catch (err) {
		return `Truncation of work table ${temp_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
	}

// persist stream data in work table for the current transaction, includes data from previous failed run
	var sql_crt_src_wrk_tbl = `INSERT INTO `+ temp_wrk_tbl +` 
	SELECT PRIZE_ID,PROGRAM_CD,RETAIL_CUSTOMER_UUID,SWEEPSTAKE_DRAW_DT,DISPLAY_NOTIFICATION_IND,CLAIM_EXPIRATION_DT,WINNING_DETAIL_TXT,
	WIN_STATUS_CD,WINNER_EMAIL_ADDRESS_TXT,FILENAME,DW_CREATETS,METADATA$ACTION,METADATA$ISUPDATE,METADATA$ROW_ID FROM `+ src_wrk_tbl +`
	UNION ALL
	SELECT PRIZE_ID,PROGRAM_CD,RETAIL_CUSTOMER_UUID,SWEEPSTAKE_DRAW_DT,DISPLAY_NOTIFICATION_IND,CLAIM_EXPIRATION_DT,WINNING_DETAIL_TXT,
	WIN_STATUS_CD,WINNER_EMAIL_ADDRESS_TXT,FILENAME,DW_CREATETS,METADATA$ACTION,METADATA$ISUPDATE,METADATA$ROW_ID FROM `+ src_rerun_tbl +``;
	try {
		snowflake.execute ({sqlText: sql_crt_src_wrk_tbl });
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
	
// *********************** Load for Meal_Plan_Recipe table BEGIN *****************
// identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
	var create_tgt_wrk_table = `INSERT INTO ${tgt_wrk_tbl}
								SELECT DISTINCT 
								src.PRIZE_ID,
								src.PROGRAM_CD,
								src.RETAIL_CUSTOMER_UUID,
								TO_DATE(src.SWEEPSTAKE_DRAW_DT),
								TO_BOOLEAN(src.DISPLAY_NOTIFICATION_IND),
								TO_DATE(src.CLAIM_EXPIRATION_DT),
								src.WINNING_DETAIL_TXT,
								src.WIN_STATUS_CD,
								src.WINNER_EMAIL_ADDRESS_TXT,
								tgt.DW_CREATE_TS,
								src.FILENAME,
								src.DW_LOGICAL_DELETE_IND,
								case 
								when tgt.PROGRAM_CD is null then 'I'
								when tgt.PROGRAM_CD is not null and 
								((src.DISPLAY_NOTIFICATION_IND<>tgt.DISPLAY_NOTIFICATION_IND) or 
								(src.CLAIM_EXPIRATION_DT<>tgt.CLAIM_EXPIRATION_DT) or 
								(src.WINNING_DETAIL_TXT<>tgt.WINNING_DETAIL_TXT) or 
								(src.WIN_STATUS_CD<>tgt.WIN_STATUS_CD) or
								(src.WINNER_EMAIL_ADDRESS_TXT<>tgt.WINNER_EMAIL_ADDRESS_TXT)) then 'U'
								else 'R' end DML_TYPE
								FROM(
									SELECT
									PRIZE_ID,
									PROGRAM_CD,
									RETAIL_CUSTOMER_UUID,
									SWEEPSTAKE_DRAW_DT,
									DISPLAY_NOTIFICATION_IND,
									CLAIM_EXPIRATION_DT,
									WINNING_DETAIL_TXT,
									WIN_STATUS_CD,
									WINNER_EMAIL_ADDRESS_TXT,
									FILENAME,
									DW_CREATETS,
									DW_LOGICAL_DELETE_IND 
									FROM(
										SELECT 
										PRIZE_ID,
										PROGRAM_CD,
										RETAIL_CUSTOMER_UUID,
										SWEEPSTAKE_DRAW_DT,
										DISPLAY_NOTIFICATION_IND,
										CLAIM_EXPIRATION_DT,
										WINNING_DETAIL_TXT,
										WIN_STATUS_CD,
										WINNER_EMAIL_ADDRESS_TXT,
										FILENAME,
										DW_CREATETS,
										FALSE AS DW_LOGICAL_DELETE_IND,
										Row_number() OVER (
										PARTITION BY PRIZE_ID,PROGRAM_CD,RETAIL_CUSTOMER_UUID,SWEEPSTAKE_DRAW_DT
										order by DW_CREATETS DESC) as rn 
										FROM(
											SELECT
											PRIZE_ID,
											PROGRAM_CD,
											RETAIL_CUSTOMER_UUID,
											SWEEPSTAKE_DRAW_DT,
											DISPLAY_NOTIFICATION_IND,
											CLAIM_EXPIRATION_DT,
											WINNING_DETAIL_TXT,
											WIN_STATUS_CD,
											WINNER_EMAIL_ADDRESS_TXT,
											FILENAME,
											DW_CREATETS
											FROM(
												SELECT
												PRIZE_ID,
												PROGRAM_CD,
												RETAIL_CUSTOMER_UUID,
												SWEEPSTAKE_DRAW_DT,
												DISPLAY_NOTIFICATION_IND,
												CLAIM_EXPIRATION_DT,
												WINNING_DETAIL_TXT,
												WIN_STATUS_CD,
												WINNER_EMAIL_ADDRESS_TXT,
												FILENAME,
												DW_CREATETS
												FROM 
												${temp_wrk_tbl}  TWT
												)
                                            )
										)where rn=1
									)src
                                    left outer join ${tgt_tbl} TGT on 
                                    tgt.PROGRAM_CD = src.PROGRAM_CD
                                    and tgt.PRIZE_ID = src.PRIZE_ID 
				    and tgt.RETAIL_CUSTOMER_UUID = src.RETAIL_CUSTOMER_UUID
                                    and tgt.SWEEPSTAKE_DRAW_DT=src.SWEEPSTAKE_DRAW_DT
                                    and tgt.DW_CURRENT_VERSION_IND=True`;
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
							DW_CURRENT_VERSION_IND = FALSE,
							DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
							DW_LOGICAL_DELETE_IND = TRUE 
							from( 
								SELECT 
								PRIZE_ID,
								PROGRAM_CD,
								RETAIL_CUSTOMER_UUID,
								SWEEPSTAKE_DRAW_DT
								from  ${tgt_wrk_tbl} as b 
                                where DML_TYPE='U'
								) src where
							tgt.PROGRAM_CD = src.PROGRAM_CD
						    and tgt.PRIZE_ID = src.PRIZE_ID 
						    and tgt.RETAIL_CUSTOMER_UUID = src.RETAIL_CUSTOMER_UUID
						    and tgt.SWEEPSTAKE_DRAW_DT=src.SWEEPSTAKE_DRAW_DT
							and tgt.DW_CURRENT_VERSION_IND=TRUE`;
 



		
	// Processing Inserts
	var sql_inserts = `INSERT INTO ${tgt_tbl} 
						(
						PRIZE_ID,
						PROGRAM_CD,
						RETAIL_CUSTOMER_UUID,
						SWEEPSTAKE_DRAW_DT,
						DW_FIRST_EFFECTIVE_DT,
						DW_LAST_EFFECTIVE_DT,
						DISPLAY_NOTIFICATION_IND,
						CLAIM_EXPIRATION_DT,
						WINNING_DETAIL_TXT,
						WIN_STATUS_CD,
						WINNER_EMAIL_ADDRESS_TXT,
						DW_CREATE_TS,
						DW_LAST_UPDATE_TS,
						DW_LOGICAL_DELETE_IND,
						DW_SOURCE_CREATE_NM,
						DW_SOURCE_UPDATE_NM,
						DW_CURRENT_VERSION_IND          
						)
						SELECT 
						PRIZE_ID,
						PROGRAM_CD,
						RETAIL_CUSTOMER_UUID,
						SWEEPSTAKE_DRAW_DT,
						CURRENT_DATE ,           
						'31-DEC-9999' ,            
						DISPLAY_NOTIFICATION_IND,
						CLAIM_EXPIRATION_DT,
						WINNING_DETAIL_TXT,
						WIN_STATUS_CD,
						WINNER_EMAIL_ADDRESS_TXT,             
						COALESCE(DW_CREATETS,CURRENT_TIMESTAMP) as DW_CREATE_TS,                   
						CURRENT_TIMESTAMP,               
						DW_LOGICAL_DELETE_IND ,           
						Filename,            
						NULL,
						TRUE
						FROM ${tgt_wrk_tbl}
						WHERE
						DML_TYPE in ('I','U')`;
    
	var sql_commit = "COMMIT";
	var sql_rollback = "ROLLBACK";
    
	try {
        snowflake.execute ({ sqlText: sql_begin });
		snowflake.execute ({ sqlText: sql_empty_rerun_tbl });
		snowflake.execute ({ sqlText: sql_Delete_Check });
        //snowflake.execute ({ sqlText: sql_delete });
        //return sql_inserts
        snowflake.execute ({ sqlText: sql_inserts });
        snowflake.execute ({ sqlText: sql_commit });
		} 
	catch (err) {
				snowflake.execute ({ sqlText: sql_rollback });
				snowflake.execute ({ sqlText: sql_ins_rerun_tbl});
				return `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
				}	
                               
// ************** Load for OCGP_SWEEPSTAKE_WINNER table BEGIN *****************
				
$$;	



 

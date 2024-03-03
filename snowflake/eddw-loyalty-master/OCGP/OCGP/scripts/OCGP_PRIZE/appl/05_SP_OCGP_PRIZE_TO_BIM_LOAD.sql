--liquibase formatted sql
--changeset SYSTEM:SP_OCGP_PRIZE_TO_BIM_LOAD runOnChange:true splitStatements:false OBJECT_TYPE:SP

Use database <<EDM_DB_NAME>>;
Use schema DW_APPL;

CREATE OR REPLACE PROCEDURE SP_OCGP_PRIZE_TO_BIM_LOAD(SRC_WRK_TBL_PRIZE VARCHAR, SRC_WRK_TBL_INVENTORY_STATUS VARCHAR, CNF_DB VARCHAR, C_LOYAL VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS 
$$
// ************** Load for OCGP_PRIZE table BEGIN *****************
	var src_wrk_tbl_p = SRC_WRK_TBL_PRIZE;
	var src_wrk_tbl_is = SRC_WRK_TBL_INVENTORY_STATUS;
	var temp_wrk_tbl = `${CNF_DB}.${C_STAGE}.OCGP_PRIZE_TMP_WRK`;
	var cnf_schema = C_LOYAL;
	var tgt_wrk_tbl = `${CNF_DB}.${C_STAGE}.OCGP_PRIZE_WRK`;
	var tgt_tbl = `${CNF_DB}.${cnf_schema}.OCGP_PRIZE`;
	var src_rerun_tbl = `${CNF_DB}.${C_STAGE}.OCGP_PRIZE_FLAT_RERUN`;

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
	var sql_crt_src_wrk_tbl = 	`INSERT INTO `+ temp_wrk_tbl +` 
								SELECT swtp.PRIZE_ID,swtp.PROGRAM_CD,swtp.PRIZE_NM,swtp.PRIZE_DSC,swtp.PRIZE_TYPE_CD,swtp.VENDOR_NM,swtp.PRIZE_EXPIRATION_DT,
								swtp.EARN_POINTS_QTY,swtp.BURN_POINTS_QTY,swtp.PRIZE_RANKING_NBR,swtp.SWEEPSTAKE_DRAW_DT,swtp.DISCLAIMER_TXT,
								swtp.DIGITAL_PRIZE_IND,swtp.BURN_PROGRAM_NM,swtp.EARN_PROGRAM_NM,swtp.PRIZE_DETAIL_TXT,swtis.INITIAL_STOCK_QTY,
								swtis.AVAILABLE_STOCK_QTY,swtp.FILENAME,swtp.DW_CREATETS,swtp.METADATA$ACTION,swtp.METADATA$ISUPDATE,swtp.METADATA$ROW_ID 
								FROM `+ src_wrk_tbl_p +` swtp
								JOIN `+ src_wrk_tbl_is +` swtis on swtp.PRIZE_ID = swtis.PRIZE_ID
								UNION ALL
								SELECT PRIZE_ID,PROGRAM_CD,PRIZE_NM,PRIZE_DSC,PRIZE_TYPE_CD,VENDOR_NM,PRIZE_EXPIRATION_DT,
								EARN_POINTS_QTY,BURN_POINTS_QTY,PRIZE_RANKING_NBR,SWEEPSTAKE_DRAW_DT,DISCLAIMER_TXT,
								DIGITAL_PRIZE_IND,BURN_PROGRAM_NM,EARN_PROGRAM_NM,PRIZE_DETAIL_TXT,INITIAL_STOCK_QTY,
								AVAILABLE_STOCK_QTY,FILENAME,DW_CREATETS,METADATA$ACTION,METADATA$ISUPDATE,METADATA$ROW_ID
								FROM `+ src_rerun_tbl +``;
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
								src.PRIZE_NM,
								src.PRIZE_DSC,
								src.PRIZE_TYPE_CD,
								src.VENDOR_NM,
								TO_DATE(src.PRIZE_EXPIRATION_DT),
								src.EARN_POINTS_QTY,
								src.BURN_POINTS_QTY,
								src.PRIZE_RANKING_NBR,
								TO_DATE(src.SWEEPSTAKE_DRAW_DT),
								src.DISCLAIMER_TXT,
								src.DIGITAL_PRIZE_IND,
								src.BURN_PROGRAM_NM,
								src.EARN_PROGRAM_NM,
								src.PRIZE_DETAIL_TXT,
								src.INITIAL_STOCK_QTY,
								src.AVAILABLE_STOCK_QTY,
								src.FILENAME,
								tgt.DW_CREATE_TS as DW_CREATETS,
								src.DW_LOGICAL_DELETE_IND,
                                case 
                                when tgt.PRIZE_ID is null then 'I'
                                when tgt.PRIZE_ID is not null and                      
                                (src.PROGRAM_CD=tgt.PROGRAM_CD) and (
								(src.PRIZE_NM<>tgt.PRIZE_NM) 
								or (src.PRIZE_DSC<>tgt.PRIZE_DSC) 
								or (src.PRIZE_TYPE_CD<>tgt.PRIZE_TYPE_CD) 
								or (src.VENDOR_NM<>tgt.VENDOR_NM)
								or (src.PRIZE_EXPIRATION_DT<>tgt.PRIZE_EXPIRATION_DT) 
								or (src.EARN_POINTS_QTY<>tgt.EARN_POINTS_QTY) 
								or (src.BURN_POINTS_QTY<>tgt.BURN_POINTS_QTY) 
								or (src.PRIZE_RANKING_NBR<>tgt.PRIZE_RANKING_NBR) 
								or (src.SWEEPSTAKE_DRAW_DT<>tgt.SWEEPSTAKE_DRAW_DT) 
								or (src.DISCLAIMER_TXT<>tgt.DISCLAIMER_TXT) 
								or (src.DIGITAL_PRIZE_IND<>tgt.DIGITAL_PRIZE_IND) 
								or (src.BURN_PROGRAM_NM<>tgt.BURN_PROGRAM_NM) 
								or (src.EARN_PROGRAM_NM<>tgt.EARN_PROGRAM_NM) 
								or (src.PRIZE_DETAIL_TXT<>tgt.PRIZE_DETAIL_TXT) 
								or (src.INITIAL_STOCK_QTY<>tgt.INITIAL_STOCK_QTY) 
								or (src.AVAILABLE_STOCK_QTY<>tgt.AVAILABLE_STOCK_QTY)) 
								then 'U'
								else 'R'
                                end DML_TYPE
                                
								FROM(
									SELECT
									PRIZE_ID,
									PROGRAM_CD,
									PRIZE_NM,
									PRIZE_DSC,
									PRIZE_TYPE_CD,
									VENDOR_NM,
									PRIZE_EXPIRATION_DT,
									EARN_POINTS_QTY,
									BURN_POINTS_QTY,
									PRIZE_RANKING_NBR,
									SWEEPSTAKE_DRAW_DT,
									DISCLAIMER_TXT,
									DIGITAL_PRIZE_IND,
									BURN_PROGRAM_NM,
									EARN_PROGRAM_NM,
									PRIZE_DETAIL_TXT,
									INITIAL_STOCK_QTY,
									AVAILABLE_STOCK_QTY,
									FILENAME,
									DW_LOGICAL_DELETE_IND 
									FROM(
									SELECT
										PRIZE_ID,
										PROGRAM_CD,
										PRIZE_NM,
										PRIZE_DSC,
										PRIZE_TYPE_CD,
										VENDOR_NM,
										PRIZE_EXPIRATION_DT,
										EARN_POINTS_QTY,
										BURN_POINTS_QTY,
										PRIZE_RANKING_NBR,
										SWEEPSTAKE_DRAW_DT,
										DISCLAIMER_TXT,
										DIGITAL_PRIZE_IND,
										BURN_PROGRAM_NM,
										EARN_PROGRAM_NM,
										PRIZE_DETAIL_TXT,
										INITIAL_STOCK_QTY,
										AVAILABLE_STOCK_QTY,
										FILENAME,
										DW_CREATETS,
										FALSE AS DW_LOGICAL_DELETE_IND,
										Row_number() OVER (
										PARTITION BY PRIZE_ID, PROGRAM_CD
										order by DW_CREATETS DESC) as rn 
										FROM(
											SELECT
											PRIZE_ID,
											PROGRAM_CD,
											PRIZE_NM,
											PRIZE_DSC,
											PRIZE_TYPE_CD,
											VENDOR_NM,
											PRIZE_EXPIRATION_DT,
											EARN_POINTS_QTY,
											BURN_POINTS_QTY,
											PRIZE_RANKING_NBR,
											SWEEPSTAKE_DRAW_DT,
											DISCLAIMER_TXT,
											DIGITAL_PRIZE_IND,
											BURN_PROGRAM_NM,
											EARN_PROGRAM_NM,
											PRIZE_DETAIL_TXT,
											INITIAL_STOCK_QTY,
											AVAILABLE_STOCK_QTY,
											FILENAME,
											DW_CREATETS 
											FROM(
												SELECT
												PRIZE_ID,
												PROGRAM_CD,
												PRIZE_NM,
												PRIZE_DSC,
												PRIZE_TYPE_CD,
												VENDOR_NM,
												PRIZE_EXPIRATION_DT,
												EARN_POINTS_QTY,
												BURN_POINTS_QTY,
												PRIZE_RANKING_NBR,
												SWEEPSTAKE_DRAW_DT,
												DISCLAIMER_TXT,
												DIGITAL_PRIZE_IND,
												BURN_PROGRAM_NM,
												EARN_PROGRAM_NM,
												PRIZE_DETAIL_TXT,
												INITIAL_STOCK_QTY,
												AVAILABLE_STOCK_QTY,
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
                                    and tgt.PRIZE_ID=src.PRIZE_ID 
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
								PROGRAM_CD 
                                ,PRIZE_ID
								from  ${tgt_wrk_tbl} as b 
                                where DML_TYPE='U'
								) del
							where tgt.PROGRAM_CD = del.PROGRAM_CD
							and tgt.PRIZE_ID=del.PRIZE_ID                       
							AND tgt.DW_CURRENT_VERSION_IND = TRUE 
`; 


	//var sql_delete = `delete from ${tgt_tbl} where Recipe_Varient_Id in ( select Recipe_Varient_Id from ${tgt_wrk_tbl})`;					  
					  
		
	// Processing Inserts
	var sql_inserts = `INSERT INTO ${tgt_tbl} 
						(
						PRIZE_ID,
						PROGRAM_CD,
						DW_FIRST_EFFECTIVE_DT,
						DW_LAST_EFFECTIVE_DT,
						PRIZE_NM,
						PRIZE_DSC,
						PRIZE_TYPE_CD,
						VENDOR_NM,
						PRIZE_EXPIRATION_DT,
						EARN_POINTS_QTY,
						BURN_POINTS_QTY,
						PRIZE_RANKING_NBR,
						SWEEPSTAKE_DRAW_DT,
						DISCLAIMER_TXT,
						DIGITAL_PRIZE_IND,
						BURN_PROGRAM_NM,
						EARN_PROGRAM_NM,
						PRIZE_DETAIL_TXT,
						INITIAL_STOCK_QTY,
						AVAILABLE_STOCK_QTY,
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
						CURRENT_DATE ,           
						'31-DEC-9999' ,            
						PRIZE_NM,
						PRIZE_DSC,
						PRIZE_TYPE_CD,
						VENDOR_NM,
						PRIZE_EXPIRATION_DT,
						EARN_POINTS_QTY,
						BURN_POINTS_QTY,
						PRIZE_RANKING_NBR,
						SWEEPSTAKE_DRAW_DT,
						DISCLAIMER_TXT,
						DIGITAL_PRIZE_IND,
						BURN_PROGRAM_NM,
						EARN_PROGRAM_NM,
						PRIZE_DETAIL_TXT,
						INITIAL_STOCK_QTY,
						AVAILABLE_STOCK_QTY,             
						coalesce(DW_CREATETS,current_timestamp) as DW_CREATE_TS,               
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
        snowflake.execute ({ sqlText: sql_inserts });
        snowflake.execute ({ sqlText: sql_commit });
		} 
	catch (err) {
				snowflake.execute ({ sqlText: sql_rollback });
				snowflake.execute ({ sqlText: sql_ins_rerun_tbl});
				return `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
				}	
                               
// ************** Load for OCGP_PROGRAM_ATTRIBUTE table BEGIN *****************
				
$$;	

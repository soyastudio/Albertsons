--liquibase formatted sql
--changeset SYSTEM:SP_OCGP_PROGRAM_TO_BIM_LOAD runOnChange:true splitStatements:false OBJECT_TYPE:SP

Use database <<EDM_DB_NAME>>;
Use schema DW_APPL;

CREATE OR REPLACE PROCEDURE "SP_OCGP_PROGRAM_TO_BIM_LOAD"("SRC_WRK_TBL" VARCHAR(16777216), "CNF_DB" VARCHAR(16777216), "C_LOYAL" VARCHAR(16777216), "C_STAGE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
// ************** Load for OCGP_PROGRAM table BEGIN *****************
	var src_wrk_tbl = SRC_WRK_TBL;
	var temp_wrk_tbl = `${CNF_DB}.${C_STAGE}.OCGP_PROGRAM_TMP_WRK`;
	var cnf_schema = C_LOYAL;
	var tgt_wrk_tbl = `${CNF_DB}.${C_STAGE}.OCGP_PROGRAM_WRK`;
	var tgt_tbl = `${CNF_DB}.${cnf_schema}.OCGP_PROGRAM`;
	var src_rerun_tbl = `${CNF_DB}.${C_STAGE}.OCGP_PROGRAM_FLAT_RERUN`;

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
	SELECT PROGRAM_ID,PROGRAM_CD,PROGRAM_NM,PROGRAM_DSC,PROGRAM_TYPE_CD,PROGRAM_START_DT,PROGRAM_END_DT,MAINTENANCE_MODE_IND,POST_PROGRAM_LIVE_DT,FILENAME,DW_CREATETS,METADATA$ACTION,METADATA$ISUPDATE,METADATA$ROW_ID FROM `+ src_wrk_tbl +`
	UNION ALL
	SELECT PROGRAM_ID,PROGRAM_CD,PROGRAM_NM,PROGRAM_DSC,PROGRAM_TYPE_CD,PROGRAM_START_DT,PROGRAM_END_DT,MAINTENANCE_MODE_IND,POST_PROGRAM_LIVE_DT,FILENAME,DW_CREATETS,METADATA$ACTION,METADATA$ISUPDATE,METADATA$ROW_ID FROM `+ src_rerun_tbl +``;
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
								src.PROGRAM_ID,
								src.PROGRAM_CD,
								src.PROGRAM_NM,
								src.PROGRAM_DSC,
								src.PROGRAM_TYPE_CD,
								TO_DATE(src.PROGRAM_START_DT),
								TO_DATE(src.PROGRAM_END_DT),
								src.MAINTENANCE_MODE_IND,
								TO_DATE(src.POST_PROGRAM_LIVE_DT),
								src.FILENaMe as FILENAME,
								src.DW_LOGICAL_DELETE_IND,case 
								when tgt.PROGRAM_CD is null then 'I' 
								when tgt.PROGRAM_CD is not null  and 
								((src.PROGRAM_ID<>tgt.PROGRAM_ID) or 
								(src.PROGRAM_NM<>tgt.PROGRAM_NM) or 
								(src.PROGRAM_DSC<>tgt.PROGRAM_DSC) or 
								(src.PROGRAM_TYPE_CD<>tgt.PROGRAM_TYPE_CD) or 
								(src.PROGRAM_START_DT<>tgt.PROGRAM_START_DT) or 
								(src.PROGRAM_END_DT<>tgt.PROGRAM_END_DT) or 
								(src.MAINTENANCE_MODE_IND<>tgt.MAINTENANCE_MODE_IND) or 
								(src.POST_PROGRAM_LIVE_DT<>tgt.POST_PROGRAM_LIVE_DT)) then 'U'
								else 'R'
								end DML_TYPE
								,tgt.DW_CREATE_TS as DW_CREATE_TS 
								FROM(
									SELECT
									PROGRAM_ID
									,PROGRAM_CD
									,PROGRAM_NM
									,PROGRAM_DSC
									,PROGRAM_TYPE_CD
									,PROGRAM_START_DT
									,PROGRAM_END_DT
									,MAINTENANCE_MODE_IND
									,DW_CREATETS
									,FILENAME
									,POST_PROGRAM_LIVE_DT
									,DW_LOGICAL_DELETE_IND
									FROM(
										SELECT 
										PROGRAM_ID
										,PROGRAM_CD
										,PROGRAM_NM
										,PROGRAM_DSC
										,PROGRAM_TYPE_CD
										,PROGRAM_START_DT
										,PROGRAM_END_DT
										,MAINTENANCE_MODE_IND
										,DW_CREATETS
										,FILENAME
										,POST_PROGRAM_LIVE_DT
										,FALSE AS DW_LOGICAL_DELETE_IND
										,Row_number() OVER (
										PARTITION BY PROGRAM_CD
										order by DW_CREATETS DESC) as rn 
										FROM(
											SELECT
											PROGRAM_ID
											,PROGRAM_CD
											,PROGRAM_NM
											,PROGRAM_DSC
											,PROGRAM_TYPE_CD
											,PROGRAM_START_DT
											,PROGRAM_END_DT
											,MAINTENANCE_MODE_IND
											,DW_CREATETS
											,FILENAME
											,POST_PROGRAM_LIVE_DT
											FROM(
												SELECT
												PROGRAM_ID
												,PROGRAM_CD
												,PROGRAM_NM
												,PROGRAM_DSC
												,PROGRAM_TYPE_CD
												,PROGRAM_START_DT
												,PROGRAM_END_DT
												,MAINTENANCE_MODE_IND
												,DW_CREATETS
												,FILENAME
												,POST_PROGRAM_LIVE_DT
												FROM 
                                              // EDM_REFINED_DEV.DW_R_LOYALTY.OCGP_PROGRAM_FLAT TWT
												${temp_wrk_tbl}  TWT
												)
                                            )
										)where rn=1
									)src
                                    left outer join ${tgt_tbl} TGT on 
                                    tgt.PROGRAM_CD = src.PROGRAM_CD
                                    and tgt.DW_CURRENT_VERSION_IND=True`;
	try {
    //return create_tgt_wrk_table
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
                           
								from  ${tgt_wrk_tbl} as b 
                                where DML_TYPE='U'
								) del
							where tgt.PROGRAM_CD = del.PROGRAM_CD
							AND tgt.DW_CURRENT_VERSION_IND = TRUE 
`; 


	//var sql_delete = `delete from ${tgt_tbl} where Recipe_Varient_Id in ( select Recipe_Varient_Id from ${tgt_wrk_tbl})`;					  
					  
		
	// Processing Inserts
	var sql_inserts = `INSERT INTO ${tgt_tbl} 
						(
						PROGRAM_CD
						,DW_FIRST_EFFECTIVE_DT
						,DW_LAST_EFFECTIVE_DT
						,PROGRAM_ID
						,PROGRAM_NM
						,PROGRAM_DSC
						,PROGRAM_TYPE_CD
						,PROGRAM_START_DT
						,PROGRAM_END_DT
						,MAINTENANCE_MODE_IND
						,POST_PROGRAM_LIVE_DT	
						,DW_CREATE_TS
						,DW_LAST_UPDATE_TS
						,DW_LOGICAL_DELETE_IND
						,DW_SOURCE_CREATE_NM
						,DW_SOURCE_UPDATE_NM
						,DW_CURRENT_VERSION_IND
						)
						SELECT 
						PROGRAM_CD
						,CURRENT_DATE          
						,'31-DEC-9999'
						,PROGRAM_ID
						,PROGRAM_NM
						,PROGRAM_DSC
						,PROGRAM_TYPE_CD
						,PROGRAM_START_DT
						,PROGRAM_END_DT
						,MAINTENANCE_MODE_IND
						,POST_PROGRAM_LIVE_DT
						,coalesce(DW_CREATE_TS,current_timestamp)  as DW_CREATE_TS
						,current_timestamp
						,DW_LOGICAL_DELETE_IND
						,Filename           
						,NULL
						,TRUE
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
                               
// ************** Load for OCGP_PROGRAM table END *****************
				
$$;

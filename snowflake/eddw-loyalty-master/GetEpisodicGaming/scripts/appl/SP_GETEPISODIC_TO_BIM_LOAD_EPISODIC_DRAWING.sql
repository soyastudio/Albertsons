--liquibase formatted sql
--changeset SYSTEM:SP_GETEPISODIC_TO_BIM_LOAD_EPISODIC_DRAWING runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETEPISODIC_TO_BIM_LOAD_EPISODIC_DRAWING(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_LOYAL VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

		var src_wrk_tbl = SRC_WRK_TBL;
		var temp_wrk_tbl = `${CNF_DB}.${C_STAGE}.Episodic_Drawing_tmp_WRK`;
		var cnf_schema = C_LOYAL;
		var tgt_wrk_tbl = `${CNF_DB}.${C_STAGE}.Episodic_Drawing_WRK`;
		var tgt_tbl = `${CNF_DB}.${cnf_schema}.Episodic_Drawing`;
		var src_rerun_tbl = `${CNF_DB}.${C_STAGE}.Episodic_Drawing_Flat_Rerun`;
		

// Empty the rerun queue table
var sql_empty_rerun_tbl = `TRUNCATE TABLE ${src_rerun_tbl}`;




//query to load rerun queue table when encountered a failure
var sql_ins_rerun_tbl = `INSERT INTO ${src_rerun_tbl} SELECT * FROM ${temp_wrk_tbl}`;




// persist stream data in work table for the current transaction, includes data from previous failed run



var sql_crt_src_wrk_tbl = `create or replace Transient table `+ temp_wrk_tbl +` DATA_RETENTION_TIME_IN_DAYS = 0 as
SELECT * FROM `+ src_wrk_tbl +`
UNION ALL
SELECT * FROM `+ src_rerun_tbl+``;

try {
snowflake.execute (
{sqlText: sql_crt_src_wrk_tbl }
);
}
catch (err) {
throw "Creation of Source Work table "+ temp_wrk_tbl +" Failed with error: " + err; // Return a error message.
}

                       
    // **************        Load for Episodic_Drawing table BEGIN *****************
    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
	var create_tgt_wrk_table = `CREATE OR REPLACE TRANSIENT TABLE ${tgt_wrk_tbl} as 
								SELECT DISTINCT
								 src.Program_Id
								,src.Drawing_Id 
								,src.Game_Uuid
								,src.Game_Type_Cd
								,src.Start_Dt
								,src.End_Dt
								,src.Draw_Dt
								,src.Extract_Ts
								,src.FILE_NAME
								,src.DW_LOGICAL_DELETE_IND
                                ,CASE 
								    WHEN (
										     tgt.Program_Id IS NULL 
										and  tgt.Drawing_Id is NULL
								         ) 
									THEN 'I' 
									ELSE 'U' 
								END AS DML_Type
								,CASE   
									WHEN tgt.DW_First_Effective_dt = CURRENT_DATE 
									THEN 1 
									Else 0 
								END as Sameday_chg_ind
								FROM (   SELECT
											 Program_Id 
											,Drawing_Id 
											,Game_Uuid
											,Game_Type_Cd
											,Start_Dt 
											,End_Dt
											,Draw_Dt
											,Extract_Ts
											,FILE_NAME
											,DW_LOGICAL_DELETE_IND
										
										FROM ( 
											   SELECT
												 Program_Id 
												,Drawing_Id 
												,Game_Uuid
												,Game_Type_Cd
												,Start_Dt 
												,End_Dt
												,Draw_Dt
												,Extract_Ts
												,FILE_NAME
												,DW_CREATE_TS
												,false as  DW_LOGICAL_DELETE_IND
											,Row_number() OVER (
											 PARTITION BY Program_Id,Drawing_Id
											  order by to_timestamp_ntz(Extract_Ts) DESC) as rn
											  FROM(
                                                    SELECT
													 Program_Id 
													,Drawing_Id 
													,Game_Uuid
													,Game_Type_Cd
													,Start_Dt 
													,End_Dt
													,Draw_Dt
													,Extract_Ts 
													,FILE_NAME
													,DW_CREATE_TS
													FROM
													  (
													  SELECT  
													   Program_Id
													  ,Drawing_Id
													  ,Game_Uuid
													  ,GAME_TYPE as Game_Type_Cd
													  ,to_timestamp_ntz(START_DATE) as Start_Dt
													  ,to_timestamp_ntz(END_DATE) as End_Dt
													  ,to_timestamp_ntz(DRAW_DATE) as Draw_Dt
													  ,to_timestamp_ntz(Extract_Ts) as Extract_Ts
													  ,FILE_NAME
													  ,DW_CREATE_TS
													  FROM 
													   ${temp_wrk_tbl} S
													  )
                                                )
											)  where rn=1	AND Program_Id is NOT NULL
															AND Drawing_Id is NOT NULL
									) src
									LEFT JOIN
									( 
									SELECT  DISTINCT
											 Program_Id 
											,Drawing_Id 
											,Game_Uuid
											,Game_Type_Cd
											,Start_Dt 
											,End_Dt
											,Draw_Dt
											,Extract_Ts
											,DW_First_Effective_dt
											,DW_LOGICAL_DELETE_IND
									FROM
									${tgt_tbl} tgt
									WHERE DW_CURRENT_VERSION_IND = TRUE
									)as tgt 
									ON
										 nvl(src.Program_Id ,'-1') = nvl(tgt.Program_Id ,'-1')
									and  nvl(src.Drawing_Id,'-1') = nvl(tgt.Drawing_Id ,'-1')
									WHERE  (
										tgt.Program_Id IS  NULL
									AND tgt.Drawing_Id is  NULL
									 )
									OR
									(
										NVL(src.Program_Id,'-1') <> NVL(tgt.Program_Id,'-1')  
									 OR NVL(src.Drawing_Id ,'-1') <> NVL(tgt.Drawing_Id ,'-1')
									 OR NVL(src.Game_Uuid,'-1') <> NVL(tgt.Game_Uuid,'-1')  
									 OR NVL(src.Game_Type_Cd,'-1') <> NVL(tgt.Game_Type_Cd,'-1')
									 OR NVL(src.Start_Dt,'9999-12-31 00:00:00.000') <> NVL(tgt.Start_Dt,'9999-12-31 00:00:00.000')
									 OR NVL(src.End_Dt,'9999-12-31 00:00:00.000') <> NVL(tgt.End_Dt,'9999-12-31 00:00:00.000')
									 OR NVL(src.Draw_Dt,'9999-12-31 00:00:00.000') <> NVL(tgt.Draw_Dt,'9999-12-31 00:00:00.000')
									 OR NVL(src.Extract_Ts,'9999-12-31 00:00:00.000') <>NVL(tgt.Extract_Ts,'9999-12-31 00:00:00.000')
									 OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND  
									 )`;   

try {
snowflake.execute ({ sqlText: create_tgt_wrk_table });
	}
    catch (err) {

snowflake.execute ({ sqlText: sql_ins_rerun_tbl});


        return `Creation of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
				}          
            
 
// Transaction for Updates, Insert begins           
    var sql_begin = "BEGIN"
// SCD Type2 - Processing Different day updates
var sql_updates = `UPDATE ${tgt_tbl} as tgt
					SET 
					DW_Last_Effective_dt = CURRENT_DATE - 1,
					DW_CURRENT_VERSION_IND = FALSE,
					DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
					DW_SOURCE_UPDATE_NM = FILE_NAME
				
					FROM ( 
							SELECT 
								 Program_Id
								,Drawing_Id
								,FILE_NAME
							FROM ${tgt_wrk_tbl}
							WHERE 
								DML_Type = 'U' 
							AND Sameday_chg_ind = 0
					) src
					WHERE
						nvl(src.Program_Id,'-1') = nvl(tgt.Program_Id,'-1')
					AND nvl(src.Drawing_Id,'-1') = nvl(tgt.Drawing_Id,'-1')					
					AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
                   
 // SCD Type1 - Processing Sameday updates
var sql_sameday = `UPDATE ${tgt_tbl} as tgt
						SET Program_Id = src.Program_Id,
							Drawing_Id = src.Drawing_Id,
							Game_Uuid = src.Game_Uuid,
							Game_Type_Cd = src.Game_Type_Cd,
							Start_Dt = src.Start_Dt,
							End_Dt = src.End_Dt,
							Draw_Dt = src.Draw_Dt,
							Extract_Ts=src.Extract_Ts,
							DW_Logical_delete_ind = src.DW_Logical_delete_ind,
							DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
							DW_SOURCE_UPDATE_NM = FILE_NAME
					FROM ( 
							SELECT
								 Program_Id
								,Drawing_Id
								,Game_Uuid 
								,Game_Type_Cd
								,Start_Dt
								,End_Dt
								,Draw_Dt
								,Extract_Ts
								,FILE_NAME
								,DW_Logical_delete_ind
							FROM ${tgt_wrk_tbl}
							WHERE 
							DML_Type = 'U' 
							AND Sameday_chg_ind = 1
						) src WHERE
									nvl(src.Program_Id,'-1') = nvl(tgt.Program_Id,'-1')
								AND nvl(src.Drawing_Id,'-1') = nvl(tgt.Drawing_Id,'-1')	
								AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl} 
					(
					 Program_Id
					,Drawing_Id
					,DW_First_Effective_Dt
					,DW_Last_Effective_Dt
					,Game_Uuid 
					,Game_Type_Cd
					,Start_Dt
					,End_Dt
					,Draw_Dt
					,Extract_Ts
					,DW_CREATE_TS
					,DW_LOGICAL_DELETE_IND
					,DW_SOURCE_CREATE_NM
					,DW_CURRENT_VERSION_IND
					)
					SELECT
					 Program_Id 
					,Drawing_Id
					,CURRENT_DATE
					,'31-DEC-9999'
					,Game_Uuid
					,Game_Type_Cd
					,Start_Dt
					,End_Dt
					,Draw_Dt
					,Extract_Ts 
					,CURRENT_TIMESTAMP
					,DW_LOGICAL_DELETE_IND
					,FILE_NAME
					,TRUE 
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
		
		
		return `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
					}		
                               
                // **************        Load for Episodic_Drawing Table ENDs *****************
				

$$;

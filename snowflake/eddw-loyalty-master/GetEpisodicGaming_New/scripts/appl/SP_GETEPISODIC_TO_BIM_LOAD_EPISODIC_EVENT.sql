--liquibase formatted sql
--changeset SYSTEM:SP_GETEPISODIC_TO_BIM_LOAD_EPISODIC_EVENT runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETEPISODIC_TO_BIM_LOAD_EPISODIC_EVENT(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_LOYAL VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

		
		var src_wrk_tbl = SRC_WRK_TBL;
		var temp_wrk_tbl = `${CNF_DB}.${C_STAGE}.Episodic_Events_tmp_WRK`;
		var cnf_schema = C_LOYAL;
		var tgt_wrk_tbl = `${CNF_DB}.${C_STAGE}.Episodic_Events_WRK`;
		var tgt_tbl = `${CNF_DB}.${cnf_schema}.Episodic_Event`;
		var src_rerun_tbl = `${CNF_DB}.${C_STAGE}.EPISODIC_EVENTS_FLAT_RERUN`;
		
// Empty the rerun queue table
var sql_empty_rerun_tbl = `TRUNCATE TABLE ${src_rerun_tbl}`;

//query to load rerun queue table when encountered a failure
var sql_ins_rerun_tbl = `INSERT INTO ${src_rerun_tbl} SELECT * FROM ${temp_wrk_tbl}`;

// persist stream data in work table for the current transaction, includes data from previous failed run
var sql_empty_tmp_wrk_tbl = `TRUNCATE TABLE `+ temp_wrk_tbl +``;

var sql_ins_tmp_wrk_tbl = `INSERT INTO `+ temp_wrk_tbl +`
SELECT * FROM `+ src_wrk_tbl +`
UNION ALL
SELECT * FROM `+ src_rerun_tbl+``;

try {
snowflake.execute ({sqlText: sql_empty_tmp_wrk_tbl});
snowflake.execute ({sqlText: sql_ins_tmp_wrk_tbl});
}
catch (err) {
throw "Creation of Source Work table "+ temp_wrk_tbl +" Failed with error: " + err; // Return a error message.
}
		
                       
    // **************        Load for Episodic_Event table BEGIN *****************
    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
	var empty_tgt_wrk_table = `TRUNCATE TABLE ${tgt_wrk_tbl}`;
	
	var ins_tgt_wrk_table = `INSERT INTO ${tgt_wrk_tbl}
								SELECT DISTINCT
								src.Program_Id
								,src.Event_Id
								,src.Request_Time_Ts
								,src.Session_Id
								,src.Page_Nm
								,src.Category_Dsc
								,src.Action_Cd
								,src.Label_Dsc
								,src.Label_Value_Nbr
								,src.Extract_Ts
								,src.Event_Nbr
								,src.Event_Reference_Id
								,src.filename
								,src.DW_LOGICAL_DELETE_IND
                                ,CASE 
								    WHEN (
										     tgt.Program_Id IS NULL 
										and  tgt.Event_Id is NULL 
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
											,Event_Id 
											,Request_Time_Ts
											,Session_Id
											,Page_Nm 
											,Category_Dsc
											,Action_Cd
											,Label_Dsc
											,Label_Value_Nbr
											,Extract_Ts
											,Event_Nbr
											,Event_Reference_Id
											,filename
											,DW_LOGICAL_DELETE_IND
										
										FROM ( 
											   SELECT
												Program_Id 
												,Event_Id
												,Request_Time_Ts
												,Session_Id
												,Page_Nm 
												,Category_Dsc
												,Action_Cd
												,Label_Dsc
												,Label_Value_Nbr
												,Extract_Ts
												,Event_Nbr
												,Event_Reference_Id
												,filename
												,DW_Create_Ts
												,false as  DW_LOGICAL_DELETE_IND
											,Row_number() OVER (
											 PARTITION BY Program_Id,Event_Id
											  order by to_timestamp_ntz(Extract_Ts) DESC) as rn
											  FROM(
                                                    SELECT
													Program_Id 
													,Event_Id
													,Request_Time_Ts
													,Session_Id
													,Page_Nm 
													,Category_Dsc
													,Action_Cd
													,Label_Dsc
													,Label_Value_Nbr
													,Extract_Ts
													,Event_Nbr
													,Event_Reference_Id
													,filename
													,DW_Create_Ts
													FROM
													  (
													  SELECT  
													   Program_id
													  ,NULLIF(Event_id,'') as Event_Id
													  ,to_timestamp_ntz(Request_time) as Request_Time_Ts
													  ,NULLIF(Session_id,'') as Session_Id
													  ,Pagename 		as Page_Nm
													  ,Category			as Category_Dsc
													  ,Action   		as Action_Cd
													  ,Label			as Label_Dsc
													  ,NULLIF(Value,'')			as Label_Value_Nbr
													  ,to_timestamp_ntz(Extract_Ts) as Extract_Ts
													  ,DW_Create_Ts
													  ,file_name		as filename
													  ,Events_Nbr		as Event_Nbr
													  ,Reference_Id		as Event_Reference_Id
													  FROM 
													   ${temp_wrk_tbl} S
													  )
                                                )
											)  where rn=1
									) src
									LEFT JOIN
									( 
									SELECT  DISTINCT
											Program_Id 
											,Event_id 
											,Request_Time_Ts
											,Session_id
											,Page_Nm 
											,Category_Dsc
											,Action_Cd
											,Label_Dsc
											,Label_Value_Nbr
											,Extract_ts
											,Event_Nbr
											,Event_Reference_Id
											,DW_First_Effective_dt
											,DW_LOGICAL_DELETE_IND
									FROM
									${tgt_tbl} tgt
									WHERE DW_CURRENT_VERSION_IND = TRUE
									)as tgt 
									ON
										 src.Program_Id = tgt.Program_Id 
									and  src.Event_id = tgt.Event_id
									WHERE  (
									tgt.Program_Id IS  NULL
									AND tgt.Event_id is  NULL
									 )
									OR
									(
								    NVL(src.Request_Time_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Request_Time_Ts,'9999-12-31 00:00:00.000')  
									 OR NVL(src.Session_Id,'-1') <> NVL(tgt.Session_Id,'-1')
									 OR NVL(src.Page_Nm,'-1') <> NVL(tgt.Page_Nm,'-1')
									 OR NVL(src.Category_Dsc,'-1') <> NVL(tgt.Category_Dsc,'-1')
									 OR NVL(src.Action_Cd,'-1') <>NVL(tgt.Action_Cd,'-1')
									 OR NVL(src.Label_Dsc,'-1') <> NVL(tgt.Label_Dsc,'-1')
									 OR NVL(src.Label_Value_Nbr,'-1') <> NVL(tgt.Label_Value_Nbr,'-1')
									 OR NVL(src.Extract_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Extract_Ts,'9999-12-31 00:00:00.000')
									 OR NVL(src.Event_Nbr,'-1') <> NVL(tgt.Event_Nbr,'-1')
									 OR NVL(src.Event_Reference_Id,'-1') <> NVL(tgt.Event_Reference_Id,'-1')
									 OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND  
									 )`;   

try {
snowflake.execute ({sqlText: empty_tgt_wrk_table});
snowflake.execute ({sqlText: ins_tgt_wrk_table});
}
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
				
					FROM ( 
							SELECT 
								 Program_Id
								,Event_Id
								,filename
							FROM ${tgt_wrk_tbl}
							WHERE 
							DML_Type = 'U' 
							AND Sameday_chg_ind = 0
					) src
					WHERE
					nvl(src.Program_Id,'-1') = nvl(tgt.Program_Id,'-1')
					AND nvl(src.Event_Id,'-1') = nvl(tgt.Event_Id,'-1')				
					AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
                   
 // SCD Type1 - Processing Sameday updates
var sql_sameday = `UPDATE ${tgt_tbl} as tgt
					SET Program_Id = src.Program_Id,
					Event_Id = src.Event_Id,
					Request_Time_Ts = src.Request_Time_Ts,
					Session_Id = src.Session_Id,
					Page_Nm = src.Page_Nm,
					Category_Dsc = src.Category_Dsc,
					Action_Cd=src.Action_Cd,
					Label_Dsc = src.Label_Dsc,
					Label_Value_Nbr = src.Label_Value_Nbr,
					Extract_Ts = src.Extract_Ts,
					Event_Nbr = src.Event_Nbr,
					Event_Reference_Id = src.Event_Reference_Id,
					DW_Logical_delete_ind = src.DW_Logical_delete_ind,
					DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
					DW_SOURCE_UPDATE_NM = filename
					FROM ( 
							SELECT
								Program_Id
								,Event_Id
								,Request_Time_Ts 
								,Session_Id
								,Page_Nm
								,Category_Dsc
								,Action_Cd
								,Label_Dsc
								,Label_Value_Nbr
								,Extract_Ts
								,Event_Nbr
								,Event_Reference_Id
								,filename
								,DW_Logical_delete_ind
							FROM ${tgt_wrk_tbl}
							WHERE 
							DML_Type = 'U' 
							AND Sameday_chg_ind = 1
						) src WHERE
							nvl(src.Program_Id,'-1') = nvl(tgt.Program_Id,'-1')
							AND nvl(src.Event_Id,'-1') = nvl(tgt.Event_Id,'-1')
							AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl} 
					(
					Program_Id
					,Event_Id
					,Request_Time_Ts 
					,Session_Id
					,Page_Nm
					,Category_Dsc
					,Action_Cd
					,Label_Dsc
					,Label_Value_Nbr
					,Extract_Ts
					,Event_Nbr
					,Event_Reference_Id
					,DW_First_Effective_Dt
					,DW_Last_Effective_Dt
					,Dw_Create_Ts
					//,Dw_Last_Update_Ts
					,Dw_Logical_Delete_Ind
					,Dw_Source_Create_Nm
					//,Dw_Source_Update_Nm
					,Dw_Current_Version_Ind
					)
					SELECT
					Program_Id 
					,Event_Id
					,Request_Time_Ts
					,Session_Id
					,Page_Nm
					,Category_Dsc
					,Action_Cd
					,Label_Dsc
					,Label_Value_Nbr
					,Extract_Ts
					,Event_Nbr
					,Event_Reference_Id
					,CURRENT_DATE
					,'31-DEC-9999'
					,CURRENT_TIMESTAMP
					//,Dw_Last_Update_Ts
					,Dw_Logical_Delete_Ind
					,filename
					//,Dw_Source_Update_Nm
					,TRUE 
					FROM ${tgt_wrk_tbl}
					WHERE
                    Program_Id is Not Null
                 and Event_Id is Not Null
					and Sameday_chg_ind = 0`;

    
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
                               
                // **************        Load for Episcodi_Event Table ENDs *****************
$$;

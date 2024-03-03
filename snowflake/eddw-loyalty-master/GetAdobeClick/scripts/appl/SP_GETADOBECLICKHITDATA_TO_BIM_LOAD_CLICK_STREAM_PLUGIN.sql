--liquibase formatted sql
--changeset SYSTEM:SP_GETADOBECLICKHITDATA_TO_BIM_LOAD_CLICK_STREAM_PLUGIN runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_CONFIRMED_PRD;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE EDM_CONFIRMED_PRD.DW_APPL.SP_GETADOBECLICKHITDATA_TO_BIM_LOAD_CLICK_STREAM_PLUGIN(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_USER_ACT VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

		var cnf_db = CNF_DB;
		var wrk_schema = C_STAGE;
		var cnf_schema = C_USER_ACT;		
		var src_wrk_tbl = SRC_WRK_TBL;
		
		var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.CLICK_STREAM_PLUGIN_WRK`;
		var tgt_tbl = `${cnf_db}.${cnf_schema}.CLICK_STREAM_PLUGIN`;
                                              
    // **************        Load for Adobe Clickstream Plugin table BEGIN *****************
    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
	var create_tgt_wrk_table = `CREATE OR REPLACE TABLE ${tgt_wrk_tbl} as 
								WITH src_wrk_tbl_recs as
							(
							SELECT DISTINCT 
											   PLUGIN_ID
											  ,PLUGIN_NM
											  ,DW_CREATE_TS 
											  ,Row_number() OVER (PARTITION BY PLUGIN_NM order by(DW_CREATE_TS,PLUGIN_ID) DESC) as rn
							FROM
							(
								SELECT DISTINCT 
											   TRY_TO_NUMERIC(TRIM(PLUGIN_ID)) AS PLUGIN_ID
											  ,PLUGIN_NM AS PLUGIN_NM
											  ,DW_CREATETS AS DW_CREATE_TS             
								FROM ${src_wrk_tbl}
							)
							)
							SELECT 
											   src.PLUGIN_ID
											  ,src.PLUGIN_NM
											  ,src.DW_CREATE_TS  
											  ,src.DW_LOGICAL_DELETE_IND
											  ,CASE WHEN tgt.PLUGIN_NM IS NULL THEN 'I' ELSE 'U' END AS DML_Type
											  ,CASE WHEN tgt.DW_First_Effective_dt = CURRENT_DATE THEN 1 Else 0 END as Sameday_chg_ind
							FROM(
							SELECT  
											   PLUGIN_ID
											  ,PLUGIN_NM
											  ,DW_CREATE_TS  
											  ,FALSE as DW_LOGICAL_DELETE_IND				  
							FROM src_wrk_tbl_recs 
							WHERE rn = 1
							)src
							LEFT JOIN
							(  
							SELECT DISTINCT 
								   tgt.PLUGIN_ID
								  ,tgt.PLUGIN_NM
								  ,tgt.DW_CREATE_TS
								  ,tgt.DW_LOGICAL_DELETE_IND
								  ,tgt.DW_First_Effective_dt
							from ${tgt_tbl} tgt
							WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
							)tgt
							on src.PLUGIN_NM = tgt.PLUGIN_NM
							WHERE
							(tgt.PLUGIN_NM IS NULL)
							OR
							(src.PLUGIN_ID <> tgt.PLUGIN_ID)`;   

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
					 DW_Last_Effective_dt = CURRENT_DATE - 1
					,DW_CURRENT_VERSION_IND = FALSE
					,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
					,DW_SOURCE_UPDATE_NM = 'Adobe'
				
					FROM ( 
							SELECT 
								 PLUGIN_ID
								,PLUGIN_NM
								,DW_CREATE_TS
							FROM ${tgt_wrk_tbl}
							WHERE 
							DML_Type = 'U' 
							AND Sameday_chg_ind = 0
					) src
					WHERE 
						src.PLUGIN_NM = tgt.PLUGIN_NM
					AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
                   
 // SCD Type1 - Processing Sameday updates
var sql_sameday = `UPDATE ${tgt_tbl} as tgt
					SET 
					 PLUGIN_ID = src.PLUGIN_ID
					,DW_Logical_delete_ind = src.DW_Logical_delete_ind
					,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
					,DW_SOURCE_UPDATE_NM = 'Adobe'
					FROM ( 
							SELECT
								 PLUGIN_ID
								,PLUGIN_NM
								,DW_CREATE_TS
								,DW_Logical_delete_ind
							FROM ${tgt_wrk_tbl}
							WHERE 
							DML_Type = 'U' 
							AND Sameday_chg_ind = 1
						) src WHERE 
							src.PLUGIN_NM = tgt.PLUGIN_NM
						AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
						
// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl} 
					(
					 PLUGIN_NM					
                    ,DW_First_Effective_Dt
                    ,DW_Last_Effective_Dt
                    ,PLUGIN_ID
					,DW_CREATE_TS
                    ,DW_LOGICAL_DELETE_IND
                    ,DW_SOURCE_CREATE_NM
                    ,DW_CURRENT_VERSION_IND  
					)
					SELECT
					 PLUGIN_NM
					,CURRENT_DATE
					,'31-DEC-9999'
					,PLUGIN_ID
					,CURRENT_TIMESTAMP
					,DW_LOGICAL_DELETE_IND
					,'Adobe'				
					,TRUE 
					FROM ${tgt_wrk_tbl}
					WHERE 
					Sameday_chg_ind = 0                                 
               `;
    
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
                // **************        Load for Adobe Clickstream Plugin table ENDs *****************

$$;
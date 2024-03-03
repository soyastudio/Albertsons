--liquibase formatted sql
--changeset SYSTEM:SP_GETADOBECLICKHITDATA_TO_BIM_LOAD_CLICK_STREAM_UNASSIGNED runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_CONFIRMED_PRD;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE EDM_CONFIRMED_PRD.DW_APPL.SP_GETADOBECLICKHITDATA_TO_BIM_LOAD_CLICK_STREAM_UNASSIGNED(SRC_TBL VARCHAR, CNF_DB VARCHAR, C_USER_ACT VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$


		var cnf_db = CNF_DB;
		var wrk_schema = C_STAGE;
		var cnf_schema = C_USER_ACT;
		var src_tbl = SRC_TBL;
		var unassigned_tbl_nm = 'Click_Stream_Unassigned'; 
		var cntrl_tbl_nm = 'CLICK_STREAM_CONTROL_TABLE';
		var sp_name	= 'SP_GetAdobeClickHitData_TO_BIM_LOAD_Click_Stream_Unassigned';

		var src_wrk_tbl = `${cnf_db}.${wrk_schema}.${unassigned_tbl_nm}_src_WRK`;		
        var src_rerun_tbl = `${cnf_db}.${wrk_schema}.${unassigned_tbl_nm}_Rerun`;
		var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.${unassigned_tbl_nm}_WRK`;
		var tgt_tbl = `${cnf_db}.${cnf_schema}.${unassigned_tbl_nm}`;
		var tgt_exp_tbl = `${cnf_db}.${wrk_schema}.${unassigned_tbl_nm}_EXCEPTION`;
	
	function log(status,msg){
	var cnf_msg = msg;
	try{	
    snowflake.createStatement( 
	{ sqlText: `call SP_GetAdobeClickHitData_Click_Stream_Log('${cnf_db}',
															  '${wrk_schema}',
															  '${cnf_msg}',
															  '${status }',
															  '${sp_name}')`}).execute();																						 
		}catch(err)
		{
		snowflake.createStatement( 
		{ sqlText: `call SP_GetAdobeClickHitData_Click_Stream_Log('${cnf_db}',
																  '${wrk_schema}',
																  'Unable to insert exception',
																  '${status }',
																  '${sp_name}')`}).execute();							
		}}	
	log('STARTED','Load for Click Stream Unassigned table BEGIN');
	
                       
    // **************        Load for Click Stream Unassigned table BEGIN *****************
    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 

// persist stream data in work table for the current transaction, includes data from previous failed run
	var sql_crt_src_wrk_tbl = `create or replace table ${src_wrk_tbl} DATA_RETENTION_TIME_IN_DAYS = 0 as
    SELECT * FROM ${src_tbl}
    UNION ALL
    SELECT * FROM ${src_rerun_tbl} `;
    try {
        snowflake.execute({ sqlText: sql_crt_src_wrk_tbl });
		log('SUCCEEDED',`Successfuly created Source Work table ${src_wrk_tbl}`);
    } catch (err)  {
		log('FAILED',`Creation of Source Work table ${src_wrk_tbl} Failed with error: ${err}`);
        throw `Creation of Source Work table ${src_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
    }
	
	// Empty the rerun queue table
	var sql_empty_rerun_tbl = `TRUNCATE TABLE ${src_rerun_tbl}`;
	try {
        snowflake.execute({ sqlText: sql_empty_rerun_tbl });
		log('SUCCEEDED',`Successfuly truncated rerun queue table ${src_rerun_tbl}`);
    } catch (err) {
		log('FAILED',`Truncation of rerun queue table ${src_rerun_tbl} Failed with error: ${err}`);
        throw `Truncation of rerun queue table ${src_rerun_tbl} Failed with error: ${err}`;   // Return a error message.
    }
    
	// query to load rerun queue table when encountered a failure
	var sql_ins_rerun_tbl = `CREATE OR REPLACE TABLE ${src_rerun_tbl} as SELECT * FROM ${src_wrk_tbl}`;
   
   
     // **************        Load for Click Stream Shop table BEGIN *****************
    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 


var create_tgt_wrk_table =	`CREATE OR REPLACE TABLE ${tgt_wrk_tbl} as 
 
							WITH src_wrk_tbl_recs as
                            (SELECT DISTINCT 
							 HITID_HIGH
							,HITID_LOW
							,VISIT_PAGE_NUM
							,VISIT_NUM
							,EXCLUDE_HIT
							,HIT_SOURCE
							,POST_EVAR110
							,POST_EVAR208
							,POST_EVAR209
							,POST_EVAR210
							,POST_EVAR211
							,POST_EVAR213
							,POST_EVAR214
							,POST_EVAR215
							,POST_EVAR216
							,POST_EVAR217
							,POST_EVAR218
							,POST_EVAR219
							,POST_EVAR220
							,POST_EVAR221
							,POST_EVAR222
							,POST_EVAR223
							,POST_EVAR224
							,POST_EVAR225
							,POST_EVAR226
							,POST_EVAR227
							,POST_EVAR228
							,POST_EVAR229	
							,POST_EVAR230
							,POST_EVAR231
							,POST_EVAR232
							,POST_EVAR233
							,POST_EVAR234
							,POST_EVAR235
							,POST_EVAR236
							,POST_EVAR237
							,POST_EVAR238
							,POST_EVAR239
							,POST_EVAR240
							,POST_EVAR241
							,POST_EVAR242
							,POST_EVAR243
							,POST_EVAR244
							,POST_EVAR245
							,POST_EVAR246
							,POST_EVAR247
							,POST_EVAR248
							,POST_EVAR249
							,POST_EVAR250
							,POST_EVAR27
							,POST_EVAR28
							,POST_EVAR29
							,POST_EVAR30
							,POST_EVAR31
							,POST_EVAR34
							,POST_EVAR39
							,POST_EVAR41
							,POST_EVAR42
							,POST_EVAR43
							,POST_EVAR44
							,POST_EVAR45
							,POST_EVAR48
							,POST_EVAR50
							,POST_EVAR69
							,POST_EVAR72
							,POST_EVAR79
							,POST_EVAR82
							,POST_EVENT_LIST
							,DW_SOURCE_CREATE_NM 
							,row_number() over(partition by HITID_HIGH,HITID_LOW,VISIT_PAGE_NUM,VISIT_NUM  
										  ORDER BY(DW_CREATETS) DESC) as rn
							FROM ${src_wrk_tbl} 
							WHERE  	HITID_HIGH 			IS NOT NULL	
									AND HITID_LOW  		IS NOT NULL	
									AND VISIT_PAGE_NUM  IS NOT NULL	
									AND VISIT_NUM  		IS NOT NULL	
							)
							SELECT 
							CLICK_STREAM_INTEGRATION_ID
                            ,src.Hit_Id_High
							,src.Hit_Id_Low
							,src.Visit_Page_Nbr
							,src.Visit_Nbr
							,EXCLUDE_ROW_IND
							,Text_V110
							,Text_V208
							,Text_V209
							,Text_V210
							,Text_V211
							,Text_V213
							,Text_V214
							,Text_V215
							,Text_V216
							,Text_V217
							,Text_V218
							,Text_V219
							,Text_V220
							,Text_V221
							,Text_V222 
							,Text_V223
							,Text_V224
							,Text_V225
							,Text_V226
							,Text_V227
							,Text_V228
							,Text_V229
							,Text_V230
							,Text_V231
							,Text_V232
							,Text_V233
							,Text_V234
							,Text_V235
							,Text_V236
							,Text_V237
							,Text_V238
							,Text_V239
							,Text_V240
							,Text_V241
							,Text_V242
							,Text_V243
							,Text_V244
							,Text_V245 
							,Text_V246
							,Text_V247
							,Text_V248
							,Text_V249
							,Text_V250
							,Text_V27
							,Text_V28
							,Text_V29
							,Text_V30
							,Text_V31
							,Text_V34
							,Text_V39
							,Text_V41
							,Text_V42
							,Text_V43
							,Text_V44
							,Text_V45
							,Text_V48
							,Text_V50
							,Text_V69
							,Text_V72
							,Text_V79
							,Text_V82
							,Post_Event_List_Txt
							,DW_SOURCE_CREATE_NM 
							FROM 
							(
							SELECT 
							s.CLICK_STREAM_INTEGRATION_ID
                            ,s.HITID_HIGH		as Hit_Id_High
							,s.HITID_LOW		as Hit_Id_Low
							,s.VISIT_PAGE_NUM	as Visit_Page_Nbr
							,s.VISIT_NUM		as Visit_Nbr
							,CASE WHEN(s.EXCLUDE_HIT= 0 and s.HIT_SOURCE not in (5, 7, 8, 9)) THEN 0 ELSE 1 END as EXCLUDE_ROW_IND 
							,s.POST_EVAR110	as Text_V110
							,s.POST_EVAR208	as Text_V208
							,s.POST_EVAR209	as Text_V209
							,s.POST_EVAR210	as Text_V210
							,s.POST_EVAR211	as Text_V211
							,s.POST_EVAR213	as Text_V213
							,s.POST_EVAR214	as Text_V214
							,s.POST_EVAR215	as Text_V215
							,s.POST_EVAR216	as Text_V216
							,s.POST_EVAR217	as Text_V217
							,s.POST_EVAR218	as Text_V218
							,s.POST_EVAR219	as Text_V219
							,s.POST_EVAR220	as Text_V220
							,s.POST_EVAR221	as Text_V221
							,s.POST_EVAR222	as Text_V222
							,s.POST_EVAR223	as Text_V223
							,s.POST_EVAR224	as Text_V224
							,s.POST_EVAR225	as Text_V225
							,s.POST_EVAR226	as Text_V226
							,s.POST_EVAR227	as Text_V227
							,s.POST_EVAR228	as Text_V228
							,s.POST_EVAR229	as Text_V229
							,s.POST_EVAR230	as Text_V230
							,s.POST_EVAR231	as Text_V231
							,s.POST_EVAR232	as Text_V232
							,s.POST_EVAR233	as Text_V233
							,s.POST_EVAR234 as Text_V234
							,s.POST_EVAR235	as Text_V235
							,s.POST_EVAR236 as Text_V236
							,s.POST_EVAR237	as Text_V237
							,s.POST_EVAR238	as Text_V238
							,s.POST_EVAR239	as Text_V239
							,s.POST_EVAR240	as Text_V240
							,s.POST_EVAR241 as Text_V241
							,s.POST_EVAR242	as Text_V242
							,s.POST_EVAR243	as Text_V243
							,s.POST_EVAR244	as Text_V244
							,s.POST_EVAR245	as Text_V245
							,s.POST_EVAR246 as Text_V246
							,s.POST_EVAR247	as Text_V247
							,s.POST_EVAR248	as Text_V248
							,s.POST_EVAR249	as Text_V249
							,s.POST_EVAR250	as Text_V250
							,s.POST_EVAR27	as Text_V27
							,s.POST_EVAR28	as Text_V28
							,s.POST_EVAR29	as Text_V29
							,s.POST_EVAR30	as Text_V30
							,s.POST_EVAR31	as Text_V31
							,s.POST_EVAR34	as Text_V34
							,s.POST_EVAR39	as Text_V39
							,s.POST_EVAR41	as Text_V41
							,s.POST_EVAR42	as Text_V42
							,s.POST_EVAR43	as Text_V43
							,s.POST_EVAR44	as Text_V44
							,s.POST_EVAR45	as Text_V45
							,s.POST_EVAR48	as Text_V48
							,s.POST_EVAR50	as Text_V50
							,s.POST_EVAR69	as Text_V69
							,s.POST_EVAR72	as Text_V72
							,s.POST_EVAR79	as Text_V79
							,s.POST_EVAR82	as Text_V82
							,s.POST_EVENT_LIST as Post_Event_List_Txt
							,s.DW_SOURCE_CREATE_NM 
							FROM 
							(
							SELECT  
							sct.CLICK_STREAM_INTEGRATION_ID
							,HITID_HIGH
							,HITID_LOW
							,VISIT_PAGE_NUM
							,VISIT_NUM
							,EXCLUDE_HIT
							,HIT_SOURCE
							,POST_EVAR110
							,POST_EVAR208
							,POST_EVAR209
							,POST_EVAR210
							,POST_EVAR211
							,POST_EVAR213
							,POST_EVAR214
							,POST_EVAR215
							,POST_EVAR216
							,POST_EVAR217
							,POST_EVAR218
							,POST_EVAR219
							,POST_EVAR220
							,POST_EVAR221
							,POST_EVAR222
							,POST_EVAR223
							,POST_EVAR224
							,POST_EVAR225
							,POST_EVAR226
							,POST_EVAR227
							,POST_EVAR228
							,POST_EVAR229	
							,POST_EVAR230
							,POST_EVAR231
							,POST_EVAR232
							,POST_EVAR233
							,POST_EVAR234
							,POST_EVAR235
							,POST_EVAR236
							,POST_EVAR237
							,POST_EVAR238
							,POST_EVAR239
							,POST_EVAR240
							,POST_EVAR241
							,POST_EVAR242
							,POST_EVAR243
							,POST_EVAR244
							,POST_EVAR245
							,POST_EVAR246
							,POST_EVAR247
							,POST_EVAR248
							,POST_EVAR249
							,POST_EVAR250
							,POST_EVAR27
							,POST_EVAR28
							,POST_EVAR29
							,POST_EVAR30
							,POST_EVAR31
							,POST_EVAR34
							,POST_EVAR39
							,POST_EVAR41
							,POST_EVAR42
							,POST_EVAR43
							,POST_EVAR44
							,POST_EVAR45
							,POST_EVAR48
							,POST_EVAR50
							,POST_EVAR69
							,POST_EVAR72
							,POST_EVAR79
							,POST_EVAR82
							,POST_EVENT_LIST
							,DW_SOURCE_CREATE_NM 
							FROM src_wrk_tbl_recs s, ${cnf_db}.${cnf_schema}.${cntrl_tbl_nm} sct
							WHERE rn = 1
								AND s.HITID_HIGH = sct.HIT_ID_HIGH   
								AND s.HITID_LOW = sct.HIT_ID_LOW  
								AND s.VISIT_PAGE_NUM = sct.VISIT_PAGE_NBR 
								AND s.VISIT_NUM = sct.VISIT_NBR 
								AND	sct.CLICK_STREAM_INTEGRATION_ID IS NOT NULL									
							) s
							
							
						) src
						
						LEFT JOIN 
                          (SELECT  DISTINCT
									 tgt.Hit_Id_High
									,tgt.Hit_Id_Low
									,tgt.Visit_Page_Nbr
									,tgt.Visit_Nbr
                          FROM ${tgt_tbl} tgt 
                          ) tgt 
                          ON tgt.Hit_Id_High = src.Hit_Id_High
						  AND tgt.Hit_Id_Low = src.Hit_Id_Low
						  AND tgt.Visit_Page_Nbr = src.Visit_Page_Nbr
						  AND tgt.Visit_Nbr = src.Visit_Nbr
                          WHERE  (tgt.Hit_Id_High IS NULL AND tgt.Hit_Id_Low IS NULL AND tgt.Visit_Page_Nbr IS NULL AND tgt.Visit_Nbr IS NULL)`;
            				
try {
snowflake.execute ({sqlText: create_tgt_wrk_table});
log('SUCCEEDED',`Successfuly created work table ${tgt_wrk_tbl}`);
	}
    catch (err) { 
	    snowflake.execute ( {sqlText: sql_ins_rerun_tbl} );
		log('FAILED',`Creation of work table ${tgt_wrk_tbl} Failed with error: ${err}`);
        return `Creation of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
				}   			

 
// Transaction for Updates, Insert begins           
    var sql_begin = "BEGIN"				
				
						
// Processing Inserts
	var sql_inserts = `INSERT INTO ${tgt_tbl} 
						(
						CLICK_STREAM_INTEGRATION_ID
                            ,Hit_Id_High
							,Hit_Id_Low
							,Visit_Page_Nbr
							,Visit_Nbr
							,EXCLUDE_ROW_IND
							,Text_V110
							,Text_V208
							,Text_V209
							,Text_V210
							,Text_V211
							,Text_V213
							,Text_V214
							,Text_V215
							,Text_V216
							,Text_V217
							,Text_V218
							,Text_V219
							,Text_V220
							,Text_V221
							,Text_V222 
							,Text_V223
							,Text_V224
							,Text_V225
							,Text_V226
							,Text_V227
							,Text_V228
							,Text_V229
							,Text_V230
							,Text_V231
							,Text_V232
							,Text_V233
							,Text_V234
							,Text_V235
							,Text_V236
							,Text_V237
							,Text_V238
							,Text_V239
							,Text_V240
							,Text_V241
							,Text_V242
							,Text_V243
							,Text_V244
							,Text_V245 
							,Text_V246
							,Text_V247
							,Text_V248
							,Text_V249
							,Text_V250
							,Text_V27
							,Text_V28
							,Text_V29
							,Text_V30
							,Text_V31
							,Text_V34
							,Text_V39
							,Text_V41
							,Text_V42
							,Text_V43
							,Text_V44
							,Text_V45
							,Text_V48
							,Text_V50
							,Text_V69
							,Text_V72
							,Text_V79
							,Text_V82
							,Post_Event_List_Txt
							,DW_CREATE_TS 
							,DW_LAST_UPDATE_TS
							,DW_LOGICAL_DELETE_IND
							,DW_SOURCE_CREATE_NM
							,DW_SOURCE_UPDATE_NM
							,DW_CURRENT_VERSION_IND
						)
				      
					    SELECT 
						CLICK_STREAM_INTEGRATION_ID
                            ,Hit_Id_High
							,Hit_Id_Low
							,Visit_Page_Nbr
							,Visit_Nbr
							,EXCLUDE_ROW_IND
							,Text_V110
							,Text_V208
							,Text_V209
							,Text_V210
							,Text_V211
							,Text_V213
							,Text_V214
							,Text_V215
							,Text_V216
							,Text_V217
							,Text_V218
							,Text_V219
							,Text_V220
							,Text_V221
							,Text_V222 
							,Text_V223
							,Text_V224
							,Text_V225
							,Text_V226
							,Text_V227
							,Text_V228
							,Text_V229
							,Text_V230
							,Text_V231
							,Text_V232
							,Text_V233
							,Text_V234
							,Text_V235
							,Text_V236
							,Text_V237
							,Text_V238
							,Text_V239
							,Text_V240
							,Text_V241
							,Text_V242
							,Text_V243
							,Text_V244
							,Text_V245 
							,Text_V246
							,Text_V247
							,Text_V248
							,Text_V249
							,Text_V250
							,Text_V27
							,Text_V28
							,Text_V29
							,Text_V30
							,Text_V31
							,Text_V34
							,Text_V39
							,Text_V41
							,Text_V42
							,Text_V43
							,Text_V44
							,Text_V45
							,Text_V48
							,Text_V50
							,Text_V69
							,Text_V72
							,Text_V79
							,Text_V82
							,Post_Event_List_Txt
							,CURRENT_TIMESTAMP as DW_CREATE_TS 
							,NULL as DW_LAST_UPDATE_TS
							,FALSE as DW_LOGICAL_DELETE_IND
							,'Adobe' as DW_SOURCE_CREATE_NM
							,NULL as DW_SOURCE_UPDATE_NM
							,TRUE as DW_CURRENT_VERSION_IND
							FROM ${tgt_wrk_tbl}`;
						
						
						
var sql_commit = "COMMIT"
var sql_rollback = "ROLLBACK"
try {
        snowflake.execute({sqlText: sql_begin});
        snowflake.execute({sqlText: sql_inserts});
        snowflake.execute({sqlText: sql_commit});
		log('COMPLETED',`Load for Click Stream Unassigned table completed`);
	}
	
    catch (err)  {
        snowflake.execute ( {sqlText: sql_rollback } );
        snowflake.execute ( {sqlText: sql_ins_rerun_tbl} );
		log('FAILED',`Loading of table ${tgt_tbl} Failed with error: ${err}`);
        return `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
        }
		
	

// ************** Load for Click Stream Unassigned table ENDs *****************


$$;
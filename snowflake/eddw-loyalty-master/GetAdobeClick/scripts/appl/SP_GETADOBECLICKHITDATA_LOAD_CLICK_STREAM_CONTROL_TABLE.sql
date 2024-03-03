--liquibase formatted sql
--changeset SYSTEM:SP_GETADOBECLICKHITDATA_LOAD_CLICK_STREAM_CONTROL_TABLE runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_CONFIRMED_PRD;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE EDM_CONFIRMED_PRD.DW_APPL.SP_GETADOBECLICKHITDATA_LOAD_CLICK_STREAM_CONTROL_TABLE(LOAD_TYPE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
		
	var cur_db = snowflake.execute( {sqlText: `Select current_database()`} ); 
	cur_db.next(); 
	var env = cur_db.getColumnValue(1);
	env = env.split('_');
	env = env[env.length - 1];
	var env_tbl_nm = `EDM_Environment_Variable_${env}`;
	var env_schema_nm = 'DW_R_MASTERDATA';
	var env_db_nm = `EDM_REFINED_${env}`; 
	var sp_name = 'sp_GetAdobeClickHitData_LOAD_CLICK_STREAM_Control_Table';
	
	try { 
		var rs = snowflake.execute( {sqlText: `SELECT * FROM ${env_db_nm}.${env_schema_nm}.${env_tbl_nm}`} ); 
		var metaparams = {};
		while (rs.next()) {
			metaparams[rs.getColumnValue(1)] = rs.getColumnValue(2);
		}
		// Global variables
		var cnf_db = metaparams['CNF_DB']; 
		var cnf_schema = metaparams['C_USER_ACT']; 
		var app_schema = metaparams['APPL']; 
		var wrk_schema = metaparams['C_STAGE'];
		var warehouse = metaparams['WAREHOUSE'];
	} catch (err) { 
		throw `Error while fetching data from EDM_Environment_Variable_${env}`;
	}
	// Global variables
	
	var load_type = LOAD_TYPE;
	var source_tbl_nm = 'CLICK_HIT_DATA'; 
	var hist_tbl_nm = 'CLICK_HIT_HISTORY';
	var param_tbl_nm = 'CLICK_STREAM_PARAM_TABLE'; 
	var tgt_name = 'CLICK_STREAM'; 
	var bod_nm = 'GetAdobeClickHitData';
	var control_tbl = 'Click_Stream_Control_Table';
	var clms_param_key = 'CLICK_HIT_COLUMNS';
    var short_bod_nm = bod_nm.substring(3);		
	var src_stream = `${cnf_db}.${app_schema}.${source_tbl_nm}_C_STREAM`;
	var hist_stream = `${cnf_db}.${app_schema}.${hist_tbl_nm}_H_STREAM`;
	var src_wrk_tbl = `${cnf_db}.${wrk_schema}.${source_tbl_nm}_wrk`;
	var src_rerun_tbl = `${cnf_db}.${wrk_schema}.${source_tbl_nm}_Rerun`;
	var tgt_control_tbl = `${cnf_db}.${cnf_schema}.${control_tbl}`;
	var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.${tgt_name}_wrk`;
	var tbl_clms = '';
	var src_tbl = '';
	
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
						     
	log('STARTED',`Load for Click Stream Control table BEGIN`);
	
	try{
	if(load_type.toLowerCase()=='hist'){
	log('SUCCEEDED','History Load type started');	
			src_tbl = hist_stream;}
	else{
	src_tbl = src_stream;
	log('SUCCEEDED','Daily Load type started');	
	}
	}	catch (err)  {
	log('FAILED',`Failed to evaluate load_type with error: ${err}`);	
        throw `Failed to evaluate load_type with error: ${err}`;   // Return an error message.
    }
	
	try{
		var param_rs = snowflake.execute( {sqlText: `SELECT * FROM ${cnf_db}.${wrk_schema}.${param_tbl_nm} where PARAM_KEY = '${clms_param_key}'`} ); 
		while (param_rs.next()) {
			tbl_clms = param_rs.getColumnValue(2);
			}
		log('SUCCEEDED',`Successfuly got column list from ${cnf_db}.${wrk_schema}.${param_tbl_nm}`);	
		}	catch (err)  {
		log('FAILED',`Failed to get column list from ${cnf_db}.${wrk_schema}.${param_tbl_nm} with error: ${err}`);	
        throw `Failed to get column list from ${cnf_db}.${wrk_schema}.${param_tbl_nm} with error: ${err}`;   // Return a error message.
    }
		
	// persist stream data in work table for the current transaction, includes data from previous failed run
	var sql_crt_src_wrk_tbl = `create or replace table ${src_wrk_tbl} as                            
							select * from ${src_tbl} 
                            UNION ALL 
                            select * from ${src_rerun_tbl}`;
    try {
        snowflake.execute ({ sqlText: sql_crt_src_wrk_tbl });
		log('SUCCEEDED',`Successfuly created Source Work table ${src_wrk_tbl}`);
    } catch (err)  {
		log('FAILED',`Creation of Source Work table ${src_wrk_tbl} Failed with error: ${err}`);
        throw `Creation of Source Work table ${src_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
    }
	
	// Empty the rerun queue table
	var sql_empty_rerun_tbl = `TRUNCATE TABLE ${src_rerun_tbl}`;
	
    try {
        snowflake.execute ({sqlText: sql_empty_rerun_tbl });
		log('SUCCEEDED',`Successfuly truncated rerun queue table ${src_rerun_tbl}`);
    } catch (err) { 
		log('FAILED',`Truncation of rerun queue table ${src_rerun_tbl} Failed with error: ${err}`);
        throw `Truncation of rerun queue table ${src_rerun_tbl} Failed with error: ${err}`;   // Return a error message.
    }
	
	// query to load rerun queue table when encountered a failure
	var sql_ins_rerun_tbl = `CREATE OR REPLACE TABLE ${src_rerun_tbl} DATA_RETENTION_TIME_IN_DAYS = 0 AS 
							            SELECT * FROM ${src_wrk_tbl}`;
										
	var sql_inserts = `INSERT INTO ${tgt_control_tbl} 
(
	 Click_Stream_Integration_Id
	,Hit_Id_High
	,Hit_Id_Low
	,Visit_Page_Nbr
	,Visit_Nbr
	,Source_Create_Ts
)
SELECT 
 (SELECT NVL(max(click_stream_integration_id),0) FROM ${tgt_control_tbl}) +
  Row_number() OVER(ORDER BY Hit_Id_High,Hit_Id_Low,Visit_Page_Nbr,Visit_Nbr ASC) AS Click_Stream_Integration_Id
 ,Hit_Id_High
 ,Hit_Id_Low
 ,Visit_Page_Nbr
 ,Visit_Nbr 
 ,DW_CREATETS
from
(
  SELECT DISTINCT     
	 src.Hit_Id_High
	,src.Hit_Id_Low
	,src.Visit_Page_Nbr
	,src.Visit_Nbr
	,src.DW_CREATETS
	,src.rn
FROM
(
SELECT DISTINCT  
       HitId_High as Hit_Id_High
      ,HitId_Low as Hit_Id_Low
      ,Visit_Page_NUM as Visit_Page_Nbr
      ,Visit_Num as Visit_Nbr
	  ,DW_CREATETS
	  ,row_number() over(partition by HITID_HIGH,HITID_LOW,VISIT_PAGE_NUM,VISIT_NUM  
										  ORDER BY(DW_CREATETS) DESC) as rn
FROM  ${src_wrk_tbl} 
WHERE (HitId_High IS NOT NULL OR HitId_Low IS NOT NULL OR Visit_Page_NUM IS NOT NULL OR Visit_Num IS NOT NULL)
)src
LEFT  JOIN
(
SELECT 
	 Hit_Id_High
	,Hit_Id_Low
	,Visit_Page_Nbr
	,Visit_Nbr 
FROM ${tgt_control_tbl}
)tgt
ON src.Hit_Id_High = tgt.Hit_Id_High
AND src.Hit_Id_Low = tgt.Hit_Id_Low
AND src.Visit_Page_Nbr = tgt.Visit_Page_Nbr
AND src.Visit_Nbr = tgt.Visit_Nbr
WHERE rn = 1  and 
(tgt.Hit_Id_High IS NULL AND tgt.Hit_Id_Low IS NULL AND tgt.Visit_Page_Nbr IS NULL AND tgt.Visit_Nbr IS NULL)
)`

	// Empty the rerun queue table
	var sql_empty_tgt_wrk_tbl = `TRUNCATE TABLE ${tgt_wrk_tbl}`;
	
    try {
        snowflake.execute ({sqlText: sql_empty_tgt_wrk_tbl });
		log('SUCCEEDED',`Successfuly truncated work table ${tgt_wrk_tbl}`);
    } catch (err) { 
		log('FAILED',`Truncation of rerun queue table ${tgt_wrk_tbl} Failed with error: ${err}`);
        throw `Truncation of rerun queue table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
    }
	
	var sql_crt_tgt_wrk_tbl = `insert into ${tgt_wrk_tbl} (${tbl_clms}) select ${tbl_clms} from ${src_wrk_tbl}`;  
	
	var sql_begin = "BEGIN"
	var sql_commit = "COMMIT"
	var sql_rollback = "ROLLBACK"	
	
	try {
		snowflake.execute({sqlText: sql_begin});
        snowflake.execute({sqlText: sql_inserts});	
		log('SUCCEEDED',`Successfuly inserted records in table ${tgt_control_tbl}`);
		try {
			snowflake.execute ({ sqlText: sql_crt_tgt_wrk_tbl });
			snowflake.execute({sqlText: sql_commit});
			log('SUCCEEDED',`Successfuly created Work table ${tgt_wrk_tbl}`);
			log('COMPLETED',`Load for Click Stream Control completed`);
		} catch (err)  {
			log('FAILED',`Creation of Source Work table ${tgt_wrk_tbl} Failed with error: ${err}`);
			snowflake.execute({sqlText: sql_rollback });
			snowflake.execute ( {sqlText: sql_ins_rerun_tbl} );
			throw `Creation of Source Work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
		}
		
    } catch (err) {         
		snowflake.execute({sqlText: sql_rollback });
		snowflake.execute ( {sqlText: sql_ins_rerun_tbl} );
		log('FAILED',`Loading of table ${tgt_control_tbl} Failed with error: ${err}`);
        throw `Loading of table ${tgt_control_tbl} Failed with error: ${err}`;   // Return a error message.
    }	
						

$$;
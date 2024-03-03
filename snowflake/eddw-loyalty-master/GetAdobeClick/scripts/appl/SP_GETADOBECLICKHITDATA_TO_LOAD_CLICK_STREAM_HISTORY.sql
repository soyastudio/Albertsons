--liquibase formatted sql
--changeset SYSTEM:SP_GETADOBECLICKHITDATA_TO_LOAD_CLICK_STREAM_HISTORY runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_CONFIRMED_PRD;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE EDM_CONFIRMED_PRD.DW_APPL.SP_GETADOBECLICKHITDATA_TO_LOAD_CLICK_STREAM_HISTORY(CNF_DB VARCHAR, C_STAGE VARCHAR, C_USER_ACT VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
	
	var cnf_db = CNF_DB;
	var wrk_schema = C_STAGE;
	var c_act_schema = C_USER_ACT;
	var hist_ctrl_tbl = 'CLICK_STREAM_HISTORY_CONTROL_TABLE';
	var hist_tbl = 'CLICK_HIT_HISTORY';
	var click_hit_tbl = 'CLICK_HIT_DATA';
	var seq_ctrl_tbl = 'CLICK_STREAM_CONTROL_TABLE';
	var sp_name = 'sp_GetAdobeClickHitData_TO_LOAD_CLICK_STREAM_HISTORY';
	var edt = '';
	var sdt = '';
	var cnt = 1;	
	
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

log('STARTED','Load for Click Stream History table BEGIN');

try { 
		var rs = snowflake.execute( {sqlText: `SELECT DATE(DATEADD(DAY,-COUNTER,END_DATE)) AS START_DATE,END_DATE,COUNTER from(
select top 1 * from ${cnf_db}.${wrk_schema}.${hist_ctrl_tbl} WHERE START_DATE IS NULL 
order by DW_CREATE_TS desc)`} ); 
		while (rs.next()) {
			sdt = rs.getColumnValueAsString('START_DATE');
			edt = rs.getColumnValueAsString('END_DATE');
			cnt = rs.getColumnValue('COUNTER');
		}
	log('SUCCEEDED',`Successfuly read table records s_date: ${sdt} e_date: ${edt} counter: ${cnt}`);		
	} catch (err) { 
	log('FAILED',`Fail to read table records with error: ${err}`);		
		return `Fail to read table records with error: ${err}`;
	}
	

//truncate history table 
var trun_hist = `TRUNCATE ${cnf_db}.${wrk_schema}.${hist_tbl}`;

 try {
        snowflake.execute({ sqlText: trun_hist });
		log('SUCCEEDED',`Successfuly truncated history table ${cnf_db}.${wrk_schema}.${hist_tbl}`);
    } catch (err)  {
		log('FAILED',`Failed to truncate history table ${cnf_db}.${wrk_schema}.${hist_tbl} with error: ${err}`);
        throw `Failed to truncate history table ${cnf_db}.${wrk_schema}.${hist_tbl} with error: ${err}`;   // Return a error message.
    }

var row_count = 0;

var hit_tbl_row = `SELECT H.*
FROM ${cnf_db}.${c_act_schema}.${click_hit_tbl} H
LEFT JOIN  ${cnf_db}.${c_act_schema}.${seq_ctrl_tbl} S
ON  H.HITID_HIGH = S.HIT_ID_HIGH   
      AND H.HITID_LOW = S.HIT_ID_LOW  
      AND H.VISIT_PAGE_NUM = S.VISIT_PAGE_NBR 
      AND H.VISIT_NUM = S.VISIT_NBR 
WHERE
H.DW_CREATETS >= '${sdt}' AND H.DW_CREATETS <'${edt}'
AND S.CLICK_STREAM_INTEGRATION_ID IS NULL`;	

var hit_tbl_cnt = `SELECT COUNT(*) AS CNT FROM (${hit_tbl_row})`;	

try { 
		var rs = snowflake.execute( {sqlText: `${hit_tbl_cnt}`} ); 
		while (rs.next()) {
			row_count = rs.getColumnValueAsString('CNT');
		}
	log('SUCCEEDED',`Successfuly read inserted row count from  ${cnf_db}.${wrk_schema}.${hist_tbl}: ${row_count} rows inserted`);		
	} catch (err) { 
	log('FAILED',`Fail to read inserted row count from  ${cnf_db}.${wrk_schema}.${hist_tbl}: ${row_count} with error: ${err}`);		
	}		

var upd_ctrl_tbl = `UPDATE ${cnf_db}.${wrk_schema}.${hist_ctrl_tbl} SET
					START_DATE = DATE('${sdt}')
				   ,DW_UPDATE_TS = current_timestamp()
				   ,RECORDS_COUNT = ${row_count}
				   WHERE END_DATE = DATE('${edt}') AND START_DATE IS NULL `;
				   
var ins_ctrl_tbl = `INSERT INTO ${cnf_db}.${wrk_schema}.${hist_ctrl_tbl} 
					(
					 END_DATE
					,COUNTER
					)
					VALUES
					(
					 DATE('${sdt}')
					,${cnt}
					)`

//Insert Statement				
var sql_inserts = `INSERT INTO ${cnf_db}.${wrk_schema}.${hist_tbl} ${hit_tbl_row}`	

// Transaction for Updates, Insert begins           
var sql_begin = "BEGIN"					
var sql_commit = "COMMIT"
var sql_rollback = "ROLLBACK"

try {
        snowflake.execute({sqlText: sql_begin});
		snowflake.execute({sqlText: sql_inserts});
		snowflake.execute({sqlText: upd_ctrl_tbl});
		snowflake.execute({sqlText: ins_ctrl_tbl});        
        snowflake.execute({sqlText: sql_commit});
		log('COMPLETED',`Load for Click Stream ${hist_tbl} table completed`);
	}	
    catch (err)  {
        snowflake.execute({sqlText: sql_rollback });
		log('FAILED',`Loading of table ${hist_tbl} Failed with error: ${err}`);
        return `Loading of table ${hist_tbl} Failed with error: ${err}` ;   // Return a error message.
        }		

	var call_hist_sp = `call sp_GetAdobeClickHitData_LOAD_CLICK_STREAM_Control_Table('hist')`;

	 try {
		  snowflake.execute({ sqlText: call_hist_sp });
		  log('SUCCEEDED',`Successfuly completed historical control table load from the task`);
		  }
		  catch (err)  {
			log('FAILED',`Historical load from the task Failed with error: ${err}`);
			}	

$$;
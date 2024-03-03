--liquibase formatted sql
--changeset SYSTEM:SP_GETADOBECLICKHITDATA_CLICK_STREAM_SOURCE_STREAM_DDL runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_CONFIRMED_PRD;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE EDM_CONFIRMED_PRD.DW_APPL.SP_GETADOBECLICKHITDATA_CLICK_STREAM_SOURCE_STREAM_DDL()
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

	try { 
		var rs = snowflake.execute( {sqlText: `SELECT * FROM ${env_db_nm}.${env_schema_nm}.${env_tbl_nm}`} ); 
		var metaparams = {};
		while (rs.next()) {
			metaparams[rs.getColumnValue(1)] = rs.getColumnValue(2);
		}
		// Global variables
		var cnf_db = metaparams['CNF_DB']; 
		var cnf_schema = metaparams['C_USER_ACT']; 
		var analytic_db= metaparams['ANLYS_DB']; 
		var app_schema = metaparams['APPL']; 
		var wrk_schema = metaparams['C_STAGE'];
		var warehouse = metaparams['WAREHOUSE'];
		
	} catch (err) { 
		throw `Error while fetching data from EDM_Environment_Variable_${env}`;
	}

	// Global variables
	var source_tbl_nm = 'CLICK_HIT_DATA'; 
	var hist_tbl_nm = 'CLICK_HIT_HISTORY';
	var other_tbl_nm='Click_Stream_Other'; 
	var shop_tbl_nm='Click_Stream_Shop'; 
	var loyalty_tbl_nm='Click_Stream_Loyalty'; 
	var metrics_tbl_nm='Click_Stream_Metrics'; 
	var unassigned_tbl_nm='Click_Stream_Unassigned';
	var param_tbl_nm = 'CLICK_STREAM_PARAM_TABLE'; 
	var tgt_name = 'CLICK_STREAM';
	var bod_nm = 'GetAdobeClickHitData';
    var clms_param_key = 'CLICK_HIT_COLUMNS';
	var short_bod_nm = bod_nm.substring(3);	
	var source_stream_tbl = `${cnf_db}.${app_schema}.${source_tbl_nm}_C_STREAM`;
	var hist_stream_tbl = `${cnf_db}.${app_schema}.${hist_tbl_nm}_H_STREAM`;
	var source_stream_rerun_tbl = `${cnf_db}.${wrk_schema}.${source_tbl_nm}_Rerun`;
	var click_other_stream_rerun_tbl = `${cnf_db}.${wrk_schema}.${other_tbl_nm}_Rerun`;
	var click_shop_stream_rerun_tbl = `${cnf_db}.${wrk_schema}.${shop_tbl_nm}_Rerun`;
	var click_loyalty_stream_rerun_tbl = `${cnf_db}.${wrk_schema}.${loyalty_tbl_nm}_Rerun`;
	var click_metrics_stream_rerun_tbl = `${cnf_db}.${wrk_schema}.${metrics_tbl_nm}_Rerun`;
	var unassigned_stream_rerun_tbl = `${cnf_db}.${wrk_schema}.${unassigned_tbl_nm}_Rerun`;
	var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.${tgt_name}_wrk`;	
	var click_other_stream = `${cnf_db}.${app_schema}.CLICK_HIT_OTHER_C_STREAM`;
	var click_shop_stream = `${cnf_db}.${app_schema}.CLICK_HIT_SHOP_C_STREAM`;
	var click_loyalty_stream = `${cnf_db}.${app_schema}.CLICK_HIT_LOYALTY_C_STREAM`;
	var click_metrics_stream = `${cnf_db}.${app_schema}.CLICK_HIT_METRICS_C_STREAM`;	
	var click_unassigned_stream = `${cnf_db}.${app_schema}.CLICK_HIT_UNASSIGNED_C_STREAM`;
	var tbl_clms = '';
		
	try
	{
		var del_click_hit_clms = `delete from ${cnf_db}.${wrk_schema}.${param_tbl_nm} where PARAM_KEY = '${clms_param_key}'`;
		snowflake.execute ( {sqlText: del_click_hit_clms} );
	}
	 catch (err)  {    
        throw `deletion of CLICK_HIT_COLUMNS Failed with error: ${err}`;   // Return an error message.
    }
	
	try
	{
		var create_hist_tbl = `create table if not exists ${cnf_db}.${wrk_schema}.${hist_tbl_nm} as 
		select * from ${cnf_db}.${cnf_schema}.${source_tbl_nm} where 1=2`;
		snowflake.execute ( {sqlText: create_hist_tbl} );
	}
	 catch (err)  {    
        throw `creation of history Failed with error: ${err}`;   // Return an error message.
    }
	
	// To alter table
	var alter_stmt = `ALTER TABLE ${cnf_db}.${wrk_schema}.${hist_tbl_nm} SET CHANGE_TRACKING = TRUE;`

	try {
        snowflake.execute({ sqlText: alter_stmt });
    }
    catch (err)  {
        throw `alter of Table Failed with error: ${err}`;   // Return a error message.
    }
		
	try
	{	
		var click_hit_clms = snowflake.execute ({ sqlText: `select listagg(column_name,',') from(
		select column_name from information_schema.columns where table_catalog = '${cnf_db}'
		and TABLE_SCHEMA = '${cnf_schema}' and  table_name = '${source_tbl_nm}')` });

		while (click_hit_clms.next()) {
				tbl_clms = click_hit_clms.getColumnValue(1); 				
				}		
		var sql_crt_tgt_wrk_tbl = `insert into ${cnf_db}.${wrk_schema}.${param_tbl_nm}(PARAM_KEY,PARAM_VALUE,DW_CREATE_TS) VALUES 
		('${clms_param_key}','${tbl_clms}',current_timestamp())`;  
		snowflake.execute ( {sqlText: sql_crt_tgt_wrk_tbl} );
		
    } catch (err)  {    
        throw `insertion of CLICK_HIT_COLUMNS Failed with error: ${err}`;   // Return a error message.
    }	
	
	// Create Stream on ClickHitTable table
	var create_stream_on_source = `CREATE OR REPLACE STREAM ${source_stream_tbl} ON TABLE ${cnf_db}.${cnf_schema}.${source_tbl_nm} append_only = false`;
	try {
        snowflake.execute({ sqlText: create_stream_on_source });
    } catch (err) {
        throw `Creation of stream on ${source_tbl_nm} table failed with error ${create_stream_on_source} on table  ${cnf_db}.${cnf_schema}.${source_tbl_nm} : ${err}`;   // Return a error message.
    }
	
	// Create Stream on ClickHitHistory Table 
	var create_stream_on_hist = `CREATE OR REPLACE STREAM ${hist_stream_tbl} ON TABLE ${cnf_db}.${wrk_schema}.${hist_tbl_nm} append_only = true`;
	try {
        snowflake.execute({ sqlText: create_stream_on_hist });
    } catch (err) {
        throw `Creation of stream on ${hist_tbl_nm} table failed with error ${create_stream_on_hist} on table  ${cnf_db}.${wrk_schema}.${hist_tbl_nm} : ${err}`;   // Return a error message.
    }
  	 
	// check if rerun queue table exists otherwise create it
	var source_sql_crt_rerun_tbl = `CREATE TABLE IF NOT EXISTS ${source_stream_rerun_tbl} DATA_RETENTION_TIME_IN_DAYS = 0 
      AS SELECT * FROM ${source_stream_tbl} where 1=2`;
	try {
		snowflake.execute({ sqlText: source_sql_crt_rerun_tbl });
	} catch (err) {
		throw `Creation of rerun queue table ${source_stream_rerun_tbl} Failed with error ${source_sql_crt_rerun_tbl}: ${err}`;   // Return a error message.
	}
		
	var tgt_crt_rerun_tbl = `CREATE TABLE IF NOT EXISTS ${tgt_wrk_tbl} DATA_RETENTION_TIME_IN_DAYS = 0 
      AS SELECT ${tbl_clms} FROM ${source_stream_rerun_tbl} where 1=2 `;
	try {
		snowflake.execute({ sqlText: tgt_crt_rerun_tbl });
	} catch (err) {
		throw `Creation of rerun queue table ${tgt_wrk_tbl} Failed with error ${tgt_crt_rerun_tbl}: ${err}`;   // Return a error message.
	}
	
	// To alter table
	var alter_stmt = `ALTER TABLE ${tgt_wrk_tbl} SET CHANGE_TRACKING = TRUE;`

	try {
        snowflake.execute({ sqlText: alter_stmt });
    }
    catch (err)  {
        throw `alter of Table Failed with error: ${err}`;   // Return a error message.
    }
	
	// Create Stream on ClickHitOtherTable table
	var create_stream_on_other = `CREATE OR REPLACE STREAM ${click_other_stream} ON TABLE ${tgt_wrk_tbl} append_only = false`;
	try {
        snowflake.execute({ sqlText: create_stream_on_other });
    } catch (err) {
        throw `Creation of stream on ${click_other_stream} table failed with error ${create_stream_on_other} on table ${tgt_wrk_tbl} : ${err}`;   // Return a error message.
    }
	
	// check if rerun queue table exists otherwise create it
	var other_sql_crt_rerun_tbl = `CREATE TABLE IF NOT EXISTS ${click_other_stream_rerun_tbl} DATA_RETENTION_TIME_IN_DAYS = 0 AS SELECT * FROM ${click_other_stream} where 1=2 `;
	try {
        snowflake.execute({ sqlText: other_sql_crt_rerun_tbl });
    }
    catch (err)  {
        throw `Creation of rerun queue table ${other_sql_crt_rerun_tbl} Failed with error: ${err}`;   // Return a error message.
    }
	
	// Create Stream on ClickHitShopTable table
	var create_stream_on_shop = `CREATE OR REPLACE STREAM ${click_shop_stream} ON TABLE ${tgt_wrk_tbl} append_only = false`;
	try {
        snowflake.execute({ sqlText: create_stream_on_shop });
    } catch (err) {
         throw `Creation of rerun queue table ${create_stream_on_shop} Failed with error: ${err}`;   // Return a error message.
    }
	
	// check if rerun queue table exists otherwise create it
	var shop_sql_crt_rerun_tbl = `CREATE TABLE IF NOT EXISTS ${click_shop_stream_rerun_tbl} DATA_RETENTION_TIME_IN_DAYS = 0 AS SELECT * FROM ${click_shop_stream} where 1=2 `;
	try {
        snowflake.execute({ sqlText: shop_sql_crt_rerun_tbl });
    }
    catch (err)  {
        throw `Creation of rerun queue table ${shop_sql_crt_rerun_tbl} Failed with error: ${err}`;   // Return a error message.
    }
	
	// Create Stream on ClickHitLoayltyTable table
	var create_stream_on_loyalty = `CREATE OR REPLACE STREAM ${click_loyalty_stream} ON TABLE ${tgt_wrk_tbl} append_only = false`;
	try {
        snowflake.execute({ sqlText: create_stream_on_loyalty });
    } catch (err) {
        throw `Creation of stream on ${tgt_wrk_tbl} table failed with error ${create_stream_on_loyalty} on table ${tgt_wrk_tbl} : ${err}`;   // Return a error message.
    }
	
	// check if rerun queue table exists otherwise create it
	var loyalty_sql_crt_rerun_tbl = `CREATE TABLE IF NOT EXISTS ${click_loyalty_stream_rerun_tbl} DATA_RETENTION_TIME_IN_DAYS = 0 AS SELECT * FROM ${click_loyalty_stream} where 1=2 `;
	try {
        snowflake.execute({ sqlText: loyalty_sql_crt_rerun_tbl });
    }
    catch (err)  {
        throw `Creation of rerun queue table ${loyalty_sql_crt_rerun_tbl} Failed with error: ${err}`;   // Return a error message.
    }
	
	// Create Stream on ClickHitMetricsTable table
	var create_stream_on_metrics = `CREATE OR REPLACE STREAM ${click_metrics_stream} ON TABLE ${tgt_wrk_tbl} append_only = false`;
	try {
        snowflake.execute({ sqlText: create_stream_on_metrics });
    } catch (err) {
        throw `Creation of stream on ${tgt_wrk_tbl} table failed with error ${create_stream_on_metrics} on table ${tgt_wrk_tbl} : ${err}`;   // Return a error message.
    }
	
	// check if rerun queue table exists otherwise create it
	var metrics_sql_crt_rerun_tbl = `CREATE TABLE IF NOT EXISTS ${click_metrics_stream_rerun_tbl} DATA_RETENTION_TIME_IN_DAYS = 0 AS SELECT * FROM ${click_metrics_stream} where 1=2 `;
	try {
        snowflake.execute({ sqlText: metrics_sql_crt_rerun_tbl });
    }
    catch (err)  {
        throw `Creation of rerun queue table ${metrics_sql_crt_rerun_tbl} Failed with error: ${err}`;   // Return a error message.
    }
		
	// Create Stream on ClickHitUnassignedTable table
	var create_stream_on_unassigned = `CREATE OR REPLACE STREAM ${click_unassigned_stream} ON TABLE ${tgt_wrk_tbl} append_only = false`;
	try {
        snowflake.execute({ sqlText: create_stream_on_unassigned });
    } catch (err) {
        throw `Creation of stream on ${tgt_wrk_tbl} table failed with error ${create_stream_on_unassigned} on table ${tgt_wrk_tbl} : ${err}`;   // Return a error message.
    }
	
	// check if rerun queue table exists otherwise create it
	var unassigned_sql_crt_rerun_tbl = `CREATE TABLE IF NOT EXISTS ${unassigned_stream_rerun_tbl} DATA_RETENTION_TIME_IN_DAYS = 0 AS SELECT * FROM ${click_unassigned_stream} where 1=2 `;
	try {
        snowflake.execute({ sqlText: unassigned_sql_crt_rerun_tbl });
    }
    catch (err)  {
        throw `Creation of rerun queue table ${unassigned_sql_crt_rerun_tbl} Failed with error: ${err}`;   // Return a error message.
    }	
	
// Task for Click_Stream_Control_Table_Load Table for incremental run
	var create_task = `CREATE OR REPLACE TASK Click_Stream_Control_Table_Daily_Load_TASK
	WAREHOUSE = '${warehouse}'
	SCHEDULE = '120 minute'
	WHEN
	SYSTEM$STREAM_HAS_DATA('${source_stream_tbl}')
	AS 
	call sp_GetAdobeClickHitData_LOAD_CLICK_STREAM_Control_Table('daily');`
	
	try {
        snowflake.execute({ sqlText: create_task });
    }
    catch (err)  {
        throw `Creation of Task ${sqlText} Failed with error: ${err}`;   // Return a error message.
    }
	
// Task for Click_Stream_Control_Table_Load Table for hitorical run
	var create_task = `CREATE OR REPLACE TASK Click_Stream_Control_Table_History_Load_TASK
	WAREHOUSE = '${warehouse}'
	SCHEDULE = '120 minute'
	WHEN
	SYSTEM$STREAM_HAS_DATA('${hist_stream_tbl}')
	AS 
	call sp_GetAdobeClickHitData_LOAD_CLICK_STREAM_Control_Table('hist');`
	
	try {
        snowflake.execute({ sqlText: create_task });
    }
    catch (err)  {
        throw `Creation of Task ${sqlText} Failed with error: ${err}`;   // Return a error message.
    }
		
// Task for Click_Stream_Unassigned Table
	var create_task = `CREATE OR REPLACE TASK Click_Stream_Unassigned_Load_TASK
	WAREHOUSE = '${warehouse}'
	SCHEDULE = '1 minute'
	WHEN
	SYSTEM$STREAM_HAS_DATA('${click_unassigned_stream}')
	AS 
	call SP_GetAdobeClickHitData_TO_BIM_LOAD_Click_Stream_Unassigned('${click_unassigned_stream}','${cnf_db}','${cnf_schema}','${wrk_schema}');`
	
	try {
        snowflake.execute({ sqlText: create_task });
    }
    catch (err)  {
        throw `Creation of Task ${sqlText} Failed with error: ${err}`;   // Return a error message.
    }	
		
// Task for Click_Stream_Shop Table
	var create_task1 = `CREATE OR REPLACE TASK Click_Stream_Shop_Load_TASK
	WAREHOUSE = '${warehouse}'
	SCHEDULE = '1 minute'
	WHEN
	SYSTEM$STREAM_HAS_DATA('${click_shop_stream}')
	AS 
	call SP_GetAdobeClickHitData_TO_BIM_LOAD_Click_Stream_Shop('${click_shop_stream}','${cnf_db}','${cnf_schema}','${wrk_schema}');`
	
	try {
        snowflake.execute({ sqlText: create_task1 });
    }
    catch (err)  {
        throw `Creation of Task ${sqlText} Failed with error: ${err}`;   // Return a error message.
    }
			
// Task for Click_Stream_Loyalty Table
	var create_task1 = `CREATE OR REPLACE TASK Click_Stream_Loyalty_Load_TASK
	WAREHOUSE = '${warehouse}'
	SCHEDULE = '1 minute'
	WHEN
	SYSTEM$STREAM_HAS_DATA('${click_loyalty_stream}')
	AS 
	call  SP_GetAdobeClickHitData_TO_BIM_LOAD_Click_Stream_Loyalty('${click_loyalty_stream}','${cnf_db}','${cnf_schema}','${wrk_schema}');`

	
	try {
        snowflake.execute({ sqlText: create_task1 });
    }
    catch (err)  {
        throw `Creation of Task ${sqlText} Failed with error: ${err}`;   // Return a error message.
    }
			
// Task for Click_Stream_Metrics Table
	var create_task1 = `CREATE OR REPLACE TASK Click_Stream_Metrics_Load_TASK
	WAREHOUSE = '${warehouse}'
	SCHEDULE = '1 minute'
	WHEN
	SYSTEM$STREAM_HAS_DATA('${click_metrics_stream}')
	AS 
	call  SP_GetAdobeClickHitData_TO_BIM_LOAD_Click_Stream_Metrics('${click_metrics_stream}','${cnf_db}','${cnf_schema}','${wrk_schema}');`

	
	try {
        snowflake.execute({ sqlText: create_task1 });
    }
    catch (err)  {
        throw `Creation of Task ${sqlText} Failed with error: ${err}`;   // Return a error message.
    }
   
   // Task for Click_Stream_Other Table
	var create_task1 = `CREATE OR REPLACE TASK Click_Stream_Other_Load_TASK
	WAREHOUSE = '${warehouse}'
	SCHEDULE = '1 minute'
	WHEN
	SYSTEM$STREAM_HAS_DATA('${click_other_stream}')
	AS 
	call  SP_GetAdobeClickHitData_TO_BIM_LOAD_Click_Stream_Other('${click_other_stream}','${cnf_db}','${cnf_schema}','${wrk_schema}');`

	try {
        snowflake.execute({ sqlText: create_task1 });
    }
    catch (err)  {
        throw `Creation of Task ${sqlText} Failed with error: ${err}`;   // Return a error message.
    }

$$;
--liquibase formatted sql
--changeset SYSTEM:SP_CREATE_TASKS_LOOKUP_TABLES runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_CONFIRMED_PRD;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE EDM_CONFIRMED_PRD.DW_APPL.SP_CREATE_TASKS_LOOKUP_TABLES()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

    // Get Metadata from EDM_Environment_Variable Table 
    
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
        while (rs.next()){
            metaparams[rs.getColumnValue(1)] = rs.getColumnValue(2);
        }
        var cnf_src_db = metaparams['CNF_DB'];
		var cnf_tgt_db = metaparams['CNF_DB'];
        var cnf_src_schema = metaparams['C_PROD'];
		var cnf_tgt_schema = metaparams['C_USER_ACT'];
        var wrk_schema = metaparams['C_STAGE'];
        var app_schema = metaparams['APPL']; 
        var views_db = `${metaparams['VIEWS_DB']}_${env}`;
        var views_schema = metaparams['VIEWS'];
        var warehouse = metaparams['WAREHOUSE'];
    } catch (err) {
        throw `Error while fetching data from EDM_Environment_Variable_${env}`;
    }


// Task for Click_Stream_Click_Event Table
 var create_task = `CREATE OR REPLACE  TASK Click_Stream_Click_Event_C_TASK
 WAREHOUSE = '${warehouse}'
 SCHEDULE = '480 minute'
 AS
 call SP_GetAdobeClickHitData_TO_BIM_LOAD_Click_Stream_Click_Event('${views_db}.${views_schema}.CLICK_EVENT','${cnf_tgt_db}','${cnf_tgt_schema}','${wrk_schema}');`

try {
        snowflake.execute({ sqlText: create_task });
    }
    catch (err)  {
        throw `Creation of Task Failed with error: ${err}`;   // Return a error message.
    }	
	
// Task for Click_Stream_Connection_Type Table
 var create_task = `CREATE OR REPLACE  TASK Click_Stream_Connection_Type_C_TASK
 WAREHOUSE = '${warehouse}'
 SCHEDULE = '480 minute'
 AS
 call SP_GetAdobeClickHitData_TO_BIM_LOAD_Click_Stream_Connection_Type('${views_db}.${views_schema}.CLICK_CONNECTIONTYPE','${cnf_tgt_db}','${cnf_tgt_schema}','${wrk_schema}');`

try {
        snowflake.execute({ sqlText: create_task });
    }
    catch (err)  {
        throw `Creation of Task Failed with error: ${err}`;   // Return a error message.
    }
	
// Task for Click_Stream_Country Table
 var create_task = `CREATE OR REPLACE  TASK Click_Stream_Country_C_TASK
 WAREHOUSE = '${warehouse}'
 SCHEDULE = '480 minute'
 AS
 call SP_GetAdobeClickHitData_TO_BIM_LOAD_Click_Stream_Country('${views_db}.${views_schema}.CLICK_COUNTRY','${cnf_tgt_db}','${cnf_tgt_schema}','${wrk_schema}');`

try {
        snowflake.execute({ sqlText: create_task });
    }
    catch (err)  {
        throw `Creation of Task Failed with error: ${err}`;   // Return a error message.
    }
	

// Task for Click_Stream_Language Table
 var create_task = `CREATE OR REPLACE  TASK Click_Stream_Language_C_TASK
 WAREHOUSE = '${warehouse}'
 SCHEDULE = '480 minute'
 AS
 call SP_GetAdobeClickHitData_TO_BIM_LOAD_Click_Stream_Language('${views_db}.${views_schema}.CLICK_LANGUAGE','${cnf_tgt_db}','${cnf_tgt_schema}','${wrk_schema}');`

try {
        snowflake.execute({ sqlText: create_task });
    }
    catch (err)  {
        throw `Creation of Task Failed with error: ${err}`;   // Return a error message.
    }

// Task for Click_Stream_Operating_System Table
 var create_task = `CREATE OR REPLACE  TASK Click_Stream_Operating_System_C_TASK
 WAREHOUSE = '${warehouse}'
 SCHEDULE = '480 minute'
 AS
 call SP_GetAdobeClickHitData_TO_BIM_LOAD_Click_Stream_Operating_System('${views_db}.${views_schema}.CLICK_OPERATINGSYSTEM','${cnf_tgt_db}','${cnf_tgt_schema}','${wrk_schema}');`

try {
        snowflake.execute({ sqlText: create_task });
    }
    catch (err)  {
        throw `Creation of Task Failed with error: ${err}`;   // Return a error message.
    }

// Task for Click_Stream_Plugin Table
 var create_task = `CREATE OR REPLACE  TASK Click_Stream_Plugin_C_TASK
 WAREHOUSE = '${warehouse}'
 SCHEDULE = '480 minute'
 AS
 call SP_GetAdobeClickHitData_TO_BIM_LOAD_Click_Stream_Plugin('${views_db}.${views_schema}.CLICK_PLUGIN','${cnf_tgt_db}','${cnf_tgt_schema}','${wrk_schema}');`

try {
        snowflake.execute({ sqlText: create_task });
    }
    catch (err)  {
        throw `Creation of Task Failed with error: ${err}`;   // Return a error message.
    }
	
// Task for Click_Stream_Resolution Table
 var create_task = `CREATE OR REPLACE  TASK Click_Stream_Resolution_C_TASK
 WAREHOUSE = '${warehouse}'
 SCHEDULE = '480 minute'
 AS
 call SP_GetAdobeClickHitData_TO_BIM_LOAD_Click_Stream_Resolution('${views_db}.${views_schema}.CLICK_RESOLUTION','${cnf_tgt_db}','${cnf_tgt_schema}','${wrk_schema}');`

try {
        snowflake.execute({ sqlText: create_task });
    }
    catch (err)  {
        throw `Creation of Task Failed with error: ${err}`;   // Return a error message.
    }
	
	
// Task for Click_Stream_Search_Engine Table
 var create_task = `CREATE OR REPLACE  TASK Click_Stream_Search_Engine_C_TASK
 WAREHOUSE = '${warehouse}'
 SCHEDULE = '480 minute'
 AS
 call SP_GetAdobeClickHitData_TO_BIM_LOAD_Click_Stream_Search_Engine('${views_db}.${views_schema}.CLICK_SEARCHENGINE','${cnf_tgt_db}','${cnf_tgt_schema}','${wrk_schema}');`

try {
        snowflake.execute({ sqlText: create_task });
    }
    catch (err)  {
        throw `Creation of Task Failed with error: ${err}`;   // Return a error message.
    }	
	
// Task for Click_Stream_Browser Table
 var create_task = `CREATE OR REPLACE  TASK Click_Stream_Browser_C_TASK
 WAREHOUSE = '${warehouse}'
 SCHEDULE = '480 minute'
 AS
 call SP_GetAdobeClickHitData_TO_BIM_LOAD_Click_Stream_Browser('${views_db}.${views_schema}.CLICK_BROWSER','${cnf_tgt_db}','${cnf_tgt_schema}','${wrk_schema}');`

try {
        snowflake.execute({ sqlText: create_task });
    }
    catch (err)  {
        throw `Creation of Task Failed with error: ${err}`;   // Return a error message.
    }
				     
// Task for Click_Stream_Referrer_Type Table
 var create_task = `CREATE OR REPLACE  TASK Click_Stream_Referrer_Type_C_TASK
 WAREHOUSE = '${warehouse}'
 SCHEDULE = '480 minute'
 AS
 call SP_GetAdobeClickHitData_TO_BIM_LOAD_Click_Stream_Referrer_Type('${views_db}.${views_schema}.CLICK_REFERRERTYPE','${cnf_tgt_db}','${cnf_tgt_schema}','${wrk_schema}');`

try {
        snowflake.execute({ sqlText: create_task });
    }
    catch (err)  {
        throw `Creation of Task Failed with error: ${err}`;   // Return a error message.
    }


$$;
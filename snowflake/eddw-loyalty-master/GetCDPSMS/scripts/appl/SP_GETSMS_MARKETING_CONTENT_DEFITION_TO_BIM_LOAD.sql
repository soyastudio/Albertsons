--liquibase formatted sql
--changeset SYSTEM:SP_GETSMS_MARKETING_CONTENT_DEFITION_TO_BIM_LOAD runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETSMS_MARKETING_CONTENT_DEFITION_TO_BIM_LOAD()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

   var sub_proc_nms = ['SP_GetSMSdetails_TO_BIM_LOAD_Marketing_Content_Definition'
					  ]
    	
	
	// Get Metadata from EDM_Environment_Variable Table 
    var bod_nm = sub_proc_nms[0].split('_')[1]; 
    var cur_db = snowflake.execute( {sqlText: `Select current_database()`} ); 
    cur_db.next(); 
    var env = cur_db.getColumnValue(1); 
    env = env.split('_'); 
    env = env[env.length - 1]; 
    var env_tbl_nm = `EDM_Environment_Variable_${env}`; 
    var env_schema_nm = "DW_R_MASTERDATA";
    var env_db_nm = `EDM_REFINED_${env}`; 
    try {
        var rs = snowflake.execute( {sqlText: `SELECT * FROM ${env_db_nm}.${env_schema_nm}.${env_tbl_nm}`} );
        var metaparams = {};
        while (rs.next()){
            metaparams[rs.getColumnValue(1)] = rs.getColumnValue(2);
        }
        var ref_db = metaparams['REF_DB'];
        var ref_schema = metaparams['R_LOYAL'];
        var cnf_db = metaparams['CNF_DB'];
        var wrk_schema = metaparams['C_STAGE'];
        var app_schema = metaparams['APPL'];
		

    } catch (err) {
        throw `Error while fetching data from EDM_Environment_Variable_${env}`;
    }

    function get_args(sub_proc_nm) {
        try {
          var get_args_query = `select ARGUMENT_SIGNATURE from ${cnf_db}.INFORMATION_SCHEMA.PROCEDURES
          WHERE PROCEDURE_NAME = '${sub_proc_nm.toUpperCase()}'
          AND PROCEDURE_SCHEMA = 'DW_APPL'
          ORDER BY CREATED DESC;`;
          var args_rs = snowflake.execute( {sqlText: get_args_query} );
          if (args_rs.next()) {
              var args = args_rs.getColumnValue(1);
          }
          args = args.slice(1, args.length - 1);
          args = args.split(',');
          var numArgs = args.length;
          var pars = [];
          for (var i = 0; i < numArgs; i++) {
              var tmp = args[i].trim();
              pars.push(tmp.split(' ')[0]);
          }
          return pars;
      } catch (err) {
          throw `Error while fetching arguments from child stored procedure`;
      }
    }

    // Global Variable Declaration
	var src_tbl = `${ref_db}.${app_schema}.Get_SMS_Marketing_Defition_Flat_R_STREAM`;
	var src_rerun_tbl = `${cnf_db}.${wrk_schema}.Get_SMS_Marketing_Defition_Flat_Rerun`;
    var src_wrk_tbl = `${cnf_db}.${wrk_schema}.SMS_MARKETING_CONTENT_DEFITION_Flat_wrk`;

	
	// persist stream data in work table for the current transaction, includes data from previous failed run
	var sql_crt_src_wrk_tbl = `create or replace table ${src_wrk_tbl} DATA_RETENTION_TIME_IN_DAYS = 0 as
    SELECT * FROM ${src_tbl}
    UNION ALL
    SELECT * FROM ${src_rerun_tbl} `;
    try {
        snowflake.execute({ sqlText: sql_crt_src_wrk_tbl });
    } catch (err)  {
        throw `Creation of Source Work table ${src_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
    }
	
	// Empty the rerun queue table
	var sql_empty_rerun_tbl = `TRUNCATE TABLE ${src_rerun_tbl}`;
	try {
        snowflake.execute({ sqlText: sql_empty_rerun_tbl });
    } catch (err) {
        throw `Truncation of rerun queue table ${src_rerun_tbl} Failed with error: ${err}`;   // Return a error message.
    }
    
	// query to load rerun queue table when encountered a failure
	var sql_ins_rerun_tbl = `CREATE OR REPLACE TABLE ${src_rerun_tbl} as SELECT * FROM ${src_wrk_tbl}`;
   
   
    // function to facilitate child stored procedure executions   
    function execSubProc(sub_proc_nm, params) {
        try {
             ret_obj = snowflake.execute ( {sqlText: `call ${cnf_db}.DW_APPL.${sub_proc_nm}(${params})`} );
             ret_obj.next();
             ret_msg = ret_obj.getColumnValue(1);
        } catch (err)  {
            return `Error executing stored procedure ${sub_proc_nm}(${params}) ${err}`;   // Return a error message.
        }
        return ret_msg;
    }

    for (index = 0; index < sub_proc_nms.length; index++) {
        sub_proc_nm = sub_proc_nms[index];
        var params = `'${src_wrk_tbl}'`
        var args = get_args(sub_proc_nm);
        for (i = 1; i < args.length; i++) {
            params += `, '${metaparams[args[i].toUpperCase()]}'`;
        }
        return_msg = execSubProc(sub_proc_nm, params);
        if (return_msg) {
            snowflake.execute ( {sqlText: sql_ins_rerun_tbl} );
            throw return_msg;
        }
    }

$$;

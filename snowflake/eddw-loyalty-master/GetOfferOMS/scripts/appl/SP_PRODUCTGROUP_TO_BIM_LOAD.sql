--liquibase formatted sql
--changeset SYSTEM:SP_PRODUCTGROUP_TO_BIM_LOAD runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_PRODUCT;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_C_PRODUCT.SP_PRODUCTGROUP_TO_BIM_LOAD()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
  
    // Global Variable Declaration
    var cnf_db = "<<EDM_DB_NAME>>";
    var dw_prd_schema = "DW_C_PRODUCT";
    var cnf_wrk_schema = "DW_C_STAGE";
    var ref_db = "<<EDM_DB_NAME_R>>";
    var ref_schema = "DW_R_PRODUCT";

    var src_tbl = ref_db + "." + ref_schema + ".ProductGroup_Flat_R_STREAM";
	var src_rerun_tbl = cnf_db + "." + cnf_wrk_schema + ".ProductGroup_Flat_Rerun";
    var src_wrk_tbl = cnf_db + "." + cnf_wrk_schema + ".ProductGroup_Flat_Main_WRK";

	
	var sql_crt_rerun_tbl = `CREATE TABLE IF NOT EXISTS `+ src_rerun_tbl + ` DATA_RETENTION_TIME_IN_DAYS = 0 AS SELECT * FROM `+ src_wrk_tbl +` where 1=2 `;

	try {
        snowflake.execute (
            {sqlText: sql_crt_rerun_tbl  }
            );
        }
    catch (err)  {
        throw "Creation of rerun queue table "+ src_rerun_tbl +" Failed with error: " + err;   // Return a error message.
        }

		
	// persist stream data in work table for the current transaction, includes data from previous failed run

	var sql_crt_src_wrk_tbl = `create or replace table `+ src_wrk_tbl +` DATA_RETENTION_TIME_IN_DAYS = 0 as 
								SELECT * FROM `+ src_tbl +` 
								UNION ALL 
								SELECT * FROM `+ src_rerun_tbl+``;
    try {
        snowflake.execute (
            {sqlText: sql_crt_src_wrk_tbl  }
            );
        }
    catch (err)  {
        throw "Creation of Source Work table "+ src_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }

	
	// Empty the rerun queue table
	var sql_empty_rerun_tbl = `TRUNCATE TABLE `+ src_rerun_tbl + ``;
	try {
        snowflake.execute (
            {sqlText: sql_empty_rerun_tbl  }
            );
        }
    catch (err)  {
        throw "Truncation of rerun queue table "+ src_rerun_tbl +" Failed with error: " + err;   // Return a error message.
        }
    
	// query to load rerun queue table when encountered a failure
	var sql_ins_rerun_tbl = `CREATE OR REPLACE TABLE `+ src_rerun_tbl +`  as SELECT * FROM `+ src_wrk_tbl +`;`;
   
   

    // function to facilitate child stored procedure executions 
    function execSubProc(sub_proc_nm, params)
    {
        try {
             ret_obj = snowflake.execute (
                        {sqlText: "call " + sub_proc_nm + "("+ params +")"  }
                        );
             ret_obj.next();
             ret_msg = ret_obj.getColumnValue(1);
            
            }
        catch (err)  {
            return "Error executing stored procedure "+ sub_proc_nm + "("+ params +")" + err;   // Return a error message.
            }
        return ret_msg;
    }
	
	var sub_proc_nm = 'SP_PRODUCTGROUP_TO_BIM_LOAD_ProductGroup_Flat';
    var params = "'"+ src_wrk_tbl + "','" + cnf_db + "','" + dw_prd_schema + "','" + cnf_wrk_schema + "'";

    return_msg = execSubProc(sub_proc_nm, params);
        
    if (return_msg && return_msg != 'Done')
        {
            snowflake.execute (
                    {sqlText: sql_ins_rerun_tbl }
                    );
            throw return_msg;
        }
    
$$;

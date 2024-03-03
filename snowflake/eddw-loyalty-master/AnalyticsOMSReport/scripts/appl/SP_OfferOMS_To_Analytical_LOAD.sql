--liquibase formatted sql
--changeset SYSTEM:SP_OfferOMS_To_Analytical_LOAD runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_ANALYTICS_PRD;
use schema DW_RETAIL_EXP;

CREATE OR REPLACE PROCEDURE EDM_ANALYTICS_PRD.DW_RETAIL_EXP.SP_OFFEROMS_TO_ANALYTICAL_LOAD()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

    // Global Variable Declaration
    
    var cnf_db = "EDM_CONFIRMED_PRD";
    var dw_prd_schema = "DW_C_PRODUCT";
    var anl_db = "EDM_ANALYTICS_PRD";
    var anl_schema = "DW_RETAIL_EXP";
    var wrk_schema = "DW_STAGE";

    var src_tbl = cnf_db + "." + dw_prd_schema + ".OfferOMS_Flat_C_STREAM";
    var src_rerun_tbl = anl_db + "." + wrk_schema + ".OfferOMS_Flat_Rerun";
    var src_wrk_tbl = anl_db + "." + wrk_schema + ".OfferOMS_Flat_wrk";

    
    
    // persist stream data in work table for the current transaction, includes data from previous failed run

    var sql_crt_src_wrk_tbl = `create or replace table `+ src_wrk_tbl +` DATA_RETENTION_TIME_IN_DAYS = 1 as 
                                SELECT * FROM `+ src_tbl +` WHERE METADATA$ACTION = 'INSERT'
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
    var sql_empty_rerun_tbl = `TRUNCATE `+ src_rerun_tbl + ` `;
    try {
        snowflake.execute (
            {sqlText: sql_empty_rerun_tbl  }
            );
        }
    catch (err)  {
        throw "Truncation of rerun queue table "+ src_rerun_tbl +" Failed with error: " + err;   // Return a error message.
        }
    
    // query to load rerun queue table when encountered a failure

    var sql_ins_rerun_tbl = `INSERT OVERWRITE INTO  `+ src_rerun_tbl+` SELECT * FROM `+ src_wrk_tbl+``;
   
   

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
	
	var sub_proc_nm = 'SP_OfferOMS_TO_Analytical_Update_FACT_OFFER_REQUEST';
    var params = "'"+ src_wrk_tbl +"','"+ anl_db +"','"+ anl_schema +"', '" + wrk_schema+"'";

    return_msg = execSubProc(sub_proc_nm, params);
        
    if (return_msg && return_msg != 'Done')
        {
            snowflake.execute (
                    {sqlText: sql_ins_rerun_tbl }
                    );
            throw return_msg;
        }
    
    
$$;
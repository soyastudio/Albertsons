--liquibase formatted sql
--changeset SYSTEM:SP_ProductGroup_To_Analytics_LOAD runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_ANALYTICS_PRD;
use schema DW_RETAIL_EXP;

CREATE OR REPLACE PROCEDURE EDM_ANALYTICS_PRD.DW_RETAIL_EXP.SP_PRODUCTGROUP_TO_ANALYTICS_LOAD()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

    
    // Global Variable Declaration
    var cnf_db = "EDM_CONFIRMED_PRD";
    var dw_prd_schema = "DW_C_PRODUCT";
    var anl_db = "EDM_ANALYTICS_PRD";
    var anl_schema = "DW_RETAIL_EXP";
    var cnf_wrk_schema = "DW_C_STAGE";
    var wrk_schema = "DW_STAGE";

    var src_tbl = cnf_db + "." + dw_prd_schema + ".PRODUCTGROUP_FLAT_C_STREAM";
    var offer_req_table = cnf_db + "." + dw_prd_schema + ".GETOFFERREQUEST_FLAT";
    var src_rerun_tbl = anl_db + "." + wrk_schema + ".ProductGroup_Flat_anl_Rerun";
    var src_wrk_tbl = anl_db + "." + wrk_schema + ".ProductGroup_Flat_Anl_WRK";
    
   // persist stream data in work table for the current transaction, includes data from previous failed run

    var sql_crt_src_wrk_tbl = `create or replace table `+ src_wrk_tbl +` DATA_RETENTION_TIME_IN_DAYS = 0 as 
								SELECT * FROM `+ src_tbl +`  WHERE METADATA$ACTION = 'INSERT'
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
	var sql_ins_rerun_tbl = `INSERT OVERWRITE INTO  `+ src_rerun_tbl +`  as SELECT * FROM `+ src_wrk_tbl +``;
   
   

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

    var sub_proc_nms = [
                        'Sp_productgroup_to_analytical_load_fact_offer_request_update',
                        'SP_GETOFFERREQUEST_TO_ANALYTICAL_LOAD_DIM_PRODUCT_GROUP_UPC'
                        ]


    for (index = 0; index < sub_proc_nms.length; index++) 
    {
        sub_proc_nm = sub_proc_nms[index];
        
        if(sub_proc_nm == 'SP_GETOFFERREQUEST_TO_ANALYTICAL_LOAD_DIM_PRODUCT_GROUP_UPC') 
        {
           params = "'" + offer_req_table + "','" + src_wrk_tbl + "','" + anl_db + "','" + anl_schema + "','" + cnf_db + "','" + dw_prd_schema + "','" + wrk_schema + "'";
        }
        else
        {
            params = "'"+ src_wrk_tbl +"','"+ anl_db +"','"+ anl_schema +"', '" + wrk_schema+"'";

        }
	
    return_msg = execSubProc(sub_proc_nm, params);
        
    if (return_msg && return_msg != 'Done')
        {
            snowflake.execute (
                    {sqlText: sql_ins_rerun_tbl }
                    );
            throw return_msg;
        }
	
     }
    
    
$$;
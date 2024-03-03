--liquibase formatted sql
--changeset SYSTEM:SP_GROCERYREWARD_TO_ANALYTICAL_LOAD runOnChange:true splitStatements:false OBJECT_TYPE:SP

use database EDM_CONFIRMED_PRD;
use schema EDM_CONFIRMED_PRD.DW_APPL;

CREATE OR REPLACE PROCEDURE SP_GROCERYREWARD_TO_ANALYTICAL_LOAD()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

    // Global Variable Declaration
    
    
    var cnf_db = "EDM_CONFIRMED_PRD";
	var anl_db = "EDM_ANALYTICS_PRD";
    var appl_schema = "DW_APPL";
	var anl_schema = "DW_REFERENCE";
    var wrk_schema = "DW_STAGE";

    var src_tbl = cnf_db + "." + appl_schema + ".F_Grocery_Reward_Offer_Clips_Report_Stream";
	var src_tbl_epe = cnf_db + "." + appl_schema + ".F_Grocery_Reward_Offer_Clips_Report_EPE_Stream";
	var src_tbl_oms = cnf_db + "." + appl_schema + ".F_Grocery_Reward_Offer_Clips_Report_OMS_Stream";
    var src_rerun_tbl = anl_db + "." + wrk_schema + ".F_Grocery_Reward_Offer_Clips_Rerun";
    var src_wrk_tbl = anl_db + "." + wrk_schema + ".F_Grocery_Reward_Offer_Clips_wrk";

        
    // persist stream data in work table for the current transaction, includes data from previous failed run
	
	
    var sql_crt_src_wrk_tbl = `create or replace table `+ src_wrk_tbl +` DATA_RETENTION_TIME_IN_DAYS = 1 as 
                                SELECT offer_id FROM `+ src_tbl +` WHERE METADATA$ACTION = 'INSERT'
								Union all
								SELECT offer_id FROM `+ src_tbl_epe +` WHERE METADATA$ACTION = 'INSERT'
                                UNION ALL 
								SELECT oms_offer_id FROM `+ src_tbl_oms +` WHERE METADATA$ACTION = 'INSERT'
								union all
                                SELECT offer_id FROM `+ src_rerun_tbl+``;
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

    var sql_ins_rerun_tbl = `INSERT INTO  `+ src_rerun_tbl+` SELECT * FROM `+ src_wrk_tbl+``;
   
   
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
                        'SP_DIMENSION_LOAD_D1_Clip',
                        'SP_DIMENSION_LOAD_D1_Offer',
                        'SP_F_Grocery_Reward_Offer_Clips',
						'SP_F_Grocery_Reward_Offer_Redemption'
                        ]

    
    for (index = 0; index < sub_proc_nms.length; index++) 
    {
        sub_proc_nm = sub_proc_nms[index];
        
            params = "'"+ src_wrk_tbl +"','"+ anl_db +"','"+ anl_schema +"', '" + wrk_schema+"'";
    
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

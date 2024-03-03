--liquibase formatted sql
--changeset SYSTEM:SP_GetOfferRequest_TO_BIM_LOAD runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_PRODUCT;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_C_PRODUCT.SP_GETOFFERREQUEST_TO_BIM_LOAD()
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

    var src_tbl = ref_db + "." + ref_schema + ".GetOfferRequest_FLAT_R_STREAM";
	var src_rerun_tbl = cnf_db + "." + cnf_wrk_schema + ".GetOfferRequest_Flat_Rerun";
    var src_wrk_tbl = cnf_db + "." + cnf_wrk_schema + ".GetOfferRequest_Flat_Main_WRK";

	//rerun table has been created using work table as a part of new change ,previously it was creating using the streams
	
	
	
	var sql_crt_rerun_tbl = `CREATE TRANSIENT TABLE IF NOT EXISTS `+ src_rerun_tbl + ` DATA_RETENTION_TIME_IN_DAYS = 0 AS SELECT * FROM `+ src_wrk_tbl +` where 1=2 `;

	try {
        snowflake.execute (
            {sqlText: sql_crt_rerun_tbl  }
            );
        }
    catch (err)  {
        throw "Creation of rerun queue table "+ src_rerun_tbl +" Failed with error: " + err;   // Return a error message.
        }

	// truncate work table
	var sql_truncate_wrk_tbl =`TRUNCATE TABLE `+ src_wrk_tbl +``;
		
		
	// persist stream data in work table for the current transaction, includes data from previous failed run
	
	var sql_crt_src_wrk_tbl = `INSERT INTO `+ src_wrk_tbl +`

								SELECT * FROM `+ src_tbl +` 
								UNION ALL 
								SELECT * FROM `+ src_rerun_tbl+``;
    try {
	
		snowflake.execute (
            {sqlText: sql_truncate_wrk_tbl  }
            );
	
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
	var sql_ins_rerun_tbl = `INSERT OVERWRITE INTO `+ src_rerun_tbl+` 
							 SELECT * FROM `+ src_wrk_tbl+``;
   
   

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
	
	var sub_proc_nm = 'SP_OFFERREQUEST_TO_BIM_LOAD_GetOfferRequest_Flat';
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

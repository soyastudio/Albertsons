--liquibase formatted sql
--changeset SYSTEM:SP_GetDoordash_To_BIM_load runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETDOORDASH_TO_BIM_LOAD()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

    
       
    // Global Variable Declaration
    var cnf_db = "<<EDM_DB_NAME>>";
    var dw_prd_schema = "DW_C_LOYALTY";
    var cnf_wrk_schema = "DW_C_STAGE";
	var cnf_wrk_schema_PROD = "DW_C_LOCATION";
	var appl_schema = "DW_APPL";
    var ref_db = "<<EDM_DB_NAME_R>>";

    var src_tbl = ref_db + "." + appl_schema + ".DOORDASH_ORDER_INFO_FLAT_R_STREAM";
	var src_tbl_Tran = ref_db + "." + appl_schema + ".DOORDASH_Transaction_INFO_FLAT_R_STREAM";
	
	
	var src_rerun_tbl = cnf_db + "." + cnf_wrk_schema + ".DOORDASH_ORDER_INFO_Flat_Rerun";
	var src_rerun_tbl_Tran = cnf_db + "." + cnf_wrk_schema + ".DOORDASH_TRANSACTION_INFO_Flat_Rerun";
	
	
    var src_wrk_tbl = cnf_db + "." + cnf_wrk_schema + ".DOORDASH_ORDER_INFO_Flat_Main_WRK";
	var src_wrk_tbl_Tran = cnf_db + "." + cnf_wrk_schema + ".DOORDASH_TRANSACTION_INFO_Flat_Main_WRK";
	

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
		
	var sql_crt_src_wrk_tbl_Tran = `create or replace table `+ src_wrk_tbl_Tran +` DATA_RETENTION_TIME_IN_DAYS = 0 as 
								SELECT * FROM `+ src_tbl_Tran +` 
								UNION ALL 
								SELECT * FROM `+ src_rerun_tbl_Tran+``;
    try {
        snowflake.execute (
            {sqlText: sql_crt_src_wrk_tbl_Tran  }
            );
        }
    catch (err)  {
        throw "Creation of Source Work table "+ src_wrk_tbl_Tran +" Failed with error: " + err;   // Return a error message.
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
		
	var sql_empty_rerun_tbl_Tran = `TRUNCATE `+ src_rerun_tbl_Tran + ` `;
	try {
        snowflake.execute (
            {sqlText: sql_empty_rerun_tbl_Tran  }
            );
        }
    catch (err)  {
        throw "Truncation of rerun queue table "+ src_rerun_tbl_Tran +" Failed with error: " + err;   // Return a error message.
        }
		
	
    
	// query to load rerun queue table when encountered a failure
	var sql_ins_rerun_tbl = `CREATE OR REPLACE TABLE `+ src_rerun_tbl+` AS
							 SELECT * FROM `+ src_wrk_tbl+``;;
							 
	var sql_ins_rerun_tbl_Tran = `CREATE OR REPLACE TABLE `+ src_rerun_tbl_Tran+` AS
							 SELECT * FROM `+ src_wrk_tbl_Tran+``;;
							 
	  
   

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
	
	var sub_proc_nms = ['SP_GETDOORDASH_TO_BIM_LOAD_Partner_Grocery_Order_Customer',
						'SP_GetDoordash_To_BIM_LOAD_Partner_Grocery_Order_Detail',
						'SP_GETDoordash_TO_BIM_LOAD_Partner_Grocery_Order_Header',
						'SP_GetDoordash_To_BIM_load_Partner_Grocery_Order_Tender']
						
	for (index = 0; index < sub_proc_nms.length; index++) 
	{

if (index == 0)
	{
		sub_proc_nm = sub_proc_nms[0];
		params = "'" + src_wrk_tbl_Tran + "','" + cnf_db + "','" + dw_prd_schema + "','" + cnf_wrk_schema + "'";
        return_msg = execSubProc(sub_proc_nm, params);
		
		if (return_msg && return_msg != 'Done')
			{
				snowflake.execute (
						{sqlText: sql_ins_rerun_tbl_Tran }
						);
				snowflake.execute (
						{sqlText: sql_ins_rerun_tbl  }
						);
				throw return_msg;
			}
	}
	
	if (index == 1)
    {
		sub_proc_nm = sub_proc_nms[1];
		params = "'"+ src_wrk_tbl_Tran + "','" + cnf_db + "','" + dw_prd_schema + "','" + cnf_wrk_schema + "'";
        return_msg = execSubProc(sub_proc_nm, params);
		
		if (return_msg && return_msg != 'Done')
			{
				snowflake.execute (
						{sqlText: sql_ins_rerun_tbl_Tran }
						);
				snowflake.execute (
						{sqlText: sql_ins_rerun_tbl  }
						);
				throw return_msg;
			}
	}
	
	if (index == 2)
	{
		sub_proc_nm = sub_proc_nms[2];
		params = "'"+ src_wrk_tbl + "','" + cnf_db + "','" + dw_prd_schema + "','" + cnf_wrk_schema + "','" + cnf_wrk_schema_PROD + "'";
        return_msg = execSubProc(sub_proc_nm, params);
		
		if (return_msg && return_msg != 'Done')
			{
				snowflake.execute (
						{sqlText: sql_ins_rerun_tbl }
						);
				throw return_msg;
			}
	}
	
	if (index == 3)
	{
		sub_proc_nm = sub_proc_nms[3];
		params = "'"+ src_wrk_tbl + "','" + cnf_db + "','" + dw_prd_schema + "','" + cnf_wrk_schema + "'";
        return_msg = execSubProc(sub_proc_nm, params);
		
		if (return_msg && return_msg != 'Done')
			{
				snowflake.execute (
						{sqlText: sql_ins_rerun_tbl }
						);
				throw return_msg;
			}
	}
	}   
    
    
$$;

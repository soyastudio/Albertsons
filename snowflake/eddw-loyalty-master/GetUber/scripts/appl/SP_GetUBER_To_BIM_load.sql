--liquibase formatted sql
--changeset SYSTEM:SP_GetUBER_To_BIM_load runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETUBER_TO_BIM_LOAD()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

    
    // Global Variable Declaration
    var cnf_db = "<<EDM_DB_NAME>>";
    var dw_prd_schema = "DW_C_LOYALTY";
    var cnf_wrk_schema = "DW_C_STAGE";
	var cnf_wrk_schema_LOC = "DW_C_LOCATION";
	var cnf_wrk_schema_Cust = "DW_C_CUSTOMER";
	var appl_schema = "DW_APPL";
    var ref_db = "<<EDM_DB_NAME_R>>";


    var src_tbl = ref_db + "." + appl_schema + ".UBER_ORDER_INFO_FLAT_R_STREAM";
	var src_tbl_Tran = ref_db + "." + appl_schema + ".UBER_TRANSACTION_INFO_FLAT_R_STREAM";
	var src_tbl_Cust = ref_db + "." + appl_schema + ".UBER_CUSTOMER_INFO_FLAT_R_STREAM";
	
	var src_rerun_tbl = cnf_db + "." + cnf_wrk_schema + ".UBER_ORDER_INFO_Flat_Rerun";
	var src_rerun_tbl_Tran = cnf_db + "." + cnf_wrk_schema + ".UBER_TRANSACTION_INFO_Flat_Rerun";
	var src_rerun_tbl_Cust = cnf_db + "." + cnf_wrk_schema + ".UBER_CUSTOMER_INFO_Flat_Rerun";
	
    var src_wrk_tbl = cnf_db + "." + cnf_wrk_schema + ".UBER_ORDER_INFO_Flat_Main_WRK";
	var src_wrk_tbl_Tran = cnf_db + "." + cnf_wrk_schema + ".UBER_TRANSACTION_INFO_Flat_Main_WRK";
	var src_wrk_tbl_Cust = cnf_db + "." + cnf_wrk_schema + ".UBER_CUSTOMER_INFO_Flat_Main_WRK";

		
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
		
	var sql_crt_src_wrk_tbl_Cust = `create or replace table `+ src_wrk_tbl_Cust +` DATA_RETENTION_TIME_IN_DAYS = 0 as 
								SELECT * FROM `+ src_tbl_Cust +` 
								UNION ALL 
								SELECT * FROM `+ src_rerun_tbl_Cust+``;
    try {
        snowflake.execute (
            {sqlText: sql_crt_src_wrk_tbl_Cust  }
            );
        }
    catch (err)  {
        throw "Creation of Source Work table "+ src_wrk_tbl_Cust +" Failed with error: " + err;   // Return a error message.
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
		
	var sql_empty_rerun_tbl_Cust = `TRUNCATE `+ src_rerun_tbl_Cust + ` `;
	try {
        snowflake.execute (
            {sqlText: sql_empty_rerun_tbl_Cust  }
            );
        }
    catch (err)  {
        throw "Truncation of rerun queue table "+ src_rerun_tbl_Cust +" Failed with error: " + err;   // Return a error message.
        }
    
	// query to load rerun queue table when encountered a failure
	var sql_ins_rerun_tbl = `CREATE OR REPLACE TABLE `+ src_rerun_tbl+` AS
							 SELECT * FROM `+ src_wrk_tbl+``;;
							 
	var sql_ins_rerun_tbl_Tran = `CREATE OR REPLACE TABLE `+ src_rerun_tbl_Tran+` AS
							 SELECT * FROM `+ src_wrk_tbl_Tran+``;;
							 
	var sql_ins_rerun_tbl_Cust = `CREATE OR REPLACE TABLE `+ src_rerun_tbl_Cust+` AS
							 SELECT * FROM `+ src_wrk_tbl_Cust+``;;   
   

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
	
	var sub_proc_nms = ['SP_GETUBER_TO_BIM_LOAD_Partner_Grocery_Order_Customer',
						'SP_GETUBER_TO_BIM_LOAD_Partner_Grocery_Order_Detail',
						'SP_GETUBER_TO_BIM_LOAD_Partner_Grocery_Order_Header',
						'SP_GETUBER_TO_BIM_LOAD_Partner_Grocery_Order_Tender']
						
	for (index = 0; index < sub_proc_nms.length; index++) 
	{				
	if (index == 0)
	{
		sub_proc_nm = sub_proc_nms[0];
		params = "'"+ src_wrk_tbl_Cust + "','" + src_wrk_tbl_Tran + "','" + cnf_db + "','" + dw_PRD_schema + "','" + cnf_wrk_schema + "','" + cnf_wrk_schema_Cust + "'";
        return_msg = execSubProc(sub_proc_nm, params);
		
		if (return_msg && return_msg != 'Done')
			{
				snowflake.execute (
						{sqlText: sql_ins_rerun_tbl_Tran }
						);
				snowflake.execute (
						{sqlText: sql_ins_rerun_tbl_Cust  }
						);
				snowflake.execute (
						{sqlText: sql_ins_rerun_tbl }
						);
				throw return_msg;
			}
	}
	
	if (index == 1)
{
		sub_proc_nm = sub_proc_nms[1];
		params = "'"+ src_wrk_tbl_Tran + "','" + cnf_db + "','" + dw_PRD_schema + "','" + cnf_wrk_schema + "'";
        return_msg = execSubProc(sub_proc_nm, params);
		
		if (return_msg && return_msg != 'Done')
			{
				snowflake.execute (
						{sqlText: sql_ins_rerun_tbl_Tran }
						);
				snowflake.execute (
						{sqlText: sql_ins_rerun_tbl }
						);
				throw return_msg;
			}
	}
	
	if (index == 2)
	{
		sub_proc_nm = sub_proc_nms[2];
		params = "'"+ src_wrk_tbl + "','" + cnf_db + "','" + dw_PRD_schema + "','" + cnf_wrk_schema + "','" + cnf_wrk_schema_LOC + "'";
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
		params = "'"+ src_wrk_tbl + "','" + cnf_db + "','" + dw_PRD_schema + "','" + cnf_wrk_schema + "'";
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

--liquibase formatted sql
--changeset SYSTEM:SP_GETOFFERREQUEST_TO_ANALYTICAL_LOAD runOnChange:true splitStatements:false OBJECT_TYPE:SP

use database EDM_ANALYTICS_PRD;
use schema dw_appl;

CREATE OR REPLACE PROCEDURE SP_GETOFFERREQUEST_TO_ANALYTICAL_LOAD()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    
    // Global Variable Declaration
    
    var ref_db = "EDM_REFINED_PRD";
    var ref_schema = "DW_R_PRODUCT";
    var cnf_db = "EDM_CONFIRMED_PRD";
    var dw_prd_schema = "DW_C_PRODUCT";
    var dw_loc_schema = "DW_C_LOCATION";
    var anl_db = "EDM_ANALYTICS_PRD";
    var anl_schema = "DW_RETAIL_EXP";
    var wrk_schema = "DW_STAGE";

    var src_tbl = cnf_db + ".DW_C_PRODUCT.GetOfferRequest_Flat_C_Stream";
    var pg_src_tbl = cnf_db + "." + dw_prd_schema + ".Productgroup_flat";
    var sg_src_tbl = cnf_db + "." +  dw_prd_schema + ".Storegroup_Flat";
    var src_rerun_tbl = anl_db + "." + wrk_schema + ".GetOfferRequest_Flat_Rerun";
    var src_wrk_tbl = anl_db + "." + wrk_schema + ".GetOfferRequest_Flat_wrk";

    
    
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
	
    var sql_remove_dup = `DELETE FROM `+ src_wrk_tbl +`
                            WHERE (offerrequestid, updatets ) NOT IN 
                            (SELECT * FROM(
                                SELECT offerrequestid ,max(updatets) FROM `+ src_wrk_tbl +`  GROUP BY offerrequestid ) as t);
                            `;

    try {
        snowflake.execute (
            {sqlText: sql_remove_dup   }
            );
        }
    catch (err)  {
        throw "Creation of Source Work table "+ sql_remove_dup +" Failed with error: " + err;   // Return a error message.
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

    var sql_ins_rerun_tbl = `INSERT INTO  `+ src_rerun_tbl+`  SELECT * FROM `+ src_wrk_tbl+``;
   
   

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
                        'EDM_ANALYTICS_PRD.dw_retail_exp.SP_GetOfferRequest_To_Analytical_LOAD_DIM_Product_Group_Upc',
                        'EDM_ANALYTICS_PRD.dw_retail_exp.SP_GetOfferRequest_To_Analytical_LOAD_DIM_Store',
                        'EDM_ANALYTICS_PRD.dw_retail_exp.SP_GetOfferRequest_To_Analytical_LOAD_DIM_Rog',
                        'EDM_ANALYTICS_PRD.dw_retail_exp.SP_GetOfferRequest_To_Analytical_LOAD_dim_channel',
                        'EDM_ANALYTICS_PRD.dw_retail_exp.SP_GetOfferRequest_To_Analytical_LOAD_dim_Program', 
                        'EDM_ANALYTICS_PRD.dw_retail_exp.SP_GetOfferRequest_To_Analytical_LOAD_dim_Group', 
                        'SP_GetOfferRequest_To_Analytical_LOAD_dim_discount',
                        'EDM_ANALYTICS_PRD.dw_retail_exp.SP_GetOfferRequest_To_Analytical_LOAD_dim_offer_type',
                        'EDM_ANALYTICS_PRD.dw_retail_exp.SP_GetOfferRequest_To_Analytical_LOAD_dim_Store_Group',
			'SP_GetOfferRequest_To_Analytical_LOAD_Fact_Offer_Request'
                        ]

    
    for (index = 0; index < sub_proc_nms.length; index++) 
    {
        sub_proc_nm = sub_proc_nms[index];
        
        if(sub_proc_nm == "EDM_ANALYTICS_PRD.dw_retail_exp.SP_GetOfferRequest_To_Analytical_LOAD_DIM_Product_Group_Upc") 
        {
            params = params = "'" + src_wrk_tbl + "','"+ pg_src_tbl +"','"+ anl_db +"','"+ anl_schema +"','"+ cnf_db +"','"+ dw_prd_schema +"','"+ wrk_schema+"'";
        }
		
        else if (sub_proc_nm == "EDM_ANALYTICS_PRD.dw_retail_exp.SP_GetOfferRequest_To_Analytical_LOAD_DIM_Store")
        {
            params = "'"+ sg_src_tbl + "','"+ anl_db +"','"+ anl_schema +"','"+ cnf_db + "','" + dw_loc_schema + "','"+ wrk_schema + "'";
        }
        else if (sub_proc_nm == "EDM_ANALYTICS_PRD.dw_retail_exp.SP_GetOfferRequest_To_Analytical_LOAD_DIM_Rog") 

        {
            params = "'"+ anl_db +"','"+ anl_schema +"','"+ cnf_db +"','"+ dw_loc_schema +"','"+ wrk_schema + "'";
        }
		else if(sub_proc_nm == "SP_GetOfferRequest_To_Analytical_LOAD_Fact_Offer_Request") 
        {
            params = "'"+ src_wrk_tbl +"','"+ anl_db +"','"+ anl_schema +"','"+ wrk_schema+"','"+ cnf_db +"','"+ dw_prd_schema +"','" + dw_loc_schema + "'";
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

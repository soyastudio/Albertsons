--liquibase formatted sql
--changeset SYSTEM:SP_GETOFFERREQUEST_TO_BIM_LOAD_SP runOnChange:true splitStatements:false OBJECT_TYPE:SP

use database EDM_CONFIRMED_PRD;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE SP_GETOFFERREQUEST_TO_BIM_LOAD_SP()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS 
$$
    // Global Variable Declaration
    var cnf_db = "EDM_CONFIRMED_PRD";
    var wrk_schema = "DW_C_STAGE";
    var CNF_SCHEMA = "DW_C_PURCHASING";
	var dw_loc_schema = "DW_C_PRODUCT";
    var ref_db = "EDM_REFINED_PRD";
    var ref_schema = "DW_APPL";
	
	var src_tbl = ref_db + "." + ref_schema + ".GetOfferRequest_Flat_SP_R_STREAM";
	var src_rerun_tbl = cnf_db + "." + wrk_schema + ".GetOfferRequest_Flat_SP_rerun";
    var src_wrk_tbl = cnf_db + "." + wrk_schema + ".GetOfferRequest_Flat_SP_wrk";
	var src_flat_tbl = ref_db + ".DW_R_PRODUCT.GetOfferRequest_Flat";
	
	// persist stream data in work table for the current transaction, includes data from previous failed run
	var sql_crt_src_wrk_tbl = `create or replace table ` + src_wrk_tbl + ` DATA_RETENTION_TIME_IN_DAYS = 0 as 
								with cte as
								(
								select *,dense_rank() over(partition by offerrequestid order by creationdt desc) as rn 
								from `+ src_flat_tbl +` where 
								offerrequestid in (select offerrequestid from `+ src_tbl +`)  
								qualify rn =1
								)
								select distinct * exclude(rn), NULL as METADATA$ACTION, NULL as METADATA$ISUPDATE, NULL As METADATA$ROW_ID 
								from cte

								UNION ALL 
								SELECT * FROM `+ src_rerun_tbl+``;
    try {
        snowflake.execute (
            {sqlText: sql_crt_src_wrk_tbl  }
            );
        }
    catch (err)  {
        throw "Creation of Source Work table "+ src_wrk_tbl + " Failed with error: " + err;   // Return a error message.
        }
	
	// Empty the rerun queue table
	var sql_empty_rerun_tbl = `TRUNCATE IF EXISTS `+ src_rerun_tbl + ``;
	try {
        snowflake.execute (
            {sqlText: sql_empty_rerun_tbl  }
            );
        }
    catch (err)  {
        throw "Truncation of rerun queue table "+ src_rerun_tbl +" Failed with error: " + err;   // Return a error message.
        }
    
	// query to load rerun queue table when encountered a failure
		var sql_ins_rerun_tbl = `INSERT INTO `+ src_rerun_tbl+`  SELECT * FROM `+ src_wrk_tbl+``;

   
    // function to facilitate child stored procedure executions   
    function execSubProc(sub_proc_nm, params)
    {
        try {
             ret_obj = snowflake.execute (
                        {sqlText: "call " + sub_proc_nm + "("+ params +")"   }
                        );
             ret_obj.next();
             ret_msg = ret_obj.getColumnValue(1);
            
            }
        catch (err)  {
               return "Error executing stored procedure "+ sub_proc_nm + "("+ params +")" + err;   // Return a error message.
            }
        return ret_msg;
    }
	
						
	var sub_proc_list = [
							'sp_GetOfferRequest_To_BIM_Offer_Request_Group'
							,'sp_GetOfferRequest_To_BIM_Offer_Request_Sub_Group'
							,'sp_GetOfferRequest_To_BIM_Offer_Request'
							,'sp_GetOfferRequest_To_BIM_Offer_Request_Status'
							,'sp_GetOfferRequest_To_BIM_Offer_Request_Vendor_Promotion'
							,'sp_GetOfferRequest_To_BIM_Offer_Request_User_Update'
							,'sp_GetOfferRequest_To_BIM_LOAD_Offer_Request_Restriction_Type'
							,'sp_GetOfferRequest_To_BIM_Offer_Request_Offer_Specification'
							,'sp_GetOfferRequest_To_BIM_Load_Offer_Request_Store_Group'
							,'sp_GetOfferRequest_To_BIM_Offer_Request_Buy_Product_Group'
							,'sp_GetOfferRequest_To_BIM_Offer_Request_Offer'
							,'sp_GetOfferRequest_To_BIM_LOAD_Offer_Request_Product_Group_Tier'
							,'sp_GetOfferRequest_To_BIM_LOAD_Offer_Request_Product_Group'
							,'sp_GetOfferRequest_To_BIM_Offer_Request_Get_Discount_Version'
							,'sp_GetOfferRequest_To_BIM_Offer_Request_Air_Mile_Tier'
							,'sp_GetOfferRequest_To_BIM_Offer_Request_Discount_Version_Discount'
							,'sp_GetOfferRequest_To_BIM_Offer_Request_Discount_Tier'
							,'sp_GetOfferRequest_To_BIM_Offer_Request_POD_Display_Image'
							,'sp_GetOfferRequest_To_BIM_Offer_Request_POD_Special_Event'
							,'sp_GetOfferRequest_to_BIM_Offer_Request_Promotion_Period_Type'
							,'sp_GetOfferRequest_to_BIM_Offer_Request_Promotion_Program_Type'
							,'sp_GetOfferRequest_to_BIM_LOAD_Offer_Request_Shopping_List_Category'
							,'sp_GetOfferRequest_to_BIM_LOAD_Offer_Request_POD_Category'
							,'sp_GetOfferRequest_to_BIM_LOAD_Offer_Request_Reference'
							,'SP_GetOfferRequest_TO_BIM_Offer_Request_Requirement_Type'
							,'SP_GetOfferRequest_TO_BIM_Offer_Request_Region'
							,'SP_GetOfferRequest_To_BIM_load_Offer_Request_Change_Detail'
							,'sp_GetOfferRequest_To_BIM_Offer_Request_Fulfillment_Channel_Type'
							,'sp_GetOfferRequest_To_BIM_Offer_Request_Review_Checklist_Status'
							,'sp_GetOfferRequest_to_BIM_Offer_Request_Offer_Flag'
							,'SP_GETOFFERREQUEST_TO_BIM_Offer_Request_Excluded_Promotion'
						]

	
		for (index = 0; index < sub_proc_list.length; index++) 
			{
					sub_proc_nm = sub_proc_list[index];
					
					if(sub_proc_nm == "sp_GetOfferRequest_To_BIM_Offer_Request_Offer")
					{
						params = "'"+ src_wrk_tbl +"','"+ cnf_db +"','"+ CNF_SCHEMA  +"','"+ wrk_schema+"','"+dw_loc_schema+"'";
					}
					else
					{
						params = "'"+ src_wrk_tbl +"','"+ cnf_db +"','"+ CNF_SCHEMA  +"','"+ wrk_schema + "'";
					}
					
					return_msg = execSubProc(sub_proc_nm, params);
					if (return_msg)
					{
						snowflake.execute (
								{sqlText: sql_ins_rerun_tbl }
								);
						throw return_msg;
					}
			}
$$;

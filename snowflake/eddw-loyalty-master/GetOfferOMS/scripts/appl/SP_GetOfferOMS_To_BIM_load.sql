--liquibase formatted sql
--changeset SYSTEM:SP_GetOfferOMS_To_BIM_load runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_CONFIRMED_PRD;
use schema EDM_CONFIRMED_PRD.DW_APPL;

CREATE OR REPLACE PROCEDURE SP_GETOFFEROMS_TO_BIM_LOAD()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS 
$$    
        
    // Global Variable Declaration
    var cnf_db = "<<EDM_DB_NAME>>";
    var dw_QAPROJ_schema = "DW_C_PRODUCT";
    var cnf_wrk_schema = "DW_C_STAGE";
	var appl_schema = "DW_APPL";
    var ref_db = "<<EDM_DB_NAME_R>>";

    var src_tbl = ref_db + "." + appl_schema + ".GetOfferOMS_Flat_R_STREAM";
	var src_rerun_tbl = cnf_db + "." + cnf_wrk_schema + ".GetOfferOMS_Flat_Rerun";
    var src_wrk_tbl = cnf_db + "." + cnf_wrk_schema + ".GetOfferOMS_Flat_Main_WRK";

	
	/*var sql_crt_rerun_tbl = `CREATE TABLE IF NOT EXISTS `+ src_rerun_tbl + ` DATA_RETENTION_TIME_IN_DAYS = 0 AS SELECT * FROM `+ src_wrk_tbl +` where 1=2 `;

	try {
        snowflake.execute (
            {sqlText: sql_crt_rerun_tbl  }
            );
        }
    catch (err)  {
        throw "Creation of rerun queue table "+ src_rerun_tbl +" Failed with error: " + err;   // Return a error message.
        }*/

		
	// persist stream data in work table for the current transaction, includes data from previous failed run
	// truncate work table
	var sql_truncate_wrk_tbl =`TRUNCATE TABLE `+ src_wrk_tbl +``;
	
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
	var sql_ins_rerun_tbl = ` INSERT INTO `+ src_rerun_tbl+` 
							 SELECT * FROM `+ src_wrk_tbl+``;;
   
   

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
	
	var sub_proc_nms = ['SP_GetOfferOMS_To_BIM_load_OMS_Offer', 
						'SP_GetOfferOMS_To_BIM_load_OMS_Offer_Benefit',
						'SP_GetOfferOMS_TO_BIM_LOAD_OMS_Offer_Benefit_Discount',
						'SP_GetOfferOMS_To_BIM_load_OMS_Offer_Benefit_Discount_Tier',
						'SP_GetOfferOMS_TO_BIM_LOAD_OMS_Offer_Benefit_Points',
						'SP_GetOfferOMS_TO_BIM_LOAD_OMS_Offer_Benefit_Points_Tier',
						'SP_GetOfferOMS_TO_BIM_LOAD_OMS_Offer_Redemption_Store_Group',
						'SP_GetOfferOMS_TO_BIM_LOAD_OMS_Offer_Event',
						'SP_GetOfferOMS_TO_BIM_LOAD_OMS_Offer_Hidden_Event',
						'SP_GetOfferOMS_TO_BIM_LOAD_OMS_Offer_Excluded_Terminal',
						'SP_GetOfferOMS_TO_BIM_LOAD_OMS_Offer_Nopa_numbers',
						'SP_GetOfferOMS_TO_BIM_LOAD_OMS_Offer_POD_Store_Group',
						'SP_GetOfferOMS_TO_BIM_LOAD_OMS_Offer_Postal_Code',
						'SP_GetOfferOMS_TO_BIM_LOAD_OMS_Offer_Qualification_Attribute',
						'SP_GetOfferOMS_TO_BIM_LOAD_OMS_Offer_Qualification_Attribute_Value',
						'SP_GetOfferOMS_TO_BIM_LOAD_OMS_Offer_Qualification_Customer_Group',
						'SP_GetOfferOMS_TO_BIM_LOAD_OMS_Offer_Qualification_Points_Group',
						'SP_GetOfferOMS_TO_BIM_LOAD_OMS_Offer_Qualification_Points_Group_Tier',
						'SP_GetOfferOMS_TO_BIM_LOAD_OMS_Offer_Qualification_Product_Group',
						'SP_GetOfferOMS_TO_BIM_LOAD_OMS_Offer_Qualification_Product_Group_Tier',
						'SP_GetOfferOMS_TO_BIM_LOAD_OMS_Offer_Qualification_Trigger_Code',
						'SP_GetOfferOMS_TO_BIM_LOAD_OMS_Offer_Region',
						'SP_GetOfferOMS_TO_BIM_LOAD_OMS_Offer_Terminal',
						'SP_GetOfferOMS_TO_BIM_LOAD_OMS_Offer_Cashier_Message_Tier',
						'SP_GetOfferOMS_TO_BIM_LOAD_OMS_Offer_Printed_Message',
						'SP_GetOfferOMS_TO_BIM_LOAD_Oms_Offer_Flag',
						'SP_GetOfferOMS_TO_BIM_LOAD_Oms_Tender_Tier',
						'SP_GetOfferOMS_TO_BIM_LOAD_Oms_Offer_Qualification_Tender_Type',
                        'SP_GetOfferOMS_TO_BIM_LOAD_Oms_Offer_Fulfillment_Channel_Type',
						'SP_GetOfferOMS_To_BIM_load_Household_Target_Group_item',
						'SP_GetOfferOMS_To_BIM_load_Household_Target_Group',
						'SP_GETOFFEROMS_TO_BIM_LOAD_Oms_Excluded_Promotion',
                        'SP_GetOfferOMS_To_BIM_load_OfferOMS_Flat']
						
	for (index = 0; index < sub_proc_nms.length; index++) 
	{
        sub_proc_nm = sub_proc_nms[index];
        params = "'"+ src_wrk_tbl + "','" + cnf_db + "','" + dw_QAPROJ_schema + "','" + cnf_wrk_schema + "'";
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

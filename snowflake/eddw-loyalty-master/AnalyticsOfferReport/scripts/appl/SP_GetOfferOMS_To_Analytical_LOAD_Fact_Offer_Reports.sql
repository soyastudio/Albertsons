--liquibase formatted sql
--changeset SYSTEM:SP_GETOFFEROMS_TO_ANALYTICAL_LOAD_FACT_OFFER_REPORTS runOnChange:true splitStatements:false OBJECT_TYPE:SP

use database EDM_CONFIRMED_PRD;
use schema EDM_CONFIRMED_PRD.DW_APPL;

CREATE OR REPLACE PROCEDURE SP_GETOFFEROMS_TO_ANALYTICAL_LOAD_FACT_OFFER_REPORTS()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS  
$$
		// ************** Load for Fact_Offer_Reports table BEGIN *****************		
		
		var cnf_db = "EDM_CONFIRMED_PRD";
		var stg_schema = "DW_C_STAGE";
		var cnf_schema = "DW_C_PRODUCT";
		var cnf_schema_loc = "DW_C_LOCATION";
		var cnf_schema_purchase = "DW_C_PURCHASING";
		var app_schema = "DW_APPL";
		
		var tgt_tbl = cnf_db + "." + cnf_schema + ".Fact_Offer_Reports";
		var tgt_wrk_tbl = cnf_db + "." + stg_schema + ".Fact_Offer_Reports_WRK";
		var src_tbl = cnf_db + "." + app_schema + ".OfferOMS_Flat_Analytical_R_Stream";
						
		var OMS_OFFER_tbl = cnf_db + "." + cnf_schema + ".OMS_OFFER";
		var OMS_OFFER_EVENT_tbl = cnf_db + "." + cnf_schema + ".OMS_OFFER_EVENT";
		var Offer_tbl = cnf_db + "." + cnf_schema + ".Offer";
		var oms_offer_redemption_store_group_tbl = cnf_db + "." + cnf_schema + ".oms_offer_redemption_store_group";
		var oms_store_group_tbl = cnf_db + "." + cnf_schema + ".oms_store_group";
		var oms_offer_pod_store_group_tbl = cnf_db + "." + cnf_schema + ".oms_offer_pod_store_group";
		var OMS_OFFER_REGION_tbl = cnf_db + "." + cnf_schema + ".OMS_OFFER_REGION";
		var RETAIL_STORE_tbl = cnf_db + "." + cnf_schema_loc + ".RETAIL_STORE";
		var Facility_tbl = cnf_db + "." + cnf_schema_loc + ".Facility";
		var OMS_STORE_GROUP_STORE_tbl = cnf_db + "." + cnf_schema + ".OMS_STORE_GROUP_STORE";
		var OMS_OFFER_QUALIFICATION_CUSTOMER_GROUP_tbl = cnf_db + "." + cnf_schema + ".OMS_OFFER_QUALIFICATION_CUSTOMER_GROUP";
		var OMS_OFFER_QUALIFICATION_PRODUCT_GROUP_tbl = cnf_db + "." + cnf_schema + ".OMS_OFFER_QUALIFICATION_PRODUCT_GROUP";
		var OMS_Product_Group_tbl = cnf_db + "." + cnf_schema + ".OMS_Product_Group";
		var OMS_OFFER_BENEFIT_POINTS_TIER_tbl = cnf_db + "." + cnf_schema + ".OMS_OFFER_BENEFIT_POINTS_TIER";
		var OMS_OFFER_QUALIFICATION_TRIGGER_CODE_tbl = cnf_db + "." + cnf_schema + ".OMS_OFFER_QUALIFICATION_TRIGGER_CODE";
		var OMS_OFFER_QUALIFICATION_ATTRIBUTE_tbl = cnf_db + "." + cnf_schema + ".OMS_OFFER_QUALIFICATION_ATTRIBUTE";
		var OMS_OFFER_BENEFIT_DISCOUNT_tbl = cnf_db + "." + cnf_schema + ".OMS_OFFER_BENEFIT_DISCOUNT";
		var OMS_OFFER_BENEFIT_DISCOUNT_TIER_tbl = cnf_db + "." + cnf_schema + ".OMS_OFFER_BENEFIT_DISCOUNT_TIER";
		var OMS_OFFER_QUALIFICATION_POINTS_GROUP_tbl = cnf_db + "." + cnf_schema + ".OMS_OFFER_QUALIFICATION_POINTS_GROUP";
		var OMS_OFFER_QUALIFICATION_POINTS_GROUP_TIER_tbl = cnf_db + "." + cnf_schema + ".OMS_OFFER_QUALIFICATION_POINTS_GROUP_TIER";
		var OMS_OFFER_BENEFIT_POINTS_tbl = cnf_db + "." + cnf_schema + ".OMS_OFFER_BENEFIT_POINTS";
		var OMS_OFFER_CASHIER_MESSAGE_TIER_tbl = cnf_db + "." + cnf_schema + ".OMS_OFFER_CASHIER_MESSAGE_TIER";
		var OMS_OFFER_PRINTED_MESSAGE_tbl = cnf_db + "." + cnf_schema + ".OMS_OFFER_PRINTED_MESSAGE";
		var OMS_OFFER_TERMINAL_tbl = cnf_db + "." + cnf_schema + ".OMS_OFFER_TERMINAL";
		var OMS_OFFER_EXCLUDED_TERMINAL_tbl = cnf_db + "." + cnf_schema + ".OMS_OFFER_EXCLUDED_TERMINAL";
		var PROMOTION_STORE_GROUPING_tbl = cnf_db + "." + cnf_schema + ".PROMOTION_STORE_GROUPING";
		var Division_tbl = cnf_db + "." + cnf_schema_loc + ".Division";
		var Offer_Request_Group_tbl = cnf_db + "." + cnf_schema_purchase + ".Offer_Request_Group";
		var Offer_Request_tbl = cnf_db + "." + cnf_schema_purchase + ".Offer_Request";
		
			
		// Empty the target work table
		var sql_empty_tgt_wrk_tbl = `TRUNCATE TABLE `+ tgt_wrk_tbl +` `;
		try {
			snowflake.execute ({sqlText: sql_empty_tgt_wrk_tbl });
			}
		catch (err) { 
			throw "Truncation of table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
		}
		
		// Creating Target Work table
		var cr_src_tmp_wrk_tbl = `Insert into ` + tgt_wrk_tbl + `
                            
				SELECT * FROM (
				SELECT DISTINCT 
						Aggregator_Offer_Id as Agg
						,concat('$',ter.DISCOUNT_TIER_AMT::Number(38,2)) as Amount
						,attb.QUALIFICATION_ATTRIBUTE_TYPE_TXT as Attribute
						,csh.Cashier_Message_Beep_Duration_Nbr as Beep
						,csh.Cashier_Message_Beep_Type_Txt as Cashier_Message
						,o_off.COPIENT_CATEGORY_DSC as Category
						,Delivery_Channel_Dsc as Channel
						,Chargeback_Vendor_Nm as Chargeback_Vendor_Name 
						,dsc.CHARGEBACK_DSC as Chargeback_Department
						,Created_User_Id as Created_by
						,o_Off_Quali_Cust_Grp.Customer_Group_Nm as Customer_Group						
						,case when off_req.Offer_Effective_Day_Monday_Ind = true then 'Mon'
						      when off_req.Offer_Effective_Day_Tuesday_Ind = true then 'Tue'
							  when off_req.Offer_Effective_Day_Wednesday_Ind = true then 'Wed'
							  when off_req.Offer_Effective_Day_Thursday_Ind = true then 'Thu'
							  when off_req.Offer_Effective_Day_Friday_Ind = true then 'Fri'
							  when off_req.Offer_Effective_Day_Saturday_Ind = true then 'Sat'
							  when off_req.Offer_Effective_Day_Sunday_Ind = true then 'Sun' end as Day
						,CASE WHEN Defer_EvaluatiON_Until_EOS_Ind = 'TRUE' THEN 'Yes' ELSE 'No' END as Defer_Evaluation_Until_EOS
						,CASE WHEN o_off.externaL_offer_id LIKE '%-D' THEN NON_DIG.store_group_nm END as Digital_Store_Group
						,case when dsc.BENEFIT_VALUE_TYPE_CD = 'NO_DISCOUNT' then 'No Discount' else dsc.BENEFIT_VALUE_TYPE_DSC end as Discount
						,Display_Effective_End_Dt as Display_End_Date
						,Display_Effective_Start_Dt as Display_Start_Date						
						,ter.DOLLAR_LIMIT_AMT as Dollar_Limit
						,ECom_Dsc as eCommerce_Text
						,Effective_End_Dt as End_Date
						,QualificatiON_Enterprise_Instant_Win_Number_Of_Prizes_Qty as Enterprise_Instant_Win
						,o_off_evnt.OMS_Offer_Event_DescriptiON_Txt as Event
						,CASE WHEN o_Off_Quali_Cust_Grp.Excluded_Users_Ind = 'FALSE' THEN '' ELSE o_Off_Quali_Cust_Grp.Excluded_Users_Ind END as Excluded_Customer
						,eter.Terminal_Number_Txt as Excluded_Terminals
						,External_Offer_Id as External_Offer_ID
						,First_Update_To_RedemptiON_Engine_Ts as First_Deployed
						,First_Update_To_J4U_Ts as First_Published
						,Headline_Txt as Headline
						,CASE WHEN UPPER(Ad_Type_Cd) = 'IA' THEN 'Yes' WHEN UPPER(Ad_Type_Cd) IN ('NA','NIA') THEN 'No' ELSE Ad_Type_Cd END as In_AD
						,CASE WHEN UPPER(In_Email_Ind) = 'TRUE' THEN 'Yes' WHEN UPPER(In_Email_Ind) = 'FALSE' THEN 'No' ELSE In_Email_Ind END as In_Email
						,ter.ITEM_LIMIT_QTY as Item_Limit
						,IVIE_Image_Id as Ivie_Image_ID
						,SUB_QUR2.OMS_OFFER_REGION_CD as J4U_Regions
						,CASE WHEN o_off.externaL_offer_id LIKE '%-D' AND o_off.Is_Appliable_To_J4U_Ind = TRUE THEN J4U.store_group_nm END as Just_for_U_Store_Group
						,Last_Update_To_RedemptiON_Engine_Ts as Last_Deployed
						,First_Update_To_J4U_Ts as Last_Published
						,Updated_User_Last_Nm as Last_updated_by
						,o_off.Primary_Category_Txt as Left_Nav_Category 
						,dsc.DISCOUNT_DSC as Level
						,SUB_QUR1.Minimum_Purchase_Amt as Min_Purchase
						,CASE WHEN o_off.externaL_offer_id LIKE '%-ND' THEN NON_DIG.store_group_nm END as Non_Digital_Store_Group
						,Description_Txt as Offer_Description
						,o_off.OMS_Offer_Id::varchar as Offer_ID
						,o_off.Offer_Nm as Offer_Name
						,Offer_Prototype_Dsc as Offer_Type
						,PRODUCT_DSC1 as POD_Offer_Description
						,o_off.Disclaimer_Txt as POD_Offer_Details_Disclaimer						
						,pts.Points_Group_Id as Points_Group
						,pts.Points_Group_Nm as Points_Group_1
						,Price_value_txt as Price_Text
						,ptd.Printed_Message_Cd as Printed_Message
						,Priority_Cd as Priority
						,Qualification_Product_Disqualifier_Nm as  Product_Disqualifer
						,SUB_QUR1.Product_Group_Id as Product_Group_Id
						,SUB_QUR1.Product_Group_Nm as Product_Group_1
						,Program_Cd as Program_Code
						,o_off_bnft_pnts_tier.Points_Tier_Level_Qty as Tier_Level_Quantity
						,ptr.Tier_Qty as Tier_Quantity
						,ter.Receipt_Txt as Receipt_Text
						,CASE WHEN Program_Cd = 'MF' THEN Requested_Removal_For_All_Ind END as Removed_for_All
						,Removed_Unclipped_ON_Ts as Removed_for_Unclipped
						,Removed_ON_Ts as Removed_On
						,o_off.Offer_Request_Id as Offer_Request_ID		
						,Created_User_Id as Requested_By
						,USAGE_LIMIT_TYPE_PER_USER_DSC as Reward_Freq
						,SUB_QUR.Rog_Id as ROG
						,Product_Image_Id as Scene_7_Image_ID
						,bps.Scorecard_Txt as Scorecard_Text
						,bps.Scorecard_Nm as Scorecard
						,o_off.Primary_Category_Txt as Shopping_List_Category
						,o_off.Cashier_Message_Notification_Ind as Cashier_Show_Always
						,o_off.Printed_Message_Notification_Ind as Printed_Show_Always
						,o_off.Effective_Start_Dt as Start_Date
						,SUB_QUR.Store_Group_Nm as Store_Group
						,Removed_Unclipped_ON_Ts as Submitted
						,term.Terminal_Number_Txt as Terminals
						,Tiers_Cd as Tiers
						,Offr.Notification_Accumulation_Printed_Message_Txt as Accumulation_Message						
						,concat(off_req.Offer_Effective_Start_Tm,' - ',off_req.OFfer_Effective_End_Tm) as Time						
						,o_off_Quali_Trig_cd.QualificatiON_Trigger_Cd as Trigger_Code
						,SUB_QUR1.QUANTITY_UNIT_TYPE_DSC as Unit_of_Measure
						,POD_Usage_Limit_Type_Per_User_Dsc as Usage
						,o_off.Price_Value_Txt as price_Value
						,o_off.Savings_Value_Txt as savings_Value_Text
						,o_off.Beneifit_Value_Type_Dsc as benefit_Value_Type
						,req_grp.Group_Nm as Group_Name
						,case when dsc.Allow_Negative_Ind = 'true' then 'Allow Negative' else 'Flex Negative' end as Advanced
						,case when o_off.EFFECTIVE_END_DT < date(current_timestamp) then 'Expired' else o_off.Offer_Status_Dsc end as Offer_Status
						,SUB_QUR1.Excluded_OMS_Product_Group_Id as Excluded_Products
						,o_off.Offer_Request_Id::VARCHAR as Offer_Request_Id_Txt
						,CASE WHEN src.payload_usageLimitPerUser = 0 THEN NULL ELSE src.payload_usageLimitPerUser END as Custom_Limit
						,CASE WHEN src.payload_customPeriod = 0 THEN NULL ELSE src.payload_customPeriod END as Custom_Period
						,CASE WHEN src.payload_customType = 'Days Since Start Of Incentive' THEN src.payload_customType ELSE NULL END as Custom_Type
						,ter.Weight_Limit_Qty as Per_Lb_Limit
						,Row_number() OVER (PARTITION BY o_off.OMS_Offer_Id ORDER BY o_off.OMS_Offer_Id ASC) AS RN
				FROM ` + OMS_OFFER_tbl + ` o_off
				INNER JOIN ` + src_tbl + ` src ON src.payload_Id = o_off.OMS_Offer_Id
				LEFT JOIN ` + Offer_Request_Group_tbl + ` req_grp ON req_grp.Offer_Request_Id = o_off.Offer_Request_Id AND req_grp.DW_CURRENT_VERSION_IND = TRUE AND req_grp.DW_LOGICAL_DELETE_IND = FALSE
				LEFT JOIN ` + Offer_Request_tbl + ` off_req ON off_req.Offer_Request_Id = o_off.Offer_Request_Id AND off_req.DW_CURRENT_VERSION_IND = TRUE AND off_req.DW_LOGICAL_DELETE_IND = FALSE
				LEFT JOIN ` + OMS_OFFER_EVENT_tbl + ` o_off_evnt ON o_off.oms_offer_id = o_off_evnt.oms_offer_id AND o_off_evnt.DW_CURRENT_VERSION_IND = TRUE AND o_off_evnt.DW_LOGICAL_DELETE_IND = FALSE
				LEFT JOIN ` + Offer_tbl + ` offr on offr.Offer_ID = o_off.oms_offer_id AND offr.DW_CURRENT_VERSION_IND = TRUE AND offr.DW_LOGICAL_DELETE_IND = FALSE
				LEFT JOIN 
				(
						SELECT DISTINCT LISTAGG(ST.store_group_nm, ',') AS store_group_nm 
									,GP.oms_offer_id					   
						FROM ` + oms_offer_redemption_store_group_tbl + ` GP
						INNER JOIN ` + oms_store_group_tbl + ` ST ON ST.STORE_GROUP_ID = GP.STORE_GROUP_ID
						WHERE GP.DW_CURRENT_VERSION_IND = TRUE AND GP.DW_LOGICAL_DELETE_IND = FALSE
						AND ST.DW_CURRENT_VERSION_IND = TRUE AND ST.DW_LOGICAL_DELETE_IND = FALSE
						GROUP BY GP.oms_offer_id 
				) NON_DIG ON NON_DIG.oms_offer_id = o_off.oms_offer_id
				LEFT JOIN 
				(		SELECT DISTINCT LISTAGG(ST.store_group_nm, ',') AS store_group_nm
									,GP.oms_offer_id
						FROM ` + oms_offer_pod_store_group_tbl + ` GP
						INNER JOIN ` + oms_store_group_tbl + ` ST ON ST.STORE_GROUP_ID = GP.STORE_GROUP_ID
						WHERE GP.DW_CURRENT_VERSION_IND = TRUE AND GP.DW_LOGICAL_DELETE_IND = FALSE
						AND ST.DW_CURRENT_VERSION_IND = TRUE AND ST.DW_LOGICAL_DELETE_IND = FALSE
						GROUP BY GP.oms_offer_id 
				) J4U ON J4U.oms_offer_id = o_off.oms_offer_id
				LEFT JOIN 
				(
						SELECT o_off.Is_Appliable_To_J4U_Ind
							,o_off_reg.OMS_OFFER_REGION_CD
							,o_off.oms_offer_id
						FROM ` + OMS_OFFER_REGION_tbl + ` o_off_reg 
						INNER JOIN ` + OMS_OFFER_tbl + ` o_off ON o_off.oms_offer_id = o_off_reg.oms_offer_id
						WHERE o_off_reg.DW_CURRENT_VERSION_IND = TRUE AND o_off_reg.DW_LOGICAL_DELETE_IND = FALSE
						AND o_off.DW_CURRENT_VERSION_IND = TRUE AND o_off.DW_LOGICAL_DELETE_IND = FALSE
						AND o_off.Is_Appliable_To_J4U_Ind = 'TRUE'
				) SUB_QUR2 ON o_off.oms_offer_id = SUB_QUR2.oms_offer_id
				LEFT JOIN 
				(
						SELECT DISTINCT ret_store.Rog_Id
										,REDEM.OMS_OFFER_ID
										,GRP.Store_Group_Nm
						FROM ` + RETAIL_STORE_tbl + ` ret_store
						INNER JOIN ` + Facility_tbl + ` FAC ON FAC.FACILITY_INTEGRATION_ID = ret_store.FACILITY_INTEGRATION_ID 
																			AND FAC.DW_CURRENT_VERSION_IND = TRUE AND FAC.DW_LOGICAL_DELETE_IND = FALSE
						INNER JOIN ` + OMS_STORE_GROUP_STORE_tbl + ` o_off_grp_store ON o_off_grp_store.FACILITY_INTEGRATION_ID = FAC.FACILITY_INTEGRATION_ID AND o_off_grp_store.DW_CURRENT_VERSION_IND = TRUE AND o_off_grp_store.DW_LOGICAL_DELETE_IND = FALSE
						INNER JOIN ` + oms_store_group_tbl + ` GRP ON GRP.STORE_GROUP_ID = o_off_grp_store.STORE_GROUP_ID
																AND GRP.DW_CURRENT_VERSION_IND = TRUE AND GRP.DW_LOGICAL_DELETE_IND = FALSE
						LEFT JOIN ` + oms_offer_pod_store_group_tbl + ` POD ON GRP.STORE_GROUP_ID = POD.STORE_GROUP_ID
																AND POD.DW_CURRENT_VERSION_IND = TRUE AND POD.DW_LOGICAL_DELETE_IND = FALSE
						LEFT JOIN ` + oms_offer_redemption_store_group_tbl + ` REDEM ON REDEM.STORE_GROUP_ID = POD.STORE_GROUP_ID
																AND REDEM.DW_CURRENT_VERSION_IND = TRUE AND REDEM.DW_LOGICAL_DELETE_IND = FALSE
				) SUB_QUR ON o_off.oms_offer_id = SUB_QUR.OMS_OFFER_ID
				LEFT JOIN ` + OMS_OFFER_QUALIFICATION_CUSTOMER_GROUP_tbl + ` o_Off_Quali_Cust_Grp ON o_off.oms_offer_id = o_Off_Quali_Cust_Grp.oms_offer_id
									AND o_Off_Quali_Cust_Grp.DW_CURRENT_VERSION_IND = TRUE AND o_Off_Quali_Cust_Grp.DW_LOGICAL_DELETE_IND = FALSE
				LEFT JOIN 
				(
						SELECT DISTINCT o_off_Quali_PRD_grp.QUANTITY_UNIT_TYPE_DSC  
										,ROUND(o_off_Quali_PRD_grp.Minimum_Purchase_Amt) AS Minimum_Purchase_Amt
										,o_off_Quali_PRD_grp.Product_Group_Id AS Product_Group_Id 
										,o_PRD_grp.Product_Group_Nm 
										,o_off_Quali_PRD_grp.OMS_Offer_Id
										,o_off_Quali_PRD_grp.Excluded_OMS_Product_Group_Id
						FROM ` + OMS_OFFER_QUALIFICATION_PRODUCT_GROUP_tbl + ` o_off_Quali_PRD_grp 
						INNER JOIN ` + OMS_Product_Group_tbl + ` o_PRD_grp ON o_off_Quali_PRD_grp.Product_Group_Id = o_PRD_grp.Product_Group_Id
						WHERE o_off_Quali_PRD_grp.DW_CURRENT_VERSION_IND = TRUE AND o_off_Quali_PRD_grp.DW_LOGICAL_DELETE_IND = FALSE
						AND o_PRD_grp.DW_CURRENT_VERSION_IND = TRUE AND o_PRD_grp.DW_LOGICAL_DELETE_IND = FALSE
				) SUB_QUR1 ON SUB_QUR1.OMS_Offer_Id = o_off.oms_offer_id
				LEFT JOIN ` + OMS_OFFER_BENEFIT_POINTS_TIER_tbl + ` o_off_bnft_pnts_tier ON o_off.oms_offer_id = o_off_bnft_pnts_tier.oms_offer_id AND o_off_bnft_pnts_tier.DW_CURRENT_VERSION_IND = TRUE AND o_off_bnft_pnts_tier.DW_LOGICAL_DELETE_IND = FALSE
				LEFT JOIN ` + OMS_OFFER_QUALIFICATION_TRIGGER_CODE_tbl + ` o_off_Quali_Trig_cd ON o_off.oms_offer_id = o_off_Quali_Trig_cd.oms_offer_id AND o_off_Quali_Trig_cd.DW_CURRENT_VERSION_IND = TRUE AND o_off_Quali_Trig_cd.DW_LOGICAL_DELETE_IND = FALSE
				LEFT JOIN ` + OMS_OFFER_QUALIFICATION_ATTRIBUTE_tbl + ` attb ON attb.OMS_Offer_Id = o_off.OMS_Offer_Id AND attb.DW_CURRENT_VERSION_IND = TRUE AND attb.DW_LOGICAL_DELETE_IND = FALSE
				LEFT JOIN ` + OMS_OFFER_BENEFIT_DISCOUNT_tbl + ` dsc ON o_off.OMS_Offer_Id = dsc.OMS_Offer_Id AND dsc.DW_CURRENT_VERSION_IND = TRUE AND dsc.DW_LOGICAL_DELETE_IND = FALSE
				LEFT JOIN ` + OMS_OFFER_BENEFIT_DISCOUNT_TIER_tbl + ` ter ON o_off.OMS_Offer_Id = ter.OMS_Offer_Id AND ter.DW_CURRENT_VERSION_IND = TRUE AND ter.DW_LOGICAL_DELETE_IND = FALSE
				LEFT JOIN ` + OMS_OFFER_QUALIFICATION_POINTS_GROUP_tbl + ` pts ON o_off.OMS_Offer_Id = pts.OMS_Offer_Id AND pts.DW_CURRENT_VERSION_IND = TRUE AND pts.DW_LOGICAL_DELETE_IND = FALSE
				LEFT JOIN ` + OMS_OFFER_QUALIFICATION_POINTS_GROUP_TIER_tbl + ` ptr ON o_off.OMS_Offer_Id = ptr.OMS_Offer_Id AND ptr.DW_CURRENT_VERSION_IND = TRUE AND ptr.DW_LOGICAL_DELETE_IND = FALSE
				LEFT JOIN ` + OMS_OFFER_BENEFIT_POINTS_tbl + ` bps ON bps.OMS_Offer_Id = o_off.OMS_Offer_Id AND bps.DW_CURRENT_VERSION_IND = TRUE AND bps.DW_LOGICAL_DELETE_IND = FALSE
				LEFT JOIN ` + OMS_OFFER_CASHIER_MESSAGE_TIER_tbl + ` csh ON csh.OMS_Offer_Id = o_off.OMS_Offer_Id AND csh.DW_CURRENT_VERSION_IND = TRUE AND csh.DW_LOGICAL_DELETE_IND = FALSE
				LEFT JOIN ` + OMS_OFFER_PRINTED_MESSAGE_tbl + ` ptd ON ptd.OMS_Offer_Id = o_off.OMS_Offer_Id AND ptd.DW_CURRENT_VERSION_IND = TRUE AND ptd.DW_LOGICAL_DELETE_IND = FALSE
				LEFT JOIN 
				(
					SELECT DISTINCT LISTAGG(Terminal_Number_Txt, ',') AS Terminal_Number_Txt
										,OMS_Offer_Id 
									FROM ` + OMS_OFFER_TERMINAL_tbl + `
									WHERE DW_CURRENT_VERSION_IND = TRUE AND DW_LOGICAL_DELETE_IND = FALSE
									GROUP BY OMS_Offer_Id
				) term on term.OMS_Offer_Id = o_off.OMS_Offer_Id 
				
				LEFT JOIN 
				(
					SELECT DISTINCT LISTAGG(Terminal_Number_Txt, ',') AS Terminal_Number_Txt 
										,OMS_Offer_Id 
									FROM ` + OMS_OFFER_EXCLUDED_TERMINAL_tbl + `
									WHERE DW_CURRENT_VERSION_IND = TRUE AND DW_LOGICAL_DELETE_IND = FALSE
									GROUP BY OMS_Offer_Id
				) eter ON ETER.OMS_Offer_Id	= o_off.OMS_Offer_Id 
				WHERE o_off.DW_CURRENT_VERSION_IND = TRUE AND o_off.DW_LOGICAL_DELETE_IND = FALSE
				AND UPPER(src.METADATA$ACTION) = 'INSERT' 				
				)
				WHERE RN = 1				
				`;

		try {
			snowflake.execute (
				{sqlText: cr_src_tmp_wrk_tbl  }
			)
		}
		catch (err)  {
			throw "Creation of Fact_Offer_Reports work table Failed with error: " + err;   // Return a error message.
		}
		
		var sql_begin = "BEGIN";
		
		// Processing deletes		
		var sql_deletes = `delete from ` + tgt_tbl + `
				where (Offer_ID)
				in (select distinct Offer_ID
				from ` + tgt_wrk_tbl + `);`;
			
		var sql_updates = `update ` + tgt_tbl + ` set Offer_Status = 'Expired'
		                   where End_Date < current_date`;
						
		// Processing Inserts 
		var sql_inserts = `INSERT INTO ` + tgt_tbl + `
			(
		      Agg                         
			 ,Amount                      
			 ,Attribute                   
			 ,Beep                        
			 ,Cashier_Message             
			 ,Category                    
			 ,Channel                     
			 ,Chargeback_Vendor_Name      
			 ,Chargeback_Department       
			 ,Created_by                  
			 ,Customer_Group              
			 ,Day                         
			 ,Defer_Evaluation_Until_EOS  
			 ,Digital_Store_Group         
			 ,Discount                    
			 ,Display_End_Date            
			 ,Display_Start_Date          			             
			 ,Dollar_Limit                
			 ,eCommerce_Text              
			 ,End_Date                    
			 ,Enterprise_Instant_Win      
			 ,Event                       
			 ,Excluded_Customer           
			 ,Excluded_Terminals          
			 ,External_Offer_ID           
			 ,First_Deployed              
			 ,First_Published             
			 ,Headline                    
			 ,In_AD                       
			 ,In_Email                    
			 ,Item_Limit                  
			 ,Ivie_Image_ID               
			 ,J4U_Regions                 
			 ,Just_for_U_Store_Group      
			 ,Last_Deployed               
			 ,Last_Published              
			 ,Last_updated_by             
			 ,Left_Nav_Category           
			 ,Level                       
			 ,Min_Purchase                
			 ,Non_Digital_Store_Group     
			 ,Offer_Description           
			 ,Offer_ID                    
			 ,Offer_Name                  
			 ,Offer_Type                  
			 ,POD_Offer_Description       
			 ,POD_Offer_Details_Disclaimer
			 ,Points_Group                
			 ,Points_Group_1              
			 ,Price_Text                  
			 ,Printed_Message             
			 ,Priority                    
			 ,Product_Disqualifer         
			 ,Product_Group_Id            
			 ,Product_Group_1             
			 ,Program_Code                
			 ,Tier_Level_Quantity         
			 ,Tier_Quantity               
			 ,Receipt_Text                
			 ,Removed_for_All             
			 ,Removed_for_Unclipped       
			 ,Removed_On                  
			 ,Offer_Request_ID            
			 ,Requested_By                
			 ,Reward_Freq                 
			 ,ROG                         
			 ,Scene_7_Image_ID            
			 ,Scorecard_Text              
			 ,Scorecard                   
			 ,Shopping_List_Category      
			 ,Cashier_Show_Always         
			 ,Printed_Show_Always         
			 ,Start_Date                  
			 ,Store_Group                 
			 ,Submitted                   
			 ,Terminals                   
			 ,Tiers                       
			 ,Accumulation_Message        
			 ,Time                        
			 ,Trigger_Code                
			 ,Unit_of_Measure             
			 ,Usage                       
			 ,price_Value                 
			 ,savings_Value_Text          
			 ,benefit_Value_Type          
			 ,Advanced                    
			 ,Offer_Status                
			 ,Excluded_Products           
			 ,Offer_Request_Id_Txt 
			 ,Custom_Limit
			 ,Custom_Period
			 ,Custom_Type
			 ,Group_Name
			 ,Per_Lb_Limit
			 ,RN                          
			 ,Created_TS 
			 )
			 
		SELECT 
			  Agg                         
			 ,Amount                      
			 ,Attribute                   
			 ,Beep                        
			 ,Cashier_Message             
			 ,Category                    
			 ,Channel                     
			 ,Chargeback_Vendor_Name      
			 ,Chargeback_Department       
			 ,Created_by                  
			 ,Customer_Group              
			 ,Day                         
			 ,Defer_Evaluation_Until_EOS  
			 ,Digital_Store_Group         
			 ,Discount                    
			 ,Display_End_Date            
			 ,Display_Start_Date          
			 ,Dollar_Limit                
			 ,eCommerce_Text              
			 ,End_Date                    
			 ,Enterprise_Instant_Win      
			 ,Event                       
			 ,Excluded_Customer           
			 ,Excluded_Terminals          
			 ,External_Offer_ID           
			 ,First_Deployed              
			 ,First_Published             
			 ,Headline                    
			 ,In_AD                       
			 ,In_Email                    
			 ,Item_Limit                  
			 ,Ivie_Image_ID               
			 ,J4U_Regions                 
			 ,Just_for_U_Store_Group      
			 ,Last_Deployed               
			 ,Last_Published              
			 ,Last_updated_by             
			 ,Left_Nav_Category           
			 ,Level                       
			 ,Min_Purchase                
			 ,Non_Digital_Store_Group     
			 ,Offer_Description           
			 ,Offer_ID                    
			 ,Offer_Name                  
			 ,Offer_Type                  
			 ,POD_Offer_Description       
			 ,POD_Offer_Details_Disclaimer
			 ,Points_Group                
			 ,Points_Group_1              
			 ,Price_Text                  
			 ,Printed_Message             
			 ,Priority                    
			 ,Product_Disqualifer         
			 ,Product_Group_Id            
			 ,Product_Group_1             
			 ,Program_Code                
			 ,Tier_Level_Quantity         
			 ,Tier_Quantity               
			 ,Receipt_Text                
			 ,Removed_for_All             
			 ,Removed_for_Unclipped       
			 ,Removed_On                  
			 ,Offer_Request_ID            
			 ,Requested_By                
			 ,Reward_Freq                 
			 ,ROG                         
			 ,Scene_7_Image_ID            
			 ,Scorecard_Text              
			 ,Scorecard                   
			 ,Shopping_List_Category      
			 ,Cashier_Show_Always         
			 ,Printed_Show_Always         
			 ,Start_Date                  
			 ,Store_Group                 
			 ,Submitted                   
			 ,Terminals                   
			 ,Tiers                       
			 ,Accumulation_Message        
			 ,Time                        
			 ,Trigger_Code                
			 ,Unit_of_Measure             
			 ,Usage                       
			 ,price_Value                 
			 ,savings_Value_Text          
			 ,benefit_Value_Type          
			 ,Advanced                    
			 ,Offer_Status                
			 ,Excluded_Products           
			 ,Offer_Request_Id_Txt   
			 ,Custom_Limit
			 ,Custom_Period
			 ,Custom_Type	
			 ,Group_Name
			 ,Per_Lb_Limit
			 ,RN                          
			 ,CURRENT_TIMESTAMP
		FROM ` + tgt_wrk_tbl + `;`;
		
		var sql_commit = "COMMIT"
		var sql_rollback = "ROLLBACK"
		try {
			snowflake.execute (
				{sqlText: sql_begin}
			);
			snowflake.execute (
				{sqlText: sql_deletes}
			);
			snowflake.execute (
				{sqlText: sql_updates}
			);
			snowflake.execute (
				{sqlText: sql_inserts}
			);
			snowflake.execute (
				{sqlText: sql_commit}
			);    
		}
		catch (err) {
			snowflake.execute (
				{sqlText: sql_rollback}
			);
			throw "Loading of Fact_Offer_Request " + tgt_tbl + " Failed with error: " + err;   // Return a error message.
		}		
				
				// **************        Load for Fact_Offer_Reports ENDs *****************
				
		return "Done"

	$$;

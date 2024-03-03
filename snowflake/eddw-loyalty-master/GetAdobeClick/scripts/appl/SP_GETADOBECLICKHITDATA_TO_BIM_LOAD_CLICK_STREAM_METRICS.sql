--liquibase formatted sql
--changeset SYSTEM:SP_GETADOBECLICKHITDATA_TO_BIM_LOAD_CLICK_STREAM_METRICS runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_CONFIRMED_PRD;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE EDM_CONFIRMED_PRD.DW_APPL.SP_GETADOBECLICKHITDATA_TO_BIM_LOAD_CLICK_STREAM_METRICS(SRC_TBL VARCHAR, CNF_DB VARCHAR, C_USER_ACT VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$


		var cnf_db = CNF_DB;
		var wrk_schema = C_STAGE;
		var cnf_schema = C_USER_ACT;
		var src_tbl = SRC_TBL;
		var metrics_tbl_nm = 'CLICK_STREAM_METRICS'; 
		var cntrl_tbl_nm = 'CLICK_STREAM_CONTROL_TABLE';

		var src_wrk_tbl = `${cnf_db}.${wrk_schema}.${metrics_tbl_nm}_src_WRK`;		
        var src_rerun_tbl = `${cnf_db}.${wrk_schema}.${metrics_tbl_nm}_Rerun`;
		var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.${metrics_tbl_nm}_WRK`;
		var tgt_tbl = `${cnf_db}.${cnf_schema}.${metrics_tbl_nm}`;
		var tgt_exp_tbl = `${cnf_db}.${wrk_schema}.${metrics_tbl_nm}_EXCEPTION`;	
		var sp_name = 'SP_GetAdobeClickHitData_TO_BIM_LOAD_Click_Stream_Metrics';

function log(status,msg){
	var cnf_msg = msg;
	try{	
    snowflake.createStatement( 
	{ sqlText: `call SP_GetAdobeClickHitData_Click_Stream_Log('${cnf_db}',
															  '${wrk_schema}',
															  '${cnf_msg}',
															  '${status }',
															  '${sp_name}')`}).execute();																						 
		}catch(err)
		{
		snowflake.createStatement( 
		{ sqlText: `call SP_GetAdobeClickHitData_Click_Stream_Log('${cnf_db}',
																  '${wrk_schema}',
																  'Unable to insert exception',
																  '${status }',
																  '${sp_name}')`}).execute();							
		}}	
	
	log('STARTED','Load for Click Stream Metrics table BEGIN');		
                       
    // ********************** Load for Click Stream Metrics table BEGIN *****************
    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 


// persist stream data in work table for the current transaction, includes data from previous failed run
	var sql_crt_src_wrk_tbl = `create or replace table ${src_wrk_tbl} DATA_RETENTION_TIME_IN_DAYS = 0 as
    SELECT * FROM ${src_tbl}
    UNION ALL
    SELECT * FROM ${src_rerun_tbl}`;
    try {
        snowflake.execute({ sqlText: sql_crt_src_wrk_tbl });
		log('SUCCEEDED',`Successfuly created Source Work table ${src_wrk_tbl}`);
    } catch (err)  {
	    log('FAILED',`Creation of Source Work table ${src_wrk_tbl} Failed with error: ${err}`);
        throw `Creation of Source Work table ${src_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
    }
	
	// Empty the rerun queue table
	var sql_empty_rerun_tbl = `TRUNCATE TABLE ${src_rerun_tbl}`;
	try {
        snowflake.execute({ sqlText: sql_empty_rerun_tbl });
		log('SUCCEEDED',`Successfuly truncated rerun queue table ${src_rerun_tbl}`);
    } catch (err) {
	    log('FAILED',`Truncation of rerun queue table ${src_rerun_tbl} Failed with error: ${err}`);
        throw `Truncation of rerun queue table ${src_rerun_tbl} Failed with error: ${err}`;   // Return a error message.
    }
    
	// query to load rerun queue table when encountered a failure
	var sql_ins_rerun_tbl = `CREATE OR REPLACE TABLE ${src_rerun_tbl} as SELECT * FROM ${src_wrk_tbl}`;

var create_tgt_wrk_table = `CREATE OR REPLACE TABLE ${tgt_wrk_tbl} as 
 
							WITH src_wrk_tbl_recs as
                            (SELECT DISTINCT 
                             HITID_HIGH
							,HITID_LOW
							,VISIT_PAGE_NUM
							,VISIT_NUM
							,POST_EVENT_LIST
                            ,POST_PRODUCT_LIST
							,POST_EVAR13
                            ,POST_EVAR32
                            ,POST_EVAR182
							,EXCLUDE_HIT
							,HIT_SOURCE
							,DW_SOURCE_CREATE_NM 
							,POST_EVAR156
							,row_number() over(partition by HITID_HIGH,HITID_LOW,VISIT_PAGE_NUM,VISIT_NUM  
										  ORDER BY(DW_CREATETS) DESC) as rn
							FROM ${src_wrk_tbl}
							WHERE  	HITID_HIGH 			IS NOT NULL	
									AND HITID_LOW  		IS NOT NULL	
									AND VISIT_PAGE_NUM  IS NOT NULL	
									AND VISIT_NUM  		IS NOT NULL	
							)
							SELECT 
							s.CLICK_STREAM_INTEGRATION_ID
                            ,s.HITID_HIGH		as Hit_Id_High
							,s.HITID_LOW		as Hit_Id_Low
							,s.VISIT_PAGE_NUM	as Visit_Page_Nbr
							,s.VISIT_NUM		as Visit_Nbr
							,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,203,%' 	 then TRUE else FALSE end Application_Registration_Ind
                            ,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20339,%' then TRUE else FALSE end Base_Price_Ind
                            ,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20322,%' then TRUE else FALSE end Box_Tops_Get_Started_Ind
                            ,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20307,%' then TRUE else FALSE end Checkout_Step1_Ind
                            ,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20308,%' then TRUE else FALSE end Checkout_Step2_Ind
                            ,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20309,%' then TRUE else FALSE end Checkout_Step3_Ind
                            ,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20310,%' then TRUE else FALSE end Checkout_Step4_Ind
                            ,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20311,%' then TRUE else FALSE end Checkout_Step5_Ind
                            ,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20312,%' then TRUE else FALSE end Checkout_Step6_Ind
                            ,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20176,%' then TRUE else FALSE end Clear_Recent_Search_Term_Ind
                            ,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20342,%' then TRUE else FALSE end Clubcard_Savings_Amount_Ind
                            ,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20190,%' then TRUE else FALSE end Contact_Save_Ind
                            ,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20357,%' then TRUE else FALSE end Coupon_Applied_Number_Ind
                            ,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20355,%' then TRUE else FALSE end Coupon_Available_Number_Ind
                            ,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20305,%' then TRUE else FALSE end Coupon_Clip_Ind
                            ,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20356,%' then TRUE else FALSE end Coupon_Clipped_Number_Ind
                            ,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20159,%' then TRUE else FALSE end Coupon_Id_Clipped_Number_Ind
                            ,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20333,%' then TRUE else FALSE end Customer_Web_Registion_Ind
                            ,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20160,%' then TRUE else FALSE end Details_Ind
                            ,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20206,%' then TRUE else FALSE end Edit_Order_Cart_Ind
                            ,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20204,%' then TRUE else FALSE end Edit_Timestamp_Ind
                            ,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20345,%' then TRUE else FALSE end Employee_Savings_Amount_Ind
                            ,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20199,%' then TRUE else FALSE end Error_Ind
                            ,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20175,%' then TRUE else FALSE end Flash_Light_Ind
                            ,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,1,%' 	 then TRUE else FALSE end Gross_Order_Amount_Ind
                            ,NULL as Gross_Revenue_Amt
                            ,NULL as Gross_Units_Nbr
                            ,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20341,%' then TRUE else FALSE end   Item_Price_Amount_Ind
                            ,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20343,%' then TRUE else FALSE end   J4u_Savings_Amount_Ind
                            ,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20340,%' then TRUE else FALSE end   List_Price_Amount_Ind
                            ,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20110,%' then TRUE else FALSE end   Mini_Cart_Open_Ind
                            ,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20167,%' then TRUE else FALSE end   Modal_Click_Ind
                            ,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20267,%' then TRUE else FALSE end   Modal_View_Ind
                            ,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,207,%' 	 then TRUE else FALSE end   Null_Search_Number_Ind
                            ,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20239,%' then TRUE else FALSE end   OFF_BANNER_DELIVERY_TRUE_IND
                            ,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20240,%' then TRUE else FALSE end   OFF_BANNER_DELIVERY_FALSE_IND
							,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20172,%' then TRUE else FALSE end   Open_Ind
							,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,271,%' 	 then TRUE else FALSE end   Order_Ahead_Cart_Add_Ind
							,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,272,%' 	 then TRUE else FALSE end   Order_Ahead_Confirmation_Ind
							,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,270,%' 	 then TRUE else FALSE end   Order_Ahead_Product_View_Ind
							,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,274,%' 	 then TRUE else FALSE end   Order_Ahead_Total_Over_Due_Ind
							,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20191,%' then TRUE else FALSE end   Order_Info_Save_Ind
							,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20354,%' then TRUE else FALSE end   Order_Revenue_Ind
							,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20189,%' then TRUE else FALSE end   Order_Update_Txt_Ind
							,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,269,%' 	 then TRUE else FALSE end   Out_Of_Stock_Ind
							,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20192,%' then TRUE else FALSE end   Payment_Save_Ind
							,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20306,%' then TRUE else FALSE end   Place_Order_Count_Ind
							,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20149,%' then TRUE else FALSE end   Product_Decrease_Quantity_Ind
							,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20173,%' then TRUE else FALSE end   Product_Found_1_Ind
							,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20174,%' then TRUE else FALSE end   Product_Found_2_Ind
							,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20150,%' then TRUE else FALSE end   Product_Increase_Quantity_Ind
							,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20193,%' then TRUE else FALSE end   Promotion_Code_Ind
							,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20346,%' then TRUE else FALSE end   Promotion_Code_Savings_Amount_Ind
							,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20207,%' then TRUE else FALSE end   Purchase_Cancellation_Ind
							,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,275,%' 	 then TRUE else FALSE end   Push_Notification_Click_Ind
							,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20177,%' then TRUE else FALSE end   Recipe_Swap_Modal_Open_Ind
							,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20178,%' then TRUE else FALSE end   Recipe_Swap_Modal_Product_Ind
							,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,216,%' 	 then TRUE else FALSE end   Reserve_Time_Update_Ind
							,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20165,%' then TRUE else FALSE end   Reward_Redeemed_Ind
							,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20166,%' then TRUE else FALSE end   Rewards_Redeemed_Count_Ind
							,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20344,%' then TRUE else FALSE end   Rewards_Savings_Amount_Ind
							,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20103,%' then TRUE else FALSE end   Scan_Auto_Clip_Ind
							,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20155,%' then TRUE else FALSE end   Search_Result_Count_Ind
							,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20200,%' then TRUE else FALSE end   Shipping_Fee_Amount_Ind
							,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,209,%' 	 then TRUE else FALSE end   Sign_In_Ind
							,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20202,%' then TRUE else FALSE end   Sub_Total_Amount_Ind
							,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,20305,%' then TRUE else FALSE end   Top_Nav_Clicks_Ind
							,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,294,%' 	 then TRUE else FALSE end   User_Action_Ind
							,case when CONCAT(',', s.post_event_list, ',')  LIKE '%,215,%'   then TRUE else FALSE end   User_Logout_Ind
                            ,case when (s.exclude_hit= 0 and s.hit_source not in (5, 7, 8, 9)) then 0 else 1 end  		Exclude_Row_ind 
                            ,TRY_TO_NUMERIC(C.Facility_Integration_ID) as Facility_Integration_ID
                            ,D.ORDER_ID as ORDER_ID
                            ,E.Retail_Customer_UUID as Retail_Customer_UUID
                            ,TRY_TO_NUMERIC(D.Version_Nbr) as Version_Nbr
							,s.POST_PRODUCT_LIST
							,s.POST_EVAR156 as Clip_Actions_Per_Visit_Nbr
							FROM 
							(
							SELECT  
							 CLICK_STREAM_INTEGRATION_ID
							,HITID_HIGH
							,HITID_LOW
							,VISIT_PAGE_NUM
							,VISIT_NUM
							,POST_EVENT_LIST
							,POST_PRODUCT_LIST
                            ,POST_EVAR13
                            ,POST_EVAR32
                            ,POST_EVAR182
							,EXCLUDE_HIT
							,HIT_SOURCE
							,DW_SOURCE_CREATE_NM
							,POST_EVAR156
							FROM src_wrk_tbl_recs s, ${cnf_db}.${cnf_schema}.${cntrl_tbl_nm} sct
							WHERE rn = 1
								AND s.HITID_HIGH = sct.HIT_ID_HIGH   
								AND s.HITID_LOW = sct.HIT_ID_LOW  
								AND s.VISIT_PAGE_NUM = sct.VISIT_PAGE_NBR 
								AND s.VISIT_NUM = sct.VISIT_NBR 
								AND	sct.CLICK_STREAM_INTEGRATION_ID IS NOT NULL									
							) s							
							LEFT JOIN 
							( SELECT DISTINCT FACILITY_INTEGRATION_ID
									  ,FACILITY_NBR
								FROM ${cnf_db}.DW_C_LOCATION.FACILITY
								WHERE CORPORATION_ID ='001' 
								AND DW_CURRENT_VERSION_IND='TRUE'
								AND DATE(TEMP_CLOSE_DT)='9999-12-31'
							) C  ON s.POST_EVAR13 = C.FACILITY_NBR 
							
							LEFT JOIN 
							( SELECT  MAX(Version_Nbr) Version_Nbr ,ORDER_ID
								FROM   ${cnf_db}.DW_C_ECOMMERCE.GROCERY_ORDER_HEADER
									  WHERE DW_CURRENT_VERSION_IND='TRUE'
									  GROUP BY ORDER_ID 
							) D ON s.POST_EVAR32 = D.ORDER_ID 
		
							LEFT JOIN 
							( SELECT DISTINCT RETAIL_CUSTOMER_UUID
								FROM  ${cnf_db}.DW_C_CUSTOMER.RETAIL_CUSTOMER
								where DW_CURRENT_VERSION_IND='TRUE'
							) E ON s.POST_EVAR182 = E.RETAIL_CUSTOMER_UUID 
	
							LEFT JOIN ${tgt_tbl} TGT
								ON  s.HITID_HIGH = TGT.HIT_ID_HIGH
								AND s.HITID_LOW = TGT.HIT_ID_LOW
								AND s.VISIT_NUM = TGT.VISIT_NBR
								AND s.VISIT_PAGE_NUM = TGT.VISIT_PAGE_NBR
								 WHERE  (tgt.Hit_Id_High IS NULL AND tgt.Hit_Id_Low IS NULL AND tgt.Visit_Page_Nbr IS NULL AND tgt.Visit_Nbr IS NULL)`;				
															
							


var sql_updates = `UPDATE ${tgt_wrk_tbl}  a SET Gross_Units_Nbr=b.Units,Gross_Revenue_Amt=b.Revenue
		FROM( WITH default_result as (
				SELECT  HIT_ID_HIGH,HIT_ID_LOW,VISIT_PAGE_NBR,VISIT_NBR,POST_PRODUCT_LIST as POST_PRODUCT_LIST1,
				c.value::string as POST_PRODUCT_LIST FROM ${tgt_wrk_tbl} ,
				lateral flatten(input=>split(POST_PRODUCT_LIST, ',')) c 
				WHERE  split_part(c.value::string,';',3)!='' AND split_part(c.value::string,';',4)!='')
			SELECT HIT_ID_HIGH,HIT_ID_LOW,VISIT_PAGE_NBR,VISIT_NBR,SUM(split_part(POST_PRODUCT_LIST,';',3)) as Units
				,SUM(split_part(POST_PRODUCT_LIST,';',4)) as Revenue
			FROM default_result  
				GROUP BY  HIT_ID_HIGH,HIT_ID_LOW,VISIT_PAGE_NBR,VISIT_NBR
			) b
					WHERE   a.HIT_ID_HIGH = b.HIT_ID_HIGH
                        AND a.HIT_ID_LOW = b.HIT_ID_LOW
                        AND a.VISIT_PAGE_NBR = b.VISIT_PAGE_NBR
                        AND a.VISIT_NBR= b.VISIT_NBR`;							
										
						
try {
snowflake.execute ({sqlText: create_tgt_wrk_table});
snowflake.execute ({sqlText: sql_updates});
log('SUCCEEDED',`Successfuly created work table ${tgt_wrk_tbl}`);				  
	}
    catch (err) { 
	snowflake.execute ( {sqlText: sql_ins_rerun_tbl} );
	log('FAILED',`Creation of work table ${tgt_wrk_tbl} Failed with error: ${err}`);
    return `Creation of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
				}   
				 
// Transaction for Updates, Insert begins           
    var sql_begin = "BEGIN"			
				
						
// Processing Inserts
	var sql_inserts = `INSERT INTO ${tgt_tbl} 
						(
						 CLICK_STREAM_INTEGRATION_ID
						,HIT_ID_HIGH
						,HIT_ID_LOW
						,VISIT_PAGE_NBR
						,VISIT_NBR
						,Application_Registration_Ind
						,Base_Price_Ind
						,Box_Tops_Get_Started_Ind
						,Checkout_Step1_Ind
						,Checkout_Step2_Ind
						,Checkout_Step3_Ind
						,Checkout_Step4_Ind
						,Checkout_Step5_Ind
						,Checkout_Step6_Ind
						,Clubcard_Savings_Amount_Ind
						,Coupon_Id_Clipped_Number_Ind
						,Contact_Save_Ind
						,Coupon_Clip_Ind
						,Coupon_Available_Number_Ind
						,Coupon_Clipped_Number_Ind
						,Coupon_Applied_Number_Ind
						,Customer_Web_Registion_Ind
						,Clear_Recent_Search_Term_Ind
						,Details_Ind
						,Employee_Savings_Amount_Ind
						,Edit_Timestamp_Ind
						,Edit_Order_Cart_Ind
						,Error_Ind
						,EXCLUDE_ROW_IND
						,FACILITY_INTEGRATION_ID
						,Flash_Light_Ind
						,Gross_Order_Amount_Ind
						,Gross_Revenue_Amt
						,Gross_Units_Nbr
						,Item_Price_Amount_Ind
						,J4u_Savings_Amount_Ind
						,List_Price_Amount_Ind
						,Mini_Cart_Open_Ind
						,Modal_Click_Ind
						,Modal_View_Ind
						,Null_Search_Number_Ind
						,OFF_BANNER_DELIVERY_TRUE_IND
						,OFF_BANNER_DELIVERY_FALSE_IND
						,Open_Ind
						,ORDER_ID
						,Order_Update_Txt_Ind
						,Order_Info_Save_Ind
						,Order_Revenue_Ind
						,Order_Ahead_Product_View_Ind
						,Order_Ahead_Cart_Add_Ind
						,Order_Ahead_Confirmation_Ind
						,Order_Ahead_Total_Over_Due_Ind
						,Out_Of_Stock_Ind
						,Payment_Save_Ind
						,Place_Order_Count_Ind
						,Product_Decrease_Quantity_Ind
						,Product_Increase_Quantity_Ind
						,Product_Found_1_Ind
						,Product_Found_2_Ind
						,Promotion_Code_Ind
						,Promotion_Code_Savings_Amount_Ind
						,Purchase_Cancellation_Ind
						,Push_Notification_Click_Ind
						,Reward_Redeemed_Ind
						,Rewards_Redeemed_Count_Ind
						,Recipe_Swap_Modal_Open_Ind
						,Recipe_Swap_Modal_Product_Ind
						,Reserve_Time_Update_Ind
						,RETAIL_CUSTOMER_UUID
						,Rewards_Savings_Amount_Ind
						,Scan_Auto_Clip_Ind
						,Search_Result_Count_Ind
						,Sign_In_Ind
						,Shipping_Fee_Amount_Ind
						,Sub_Total_Amount_Ind
						,Top_Nav_Clicks_Ind
						,User_Action_Ind
						,User_Logout_Ind
						,VERSION_NBR
						,Clip_Actions_Per_Visit_Nbr
						,DW_CREATE_TS 
						,DW_LAST_UPDATE_TS
						,DW_LOGICAL_DELETE_IND
						,DW_SOURCE_CREATE_NM
						,DW_SOURCE_UPDATE_NM
						,DW_CURRENT_VERSION_IND
						)
				      
					    SELECT 
						CLICK_STREAM_INTEGRATION_ID
						,HIT_ID_HIGH
						,HIT_ID_LOW
						,VISIT_PAGE_NBR
						,VISIT_NBR
						,Application_Registration_Ind
						,Base_Price_Ind
						,Box_Tops_Get_Started_Ind
						,Checkout_Step1_Ind
						,Checkout_Step2_Ind
						,Checkout_Step3_Ind
						,Checkout_Step4_Ind
						,Checkout_Step5_Ind
						,Checkout_Step6_Ind
						,Clubcard_Savings_Amount_Ind
						,Coupon_Id_Clipped_Number_Ind
						,Contact_Save_Ind
						,Coupon_Clip_Ind
						,Coupon_Available_Number_Ind
						,Coupon_Clipped_Number_Ind
						,Coupon_Applied_Number_Ind
						,Customer_Web_Registion_Ind
						,Clear_Recent_Search_Term_Ind
						,Details_Ind
						,Employee_Savings_Amount_Ind
						,Edit_Timestamp_Ind
						,Edit_Order_Cart_Ind
						,Error_Ind
						,EXCLUDE_ROW_IND
						,FACILITY_INTEGRATION_ID
						,Flash_Light_Ind
						,Gross_Order_Amount_Ind
						,TRY_TO_DECIMAL(Gross_Revenue_Amt,16,4) as Gross_Revenue_Amt
						,TRY_TO_NUMERIC(Gross_Units_Nbr) as Gross_Units_Nbr
						,Item_Price_Amount_Ind
						,J4u_Savings_Amount_Ind
						,List_Price_Amount_Ind
						,Mini_Cart_Open_Ind
						,Modal_Click_Ind
						,Modal_View_Ind
						,Null_Search_Number_Ind
						,OFF_BANNER_DELIVERY_TRUE_IND
						,OFF_BANNER_DELIVERY_FALSE_IND
						,Open_Ind
						,ORDER_ID
						,Order_Update_Txt_Ind
						,Order_Info_Save_Ind
						,Order_Revenue_Ind
						,Order_Ahead_Product_View_Ind
						,Order_Ahead_Cart_Add_Ind
						,Order_Ahead_Confirmation_Ind
						,Order_Ahead_Total_Over_Due_Ind
						,Out_Of_Stock_Ind
						,Payment_Save_Ind
						,Place_Order_Count_Ind
						,Product_Decrease_Quantity_Ind
						,Product_Increase_Quantity_Ind
						,Product_Found_1_Ind
						,Product_Found_2_Ind
						,Promotion_Code_Ind
						,Promotion_Code_Savings_Amount_Ind
						,Purchase_Cancellation_Ind
						,Push_Notification_Click_Ind
						,Reward_Redeemed_Ind
						,Rewards_Redeemed_Count_Ind
						,Recipe_Swap_Modal_Open_Ind
						,Recipe_Swap_Modal_Product_Ind
						,Reserve_Time_Update_Ind
						,RETAIL_CUSTOMER_UUID
						,Rewards_Savings_Amount_Ind
						,Scan_Auto_Clip_Ind
						,Search_Result_Count_Ind
						,Sign_In_Ind
						,Shipping_Fee_Amount_Ind
						,Sub_Total_Amount_Ind
						,Top_Nav_Clicks_Ind
						,User_Action_Ind
						,User_Logout_Ind
						,VERSION_NBR
						,Clip_Actions_Per_Visit_Nbr
						,CURRENT_TIMESTAMP as DW_CREATE_TS 
						,NULL as DW_LAST_UPDATE_TS
						,FALSE as DW_LOGICAL_DELETE_IND
						,'Adobe' as DW_SOURCE_CREATE_NM
						,NULL as DW_SOURCE_UPDATE_NM
						,TRUE as DW_CURRENT_VERSION_IND
						FROM ${tgt_wrk_tbl}`;
						
						
var sql_commit = "COMMIT"
var sql_rollback = "ROLLBACK"
try {
        snowflake.execute({sqlText: sql_begin});
        snowflake.execute({sqlText: sql_inserts});
        snowflake.execute({sqlText: sql_commit});
		log('COMPLETED',`Load for Click Stream Metrics table completed`);
	}
	
       
    catch (err)  {
        snowflake.execute ({sqlText: sql_rollback  });
        snowflake.execute ( {sqlText: sql_ins_rerun_tbl} );
		log('FAILED',`Loading of table ${tgt_tbl} Failed with error: ${err}`);
        return `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
        }
		
		
	
// ************** Load for Click Stream  Metrics table ENDs *****************

$$;
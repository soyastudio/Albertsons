--liquibase formatted sql
--changeset SYSTEM:SP_GETADOBECLICKHITDATA_TO_BIM_LOAD_CLICK_STREAM_SHOP runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_CONFIRMED_PRD;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE EDM_CONFIRMED_PRD.DW_APPL.SP_GETADOBECLICKHITDATA_TO_BIM_LOAD_CLICK_STREAM_SHOP(SRC_TBL VARCHAR, CNF_DB VARCHAR, C_USER_ACT VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

		var cnf_db = CNF_DB;
		var wrk_schema = C_STAGE;
		var cnf_schema = C_USER_ACT;
		var src_tbl = SRC_TBL;
		var shop_tbl_nm = 'CLICK_STREAM_SHOP'; 
		var cntrl_tbl_nm = 'CLICK_STREAM_CONTROL_TABLE';

		var src_wrk_tbl = `${cnf_db}.${wrk_schema}.${shop_tbl_nm}_src_WRK`;		
        var src_rerun_tbl = `${cnf_db}.${wrk_schema}.${shop_tbl_nm}_Rerun`;
		var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.${shop_tbl_nm}_WRK`;
		var tgt_tbl = `${cnf_db}.${cnf_schema}.${shop_tbl_nm}`;
		var tgt_exp_tbl = `${cnf_db}.${wrk_schema}.${shop_tbl_nm}_EXCEPTION`;
		var sp_name = 'SP_GetAdobeClickHitData_TO_BIM_LOAD_Click_Stream_Shop';
		
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
																						 
log('STARTED','Load for Click Shop table BEGIN');
		
		
		                     
    // **************        Load for Click Shop Unassigned table BEGIN *****************
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
		
	
                       
    // **************        Load for Click Stream Shop table BEGIN *****************
    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 


var create_tgt_wrk_table = `CREATE OR REPLACE TABLE ${tgt_wrk_tbl} as 
 
							WITH src_wrk_tbl_recs as
                            (SELECT DISTINCT 
                             HITID_HIGH
							,HITID_LOW
							,VISIT_PAGE_NUM
							,VISIT_NUM
							,POST_EVENT_LIST
							,POST_EVAR137
							,POST_EVAR202
							,POST_EVAR203
							,POST_EVAR204
							,POST_EVAR205
							,POST_EVAR206
							,POST_EVAR207
							,POST_EVAR143
							,POST_EVAR46
							,POST_EVAR47
							,POST_EVAR162
							,POST_EVAR173
							,POST_EVAR98
							,EXCLUDE_HIT
							,HIT_SOURCE
							,POST_EVAR170
							,POST_EVAR35
							,POST_EVAR198
							,POST_EVAR71
							,POST_EVAR212
							,POST_EVAR101
							,POST_EVAR32
							,POST_EVAR73
							,POST_EVAR165
							,POST_EVAR194
							,POST_EVAR160
							,POST_EVAR189
							,POST_PRODUCT_LIST
							,POST_EVAR14
							,POST_EVAR181
							,POST_EVAR77
							,POST_EVAR115
							,POST_EVAR199
							,POST_EVAR13
							,POST_EVAR9
							,POST_EVAR126
							,POST_EVAR125
							,POST_EVAR182
							,DW_SOURCE_CREATE_NM 
							,row_number() over(partition by HITID_HIGH,HITID_LOW,VISIT_PAGE_NUM,VISIT_NUM  
										  ORDER BY(DW_CREATETS) DESC) as rn
							FROM ${src_wrk_tbl} 
							WHERE  	HITID_HIGH 			IS NOT NULL	
									AND HITID_LOW  		IS NOT NULL	
									AND VISIT_PAGE_NUM  IS NOT NULL	
									AND VISIT_NUM  		IS NOT NULL	
							)
							
							
							SELECT 
							CLICK_STREAM_INTEGRATION_ID
                            ,src.Hit_Id_High
							,src.Hit_Id_Low
							,src.Visit_Page_Nbr
							,src.Visit_Nbr
							,Cart_Addition_Ind
							,Cart_Id
							,Cart_Product1_Nbr
							,Cart_Product2_Nbr
							,Cart_Product3_Nbr
							,Cart_Product4_Nbr
							,Cart_Product5_Nbr
							,Cart_Product6_Nbr
							,Cart_Type_Cd
							,Checkout_Ind
							,Club_Card_Nbr
							,Customer_HHID
							,Delivery_Tm
							,Delivery_Window_Txt
							,Ecom_Customer_Id
							,EXCLUDE_ROW_IND 
							,FACILITY_INTEGRATION_ID
							,Fulfillment_Banner_Cd
							,Fulfillment_Type_Cd
							,Gross_Order_Ind
							,Item_Refund_Cd
							,MFC_Flg
							,MTO_Flg
							,Order_Cnt
							,Order_Id
							,Order_2_ID
							,Order_3_ID
							,Order_Issue_Reported_Txt
							,Order_Substitution_Options_Txt
							,Payment_Method_Cd
							,Pickup_Location_Cd
							,Price_Information_Txt
							,Product_Finding_Method_Cd
							,Product_Lst
							,Product2_Lst
							,Product_Nm
							,Product_Pricing_Type_Cd
							,Product_Purchase_Options_Txt
							,Product_View_Ind 
							,GROSS_REVENUE_AMT
							,Store_Id
							,Store_Zip_Cd
							,Total_Cart_Amt
							,Unique_Sku_In_Cart_Nbr
							,GROSS_UNITS_NBR
							,RETAIL_CUSTOMER_UUID
							,Customer_UUID
							,Version_Nbr
							,DW_SOURCE_CREATE_NM 
							FROM 
							(
							SELECT 
							s.CLICK_STREAM_INTEGRATION_ID
                            ,s.HITID_HIGH		as Hit_Id_High
							,s.HITID_LOW		as Hit_Id_Low
							,s.VISIT_PAGE_NUM	as Visit_Page_Nbr
							,s.VISIT_NUM		as Visit_Nbr
							,CASE WHEN CONCAT(',',s.post_event_list,',')  LIKE '%,12,%'THEN TRUE else FALSE END Cart_Addition_Ind
							,s.POST_EVAR137	as Cart_Id
							,s.POST_EVAR202	as Cart_Product1_Nbr
							,s.POST_EVAR203	as Cart_Product2_Nbr
							,s.POST_EVAR204	as Cart_Product3_Nbr
							,s.POST_EVAR205	as Cart_Product4_Nbr
							,s.POST_EVAR206	as Cart_Product5_Nbr
							,s.POST_EVAR207	as Cart_Product6_Nbr
							,s.POST_EVAR143	as Cart_Type_Cd
							,CASE WHEN CONCAT(',',s.post_event_list,',') LIKE '%,11,%' THEN TRUE else FALSE END Checkout_Ind
							,s.POST_EVAR46	as Club_Card_Nbr
							,s.POST_EVAR47	as Customer_HHID
							,s.POST_EVAR162	as Delivery_Tm
							,s.POST_EVAR173	as Delivery_Window_Txt
							,s.POST_EVAR98	as Ecom_Customer_Id
							,CASE WHEN(s.EXCLUDE_HIT= 0 and s.HIT_SOURCE not in (5, 7, 8, 9)) THEN 0 ELSE 1 END as EXCLUDE_ROW_IND 
							,TRY_TO_NUMERIC(C.FACILITY_INTEGRATION_ID) as FACILITY_INTEGRATION_ID
							,s.POST_EVAR170	as Fulfillment_Banner_Cd
							,s.POST_EVAR35	as Fulfillment_Type_Cd
							,CASE WHEN CONCAT(',',s.post_event_list,',')  LIKE '%,1,%' THEN TRUE else FALSE END Gross_Order_Ind
							,s.POST_EVAR198	as Item_Refund_Cd
							,s.POST_EVAR71	as MFC_Flg
							,s.POST_EVAR212	as MTO_Flg
							,TRY_TO_NUMERIC(s.POST_EVAR101)	as Order_Cnt
							,F.ORDER_ID	as Order_Id
							,s.POST_EVAR32	as Order_2_ID
							,s.POST_EVAR73	as Order_3_ID
							,s.POST_EVAR165	as Order_Issue_Reported_Txt
							,s.POST_EVAR194	as Order_Substitution_Options_Txt
							,s.POST_EVAR160	as Payment_Method_Cd
							,s.POST_EVAR189	as Pickup_Location_Cd
							,NULL as Price_Information_Txt
							,s.POST_EVAR14	as Product_Finding_Method_Cd
							,s.POST_PRODUCT_LIST as Product_Lst
							,s.POST_EVAR181	as Product2_Lst
							,s.POST_EVAR77	as Product_Nm
							,s.POST_EVAR115	as Product_Pricing_Type_Cd
							,s.POST_EVAR199	as Product_Purchase_Options_Txt
							,CASE WHEN CONCAT(',',s.post_event_list,',')  LIKE '%,201,%' THEN TRUE else FALSE END Product_View_Ind 
							,NULL as GROSS_REVENUE_AMT 
							,s.POST_EVAR13	as Store_Id
							,s.POST_EVAR9	as Store_Zip_Cd
							,TRY_TO_DECIMAL(s.POST_EVAR126,38,17) as Total_Cart_Amt
							,s.POST_EVAR125	as Unique_Sku_In_Cart_Nbr
							,NULL as GROSS_UNITS_NBR
							,E.RETAIL_CUSTOMER_UUID
							,s.POST_EVAR182	as Customer_UUID
							,TRY_TO_NUMERIC(F.Version_Nbr) as Version_Nbr
							,s.DW_SOURCE_CREATE_NM 
							FROM 
							(
							SELECT  
							sct.CLICK_STREAM_INTEGRATION_ID
							,HITID_HIGH
							,HITID_LOW
							,VISIT_PAGE_NUM
							,VISIT_NUM
							,POST_EVENT_LIST
							,POST_EVAR137
							,POST_EVAR202
							,POST_EVAR203
							,POST_EVAR204
							,POST_EVAR205
							,POST_EVAR206
							,POST_EVAR207
							,POST_EVAR143
							,POST_EVAR46
							,POST_EVAR47
							,POST_EVAR162
							,POST_EVAR173
							,POST_EVAR98
							,EXCLUDE_HIT
							,HIT_SOURCE
							,POST_EVAR170
							,POST_EVAR35
							,POST_EVAR198
							,POST_EVAR71
							,POST_EVAR212
							,POST_EVAR101
							,POST_EVAR32	
							,POST_EVAR73
							,POST_EVAR165
							,POST_EVAR194
							,POST_EVAR160
							,POST_EVAR189
							,POST_PRODUCT_LIST
							,POST_EVAR14
							,POST_EVAR181
							,POST_EVAR77
							,POST_EVAR115
							,POST_EVAR199
							,POST_EVAR13
							,POST_EVAR9
							,POST_EVAR126
							,POST_EVAR125
							,POST_EVAR182
							,DW_SOURCE_CREATE_NM 
							FROM src_wrk_tbl_recs s, ${cnf_db}.${cnf_schema}.${cntrl_tbl_nm} sct
							WHERE rn = 1
								AND s.HITID_HIGH = sct.HIT_ID_HIGH   
								AND s.HITID_LOW = sct.HIT_ID_LOW  
								AND s.VISIT_PAGE_NUM = sct.VISIT_PAGE_NBR 
								AND s.VISIT_NUM = sct.VISIT_NBR 
								AND	sct.CLICK_STREAM_INTEGRATION_ID IS NOT NULL									
							) s
							
							LEFT JOIN 
							( SELECT DISTINCT FACILITY_INTEGRATION_ID,FACILITY_NBR
								FROM ${cnf_db}.DW_C_LOCATION.FACILITY
								WHERE CORPORATION_ID ='001' 
								AND DW_CURRENT_VERSION_IND='TRUE'
								AND DATE(TEMP_CLOSE_DT)='9999-12-31'
							) C ON s.POST_EVAR13 = C.FACILITY_NBR 
							
							LEFT JOIN 
							( SELECT DISTINCT RETAIL_CUSTOMER_UUID
								FROM  ${cnf_db}.DW_C_CUSTOMER.RETAIL_CUSTOMER
								WHERE DW_CURRENT_VERSION_IND='TRUE'
							) E ON s.POST_EVAR182 = E.RETAIL_CUSTOMER_UUID 
	
							LEFT JOIN 
							( SELECT  MAX(Version_Nbr) Version_Nbr ,Order_id
								FROM   ${cnf_db}.DW_C_ECOMMERCE.GROCERY_ORDER_HEADER
									  WHERE DW_CURRENT_VERSION_IND='TRUE'
									  GROUP BY Order_id 
							) F ON s.POST_EVAR32=  F.Order_id
						) src
						
						
						 LEFT JOIN 
                          (SELECT  DISTINCT
									 tgt.Hit_Id_High
									,tgt.Hit_Id_Low
									,tgt.Visit_Page_Nbr
									,tgt.Visit_Nbr
                          FROM ${tgt_tbl} tgt 
                          ) tgt 
                          ON tgt.Hit_Id_High = src.Hit_Id_High
						  AND tgt.Hit_Id_Low = src.Hit_Id_Low
						  AND tgt.Visit_Page_Nbr = src.Visit_Page_Nbr
						  AND tgt.Visit_Nbr = src.Visit_Nbr
                          WHERE  (tgt.Hit_Id_High IS NULL AND tgt.Hit_Id_Low IS NULL AND tgt.Visit_Page_Nbr IS NULL AND tgt.Visit_Nbr IS NULL)`;
										
									
										
	var sql_updates = `UPDATE ${tgt_wrk_tbl}  a SET GROSS_UNITS_NBR=b.Units,GROSS_REVENUE_AMT=b.Revenue
		FROM( WITH default_result as (
				SELECT  HIT_ID_HIGH,HIT_ID_LOW,VISIT_PAGE_NBR,VISIT_NBR,Product_Lst as POST_PRODUCT_LIST1,
				c.value::string as POST_PRODUCT_LIST FROM ${tgt_wrk_tbl} ,
				lateral flatten(input=>split(Product_Lst, ',')) c 
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
						,Hit_Id_High
						,Hit_Id_Low
						,Visit_Page_Nbr
						,Visit_Nbr
						,Cart_Addition_Ind
						,Cart_Id
						,Cart_Product1_Nbr
						,Cart_Product2_Nbr
						,Cart_Product3_Nbr
						,Cart_Product4_Nbr
						,Cart_Product5_Nbr
						,Cart_Product6_Nbr
						,Cart_Type_Cd
						,Checkout_Ind
						,Club_Card_Nbr
						,Customer_HHID
						,Delivery_Tm
						,Delivery_Window_Txt
						,Ecom_Customer_Id
						,Exclude_Row_Ind
						,Facility_Integration_ID
						,Fulfillment_Banner_Cd
						,Fulfillment_Type_Cd
						,Gross_Order_Ind
						,Item_Refund_Cd
						,MFC_Flg
						,MTO_Flg
						,Order_Cnt
						,Order_Id
						,Order_2_ID
						,Order_3_ID
						,Order_Issue_Reported_Txt
						,Order_Substitution_Options_Txt
						,Payment_Method_Cd
						,Pickup_Location_Cd
						,Price_Information_Txt
						,Product_Finding_Method_Cd
						,Product_Lst
						,Product2_Lst
						,Product_Nm
						,Product_Pricing_Type_Cd
						,Product_Purchase_Options_Txt
						,Product_View_Ind
						,GROSS_REVENUE_AMT
						,Store_Id
						,Store_Zip_Cd
						,Total_Cart_Amt
						,Unique_Sku_In_Cart_Nbr
						,GROSS_UNITS_NBR
						,Retail_Customer_UUID
						,Customer_UUID
						,Version_Nbr
						,DW_CREATE_TS 
						,DW_LAST_UPDATE_TS
						,DW_LOGICAL_DELETE_IND
						,DW_SOURCE_CREATE_NM
						,DW_SOURCE_UPDATE_NM
						,DW_CURRENT_VERSION_IND
						)
				      
					    SELECT 
						CLICK_STREAM_INTEGRATION_ID
						,Hit_Id_High
						,Hit_Id_Low
						,Visit_Page_Nbr
						,Visit_Nbr
						,Cart_Addition_Ind
						,Cart_Id
						,Cart_Product1_Nbr
						,Cart_Product2_Nbr
						,Cart_Product3_Nbr
						,Cart_Product4_Nbr
						,Cart_Product5_Nbr
						,Cart_Product6_Nbr
						,Cart_Type_Cd
						,Checkout_Ind
						,Club_Card_Nbr
						,Customer_HHID
						,Delivery_Tm
						,Delivery_Window_Txt
						,Ecom_Customer_Id
						,Exclude_Row_Ind
						,Facility_Integration_ID
						,Fulfillment_Banner_Cd
						,Fulfillment_Type_Cd
						,Gross_Order_Ind
						,Item_Refund_Cd
						,MFC_Flg
						,MTO_Flg
						,Order_Cnt
						,Order_Id
						,Order_2_ID
						,Order_3_ID
						,Order_Issue_Reported_Txt
						,Order_Substitution_Options_Txt
						,Payment_Method_Cd
						,Pickup_Location_Cd
						,Price_Information_Txt
						,Product_Finding_Method_Cd
						,Product_Lst
						,Product2_Lst
						,Product_Nm
						,Product_Pricing_Type_Cd
						,Product_Purchase_Options_Txt
						,Product_View_Ind
						,TRY_TO_DECIMAL(GROSS_REVENUE_AMT,16,4) as GROSS_REVENUE_AMT
						,Store_Id
						,Store_Zip_Cd
						,Total_Cart_Amt
						,Unique_Sku_In_Cart_Nbr
						,TRY_TO_DECIMAL(GROSS_UNITS_NBR,16,4) as GROSS_UNITS_NBR
						,Retail_Customer_UUID
						,Customer_UUID
						,Version_Nbr
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
		log('COMPLETED',`Load for Click Stream Shop table completed`);
	}
	
     
    catch (err)  {
        snowflake.execute ({sqlText: sql_rollback  });
        snowflake.execute ( {sqlText: sql_ins_rerun_tbl} );
		log('FAILED',`Loading of table ${tgt_tbl} Failed with error: ${err}`);
        return `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
        }
		
	

// ************** Load for Click Stream Shop table ENDs *****************


$$;
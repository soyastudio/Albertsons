--liquibase formatted sql
--changeset SYSTEM:SP_GETADOBECLICKHITDATA_TO_BIM_LOAD_CLICK_STREAM_LOYALTY runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_CONFIRMED_PRD;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE EDM_CONFIRMED_PRD.DW_APPL.SP_GETADOBECLICKHITDATA_TO_BIM_LOAD_CLICK_STREAM_LOYALTY(SRC_TBL VARCHAR, CNF_DB VARCHAR, C_USER_ACT VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

		var cnf_db = CNF_DB;
		var wrk_schema = C_STAGE;
		var cnf_schema = C_USER_ACT;
		var src_tbl = SRC_TBL;
		var loyalty_tbl_nm = 'CLICK_STREAM_LOYALTY'; 
		var cntrl_tbl_nm = 'CLICK_STREAM_CONTROL_TABLE';
		var sp_name	= 'SP_GetAdobeClickHitData_TO_BIM_LOAD_Click_Stream_Loyalty'; 		

		var src_wrk_tbl = `${cnf_db}.${wrk_schema}.${loyalty_tbl_nm}_src_WRK`;		
        var src_rerun_tbl = `${cnf_db}.${wrk_schema}.${loyalty_tbl_nm}_Rerun`;
		var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.${loyalty_tbl_nm}_WRK`;
		var tgt_tbl = `${cnf_db}.${cnf_schema}.${loyalty_tbl_nm}`;
		var tgt_exp_tbl = `${cnf_db}.${wrk_schema}.${loyalty_tbl_nm}_EXCEPTION`;

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
	
	log('STARTED','Load for Click Stream Loyalty table BEGIN');
	
    // **************        Load for Click Stream Loyalty table BEGIN *****************
    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 

// persist stream data in work table for the current transaction, includes data from previous failed run
	var sql_crt_src_wrk_tbl = `create or replace table ${src_wrk_tbl} DATA_RETENTION_TIME_IN_DAYS = 0 as
    SELECT * FROM ${src_tbl}
    UNION ALL
    SELECT * FROM ${src_rerun_tbl} 
	`;
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
                            ,EXCLUDE_HIT
                            ,HIT_SOURCE
                            ,POST_EVAR13
                            ,POST_EVAR32
                            ,POST_EVAR98
							,POST_EVAR155
							,POST_EVAR136
							,POST_EVAR102
							,POST_EVAR103
							,POST_EVAR106
							,POST_EVAR104
							,POST_EVAR123
							,POST_EVAR105
							,POST_EVAR159
							,POST_EVAR130
							,POST_EVAR146
							,POST_EVAR147
							,POST_EVAR56 
							,POST_EVAR138
							,POST_EVAR128
							,POST_EVAR127
							,POST_EVAR21 
							,POST_EVAR20 
							,POST_EVAR112
							,POST_EVAR114
							,POST_EVAR113
							,POST_EVAR97 
							,POST_EVAR167
							,POST_EVAR166
							,POST_EVAR135
							,POST_EVAR111
							,POST_EVAR182
                            ,DW_CREATETS
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
                            ,CASE WHEN CONCAT(',', s.post_event_list, ',') LIKE '%,20155%' and s.EXCLUDE_HIT= 0 and s.HIT_SOURCE not in (5, 7, 8, 9) then TRY_TO_NUMERIC(split_part(regexp_substr(post_event_list,'20155=([^,]+)'),'=',2)) else null end as Clip_Actions_Per_Visit_Nbr
                            ,s.POST_EVAR111 as Coupon_Carousel_Section_Nm 
							,s.POST_EVAR155 as Coupon_Clip_Method_Cd
							,s.POST_EVAR136 as Coupon_Clipped_Txt
							,s.POST_EVAR102 as Coupon_Id
							,s.POST_EVAR103 as Coupon_Nm
							,s.POST_EVAR106 as Coupon_Product_SKU
							,s.POST_EVAR104 as Coupon_Savings_Amt
							,s.POST_EVAR123 as Coupon_Status_Cd
							,s.POST_EVAR105 as Coupon_Type_Cd
							,s.POST_EVAR159 as Email_Clipall_Coupon_Txt
							,s.POST_EVAR130 as Email_Offer_Id_Url_Parameter_Txt
							,CASE WHEN(s.EXCLUDE_HIT= 0 and s.HIT_SOURCE not in (5, 7, 8, 9)) THEN 0 ELSE 1 END as EXCLUDE_ROW_IND
							,TRY_TO_NUMERIC(Facility.Facility_Integration_Id)  as FACILITY_INTEGRATION_ID
							,s.POST_EVAR146 as J4U_Coupon_Source_Cd
							,TRY_TO_NUMERIC(s.POST_EVAR147) as J4U_Coupons_Available_Nbr
							,s.POST_EVAR56 as J4U_Filter_Type_Dsc
							,s.POST_EVAR138 as Loyalty_Application_Column_Vw
							,s.POST_EVAR128 as Loyalty_Store_Id
							,s.POST_EVAR127 as Loyalty_Zip_Cd
							,s.POST_EVAR21 as Offer_ID
							,s.POST_EVAR20 as Offer_Type_Cd
							,GOH.order_id as order_id
							,TRY_TO_NUMERIC(s.POST_EVAR112) as Product_Coupon_Clipped_Txt
							,s.POST_EVAR114 as Product_Coupon_Id
							,TRY_TO_NUMERIC(s.POST_EVAR113) as Product_Coupons_Available_Nbr
							,s.POST_EVAR97  as PROMOTION_CD
							,TRY_TO_NUMERIC(s.POST_EVAR167) as Redeemed_Rewards_Nbr
							,Retail.Retail_Customer_UUID as Retail_Customer_UUID
							,s.POST_EVAR166 as Reward_Id
							,s.POST_EVAR135 as Total_Coupon_Clipped_Nbr
							,TRY_TO_NUMERIC(GOH.Version_Nbr) as Version_Nbr
							,s.DW_CREATETS
							,s.POST_EVAR98 as RETAIL_CUSTOMER_GUID
							,s.POST_EVAR182
							,CASE WHEN (TGT.HIT_ID_HIGH IS NULL AND TGT.HIT_ID_LOW IS NULL AND TGT.VISIT_NBR IS NULL AND TGT.VISIT_PAGE_NBR IS NULL ) then 'I' else 'U' end DML_TYPE
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
                            ,EXCLUDE_HIT
                            ,HIT_SOURCE
                            ,POST_EVAR13
                            ,POST_EVAR32
                            ,POST_EVAR98
							,POST_EVAR155
							,POST_EVAR136
							,POST_EVAR102
							,POST_EVAR103
							,POST_EVAR106
							,POST_EVAR104
							,POST_EVAR123
							,POST_EVAR105
							,POST_EVAR159
							,POST_EVAR130
							,POST_EVAR146
							,POST_EVAR147
							,POST_EVAR56 
							,POST_EVAR138
							,POST_EVAR128
							,POST_EVAR127
							,POST_EVAR21 
							,POST_EVAR20 
							,POST_EVAR112
							,POST_EVAR114
							,POST_EVAR113
							,POST_EVAR97 
							,POST_EVAR167
							,POST_EVAR166
							,POST_EVAR135
							,POST_EVAR111
							,POST_EVAR182
                            ,DW_CREATETS
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
							) Facility ON s.POST_EVAR13 = Facility.FACILITY_NBR 
							
							LEFT JOIN 
							( SELECT DISTINCT max(version_nbr) as version_nbr,ORDER_ID
								FROM  ${cnf_db}.DW_C_ECOMMERCE.GROCERY_ORDER_HEADER
								where DW_CURRENT_VERSION_IND='TRUE'
                              group by ORDER_ID
							) GOH ON s.POST_EVAR32 = GOH.ORDER_ID 
		
							LEFT JOIN 
							( SELECT DISTINCT RETAIL_CUSTOMER_UUID
								FROM  ${cnf_db}.DW_C_CUSTOMER.RETAIL_CUSTOMER
								where DW_CURRENT_VERSION_IND='TRUE'
							) Retail ON s.POST_EVAR182 = Retail.RETAIL_CUSTOMER_UUID
							
							LEFT JOIN ${tgt_tbl} TGT
								ON  s.HITID_HIGH = TGT.HIT_ID_HIGH
								AND s.HITID_LOW = TGT.HIT_ID_LOW
								AND s.VISIT_NUM = TGT.VISIT_NBR
								AND s.VISIT_PAGE_NUM = TGT.VISIT_PAGE_NBR								
							`
;						
					
try {
snowflake.execute ({sqlText: create_tgt_wrk_table});
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
						,Clip_Actions_Per_Visit_Nbr
						,Coupon_Carousel_Section_Nm
						,Coupon_Clip_Method_Cd
						,Coupon_Clipped_Txt
						,Coupon_Id
						,Coupon_Nm
						,Coupon_Product_SKU
						,Coupon_Savings_Amt
						,Coupon_Status_Cd
						,Coupon_Type_Cd
						,Email_Clipall_Coupon_Txt
						,Email_Offer_Id_Url_Parameter_Txt
						,EXCLUDE_ROW_IND
						,FACILITY_INTEGRATION_ID
						,J4U_Coupon_Source_Cd
						,J4U_Coupons_Available_Nbr
						,J4U_Filter_Type_Dsc
						,Loyalty_Application_Column_Vw
						,Loyalty_Store_Id
						,Loyalty_Zip_Cd
						,Offer_ID
						,Offer_Type_Cd
						,ORDER_ID
						,Product_Coupon_Clipped_Txt
						,Product_Coupon_Id
						,Product_Coupons_Available_Nbr
						,PROMOTION_CD
						,Redeemed_Rewards_Nbr
						,RETAIL_CUSTOMER_UUID
						,Reward_Id
						,Total_Coupon_Clipped_Nbr
						,VERSION_NBR
						,RETAIL_CUSTOMER_GUID
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
						,Clip_Actions_Per_Visit_Nbr
						,Coupon_Carousel_Section_Nm
						,Coupon_Clip_Method_Cd
						,Coupon_Clipped_Txt
						,Coupon_Id
						,Coupon_Nm
						,Coupon_Product_SKU
						,Coupon_Savings_Amt
						,Coupon_Status_Cd
						,Coupon_Type_Cd
						,Email_Clipall_Coupon_Txt
						,Email_Offer_Id_Url_Parameter_Txt
						,EXCLUDE_ROW_IND
						,FACILITY_INTEGRATION_ID
						,J4U_Coupon_Source_Cd
						,J4U_Coupons_Available_Nbr
						,J4U_Filter_Type_Dsc
						,Loyalty_Application_Column_Vw
						,Loyalty_Store_Id
						,Loyalty_Zip_Cd
						,Offer_ID
						,Offer_Type_Cd
						,ORDER_ID
						,Product_Coupon_Clipped_Txt
						,Product_Coupon_Id
						,Product_Coupons_Available_Nbr
						,PROMOTION_CD
						,Redeemed_Rewards_Nbr
						,RETAIL_CUSTOMER_UUID
						,Reward_Id
						,Total_Coupon_Clipped_Nbr
						,VERSION_NBR
						,RETAIL_CUSTOMER_GUID
						,CURRENT_TIMESTAMP as DW_CREATE_TS
						,Null as DW_LAST_UPDATE_TS
						,False as DW_LOGICAL_DELETE_IND
						,'Adobe' as DW_SOURCE_CREATE_NM
						,Null as DW_SOURCE_UPDATE_NM
						,True as DW_CURRENT_VERSION_IND
						FROM ${tgt_wrk_tbl} WHERE DML_TYPE='I'`;						
						
var sql_commit = "COMMIT"
var sql_rollback = "ROLLBACK"
try {
        snowflake.execute({sqlText: sql_begin});
        snowflake.execute({sqlText: sql_inserts});
        snowflake.execute({sqlText: sql_commit});
		log('COMPLETED',`Load for Click Stream Loyalty table completed`);
	}
	
    catch (err)  {
        snowflake.execute({sqlText: sql_rollback });
        snowflake.execute({sqlText: sql_ins_rerun_tbl});
		log('FAILED',`Loading of table ${tgt_tbl} Failed with error: ${err}`);
        return `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
        }	
	
// ************** Load for Click Stream Loyalty table ENDs *****************


$$;
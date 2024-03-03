--liquibase formatted sql
--changeset SYSTEM:SP_CLICK_STREAM_BOT_LOGIC_PRC runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_CONFIRMED_PRD;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE EDM_CONFIRMED_PRD.DW_APPL.SP_CLICK_STREAM_BOT_LOGIC_PRC()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

//--------------------------------------------------------------------------------------------------------------------------
//*
//* Description : Program to identify the BOT user logins in Adobe system
//* version				Updated By				Comments
//* ------------			---------------			-------------------
//* v1.0					rbano00					Intitial Version 
//
//---- Variable declarations
var cur_db 	= snowflake.execute( {sqlText: `Select current_database()`} );
cur_db.next();
var env 	= cur_db.getColumnValue(1);
env 		= env.split(`_`);
env 		= env[env.length - 1];

//--------- DB Environment Declaration -----------------
var edm_src_db 			= `EDM_CONFIRMED_${env}`;
var edm_src_vw_db 	    = `EDM_VIEWS_${env}`;
var edm_vw	 			= `DW_VIEWS`;
var wrk_stg_schema      = 'DW_C_STAGE';
var wrk_tbl_schema 		= `DW_C_USER_ACTIVITY`;

//-------- Objects Declarations ------------------------
//------ Source Data table/view 
var src_hit_vw		= `${edm_src_vw_db}.${edm_vw}.CLICK_HIT_DATA`;
var src_other_tbl	= `${edm_src_db}.${wrk_tbl_schema}.CLICK_STREAM_OTHER`;
var src_shop_tbl	= `${edm_src_db}.${wrk_tbl_schema}.CLICK_STREAM_SHOP`;
var src_loyalty_tbl	= `${edm_src_db}.${wrk_tbl_schema}.CLICK_STREAM_LOYALTY`;
var src_metrics_tbl	= `${edm_src_db}.${wrk_tbl_schema}.CLICK_STREAM_METRICS`;

//----- Local/temp Object created info-------------------

var tgt_wrk_delta_tbl	= `${edm_src_db}.${wrk_stg_schema}.CLICK_STREAM_CLICK_HIT_BOT_DELTA_DATA`;
var tgt_wrk_logic_tbl	= `${edm_src_db}.${wrk_stg_schema}.CLICK_STREAM_BOT_LOGIC_DATA`;

//----- Final objects ----------------------------------
var tgt_hit_hist_tbl	= `${edm_src_db}.${wrk_tbl_schema}.CLICK_STREAM_CLICK_HIT_BOT_DATA`;
var tgt_bot_tbl			= `${edm_src_db}.${wrk_tbl_schema}.CLICK_STREAM_BOT_FLAG_DATA`;
var tgt_bot_data_tbl	= `${edm_src_db}.${wrk_tbl_schema}.CLICK_STREAM_BOT_DATA`;
var tgt_lookup_tbl		= `${edm_src_db}.${wrk_tbl_schema}.CLICK_STREAM_LOOKUPS`;

//----- Exception Table info-------------------
var tgt_bot_log_tbl	= `${edm_src_db}.${wrk_stg_schema}.CLICK_STREAM_LOG_TABLE`;
var sp_name = 'sp_click_stream_bot_logic_prcsp_click_stream_order_staging_population';

//--------------------------------------------------------------------------------------------------------------------------
//---- One time Load the data into this table for BOT identification CLICK_STREAM_CLICK_HIT_BOT_DATA
//---- Captured the Delta load week on week and populate the data into click_stream_click_hit_bot_delta_data for bot identification 

function log(status,msg){
	var cnf_msg = msg;
	try{	
    snowflake.createStatement( 
	{ sqlText: `call SP_GetAdobeClickHitData_Click_Stream_Log('${edm_src_db}',
															  '${wrk_stg_schema}',
															  '${cnf_msg}',
															  '${status }',
															  '${sp_name}')`}).execute();																						 
		}catch(err)
		{
		snowflake.createStatement( 
		{ sqlText: `call SP_GetAdobeClickHitData_Click_Stream_Log('${edm_src_db}',
																  '${wrk_stg_schema}',
																  '${cnf_msg}',
																  '${status }',
																  '${sp_name}')`}).execute();							
		}}

var sql_insert_delta_logic =`INSERT OVERWRITE ALL 
        INTO ${tgt_wrk_delta_tbl}  
            (click_stream_integration_id
            , visit_id
            , visitor_id
            , ip
            , guid
            , gross_order_flag
            , coupon_clip_flag
            , cart_addition_flag
            , visitor_ip
            , visitor_guid
            , visitor_ip_guid
            , dw_source_create_nm
            , dw_createts)
       INTO ${tgt_wrk_logic_tbl}  
			( click_stream_integration_id
			, visit_id
			, visitor_id
			, ip
			, guid
			, gross_order_flag
			, coupon_clip_flag
			, cart_addition_flag
            , visitor_ip
            , visitor_guid
            , visitor_ip_guid
			, dw_source_create_nm
			, dw_createts )            
            WITH lookup_data AS 
            (SELECT MAX(to_date(refresh_dt)) refresh_dt,MAX(attribute1) attribute1 FROM  ${tgt_lookup_tbl} WHERE refresh_dt IS NOT NULL)
            SELECT CONCAT(hitid_low, hitid_high, visit_page_num, visit_num)                 									AS click_stream_integration_id
                , CONCAT(post_visid_high, post_visid_low, visit_num, visit_start_time_gmt)  									AS visit_id
                , CONCAT(post_visid_high, post_visid_low)                                   									AS visitor_id
                , post_evar88 																									AS ip
                , post_evar98 	                                                            									AS guid
                , IFF( CONCAT(',', post_event_list, ',') LIKE '%,1,%' --- Orders
                    AND post_product_list NOT LIKE ';;;;%' ---Remove orders with no bpn/item_qty/Price
                    AND duplicate_purchase <> 1, 1, 0) 		                    			            						AS gross_order_flag
                , CONCAT(',', post_event_list, ',') LIKE '%,20305,%'	                                						AS Coupon_Clip_Flag
                , CONCAT(',', post_event_list, ',') LIKE '%,12,%' 		                                						AS Cart_Addition_Flag
                , MD5(CONCAT(NVL(post_visid_high,'X'), NVL(post_visid_low,'X'),NVL(post_evar88,'X')))   						AS visitor_ip
                , MD5(CONCAT(NVL(post_visid_high,'X'), NVL(post_visid_low,'X'),NVL(post_evar98,'X')))                  			AS visitor_guid
                , MD5(CONCAT(NVL(post_visid_high,'X'), NVL(post_visid_low,'X'),NVL(post_evar88,'X'),NVL(post_evar98,'X')))      AS visitor_ip_guid
                , hit.dw_source_create_nm												    									AS dw_source_create_nm
                , dw_createts                                                               									AS dw_createts 
            FROM ${src_hit_vw} hit JOIN lookup_data
              ON to_date(dw_createts) >= refresh_dt AND dw_createts> attribute1 
            WHERE exclude_hit = '0' 
            AND hit_source NOT IN ('5','7','8','9')
			ORDER BY MD5(CONCAT(NVL(post_visid_high,'X'), NVL(post_visid_low,'X'),NVL(post_evar88,'X'),NVL(post_evar98,'X')))
          ;`;
try {
snowflake.execute ({ sqlText: sql_insert_delta_logic });
log('SUCCEEDED',`Successfuly stage tables`);
	}
    catch (err) {
		log('FAILED',`Stage table failed with error: ${err}`);
        return `Stage table failed with error: ${err}`;   // Return a error message.
				}  

//-- Creating the Cluster Key on Delta table to read the data faster 

//var sql_create_cluster_key_delta_tbl = `ALTER TABLE ${tgt_wrk_delta_tbl} CLUSTER BY (visitor_ip_guid);`; 
//var sql_create_cluster_key_hit_hist_tbl = `ALTER TABLE ${tgt_hit_hist_tbl} CLUSTER BY (visitor_ip_guid);`; 

//try {
//snowflake.execute ({ sqlText: sql_create_cluster_key_delta_tbl });
//snowflake.execute ({ sqlText:sql_create_cluster_key_hit_hist_tbl}); 
//log('SUCCEEDED',`Cluster key created on stage tables`);
//	}
//    catch (err) {
//		log('FAILED',`Stage tables cluster key creation failed with error: ${err}`);
//        return `Stage tables cluster key creation failed with error: ${err}`;   // Return a error message.
//				}
                
// --- Loading the delta as well as existing hit data into the table

var sql_insert_delta_hist = 
`INSERT INTO ${tgt_wrk_logic_tbl}  
			( click_stream_integration_id
			, visit_id
			, visitor_id
			, ip
			, guid
			, gross_order_flag
			, coupon_clip_flag
			, cart_addition_flag
            , visitor_ip
            , visitor_guid
            , visitor_ip_guid
			, dw_source_create_nm
			, dw_createts )
SELECT hit.click_stream_integration_id
     , hit.visit_id
     , hit.visitor_id
     , hit.ip
     , hit.guid
     , hit.gross_order_flag
     , hit.coupon_clip_flag
     , hit.cart_addition_flag
     , hit.visitor_ip
     , hit.visitor_guid
     , hit.visitor_ip_guid
     , hit.dw_source_create_nm
     , hit.dw_createts 
FROM ${tgt_hit_hist_tbl} hit 
WHERE EXISTS (SELECT 1 FROM ${tgt_wrk_delta_tbl} delta 
              WHERE hit.visitor_ip_guid = delta.visitor_ip_guid
                AND hit.click_stream_integration_id <> delta.click_stream_integration_id);`;

try {
snowflake.execute ({ sqlText: sql_insert_delta_hist });
log('SUCCEEDED',`Delta table populated with Existing  Visitor and IP`);
	}
    catch (err) {
		log('FAILED',`Existing Visitor and IP detlta data population failed with error: ${err}`);
        return `Existing Visitor and IP detlta data population failed with error: ${err}`;   // Return a error message.
				}
                  
//-- Creating the Cluster Key on Logic table to update the data faster 

//var sql_create_cluster_key_logic_tbl = `ALTER TABLE ${tgt_wrk_logic_tbl} CLUSTER BY (click_stream_integration_id,visitor_ip_guid,ip,guid);`; 

//try {
//snowflake.execute ({ sqlText: sql_create_cluster_key_logic_tbl });
//log('SUCCEEDED',`Sucessfully cluster key created on logic table`);
//	}
//    catch (err) {
//		log('FAILED',`Logic table cluster key creating failed with error: ${err}`);
//        return `Logic table cluster key creating failed with error: ${err}`;   // Return a error message.
//				}
                
//---- Update the BOT flag in logic table for delta and existing based on ip and guid

var sql_update_bot_logic = 
`UPDATE ${tgt_wrk_logic_tbl} logic SET bot_flag = 'TRUE'
  FROM ${tgt_wrk_logic_tbl} logic1 
 WHERE 1 = 1
   AND logic1.visitor_ip_guid = logic.visitor_ip_guid 
   AND logic1.bot_flag = 'TRUE';`;

try {
snowflake.execute ({ sqlText: sql_update_bot_logic });
log('SUCCEEDED',`Sucessfully BOT flag updated`);
	}
    catch (err) {
		log('FAILED',`BOT flag update failed with error: ${err}`);
        return `BOT flag update failed with error: ${err}`;   // Return a error message.
				}

//----- Bot Implimentation logic is started .. Populating IP and Customer's 
//---- If visitor count is greated than 1000 and sum of gross order, Cart & coupon is ZERO then mark as BOT login     

var sql_insert_bot_flag = 
`INSERT INTO ${tgt_bot_tbl} (ip_guid,bot_flag,flag)
WITH bot_logic AS (
					SELECT ip 							AS ip
						 , visitor_id 					AS visitor_id
             			 , SUM(gross_order_flag) 		AS gross_order_ct
             			 , COUNT_IF(coupon_clip_flag) 	AS coupon_clip_ct
             			 , COUNT_IF(cart_addition_flag) AS cart_addition_ct
             			 , 'IP' flag
        			FROM ${tgt_wrk_logic_tbl} WHERE bot_flag IS NULL
        		GROUP BY ip, visitor_id
		UNION ALL
					SELECT guid AS guid
             			 , visitor_id
             			 , sum(gross_order_flag) AS gross_order_ct
             			 , COUNT_IF(coupon_clip_flag) AS coupon_clip_ct
             			 , COUNT_IF(cart_addition_flag) AS cart_addition_ct
             			 , 'GUID' flag
        			FROM ${tgt_wrk_logic_tbl} WHERE bot_flag IS NULL
        		GROUP BY guid, visitor_id
				)
, visitors_by_ip AS (
        			SELECT ip
             			 , COUNT(*) AS visitor_ct
             			 , SUM(gross_order_ct) AS gross_order_ct
             			 , SUM(coupon_clip_ct) AS coupon_clip_ct
             			 , SUM(cart_addition_ct) AS cart_addition_ct
             			 , flag
        			FROM bot_logic WHERE flag = 'IP'
        		GROUP BY ip ,flag
		UNION ALL
        		SELECT ip -- guid
             		 , COUNT(*) AS visitor_ct
             		 , SUM(gross_order_ct) AS gross_order_ct
             		 , SUM(coupon_clip_ct) AS coupon_clip_ct
             		 , SUM(cart_addition_ct) AS cart_addition_ct
             		 , flag
        		FROM bot_logic WHERE flag = 'GUID'
        	GROUP BY ip  , flag     
        			)
, bot_ips AS (
        	SELECT ip
        		 , 'TRUE' AS bot_flag, flag
        	FROM visitors_by_ip 
           WHERE visitor_ct >= 1000
           	 AND gross_order_ct = 0
             AND cart_addition_ct = 0
             AND coupon_clip_ct = 0
             AND flag = 'IP'
	UNION ALL
        	SELECT ip
        		 , 'TRUE' AS bot_flag, flag
        	FROM visitors_by_ip 
		   WHERE visitor_ct >= 1000
             AND gross_order_ct = 0
             AND cart_addition_ct = 0
             AND coupon_clip_ct = 0
             AND flag = 'GUID'      
        	) 
SELECT * FROM bot_ips;`;

try {
snowflake.execute ({ sqlText: sql_insert_bot_flag });
log('SUCCEEDED',`Sucessfully New BOT data populated`);
	}
    catch (err) {
		log('FAILED',`New BOT data population failed with error: ${err}`);
        return `New BOT data population failed with error: ${err}`;   // Return a error message.
				}

//--- Populate the visitor id along with ip address and customer (guid) 

var sql_final_bot_data = 
`INSERT INTO ${tgt_bot_data_tbl} (click_stream_integration_id, ip, guid, visitor_id, visit_id, ip_guid_status)
SELECT click_stream_integration_id,ip,guid,visitor_id,visit_id,'IP' flag
  FROM ${tgt_wrk_logic_tbl} logic
 WHERE EXISTS (SELECT 1 FROM ${tgt_bot_tbl} bot WHERE logic.ip = CASE WHEN flag = 'IP' THEN ip_guid END AND ip_guid IS NOT NULL)
UNION ALL 
SELECT click_stream_integration_id,ip,guid,visitor_id,visit_id,'GUID' flag
  FROM ${tgt_wrk_logic_tbl} logic
 WHERE EXISTS (SELECT 1 FROM ${tgt_bot_tbl} bot WHERE logic.guid = CASE WHEN flag = 'GUID' THEN ip_guid END AND ip_guid IS NOT NULL);`;
 
try {
snowflake.execute ({ sqlText: sql_final_bot_data });
log('SUCCEEDED',`Sucessfully Final BOT data populated`);
	}
    catch (err) {
		log('FAILED',`Final BOT data population failed with error: ${err}`);
        return `Final BOT data population failed with error: ${err}`;   // Return a error message.
				}
                
//------- Populate the delta data into click hit history table 

var sql_insert_hit =    
`INSERT INTO ${tgt_hit_hist_tbl}  
  			( click_stream_integration_id
			, visit_id
			, visitor_id
			, ip
			, guid
			, gross_order_flag
			, coupon_clip_flag
			, cart_addition_flag
            , visitor_ip
            , visitor_guid
            , visitor_ip_guid
			, dw_source_create_nm
			, dw_createts
			, bot_flag)
	  SELECT  click_stream_integration_id
			, visit_id
			, visitor_id
			, ip
			, guid
			, gross_order_flag
			, coupon_clip_flag
			, cart_addition_flag
            , visitor_ip
            , visitor_guid
            , visitor_ip_guid
			, dw_source_create_nm
			, dw_createts 
			, bot_flag
	   FROM ${tgt_wrk_logic_tbl} logic
      WHERE NOT EXISTS (SELECT 1 FROM ${tgt_hit_hist_tbl} hit WHERE hit.click_stream_integration_id = logic.click_stream_integration_id);`;

try {
snowflake.execute ({ sqlText: sql_insert_hit });
log('SUCCEEDED',`Sucessfully BOT hist data populated`);
	}
    catch (err) {
		log('FAILED',`BOT hist population failed with error: ${err}`);
        return `BOT hist population failed with error: ${err}`;   // Return a error message.
				}

//---- Populate the load date into lookup table for next refresh refernce. 

var sql_insert_lookup =    
`INSERT INTO ${tgt_lookup_tbl} (refresh_dt,attribute1,description)
SELECT MAX(to_date(dw_createts)),MAX(dw_createts),'BOT Load Date' FROM ${tgt_wrk_logic_tbl}  logic ;`;

try {
snowflake.execute ({ sqlText: sql_insert_lookup });
log('SUCCEEDED',`Sucessfully Lookup data populated`);
	}
    catch (err) {
		log('FAILED',`Lookup population failed with error: ${err}`);
        return `Lookup population failed with error: ${err}`;   // Return a error message.
				}

//-- Drop the Cluster Key on Logic table to update the data faster 

//var delta_drop_cluster_key       = `ALTER TABLE ${tgt_wrk_delta_tbl} drop clustering key;`;
//var hist_drop_cluster_key        = `ALTER TABLE ${tgt_hit_hist_tbl} drop clustering key;`;
//var logic_drop_cluster_key       = `ALTER TABLE ${tgt_wrk_logic_tbl} drop clustering key;`; 
//try {
//        snowflake.execute({sqlText:delta_drop_cluster_key}); 
//        snowflake.execute({sqlText:hist_drop_cluster_key}); 
//        snowflake.execute({sqlText:logic_drop_cluster_key});  
//        log('SUCCEEDED',`Sucessfully Dropped Cluster Keys`);
//    }
//    catch (err) {
//                log('FAILED',`Bot Staging tables cluster Key drop failed with error: ${err}`);
//                return "Bot Staging tables cluster Key drop failed with error : " + err; // Return a error message.
//                }           

$$;
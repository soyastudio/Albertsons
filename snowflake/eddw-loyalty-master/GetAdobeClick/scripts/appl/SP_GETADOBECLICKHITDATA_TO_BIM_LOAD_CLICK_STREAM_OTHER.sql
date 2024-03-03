--liquibase formatted sql
--changeset SYSTEM:SP_GetAdobeClickHitData_TO_BIM_LOAD_Click_Stream_Other runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_SCHEMA_APPL>>;

CREATE OR REPLACE PROCEDURE SP_GetAdobeClickHitData_TO_BIM_LOAD_Click_Stream_Other( SRC_TBL 	VARCHAR
										  , CNF_DB 		VARCHAR
										  , C_USER_ACT 	VARCHAR
										  , C_STAGE 	VARCHAR) 
RETURNS STRING
LANGUAGE JAVASCRIPT
AS 
$$
//*
//* Description : Program to populate the staging tables for Order Program
//* version				Updated By				Comments
//* ------------		---------------			-------------------
//* v1.0				rbano00					Fixed for EEDM-35938 - Duplicate Fix for Browser and Operating System
//* v1.1				rbano00					Fixed for EEDM-37681 - Duplicate Fix for Retail Customer and Facillity
//* v1.2				rbano00					Fixed for EEDM-37681 - Performance fix 
//*//

	var cnf_db 			= CNF_DB;
	var wrk_schema 		= C_STAGE;
	var cnf_schema 		= C_USER_ACT;
	var src_tbl 		= SRC_TBL;
	var other_tbl_nm 	= 'CLICK_STREAM_OTHER'; 
	var cntrl_tbl_nm 	= 'CLICK_STREAM_CONTROL_TABLE';

	var src_wrk_tbl 	= `${cnf_db}.${wrk_schema}.${other_tbl_nm}_SRC_WRK`;		
	var src_rerun_tbl 	= `${cnf_db}.${wrk_schema}.${other_tbl_nm}_Rerun`;
	var tgt_wrk_tbl 	= `${cnf_db}.${wrk_schema}.${other_tbl_nm}_WRK`;
	var tgt_tbl 		= `${cnf_db}.${cnf_schema}.${other_tbl_nm}`;
	var tgt_exp_tbl 	= `${cnf_db}.${wrk_schema}.${other_tbl_nm}_EXCEPTION`;
	var sp_name 		= 'SP_GetAdobeClickHitData_TO_BIM_LOAD_Click_Stream_Other';
		

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
																						 
log('STARTED','Load for Click Stream Other table BEGIN');																						 
// **************        Load for Adobe Clickstream Other table BEGIN *****************
//-- identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 

//-- persist stream data in work table for the current transaction, includes data from previous failed run
	var sql_crt_src_wrk_tbl = `CREATE OR REPLACE TRANSIENT TABLE ${src_wrk_tbl} DATA_RETENTION_TIME_IN_DAYS = 0 AS
    SELECT * FROM ${src_tbl}
    UNION ALL
    SELECT * FROM ${src_rerun_tbl} 
	`;
    try {
        snowflake.execute({ sqlText: sql_crt_src_wrk_tbl });
		log('SUCCEEDED',`Successfuly created Source Work table ${src_wrk_tbl}`);
    } catch (err)  {
		log('FAILED',`Creation of Source Work table ${src_wrk_tbl} Failed with error: ${err}`);
        throw `Creation of Source Work table ${src_wrk_tbl} Failed with error: ${err}`;   // -- Return a error message.
    }
	
	// -- Empty the rerun queue table
	var sql_empty_rerun_tbl = `TRUNCATE TABLE ${src_rerun_tbl}`;
	try {
        snowflake.execute({ sqlText: sql_empty_rerun_tbl });
		log('SUCCEEDED',`Successfuly truncated rerun queue table ${src_rerun_tbl}`);
    } catch (err) {
		log('FAILED',`Truncation of rerun queue table ${src_rerun_tbl} Failed with error: ${err}`);
        throw `Truncation of rerun queue table ${src_rerun_tbl} Failed with error: ${err}`;   // -- Return a error message.
    }
    
	// -- query to load rerun queue table when encountered a failure
	var sql_ins_rerun_tbl = `CREATE OR REPLACE TRANSIENT TABLE ${src_rerun_tbl} 
	                         AS SELECT * FROM ${src_wrk_tbl}`;

var create_tgt_wrk_table = `CREATE OR REPLACE TRANSIENT TABLE ${tgt_wrk_tbl} AS 
WITH src_wrk_tbl_recs AS (
                          SELECT DISTINCT
                                 accept_language
                               , browser
                               , connection_type
                               , country
                               , language
                               , plugins
                               , resolution
                               , post_search_engine
                               , post_evar13
                               , post_evar98
                               , post_visid_high
                               , post_visid_low
                               , visit_start_time_gmt
                               , post_evar32
                               , carrier
                               , color
                               , date_time
                               , exclude_hit
                               , first_hit_ref_type
                               , hit_source
                               , hitid_high
                               , hitid_low
                               , javascript
                               , os
                               , post_browser_height
                               , post_browser_width
                               , post_campaign
                               , post_clickmaplink
                               , post_clickmaplinkbyregion
                               , post_clickmappage
                               , post_clickmapregion
                               , post_evar1
                               , post_evar10
                               , post_evar100
                               , post_evar107
                               , post_evar108
                               , post_evar109
                               , post_evar11
                               , post_evar116
                               , post_evar117
                               , post_evar118
                               , post_evar119
                               , post_evar12
                               , post_evar120
                               , post_evar121
                               , post_evar122
                               , post_evar124
                               , post_evar129
                               , post_evar131
                               , post_evar132
                               , post_evar133
                               , post_evar134
                               , post_evar139
                               , post_evar14
                               , post_evar140
                               , post_evar141
                               , post_evar142
                               , post_evar144
                               , post_evar145
                               , post_evar148
                               , post_evar149
                               , post_evar15
                               , post_evar150
                               , post_evar151
                               , post_evar152
                               , post_evar153
                               , post_evar154
                               , post_evar157
                               , post_evar158
                               , post_evar16
                               , post_evar161
                               , post_evar163
                               , post_evar164
                               , post_evar168
                               , post_evar169
                               , post_evar17
                               , post_evar171
                               , post_evar172
                               , post_evar174
                               , post_evar175
                               , post_evar176
                               , post_evar177
                               , post_evar178
                               , post_evar179
                               , post_evar18
                               , post_evar180
                               , post_evar183
                               , post_evar184
                               , post_evar185
                               , post_evar186
                               , post_evar187
                               , post_evar188
                               , post_evar19
                               , post_evar190
                               , post_evar191
                               , post_evar192
                               , post_evar193
                               , post_evar195
                               , post_evar196
                               , post_evar197
                               , post_evar2
                               , post_evar200
                               , post_evar201
                               , post_evar22
                               , post_evar23
                               , post_evar24
                               , post_evar25
                               , post_evar26
                               , post_evar3
                               , post_evar33
                               , post_evar36
                               , post_evar37
                               , post_evar38
                               , post_evar4
                               , post_evar40
                               , post_evar49
                               , post_evar5
                               , post_evar51
                               , post_evar52
                               , post_evar53
                               , post_evar54
                               , post_evar55
                               , post_evar57
                               , post_evar58
                               , post_evar59
                               , post_evar6
                               , post_evar60
                               , post_evar61
                               , post_evar62
                               , post_evar63
                               , post_evar64
                               , post_evar65
                               , post_evar66
                               , post_evar67
                               , post_evar68
                               , post_evar7
                               , post_evar70
                               , post_evar74
                               , post_evar75
                               , post_evar76
                               , post_evar78
                               , post_evar8
                               , post_evar80
                               , post_evar81
                               , post_evar83
                               , post_evar84
                               , post_evar85
                               , post_evar86
                               , post_evar87
                               , post_evar88
                               , post_evar89
                               , post_evar90
                               , post_evar91
                               , post_evar92
                               , post_evar93
                               , post_evar94
                               , post_evar95
                               , post_evar96
                               , post_evar99
                               , post_event_list
                               , post_pagename
                               , post_product_list
                               , post_purchaseid
                               , post_tnt
                               , va_closer_detail
                               , va_closer_id
                               , videoad
                               , visit_num
                               , visit_page_num
                               , ref_type
                               , post_evar182
                               , ROW_NUMBER() OVER(PARTITION BY hitid_high
							                                  , hitid_low
							                                  , visit_num
															  ,	visit_page_num 
												      ORDER BY (dw_createts) DESC) AS rn
                            FROM ${src_wrk_tbl}
                           WHERE 1=1
                              AND hitid_high 		IS NOT NULL
                              AND hitid_low 		IS NOT NULL
                              AND visit_num 		IS NOT NULL
                              AND visit_page_num 	IS NOT NULL)
// -- v1.2 Start							  
// -- Browser information							  
, browser_dtls AS (
					SELECT browser_id
                         , browser_nm 
					FROM (
						   SELECT browser_id
								, browser_nm 
								, dw_first_effective_dt
								, MAX(csb.dw_first_effective_dt) OVER (PARTITION BY csb.browser_id) max_dw_first_effective_dt						
						   FROM ${cnf_db}.${cnf_schema}.CLICK_STREAM_BROWSER csb
						  WHERE dw_current_version_ind='TRUE'
				         ) 
					WHERE dw_first_effective_dt = max_dw_first_effective_dt // v1.0 -- Start 
                   )
// -- Connection information				   
, connection_typ_dtls AS (SELECT connection_type_cd 
                               , connection_type_nm 
						  FROM ${cnf_db}.${cnf_schema}.CLICK_STREAM_CONNECTION_TYPE cnt  
                         WHERE cnt.dw_current_version_ind 	= TRUE
						 )	
// -- Country information	
, country_dtls AS (SELECT ctr.country_id 
                        , ctr.country_nm
                     FROM ${cnf_db}.${cnf_schema}.CLICK_STREAM_COUNTRY ctr
				    WHERE ctr.dw_current_version_ind 	= TRUE
				   ) 
// -- localization information
, language_dtls AS (SELECT lng.language_id 
                         , lng.language_nm
                      FROM ${cnf_db}.${cnf_schema}.CLICK_STREAM_LANGUAGE lng
	                 WHERE lng.dw_current_version_ind 	= TRUE
				   )
// -- Plugin information		
, plugin_dtls AS (SELECT plg.plugin_id 
                       , plg.plugin_nm 
                    FROM ${cnf_db}.${cnf_schema}.CLICK_STREAM_PLUGIN plg 
				   WHERE plg.dw_current_version_ind 	= TRUE)
// -- Resolution information		
, resolution_dtls AS (SELECT res1.resolution_id
                           , res1.resolution_nm
					  FROM ${cnf_db}.${cnf_schema}.CLICK_STREAM_RESOLUTION res1
					 WHERE res1.dw_current_version_ind = TRUE
					 )
// -- Search engine information		
, search_engine_dtls AS (SELECT ser.search_engine_id
                              , ser.search_engine_nm					 
                         FROM ${cnf_db}.${cnf_schema}.CLICK_STREAM_SEARCH_ENGINE ser
						WHERE ser.dw_current_version_ind 	= TRUE
						)
// -- retail customer information								
, retail_cust_dtls AS (SELECT retail_customer_uuid
						FROM (
							  SELECT retail_customer_uuid
								   , dw_first_effective_ts
								   , MAX(cus1.dw_first_effective_ts) OVER (PARTITION BY cus1.retail_customer_uuid) max_dw_first_effective_ts
							  FROM ${cnf_db}.DW_C_CUSTOMER.RETAIL_CUSTOMER cus1
							  WHERE cus1.dw_current_version_ind = TRUE 
						  ) 
						WHERE dw_first_effective_ts = max_dw_first_effective_ts //-- v1.1
					  ) 
// -- Operating system information		
, operating_sys_dtls AS ( 
						SELECT operating_system_id
				             , operating_system_nm 
					    FROM (SELECT operating_system_id
				                   , operating_system_nm
							       , csos1.dw_first_effective_dt
							       , MAX(csos1.dw_first_effective_dt) OVER (PARTITION BY csos1.operating_system_id) max_dw_first_effective_dt
			                  FROM ${cnf_db}.${cnf_schema}.CLICK_STREAM_OPERATING_SYSTEM csos1
			                 WHERE dw_current_version_ind = TRUE //-- v1.1
							 )
						WHERE dw_first_effective_dt = max_dw_first_effective_dt
						) 
// -- Facility information								
, facility_dtls AS (
					SELECT facility_integration_id
		  	             , facility_nbr
				    FROM (SELECT facility_integration_id
							   , facility_nbr
							   , MAX(facility_integration_id) OVER (PARTITION BY facility_nbr) max_facility_integration_id
						  FROM ${cnf_db}.DW_C_LOCATION.FACILITY
						 WHERE corporation_id 		= '001' 
						   AND dw_current_version_ind	= 'TRUE'
						   AND DATE(temp_close_dt)	= '9999-12-31' //-- v1.1
						 )
					WHERE facility_integration_id = max_facility_integration_id
				   ) 
// -- Point of sales - order information						   
, pos_order_dtls AS (
                     SELECT order_id
					      , version_nbr
					 FROM (
						 SELECT order_id
							  , version_nbr
							  , MAX(version_nbr) OVER (PARTITION BY order_id) AS max_version_nbr 
						   FROM ${cnf_db}.DW_C_ECOMMERCE.GROCERY_ORDER_HEADER
						  WHERE dw_current_version_ind='TRUE'
					      )
		             WHERE version_nbr = max_version_nbr
		            )
// -- v1.2 End
		   
SELECT src1.click_stream_integration_id 																					AS click_stream_integration_id
	 , brs1.browser_nm																										AS browser_nm
	 , cnt.connection_type_nm																								AS connection_type_nm
	 , ctr.country_nm																										AS country_nm
	 , NULL 																												AS event_nm
	 , lng.language_nm																										AS language_nm
	 , ops1.operating_system_nm																								AS operating_system_nm
	 , plg.plugin_nm																										AS plugin_nm
	 , src1.ref_type 																										AS referrer_type_id
	 , res1.resolution_nm																									AS resolution_nm
	 , ser.search_engine_nm																									AS search_engine_nm
	 , cus1.retail_customer_uuid																							AS retail_customer_uuid
	 , NULL 																												AS bot_flg
	 , NULL 																												AS bot_tracking_txt_v195
	 , src1.post_evar195 																									AS bot_tracking_txt
	 , TRIM(LOWER(SPLIT_PART(src1.post_evar116,':',2))) 																	AS app_banner_cd
	 , TRIM(LOWER(SPLIT_PART(src1.post_evar116,':',4))) 																	AS application_version_build_nbr
	 , TRIM(LOWER(TRIM(SPLIT_PART(TRIM(LOWER(SPLIT_PART(src1.post_evar116,':',4))) ,'(',2), ')'))) 							AS app_build_nbr
	 , TRIM(LOWER(SPLIT_PART(TRIM(LOWER(SPLIT_PART(src1.post_evar116,':',4))) ,'(',1))) 									AS application_version_cd
	 , TRIM(LOWER(SPLIT_PART(src1.post_evar116,':',1))) 																	AS application_os_txt
	 , TRIM(LOWER(SPLIT_PART(src1.post_evar116,':',3))) 																	AS application_type_cd
	 , CONCAT(src1.post_visid_high, src1.post_visid_low, src1.visit_num, src1.visit_start_time_gmt)							AS visit_id
	 , CONCAT(src1.post_visid_high, src1.post_visid_low) 																	AS visitor_id
	 , CASE WHEN(src1.exclude_hit= 0 AND src1.HIT_SOURCE NOT IN (5, 7, 8, 9)) THEN 0 ELSE 1 END 							AS exclude_row_ind
	 , CASE WHEN CONCAT(',', src1.post_event_list, ',') LIKE '%,211,%' THEN TRUE ELSE FALSE END								AS application_sign_in_ind
	 , CASE WHEN CONCAT(',', src1.post_event_list, ',') LIKE '%,200,%' THEN TRUE ELSE FALSE END								AS internal_search_txt
	 , TRY_TO_DATE(src1.date_time,'YYYY-MM-DD HH24:MI:SS') 																	AS create_dt
	 , TRY_TO_DATE(src1.post_evar66,'MM-DD-YYYY') 																			AS mobile_application_first_launch_dt
	 , CONCAT_WS('_', TRIM(LOWER(SPLIT_PART(src1.post_evar116,':',1))), TRIM(LOWER(SPLIT_PART(src1.post_evar116,':',3)))) 	AS application_os_type_cd
	 , TRY_TO_TIMESTAMP_LTZ(src1.date_time,'YYYY-MM-DD HH24:MI:SS') 														AS create_ts
	 , grc.order_id 																										AS order_id
	 , NULL 																												AS application_availability_dt
	 , CASE WHEN TRIM(LOWER(src1.post_evar158))='zero' THEN 0 ELSE TRY_TO_NUMERIC(src1.post_evar158) END 					AS accordion_edit_nbr
	 , TRY_TO_NUMERIC(fct.facility_integration_id) 																			AS facility_integration_id
	 , TRY_TO_NUMERIC(grc.version_nbr) 																						AS version_nbr
	 , TRY_TO_NUMERIC(src1.exclude_hit) 																					AS exclude_hit_flg
	 , TRY_TO_NUMERIC(src1.hit_source) 																						AS hit_source_cd
	 , TRY_TO_NUMERIC(src1.visit_num) 																						AS visit_nbr
	 , TRY_TO_NUMERIC(src1.post_evar122) 																					AS internal_search_results_nbr
	 , src1.accept_language 																								AS accepted_language_cd
	 , src1.post_evar157 																									AS accordion_edit_txt
	 , src1.post_evar131 																									AS activity_id
	 , src1.post_evar132 																									AS activity_nm
	 , src1.post_evar178 																									AS ada_flg
	 , src1.post_tnt 																										AS adobe_tnt_id
	 , src1.post_evar116 																									AS application_detail_txt
	 , src1.post_evar119 																									AS application_order_status_msg
	 , src1.post_evar149 																									AS application_separate_lst
	 , src1.post_evar23 																									AS application_type2_cd
	 , src1.post_evar25 																									AS application_user_start_by_nm
	 , src1.post_evar70 																									AS availabile_cd
	 , src1.post_evar4 																										AS banner_cd
	 , src1.post_evar129 																									AS box_tops_auth_state_cd
	 , src1.post_evar174 																									AS browser_geo_cd
	 , src1.post_browser_height 																							AS browser_height_txt
	 , src1.post_browser_width 																								AS browser_width_txt
	 , src1.post_evar193 																									AS camera_allowed_cd
	 , src1.post_evar93 																									AS campaign_affiliate_txt
	 , src1.post_evar18 																									AS campaign_stacking_txt
	 , src1.post_campaign 																									AS campaign_txt
	 , src1.post_evar175 																									AS campaign_2_txt
	 , src1.post_evar19 																									AS card_less_registration_cd
	 , src1.post_evar185 																									AS carousel_size_txt
	 , src1.carrier 																										AS carrier_nm
	 , src1.post_evar196 																									AS cda_marketing_channel_cd
	 , src1.post_evar36 																									AS channel_manager_channel_cd
	 , src1.post_evar37 																									AS channel_stacking_txt
	 , src1.color 																											AS color_cd
	 , src1.post_evar74 																									AS custom_nav_link_tracking_txt
	 , src1.post_evar100 																									AS customer_status_cd
	 , src1.post_evar161 																									AS delivery_attended_unattended_flg
	 , src1.post_evar24 																									AS detail_view_txt
	 , src1.post_evar7 																										AS ecom_login_id
	 , src1.post_evar75 																									AS ecom_nav_link_tracking_cd
	 , src1.post_evar144 																									AS elevaate_flg
	 , src1.post_evar145 																									AS elevaate_poistion_nbr
	 , src1.post_evar118 																									AS email_hhid_url_parameter_txt
	 , src1.post_evar117 																									AS email_theme_url_parameter_txt
	 , src1.post_evar99 																									AS environment_cd
	 , src1.post_evar141 																									AS error_feature_cd
	 , src1.post_evar140 																									AS error_id
	 , src1.post_evar142 																									AS error_message_dsc
	 , src1.post_evar33 																									AS error_page_dsc
	 , src1.post_evar139 																									AS event_id_url_parameter_txt
	 , src1.post_evar133 																									AS experience_nm
	 , src1.post_evar54 																									AS face_book_account_nm
	 , src1.post_evar55 																									AS face_book_banner_cd
	 , src1.post_evar64 																									AS face_book_campaign_cd
	 , src1.post_evar108 																									AS filter_section_txt
	 , src1.post_evar107 																									AS filter_type_cd
	 , src1.first_hit_ref_type 																								AS first_hit_referrer_type_cd
	 , src1.post_evar86 																									AS ga_utm_campaign_medium_txt
	 , src1.post_evar84 																									AS ga_utm_campaign_nm
	 , src1.post_evar85 																									AS ga_utm_source_cd
	 , src1.post_evar38 																									AS global_no_substitution_cd
	 , src1.post_evar22 																									AS hidden_categories_txt
	 , src1.hitid_high 																										AS hit_id_high
	 , src1.hitid_low 																										AS hit_id_low
	 , src1.post_evar26 																									AS home_page_carousel_txt
	 , src1.post_evar180 																									AS impressions_component_txt
	 , src1.post_evar2 																										AS internal_campaign_tracking_id
	 , src1.post_evar1 																										AS internal_search_terms_txt
	 , src1.post_evar120 																									AS internal_search_type_cd
	 , src1.post_evar88 																									AS ip_address_nbr
	 , src1.javascript 																										AS java_script_version_nbr
	 , src1.post_evar81 																									AS kmsi_txt
	 , src1.post_evar17 																									AS last_activity_flg
	 , src1.post_evar184 																									AS launch_rule_txt
	 , src1.post_evar76 																									AS link_detail_txt
	 , src1.post_evar63 																									AS list_interaction_type_cd
	 , src1.post_evar201 																									AS location_sharing_enabled_flg
	 , src1.post_evar16 																									AS login_kmsi_txt
	 , src1.post_evar150 																									AS map_clicks_txt
	 , src1.post_clickmaplink 																								AS map_link_dsc
	 , src1.post_clickmaplinkbyregion 																						AS map_link_by_region_nm
	 , src1.post_clickmappage 																								AS map_page_cd
	 , src1.post_clickmapregion 																							AS map_region_cd
	 , src1.va_closer_id 																									AS marketing_channel_cd
	 , src1.va_closer_detail 																								AS marketing_channel_dtl
	 , src1.post_evar80 																									AS media_placement_cd
	 , src1.post_evar148 																									AS media_type_cd
	 , src1.post_evar134 																									AS message_txt
	 , src1.post_evar57 																									AS mobile_device_id
	 , src1.post_evar58 																									AS mobile_device_model_nm
	 , src1.post_evar59 																									AS mobile_device_os_version_cd
	 , src1.post_evar53 																									AS mobile_j4u_application_version_cd
	 , src1.post_evar60 																									AS mobile_latitude_longitude_dgr
	 , src1.post_evar51 																									AS mobile_vs_non_mobile_flg
	 , src1.post_evar168 																									AS modal_name_link_nm
	 , src1.post_evar61 																									AS navigation_source_txt
	 , src1.post_evar176 																									AS network_txt
	 , src1.post_evar8 																										AS new_repeat_visitors_txt
	 , src1.post_evar192 																									AS notification_allowed_txt
	 , src1.os 																												AS operating_system_cd
	 , src1.post_pagename 																									AS page_1_nm
	 , src1.post_evar5 																										AS page_2_nm
	 , src1.post_evar11 																									AS page_url_txt
	 , src1.post_evar179 																									AS past_purchase_items_txt
	 , src1.post_evar14 																									AS pfm_detail_txt
	 , src1.post_evar12 																									AS pfm_source_cd
	 , src1.post_evar3 																										AS pfm_subsection_1_cd
	 , src1.post_evar78 																									AS placement_type_cd
	 , src1.post_evar90 																									AS platform_cd
	 , src1.post_evar90 																									AS platform_dsc
	 , src1.post_evar169 																									AS premium_slots_txt
	 , src1.post_evar10 																									AS previous_page_nm
	 , src1.post_evar177 																									AS provider_txt
	 , src1.post_purchaseid 																								AS purchase_id
	 , src1.post_evar62 																									AS push_notifications_message_id
	 , src1.post_evar190 																									AS recipe_nm
	 , src1.post_evar191 																									AS recipe_source_cd
	 , src1.post_evar87 																									AS referring_application_cd
	 , src1.post_evar183 																									AS sdk_verison_nbr
	 , src1.post_evar65 																									AS social_authors_txt
	 , src1.post_evar67 																									AS social_media_channel_cd
	 , src1.post_evar68 																									AS social_media_content_title_txt
	 , src1.post_evar40 																									AS social_platforms_txt
	 , src1.post_evar109 																									AS sort_selection_txt
	 , src1.post_evar52 																									AS source_site_type_cd
	 , src1.post_evar151 																									AS sub_section1_txt
	 , src1.post_evar152 																									AS sub_section2_txt
	 , src1.post_evar153 																									AS sub_section3_txt
	 , src1.post_evar154 																									AS sub_section4_txt
	 , src1.post_evar164 																									AS subscription_dt
	 , src1.post_evar188 																									AS subscription_funnel_cd
	 , src1.post_evar163 																									AS subscription_status_cd
	 , src1.post_evar197 																									AS syndigo_content_txt
	 , src1.post_evar15 																									AS time_parting_txt
	 , src1.post_evar172 																									AS timestamp_marketplace_txt
	 , src1.post_evar171 																									AS top_nav_usage_cd
	 , src1.post_evar121 																									AS typed_search_cnt
	 , src1.post_evar91 																									AS typed_search_term_cd
	 , src1.post_evar200 																									AS uma_application_nm
	 , src1.post_evar6 																										AS user_action_type_cd
	 , src1.post_evar95 																									AS user_action_cd
	 , src1.post_evar94 																									AS user_agent_nm
	 , src1.post_evar124 																									AS user_message_status_cd
	 , src1.post_evar96 																									AS user_messages_txt
	 , src1.post_evar92 																									AS user_type_cd
	 , src1.videoad 																										AS video_ad_load_txt
	 , src1.visit_page_num 																									AS visit_page_nbr
	 , src1.post_evar49 																									AS visitor2_id
	 , src1.post_evar83 																									AS visitor_interacted_hh_mm_tm
	 , src1.post_evar89 																									AS wearable_device_cd
	 , src1.post_evar186 																									AS ztp_default_method_cd
	 , src1.post_evar187 																									AS ztp_method_cd 
	 , src1.post_evar182																									AS post_evar182
	 , CASE WHEN CONCAT(',', post_event_list, ',')  LIKE '%,20335,%' THEN TRUE ELSE FALSE END 								AS top_nav_clicks_txt
	 , CASE WHEN (TGT.hit_id_high IS NULL AND tgt.hit_id_low IS NULL AND tgt.visit_nbr IS NULL AND tgt.visit_page_nbr IS NULL ) THEN 'I' ELSE 'U' END AS dml_type
FROM (
	  SELECT
			  click_stream_integration_id
			, connection_type
			, country
			, language
			, plugins
			, resolution
			, post_search_engine
			, post_evar13
			, post_evar98
			, post_visid_high
			, post_visid_low
			, visit_start_time_gmt
			, post_evar32
			, accept_language
			, browser
			, carrier
			, color
			, date_time
			, exclude_hit
			, first_hit_ref_type
			, hit_source
			, hitid_high
			, hitid_low
			, javascript
			, os
			, post_browser_height
			, post_browser_width
			, post_campaign
			, post_clickmaplink
			, post_clickmaplinkbyregion
			, post_clickmappage
			, post_clickmapregion
			, post_evar1
			, post_evar10
			, post_evar100
			, post_evar107
			, post_evar108
			, post_evar109
			, post_evar11
			, post_evar116
			, post_evar117
			, post_evar118
			, post_evar119
			, post_evar12
			, post_evar120
			, post_evar121
			, post_evar122
			, post_evar124
			, post_evar129
			, post_evar131
			, post_evar132
			, post_evar133
			, post_evar134
			, post_evar139
			, post_evar14
			, post_evar140
			, post_evar141
			, post_evar142
			, post_evar144
			, post_evar145
			, post_evar148
			, post_evar149
			, post_evar15
			, post_evar150
			, post_evar151
			, post_evar152
			, post_evar153
			, post_evar154
			, post_evar157
			, post_evar158
			, post_evar16
			, post_evar161
			, post_evar163
			, post_evar164
			, post_evar168
			, post_evar169
			, post_evar17
			, post_evar171
			, post_evar172
			, post_evar174
			, post_evar175
			, post_evar176
			, post_evar177
			, post_evar178
			, post_evar179
			, post_evar18
			, post_evar180
			, post_evar183
			, post_evar184
			, post_evar185
			, post_evar186
			, post_evar187
			, post_evar188
			, post_evar19
			, post_evar190
			, post_evar191
			, post_evar192
			, post_evar193
			, post_evar195
			, post_evar196
			, post_evar197
			, post_evar2
			, post_evar200
			, post_evar201
			, post_evar22
			, post_evar23
			, post_evar24
			, post_evar25
			, post_evar26
			, post_evar3
			, post_evar33
			, post_evar36
			, post_evar37
			, post_evar38
			, post_evar4
			, post_evar40
			, post_evar49
			, post_evar5
			, post_evar51
			, post_evar52
			, post_evar53
			, post_evar54
			, post_evar55
			, post_evar57
			, post_evar58
			, post_evar59
			, post_evar6
			, post_evar60
			, post_evar61
			, post_evar62
			, post_evar63
			, post_evar64
			, post_evar65
			, post_evar66
			, post_evar67
			, post_evar68
			, post_evar7
			, post_evar70
			, post_evar74
			, post_evar75
			, post_evar76
			, post_evar78
			, post_evar8
			, post_evar80
			, post_evar81
			, post_evar83
			, post_evar84
			, post_evar85
			, post_evar86
			, post_evar87
			, post_evar88
			, post_evar89
			, post_evar90
			, post_evar91
			, post_evar92
			, post_evar93
			, post_evar94
			, post_evar95
			, post_evar96
			, post_evar99
			, post_event_list
			, post_pagename
			, post_product_list
			, post_purchaseid
			, post_tnt
			, va_closer_detail
			, va_closer_id
			, videoad
			, visit_num
			, visit_page_num
			, ref_type
			, post_evar182
	   FROM src_wrk_tbl_recs src,
		    ${cnf_db}.${cnf_schema}.${cntrl_tbl_nm} sct
	  WHERE src.rn 				= 1
		AND src.hitid_high 		= sct.hit_id_high
		AND src.hitid_low 		= sct.hit_id_low
		AND src.visit_num 		= sct.visit_nbr
		AND src.visit_page_num 	= sct.visit_page_nbr
		AND sct.click_stream_integration_id IS NOT NULL
) src1
// -- v1.2 Start
  LEFT JOIN browser_dtls 		brs1	ON brs1.browser_id 				= src1.browser 
  LEFT JOIN connection_typ_dtls cnt 	ON cnt.connection_type_cd		= src1.connection_type 
  LEFT JOIN country_dtls 		ctr		ON ctr.country_id 				= src1.country 
  LEFT JOIN language_dtls		lng 	ON lng.language_id 				= src1.language
  LEFT JOIN plugin_dtls			plg 	ON plg.plugin_id 				= src1.plugins
  LEFT JOIN resolution_dtls		res1	ON res1.resolution_id 			= src1.resolution
  LEFT JOIN search_engine_dtls	ser		ON ser.search_engine_id 		= src1.post_search_engine
  LEFT JOIN retail_cust_dtls	cus1	ON cus1.retail_customer_uuid	= src1.post_evar182 		
  LEFT JOIN operating_sys_dtls	ops1	ON ops1.operating_system_id 	= src1.os
  LEFT JOIN facility_dtls		fct		ON fct.facility_nbr				= src1.post_evar13 
  LEFT JOIN pos_order_dtls		grc		ON grc.order_id					= src1.post_evar32
// -- v1.2 End  
  LEFT JOIN ${tgt_tbl} tgt
		 ON (src1.hitid_high 		= tgt.hit_id_high
		AND  src1.hitid_low 		= tgt.hit_id_low
		AND  src1.visit_num 		= tgt.visit_nbr
		AND  src1.visit_page_num	= tgt.visit_page_nbr)`;

try {
snowflake.execute ({ sqlText: create_tgt_wrk_table });
log('SUCCEEDED',`Successfuly created work table ${tgt_wrk_tbl}`);
	}
    catch (err) {
		log('FAILED',`Creation of work table ${tgt_wrk_tbl} Failed with error: ${err}`);
        return `Creation of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // -- Return a error message.
				} 

// -- Transaction for Updates, Insert begins           
    var sql_begin = "BEGIN"
// -- Processing Inserts
var sql_inserts = ` INSERT INTO ${tgt_tbl} 
		(
		  click_stream_integration_id
		, hit_id_high
		, hit_id_low
		, visit_nbr
		, visit_page_nbr
		, accepted_language_cd
		, accordion_edit_nbr
		, accordion_edit_txt
		, activity_id
		, activity_nm
		, ada_flg
		, adobe_tnt_id
		, app_banner_cd
		, app_build_nbr
		, application_availability_dt
		, application_detail_txt
		, application_order_status_msg
		, application_os_txt
		, application_os_type_cd
		, application_separate_lst
		, application_sign_in_ind
		, application_type_cd
		, application_type2_cd
		, application_user_start_by_nm
		, application_version_build_nbr
		, application_version_cd
		, availabile_cd
		, banner_cd
		, bot_tracking_txt
		, box_tops_auth_state_cd
		, browser_geo_cd
		, browser_height_txt
		, browser_nm
		, browser_width_txt
		, camera_allowed_cd
		, campaign_affiliate_txt
		, campaign_stacking_txt
		, campaign_txt
		, campaign_2_txt
		, card_less_registration_cd
		, carousel_size_txt
		, carrier_nm
		, cda_marketing_channel_cd
		, channel_manager_channel_cd
		, channel_stacking_txt
		, color_cd
		, connection_type_nm
		, country_nm
		, create_dt
		, create_ts
		, custom_nav_link_tracking_txt
		, customer_status_cd
		, delivery_attended_unattended_flg
		, detail_view_txt
		, ecom_login_id
		, ecom_nav_link_tracking_cd
		, elevaate_flg
		, elevaate_poistion_nbr
		, email_hhid_url_parameter_txt
		, email_theme_url_parameter_txt
		, environment_cd
		, error_feature_cd
		, error_id
		, error_message_dsc
		, error_page_dsc
		, event_id_url_parameter_txt
		, event_nm
		, exclude_hit_flg
		, exclude_row_ind
		, experience_nm
		, face_book_account_nm
		, face_book_banner_cd
		, face_book_campaign_cd
		, facility_integration_id
		, filter_section_txt
		, filter_type_cd
		, first_hit_referrer_type_cd
		, ga_utm_campaign_medium_txt
		, ga_utm_campaign_nm
		, ga_utm_source_cd
		, global_no_substitution_cd
		, hidden_categories_txt
		, hit_source_cd
		, home_page_carousel_txt
		, impressions_component_txt
		, internal_campaign_tracking_id
		, internal_search_results_nbr
		, internal_search_terms_txt
		, internal_search_txt
		, internal_search_type_cd
		, ip_address_nbr
		, java_script_version_nbr
		, kmsi_txt
		, language_nm
		, last_activity_flg
		, launch_rule_txt
		, link_detail_txt
		, list_interaction_type_cd
		, location_sharing_enabled_flg
		, login_kmsi_txt
		, map_clicks_txt
		, map_link_dsc
		, map_link_by_region_nm
		, map_page_cd
		, map_region_cd
		, marketing_channel_cd
		, marketing_channel_dtl
		, media_placement_cd
		, media_type_cd
		, message_txt
		, mobile_device_id
		, mobile_device_model_nm
		, mobile_device_os_version_cd
		, mobile_j4u_application_version_cd
		, mobile_latitude_longitude_dgr
		, mobile_vs_non_mobile_flg
		, modal_name_link_nm
		, navigation_source_txt
		, network_txt
		, new_repeat_visitors_txt
		, notification_allowed_txt
		, operating_system_cd
		, operating_system_nm
		, order_id
		, page_1_nm
		, page_2_nm
		, page_url_txt
		, past_purchase_items_txt
		, pfm_detail_txt
		, pfm_source_cd
		, pfm_subsection_1_cd
		, placement_type_cd
		, platform_cd
		, platform_dsc
		, plugin_nm
		, premium_slots_txt
		, previous_page_nm
		, provider_txt
		, purchase_id
		, push_notifications_message_id
		, recipe_nm
		, recipe_source_cd
		, referrer_type_id
		, referring_application_cd
		, resolution_nm
		, retail_customer_uuid
		, sdk_verison_nbr
		, search_engine_nm
		, social_authors_txt
		, social_media_channel_cd
		, social_media_content_title_txt
		, social_platforms_txt
		, sort_selection_txt
		, source_site_type_cd
		, sub_section1_txt
		, sub_section2_txt
		, sub_section3_txt
		, sub_section4_txt
		, subscription_dt
		, subscription_funnel_cd
		, subscription_status_cd
		, syndigo_content_txt
		, time_parting_txt
		, timestamp_marketplace_txt
		, top_nav_usage_cd
		, typed_search_cnt
		, typed_search_term_cd
		, uma_application_nm
		, user_action_type_cd
		, user_action_cd
		, user_agent_nm
		, user_message_status_cd
		, user_messages_txt
		, user_type_cd
		, version_nbr
		, video_ad_load_txt
		, visit_id
		, visitor_id
		, visitor2_id
		, visitor_interacted_hh_mm_tm
		, wearable_device_cd
		, ztp_default_method_cd
		, ztp_method_cd
		, mobile_application_first_launch_dt
		, top_nav_clicks_txt
		, dw_create_ts
		, dw_last_update_ts
		, dw_logical_delete_ind
		, dw_source_create_nm
		, dw_source_update_nm
		, dw_current_version_ind
		)
SELECT click_stream_integration_id
	 , hit_id_high
	 , hit_id_low
	 , visit_nbr
	 , visit_page_nbr
	 , accepted_language_cd
	 , accordion_edit_nbr
	 , accordion_edit_txt
	 , activity_id
	 , activity_nm
	 , ada_flg
	 , adobe_tnt_id
	 , app_banner_cd
	 , app_build_nbr
	 , application_availability_dt
	 , application_detail_txt
	 , application_order_status_msg
	 , application_os_txt
	 , application_os_type_cd
	 , application_separate_lst
	 , application_sign_in_ind
	 , application_type_cd
	 , application_type2_cd
	 , application_user_start_by_nm
	 , application_version_build_nbr
	 , application_version_cd
	 , availabile_cd
	 , banner_cd
	 , bot_tracking_txt
	 , box_tops_auth_state_cd
	 , browser_geo_cd
	 , browser_height_txt
	 , browser_nm
	 , browser_width_txt
	 , camera_allowed_cd
	 , campaign_affiliate_txt
	 , campaign_stacking_txt
	 , campaign_txt
	 , campaign_2_txt
	 , card_less_registration_cd
	 , carousel_size_txt
	 , carrier_nm
	 , cda_marketing_channel_cd
	 , channel_manager_channel_cd
	 , channel_stacking_txt
	 , color_cd
	 , connection_type_nm
	 , country_nm
	 , create_dt
	 , create_ts
	 , custom_nav_link_tracking_txt
	 , customer_status_cd
	 , delivery_attended_unattended_flg
	 , detail_view_txt
	 , ecom_login_id
	 , ecom_nav_link_tracking_cd
	 , elevaate_flg
	 , elevaate_poistion_nbr
	 , email_hhid_url_parameter_txt
	 , email_theme_url_parameter_txt
	 , environment_cd
	 , error_feature_cd
	 , error_id
	 , error_message_dsc
	 , error_page_dsc
	 , event_id_url_parameter_txt
	 , event_nm
	 , exclude_hit_flg
	 , exclude_row_ind
	 , experience_nm
	 , face_book_account_nm
	 , face_book_banner_cd
	 , face_book_campaign_cd
	 , facility_integration_id
	 , filter_section_txt
	 , filter_type_cd
	 , first_hit_referrer_type_cd
	 , ga_utm_campaign_medium_txt
	 , ga_utm_campaign_nm
	 , ga_utm_source_cd
	 , global_no_substitution_cd
	 , hidden_categories_txt
	 , hit_source_cd
	 , home_page_carousel_txt
	 , impressions_component_txt
	 , internal_campaign_tracking_id
	 , internal_search_results_nbr
	 , internal_search_terms_txt
	 , internal_search_txt
	 , internal_search_type_cd
	 , ip_address_nbr
	 , java_script_version_nbr
	 , kmsi_txt
	 , language_nm
	 , last_activity_flg
	 , launch_rule_txt
	 , link_detail_txt
	 , list_interaction_type_cd
	 , location_sharing_enabled_flg
	 , login_kmsi_txt
	 , map_clicks_txt
	 , map_link_dsc
	 , map_link_by_region_nm
	 , map_page_cd
	 , map_region_cd
	 , marketing_channel_cd
	 , marketing_channel_dtl
	 , media_placement_cd
	 , media_type_cd
	 , message_txt
	 , mobile_device_id
	 , mobile_device_model_nm
	 , mobile_device_os_version_cd
	 , mobile_j4u_application_version_cd
	 , mobile_latitude_longitude_dgr
	 , mobile_vs_non_mobile_flg
	 , modal_name_link_nm
	 , navigation_source_txt
	 , network_txt
	 , new_repeat_visitors_txt
	 , notification_allowed_txt
	 , operating_system_cd
	 , operating_system_nm
	 , order_id
	 , page_1_nm
	 , page_2_nm
	 , page_url_txt
	 , past_purchase_items_txt
	 , pfm_detail_txt
	 , pfm_source_cd
	 , pfm_subsection_1_cd
	 , placement_type_cd
	 , platform_cd
	 , platform_dsc
	 , plugin_nm
	 , premium_slots_txt
	 , previous_page_nm
	 , provider_txt
	 , purchase_id
	 , push_notifications_message_id
	 , recipe_nm
	 , recipe_source_cd
	 , referrer_type_id
	 , referring_application_cd
	 , resolution_nm
	 , retail_customer_uuid
	 , sdk_verison_nbr
	 , search_engine_nm
	 , social_authors_txt
	 , social_media_channel_cd
	 , social_media_content_title_txt
	 , social_platforms_txt
	 , sort_selection_txt
	 , source_site_type_cd
	 , sub_section1_txt
	 , sub_section2_txt
	 , sub_section3_txt
	 , sub_section4_txt
	 , subscription_dt
	 , subscription_funnel_cd
	 , subscription_status_cd
	 , syndigo_content_txt
	 , time_parting_txt
	 , timestamp_marketplace_txt
	 , top_nav_usage_cd
	 , typed_search_cnt
	 , typed_search_term_cd
	 , uma_application_nm
	 , user_action_type_cd
	 , user_action_cd
	 , user_agent_nm
	 , user_message_status_cd
	 , user_messages_txt
	 , user_type_cd
	 , version_nbr
	 , video_ad_load_txt
	 , visit_id
	 , visitor_id
	 , visitor2_id
	 , visitor_interacted_hh_mm_tm
	 , wearable_device_cd
	 , ztp_default_method_cd
	 , ztp_method_cd
	 , mobile_application_first_launch_dt
	 , top_nav_clicks_txt
	 , CURRENT_TIMESTAMP 	AS dw_create_ts
	 , CURRENT_TIMESTAMP	AS dw_last_update_ts
	 , FALSE 				AS dw_logical_delete_ind
	 , 'Adobe' 			    AS dw_source_create_nm
	 , split_part(CURRENT_USER(), '@',  0) AS dw_source_update_nm
	 , TRUE 				AS dw_current_version_ind
FROM ${tgt_wrk_tbl}
WHERE dml_type='I'`;
					
var sql_commit = "COMMIT";
var sql_rollback = "ROLLBACK";

try {
        snowflake.execute ({ sqlText: sql_begin });
        snowflake.execute ({ sqlText: sql_inserts });
        snowflake.execute ({ sqlText: sql_commit });
		log('COMPLETED',`Load for Click Stream Other table completed`);
	} catch (err) {
        snowflake.execute ({ sqlText: sql_rollback });
		snowflake.execute ( {sqlText: sql_ins_rerun_tbl} );
		log('FAILED',`Loading of table ${tgt_tbl} Failed with error: ${err}`);
		return `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // -- Return a error message.
					}		                      					

//-- ************** Load for Click Stream Other table ENDs *****************

$$;

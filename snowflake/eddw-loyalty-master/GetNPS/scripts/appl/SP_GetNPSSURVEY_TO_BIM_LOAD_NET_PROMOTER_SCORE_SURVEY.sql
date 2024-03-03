--liquibase formatted sql
--changeset SYSTEM:SP_GetNPSSURVEY_TO_BIM_LOAD_NET_PROMOTER_SCORE_SURVEY runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_CONFIRMED_PRD;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE EDM_CONFIRMED_PRD.DW_APPL.SP_GETNPSSURVEY_TO_BIM_LOAD_NET_PROMOTER_SCORE_SURVEY(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_LOYAL VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

		var src_wrk_tbl = SRC_WRK_TBL;
		var cnf_schema = C_LOYAL;
		var edm_v_db = "EDM_VIEWS_PRD";
		var edm_v_schema = "DW_EDW_VIEWS";
		var tgt_wrk_tbl = `${CNF_DB}.${C_STAGE}.NET_PROMOTER_SCORE_SURVEY_WRK`;
		var tgt_tbl = `${CNF_DB}.${cnf_schema}.NET_PROMOTER_SCORE_SURVEY`;
                       
                       
    // **************        Load for Net Promoter Score Survey table BEGIN *****************
    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
	var create_tgt_wrk_table = `CREATE OR REPLACE TABLE ${tgt_wrk_tbl} as 
								SELECT DISTINCT
									src.Survey_Id,
									src.Transaction_Id,
									src.Facility_Integration_ID,
									src.Survey_Type_Nm,
									src.Survey_Method_Dsc,
									src.Survey_Status_Cd,
									src.Store_Development_Cycle_Dsc,
									src.Survey_Language_Nm,
									src.Survey_Start_Ts,
									src.Survey_Response_Ts,
									src.Survey_Url_Txt,
									src.Net_Promoter_Score_Segment_Dsc,
									src.Transaction_Ts,
									src.Rolling_Transaction_Count_Txt,
									src.Lane_Nbr,
									src.Master_Parameter_Id,
									src.Survey_Elapse_Secs_Cnt,
									src.Customer_IP_Address_Txt,
									src.Last_Seen_Page_Nm,
									src.Last_Submitted_Page_Nm,
									src.Survey_Page_Path_Nbr,
									src.Survey_Page_Path_Ts,
									src.Survey_Mobile_Device_Ind,
									src.Survey_Mobile_Device_Finished_Ind,
									src.Mobile_First_Ind,
									src.Partial_Survey_Ind,
									src.Survey_Started_Ind,
									src.Survey_Pages_Submit_Cnt,
									src.Survey_Pages_Validation_Failed_Cnt,
									src.Survey_Last_Submit_Ts,
									src.Device_Brand_Nm,
									src.Device_Broswer_Txt,
									src.Device_Browser_Version_Dsc,
									src.Device_Operating_System_Nm,
									src.Device_Operating_System_Version_Dsc,
									src.FeedBack_Ind,
									src.Survey_Banner_Nm,
									src.Survey_Auto_Completed_Ts,
									src.Survey_Auto_Completed_Ind,
                                    src.DW_LOGICAL_DELETE_IND,
                                    src.Survey_Has_Comment_Ind,
									src.filename,
									CASE 
								    WHEN (tgt.Survey_Id IS NULL) 
									THEN 'I' 
									ELSE 'U' 
									END AS DML_Type,
									CASE   
									WHEN tgt.DW_First_Effective_dt = CURRENT_DATE 
									THEN 1 
									Else 0 
									END as Sameday_chg_ind
								FROM (   
										SELECT
											Survey_Id,
											Transaction_Id,
											Facility_Integration_ID,
											Survey_Type_Nm,
											Survey_Method_Dsc,
											Survey_Status_Cd,
											Store_Development_Cycle_Dsc,
											Survey_Language_Nm,
											Survey_Start_Ts,
											Survey_Response_Ts,
											Survey_Url_Txt,
											Net_Promoter_Score_Segment_Dsc,
											Transaction_Ts,
											Rolling_Transaction_Count_Txt,
											Lane_Nbr,
											Master_Parameter_Id,
											Survey_Elapse_Secs_Cnt,
											Customer_IP_Address_Txt,
											Last_Seen_Page_Nm,
											Last_Submitted_Page_Nm,
											Survey_Page_Path_Nbr,
											Survey_Page_Path_Ts,
											Survey_Mobile_Device_Ind,
											Survey_Mobile_Device_Finished_Ind,
											Mobile_First_Ind,
											Partial_Survey_Ind,
											Survey_Started_Ind,
											Survey_Pages_Submit_Cnt,
											Survey_Pages_Validation_Failed_Cnt,
											Survey_Last_Submit_Ts,
											Device_Brand_Nm,
											Device_Broswer_Txt,
											Device_Browser_Version_Dsc,
											Device_Operating_System_Nm,
											Device_Operating_System_Version_Dsc,
											FeedBack_Ind,
											Survey_Banner_Nm,
											Survey_Auto_Completed_Ts,
											Survey_Auto_Completed_Ind,
                                            DW_LOGICAL_DELETE_IND,
                                            Survey_Has_Comment_Ind,
															filename
										FROM ( 
											   SELECT
													Survey_Id,
													Transaction_Id,
													Facility_Integration_ID,
													Survey_Type_Nm,
													Survey_Method_Dsc,
													Survey_Status_Cd,
													Store_Development_Cycle_Dsc,
													Survey_Language_Nm,
													Survey_Start_Ts,
													Survey_Response_Ts,
													Survey_Url_Txt,
													Net_Promoter_Score_Segment_Dsc,
													Transaction_Ts,
													Rolling_Transaction_Count_Txt,
													Lane_Nbr,
													Master_Parameter_Id,
													Survey_Elapse_Secs_Cnt,
													Customer_IP_Address_Txt,
													Last_Seen_Page_Nm,
													Last_Submitted_Page_Nm,
													Survey_Page_Path_Nbr,
													Survey_Page_Path_Ts,
													Survey_Mobile_Device_Ind,
													Survey_Mobile_Device_Finished_Ind,
													Mobile_First_Ind,
													Partial_Survey_Ind,
													Survey_Started_Ind,
													Survey_Pages_Submit_Cnt,
													Survey_Pages_Validation_Failed_Cnt,
													Survey_Last_Submit_Ts,
													Device_Brand_Nm,
													Device_Broswer_Txt,
													Device_Browser_Version_Dsc,
													Device_Operating_System_Nm,
													Device_Operating_System_Version_Dsc,
													FeedBack_Ind,
													Survey_Banner_Nm,
													Survey_Auto_Completed_Ts,
													Survey_Auto_Completed_Ind,
                                                    DW_CREATETS,
                                                   Survey_Has_Comment_Ind,
												   false as  DW_LOGICAL_DELETE_IND,
															filename
											  ,Row_number() OVER (
											  PARTITION BY Survey_Id
											  order by(DW_CREATETS) DESC) as rn
											  FROM(
                                                     SELECT
													     Survey_Id,
														 Transaction_Id,
														Facility_Integration_ID,
														Survey_Type_Nm,
														Survey_Method_Dsc,
														Survey_Status_Cd,
														Store_Development_Cycle_Dsc,
														Survey_Language_Nm,
														Survey_Start_Ts,
														Survey_Response_Ts,
														Survey_Url_Txt,
														Net_Promoter_Score_Segment_Dsc,
														Transaction_Ts,
														Rolling_Transaction_Count_Txt,
														Lane_Nbr,
														Master_Parameter_Id,
														Survey_Elapse_Secs_Cnt,
														Customer_IP_Address_Txt,
														Last_Seen_Page_Nm,
														Last_Submitted_Page_Nm,
														Survey_Page_Path_Nbr,
														Survey_Page_Path_Ts,
														Survey_Mobile_Device_Ind,
														Survey_Mobile_Device_Finished_Ind,
														Mobile_First_Ind,
														Partial_Survey_Ind,
														Survey_Started_Ind,
														Survey_Pages_Submit_Cnt,
														Survey_Pages_Validation_Failed_Cnt,
														Survey_Last_Submit_Ts,
														Device_Brand_Nm,
														Device_Broswer_Txt,
														Device_Browser_Version_Dsc,
														Device_Operating_System_Nm,
														Device_Operating_System_Version_Dsc,
														FeedBack_Ind,
														Survey_Banner_Nm,
														Survey_Auto_Completed_Ts,
														Survey_Auto_Completed_Ind,
                                                        DW_CREATETS,
                                                        Survey_Has_Comment_Ind,
															filename
                                                        
													FROM
													(  SELECT 
														Survey_Id,
														case when S.Survey_Type_Nm in('Delivery','DUG') then S.abs_transaction_id_combined_txt
														else hdr.TXN_ID end AS Transaction_Id,
														Facility_Integration_ID,
														Survey_Type_Nm,
														Survey_Method_Dsc,
														Survey_Status_Cd,
														Store_Development_Cycle_Dsc,
														Survey_Language_Nm,
														Survey_Start_Ts,
														Survey_Response_Ts,
														Survey_Url_Txt,
														Net_Promoter_Score_Segment_Dsc,
														Transaction_Ts,
														Rolling_Transaction_Count_Txt,
														Lane_Nbr,
														Master_Parameter_Id,
														Survey_Elapse_Secs_Cnt,
														Customer_IP_Address_Txt,
														Last_Seen_Page_Nm,
														Last_Submitted_Page_Nm,
														Survey_Page_Path_Nbr,
														Survey_Page_Path_Ts,
														Survey_Mobile_Device_Ind,
														Survey_Mobile_Device_Finished_Ind,
														Mobile_First_Ind,
														Partial_Survey_Ind,
														Survey_Started_Ind,
														Survey_Pages_Submit_Cnt,
														Survey_Pages_Validation_Failed_Cnt,
														Survey_Last_Submit_Ts,
														Device_Brand_Nm,
														Device_Broswer_Txt,
														Device_Browser_Version_Dsc,
														Device_Operating_System_Nm,
														Device_Operating_System_Version_Dsc,
														FeedBack_Ind,
														Survey_Banner_Nm,
														Survey_Auto_Completed_Ts,
														Survey_Auto_Completed_Ind,
                                                        DW_CREATETS,
                                                        Survey_Has_Comment_Ind,
														filename
														FROM
													 (
													 SELECT 
															surveyid AS Survey_Id,
															TRY_TO_NUMERIC(UNITID) as UNITID,
															abs_transaction_id_combined_txt,
															abs_survey_type AS Survey_Type_Nm,
															survey_method AS Survey_Method_Dsc,
															status AS Survey_Status_Cd,
															bp_development_cycle_alt AS Store_Development_Cycle_Dsc,
															survey_language AS Survey_Language_Nm,
															NULL AS Transaction_Id,
															to_timestamp_ltz(startdate,'YYYY-MM-DD HH24:MI:SS') AS Survey_Start_Ts,
															to_timestamp_ltz(survey_response_date,'YYYY-MM-DD HH24:MI:SS') AS Survey_Response_Ts,
															survey_url AS Survey_Url_Txt,
															abs_nps_segment_combined_alt AS Net_Promoter_Score_Segment_Dsc,
															abs_pharm_transactiondate AS Transaction_Ts,
															abs_parameter_rolling_transaction_count_txt AS Rolling_Transaction_Count_Txt,
															TRY_TO_NUMERIC(abs_parameter_lane_number) AS Lane_Nbr,
															abs_parameter_master AS Master_Parameter_Id,
															elapsed_seconds AS Survey_Elapse_Secs_Cnt,
															ipaddress AS Customer_IP_Address_Txt,
															last_seen_page_name AS Last_Seen_Page_Nm,
															last_submitted_page_name AS Last_Submitted_Page_Nm,
														    page_path_page_numbers AS Survey_Page_Path_Nbr,
															page_path_timestamps AS Survey_Page_Path_Ts,
															survey_is_mobile AS Survey_Mobile_Device_Ind,
															survey_is_mobile_finished AS Survey_Mobile_Device_Finished_Ind,
															survey_is_mobile_first AS Mobile_First_Ind,
															survey_is_partial AS Partial_Survey_Ind,
															survey_is_started AS Survey_Started_Ind,
															survey_pages_submit AS Survey_Pages_Submit_Cnt,
															survey_pages_validation_failed AS Survey_Pages_Validation_Failed_Cnt,
															to_timestamp_ltz(survey_last_submit_date,'YYYY-MM-DD HH24:MI:SS') AS Survey_Last_Submit_Ts,
															user_agent_brand AS Device_Brand_Nm,
															user_agent_browser AS Device_Broswer_Txt,
															user_agent_browser_and_ver AS Device_Browser_Version_Dsc,
															user_agent_os AS Device_Operating_System_Nm,
															user_agent_os_and_ver AS Device_Operating_System_Version_Dsc,
															abs_dug_give_more_feedback_yn AS FeedBack_Ind,
															abs_brand AS Survey_Banner_Nm,
															to_timestamp_ltz(abs_auto_completed_date_datetime,'YYYY-MM-DD HH24:MI:SS') AS Survey_Auto_Completed_Ts,
															abs_auto_completed_yn AS Survey_Auto_Completed_Ind,
                                                            bp_fp_survey_has_comment_yn AS SURVEY_HAS_COMMENT_IND,
                                                            DW_CREATETS,
															filename
													 FROM                                                      
                                                      ${src_wrk_tbl} S
													  WHERE S.unitid NOT IN('(I) Ad Hoc','(I) *** Unit Pending')
                                                      ) S
													LEFT JOIN 
													( SELECT DISTINCT FACILITY_INTEGRATION_ID,FACILITY_NBR 
														FROM 
														${CNF_DB}.DW_C_LOCATION.FACILITY 
														WHERE CORPORATION_ID ='001' 
														AND DW_CURRENT_VERSION_IND='TRUE'
													)C ON C.FACILITY_NBR = S.unitid 
													LEFT JOIN 
													( SELECT DISTINCT TXN_ID,store_id,register_nbr,txn_tm
														FROM  
														${edm_v_db}.${edm_v_schema}.TXN_HDR  
													)hdr ON hdr.store_id = S.unitid   
													     AND hdr.register_nbr=S.Lane_Nbr
														 AND hdr.txn_tm=S.Transaction_Ts                                                    
                                                 )
												)
											) where rn=1	
									) src
									LEFT JOIN
									( 
									SELECT  DISTINCT
									Survey_Id,
									Transaction_Id,
											Facility_Integration_ID,
											DW_FIRST_EFFECTIVE_DT,
											DW_LAST_EFFECTIVE_DT,
											Survey_Type_Nm,
											Survey_Method_Dsc,
											Survey_Status_Cd,
											Store_Development_Cycle_Dsc,
											Survey_Language_Nm,
											Survey_Start_Ts,
											Survey_Response_Ts,
											Survey_Url_Txt,
											Net_Promoter_Score_Segment_Dsc,
											Transaction_Ts,
											Rolling_Transaction_Count_Txt,
											Lane_Nbr,
											Master_Parameter_Id,
											Survey_Elapse_Secs_Cnt,
											Customer_IP_Address_Txt,
											Last_Seen_Page_Nm,
											Last_Submitted_Page_Nm,
											Survey_Page_Path_Nbr,
											Survey_Page_Path_Ts,
											Survey_Mobile_Device_Ind,
											Survey_Mobile_Device_Finished_Ind,
											Mobile_First_Ind,
											Partial_Survey_Ind,
											Survey_Started_Ind,
											Survey_Pages_Submit_Cnt,
											Survey_Pages_Validation_Failed_Cnt,
											Survey_Last_Submit_Ts,
											Device_Brand_Nm,
											Device_Broswer_Txt,
											Device_Browser_Version_Dsc,
											Device_Operating_System_Nm,
											Device_Operating_System_Version_Dsc,
											FeedBack_Ind,
											Survey_Banner_Nm,
											Survey_Auto_Completed_Ts,
											Survey_Auto_Completed_Ind,
                                            DW_LOGICAL_DELETE_IND,
                                            Survey_Has_Comment_Ind
									FROM
									${tgt_tbl} tgt
									WHERE DW_CURRENT_VERSION_IND = TRUE
									)as tgt 
									ON
									nvl(src.Survey_Id,'-1') = nvl(tgt.Survey_Id,'-1')
									WHERE  (
									tgt.Survey_Id IS  NULL
									 )
									OR
									(
									NVL(src.Survey_Id, '-1') <> NVL(tgt.Survey_Id, '-1')
									or NVL(src.Transaction_Id, '-1') <> NVL(tgt.Transaction_Id, '-1') 
									or NVL(src.Facility_Integration_ID, '-1') <> NVL(tgt.Facility_Integration_ID, '-1') 
                                    or NVL(src.Survey_Language_Nm, '-1') <> NVL(tgt.Survey_Language_Nm, '-1')                
                                    or NVL(src.Survey_Start_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Survey_Start_Ts,'9999-12-31 00:00:00.000')                  
                                    or NVL(src.Survey_Url_Txt,'-1') <> NVL(tgt.Survey_Url_Txt,'-1')                   
                                    or NVL(src.Survey_Type_Nm,'-1') <> NVL(tgt.Survey_Type_Nm,'-1')                     
                                    or NVL(src.Survey_Method_Dsc,'-1') <> NVL(tgt.Survey_Method_Dsc,'-1')                   
                                    or NVL(src.Survey_Status_Cd,'-1') <> NVL(tgt.Survey_Status_Cd,'-1')                  
                                    or NVL(src.Store_Development_Cycle_Dsc,'-1') <> NVL(tgt.Store_Development_Cycle_Dsc,'-1')        
                                    or NVL(src.Transaction_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Transaction_Ts,'9999-12-31 00:00:00.000')          
                                    or NVL(src.Rolling_Transaction_Count_Txt,'-1') <> NVL(tgt.Rolling_Transaction_Count_Txt,'-1')       
                                    or NVL(src.Lane_Nbr,'-1') <> NVL(tgt.Lane_Nbr,'-1')                           
                                    or NVL(src.Master_Parameter_Id,'-1') <> NVL(tgt.Master_Parameter_Id,'-1')                
                                    or NVL(src.Survey_Elapse_Secs_Cnt,'-1') <> NVL(tgt.Survey_Elapse_Secs_Cnt,'-1')              
                                    or NVL(src.Net_Promoter_Score_Segment_Dsc,'-1') <> NVL(tgt.Net_Promoter_Score_Segment_Dsc,'-1')      
                                    or NVL(src.Customer_IP_Address_Txt,'-1') <> NVL(tgt.Customer_IP_Address_Txt,'-1')       
                                    or NVL(src.Last_Seen_Page_Nm,'-1') <> NVL(tgt.Last_Seen_Page_Nm,'-1')             
                                    or NVL(src.Last_Submitted_Page_Nm,'-1') <> NVL(tgt.Last_Submitted_Page_Nm,'-1')              
                                    or NVL(src.Survey_Page_Path_Nbr,'-1') <> NVL(tgt.Survey_Page_Path_Nbr,'-1')               
                                    or NVL(src.Survey_Page_Path_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Survey_Page_Path_Ts,'9999-12-31 00:00:00.000')                 
                                    or NVL(src.Survey_Response_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Survey_Response_Ts,'9999-12-31 00:00:00.000')                  
                                    or NVL(src.Survey_Mobile_Device_Ind,'-1') <> NVL(tgt.Survey_Mobile_Device_Ind,'-1')           
                                    or NVL(src.Survey_Mobile_Device_Finished_Ind,'-1') <> NVL(tgt.Survey_Mobile_Device_Finished_Ind,'-1')   
                                    or NVL(src.Mobile_First_Ind,'-1') <> NVL(tgt.Mobile_First_Ind,'-1')    
                                    or NVL(src.Partial_Survey_Ind,'-1') <> NVL(tgt.Partial_Survey_Ind,'-1')                  
                                    or NVL(src.Survey_Started_Ind,'-1') <> NVL(tgt.Survey_Started_Ind,'-1')                  
                                    or NVL(src.Survey_Pages_Submit_Cnt,'-1') <> NVL(tgt.Survey_Pages_Submit_Cnt,'-1')            
                                    or NVL(src.Survey_Pages_Validation_Failed_Cnt,'-1') <> NVL(tgt.Survey_Pages_Validation_Failed_Cnt,'-1')  
                                    or NVL(src.Survey_Last_Submit_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Survey_Last_Submit_Ts,'9999-12-31 00:00:00.000')   
                                    or NVL(src.Device_Brand_Nm,'-1') <> NVL(tgt.Device_Brand_Nm,'-1')               
                                    or NVL(src.Device_Broswer_Txt,'-1') <> NVL(tgt.Device_Broswer_Txt,'-1')                  
                                    or NVL(src.Device_Browser_Version_Dsc,'-1') <> NVL(tgt.Device_Browser_Version_Dsc,'-1')          
                                    or NVL(src.Device_Operating_System_Nm,'-1') <> NVL(tgt.Device_Operating_System_Nm,'-1')          
                                    or NVL(src.Device_Operating_System_Version_Dsc,'-1') <> NVL(tgt.Device_Operating_System_Version_Dsc,'-1')
                                    or NVL(src.FeedBack_Ind,'-1') <> NVL(tgt.FeedBack_Ind,'-1')  
                                    or NVL(src.Survey_Banner_Nm,'-1') <> NVL(tgt.Survey_Banner_Nm,'-1')                   
                                    or NVL(src.Survey_Auto_Completed_Ind,'-1') <> NVL(tgt.Survey_Auto_Completed_Ind,'-1')           
                                    or NVL(src.Survey_Auto_Completed_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Survey_Auto_Completed_Ts,'9999-12-31 00:00:00.000')
                                    or NVL(src.Survey_Has_Comment_Ind,'-1') <> NVL(tgt.Survey_Has_Comment_Ind,'-1')
									 OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND  
									 )`;   

try {
snowflake.execute ({ sqlText: create_tgt_wrk_table });
	}
    catch (err) {
        return `Creation of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
				}          
            
 
// Transaction for Updates, Insert begins           
    var sql_begin = "BEGIN"
// SCD Type2 - Processing Different day updates
var sql_updates = `UPDATE ${tgt_tbl} as tgt
					SET 
					DW_Last_Effective_dt = CURRENT_DATE - 1,
					DW_CURRENT_VERSION_IND = FALSE,
					DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
					DW_SOURCE_UPDATE_NM = filename				
					FROM ( 
				    SELECT 
					 Survey_Id,
                     filename
                         FROM ${tgt_wrk_tbl} 
							WHERE 
							DML_Type = 'U' 
							AND Sameday_chg_ind = 0
					) src
					WHERE  
					 nvl(src.Survey_ID,'-1')= nvl(tgt.Survey_ID,'-1') 					
					AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
                   
 // SCD Type1 - Processing Sameday updates
var sql_sameday = `UPDATE ${tgt_tbl} as tgt
					SET Facility_Integration_ID = src.Facility_Integration_ID,
						Transaction_Id  = src.Transaction_Id,
						Survey_Type_Nm = src.Survey_Type_Nm,
						Survey_Method_Dsc = src.Survey_Method_Dsc,
						Survey_Status_Cd = src.Survey_Status_Cd ,
						Store_Development_Cycle_Dsc = src.Store_Development_Cycle_Dsc,
						Survey_Language_Nm = src.Survey_Language_Nm,
						Survey_Start_Ts = src.Survey_Start_Ts,
						Survey_Response_Ts = src.Survey_Response_Ts,
						Survey_Url_Txt = src.Survey_Url_Txt,
						Net_Promoter_Score_Segment_Dsc = src.Net_Promoter_Score_Segment_Dsc,
						Transaction_Ts = src.Transaction_Ts,
						Rolling_Transaction_Count_Txt = src.Rolling_Transaction_Count_Txt,
						Lane_Nbr = src.Lane_Nbr,
						Master_Parameter_Id = src.Master_Parameter_Id,
						Survey_Elapse_Secs_Cnt = src.Survey_Elapse_Secs_Cnt,
						Customer_IP_Address_Txt = src.Customer_IP_Address_Txt,
						Last_Seen_Page_Nm = src.Last_Seen_Page_Nm,
						Last_Submitted_Page_Nm = src.Last_Submitted_Page_Nm,
						Survey_Page_Path_Nbr = src.Survey_Page_Path_Nbr,
						Survey_Page_Path_Ts = src.Survey_Page_Path_Ts,
						Survey_Mobile_Device_Ind = src.Survey_Mobile_Device_Ind, 
 						Survey_Mobile_Device_Finished_Ind = src.Survey_Mobile_Device_Finished_Ind,
 						Mobile_First_Ind = src.Mobile_First_Ind,
						Partial_Survey_Ind = src.Partial_Survey_Ind,
						Survey_Started_Ind = src.Survey_Started_Ind,
						Survey_Pages_Submit_Cnt = src.Survey_Pages_Submit_Cnt,
						Survey_Pages_Validation_Failed_Cnt = src.Survey_Pages_Validation_Failed_Cnt,
						Survey_Last_Submit_Ts = src.Survey_Last_Submit_Ts,
						Device_Brand_Nm = src.Device_Brand_Nm,
						Device_Broswer_Txt = src.Device_Broswer_Txt,
						Device_Browser_Version_Dsc = src.Device_Browser_Version_Dsc,
						Device_Operating_System_Nm = src.Device_Operating_System_Nm,
						Device_Operating_System_Version_Dsc = src.Device_Operating_System_Version_Dsc,
						FeedBack_Ind = src.FeedBack_Ind, 
						Survey_Banner_Nm = src.Survey_Banner_Nm,
						Survey_Auto_Completed_Ts = src.Survey_Auto_Completed_Ts,
						Survey_Auto_Completed_Ind = src.Survey_Auto_Completed_Ind,
                        Survey_Has_Comment_Ind = src.Survey_Has_Comment_Ind,
					    DW_Logical_delete_ind = src.DW_Logical_delete_ind,
					    DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
					    DW_SOURCE_UPDATE_NM = filename
					FROM ( 
							SELECT
							Survey_Id,                             
                            Facility_Integration_ID,               
                            Transaction_Id,                      
                            Survey_Language_Nm,                    
                            Survey_Start_Ts,                       
                            Survey_Url_Txt,                        
                            Survey_Type_Nm,                        
                            Survey_Method_Dsc,                     
                            Survey_Status_Cd,                      
                            Store_Development_Cycle_Dsc,           
                            Transaction_Ts,                        
                            Rolling_Transaction_Count_Txt,         
                            Lane_Nbr,                              
                            Master_Parameter_Id,                   
                            Survey_Elapse_Secs_Cnt,                
                            Net_Promoter_Score_Segment_Dsc,        
                            Customer_IP_Address_Txt,               
                            Last_Seen_Page_Nm,                     
                            Last_Submitted_Page_Nm,                
                            Survey_Page_Path_Nbr,                  
                            Survey_Page_Path_Ts,                   
                            Survey_Response_Ts,                    
                            Survey_Mobile_Device_Ind,              
                            Survey_Mobile_Device_Finished_Ind,     
                            Mobile_First_Ind,                      
                            Partial_Survey_Ind,                    
                            Survey_Started_Ind,                    
                            Survey_Pages_Submit_Cnt,               
                            Survey_Pages_Validation_Failed_Cnt,    
                            Survey_Last_Submit_Ts,                 
                            Device_Brand_Nm,                       
                            Device_Broswer_Txt,                    
                            Device_Browser_Version_Dsc,            
                            Device_Operating_System_Nm,            
                            Device_Operating_System_Version_Dsc,   
                            FeedBack_Ind,                          
                            Survey_Banner_Nm,                      
                            Survey_Auto_Completed_Ind,             
                            Survey_Auto_Completed_Ts,              
                            Survey_Has_Comment_Ind,
                            DW_Logical_delete_ind,
							filename
							FROM ${tgt_wrk_tbl}
							WHERE 
							DML_Type = 'U' 
							AND Sameday_chg_ind = 1
						) src WHERE 
						NVL(src.Survey_Id, '-1') = NVL(tgt.Survey_Id, '-1')
						AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl} 
					(
					  Survey_Id,                             
                      Facility_Integration_ID, 
                      Transaction_Id,                      
                      Survey_Language_Nm,                    
                      Survey_Start_Ts,                       
                      Survey_Url_Txt,                        
                      Survey_Type_Nm,                        
                      Survey_Method_Dsc,                     
                      Survey_Status_Cd,                      
                      Store_Development_Cycle_Dsc,           
                      Transaction_Ts,                        
                      Rolling_Transaction_Count_Txt,         
                      Lane_Nbr,                              
                      Master_Parameter_Id,                   
                      Survey_Elapse_Secs_Cnt,                
                      Net_Promoter_Score_Segment_Dsc,        
                      Customer_IP_Address_Txt,               
                      Last_Seen_Page_Nm,                     
                      Last_Submitted_Page_Nm,                
                      Survey_Page_Path_Nbr,                  
                      Survey_Page_Path_Ts,                   
                      Survey_Response_Ts,                    
                      Survey_Mobile_Device_Ind,              
                      Survey_Mobile_Device_Finished_Ind,     
                      Mobile_First_Ind,                      
                      Partial_Survey_Ind,                    
                      Survey_Started_Ind,                    
                      Survey_Pages_Submit_Cnt,               
                      Survey_Pages_Validation_Failed_Cnt,    
                      Survey_Last_Submit_Ts,                 
                      Device_Brand_Nm,                       
                      Device_Broswer_Txt,                    
                      Device_Browser_Version_Dsc,            
                      Device_Operating_System_Nm,            
                      Device_Operating_System_Version_Dsc,   
                      FeedBack_Ind,                          
                      Survey_Banner_Nm,                      
                      Survey_Auto_Completed_Ind,             
                      Survey_Auto_Completed_Ts,              
                      Survey_Has_Comment_Ind,
					  DW_CREATE_TS,
                      DW_LOGICAL_DELETE_IND,
                      DW_SOURCE_CREATE_NM,
                      DW_CURRENT_VERSION_IND,
                      DW_First_Effective_Dt,
                      DW_Last_Effective_Dt
					)
					SELECT
				                    
							Survey_Id,                             
									Facility_Integration_ID, 
									Transaction_Id,                      
									Survey_Language_Nm,                    
									Survey_Start_Ts,                       
									Survey_Url_Txt,                        
									Survey_Type_Nm,                        
									Survey_Method_Dsc,                     
									Survey_Status_Cd,                      
									Store_Development_Cycle_Dsc,           
									Transaction_Ts,                        
									Rolling_Transaction_Count_Txt,         
									Lane_Nbr,                              
									Master_Parameter_Id,                   
									Survey_Elapse_Secs_Cnt,                
									Net_Promoter_Score_Segment_Dsc,        
									Customer_IP_Address_Txt,               
									Last_Seen_Page_Nm,                     
									Last_Submitted_Page_Nm,                
									Survey_Page_Path_Nbr,                  
									Survey_Page_Path_Ts,                   
									Survey_Response_Ts,                    
									Survey_Mobile_Device_Ind,              
									Survey_Mobile_Device_Finished_Ind,     
									Mobile_First_Ind,                      
									Partial_Survey_Ind,                    
									Survey_Started_Ind,                    
									Survey_Pages_Submit_Cnt,               
									Survey_Pages_Validation_Failed_Cnt,    
									Survey_Last_Submit_Ts,                 
									Device_Brand_Nm,                       
									Device_Broswer_Txt,                    
									Device_Browser_Version_Dsc,            
									Device_Operating_System_Nm,            
									Device_Operating_System_Version_Dsc,   
									FeedBack_Ind,                          
									Survey_Banner_Nm,                      
									Survey_Auto_Completed_Ind,             
									Survey_Auto_Completed_Ts,              
									Survey_Has_Comment_Ind,
                                    CURRENT_DATE,
                                    DW_LOGICAL_DELETE_IND,
                                    filename,
                                    TRUE,
                                    CURRENT_DATE,
					                '31-DEC-9999'
					FROM ${tgt_wrk_tbl} 
					WHERE 
					Sameday_chg_ind = 0`;
    
var sql_commit = "COMMIT";
var sql_rollback = "ROLLBACK";
    
try {
        snowflake.execute ({ sqlText: sql_begin });
        snowflake.execute ({ sqlText: sql_updates });
        snowflake.execute ({ sqlText: sql_sameday });
        snowflake.execute ({ sqlText: sql_inserts });
        snowflake.execute ({ sqlText: sql_commit });
	} catch (err) {
        snowflake.execute ({ sqlText: sql_rollback });
		return `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
					}		
                               
// **************        Load for Net Promoter Score Survey table ENDs *****************

$$;
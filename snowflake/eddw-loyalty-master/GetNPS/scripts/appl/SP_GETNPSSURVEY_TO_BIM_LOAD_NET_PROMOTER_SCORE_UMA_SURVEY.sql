--liquibase formatted sql
--changeset SYSTEM:SP_GETNPSSURVEY_TO_BIM_LOAD_NET_PROMOTER_SCORE_UMA_SURVEY runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETNPSSURVEY_TO_BIM_LOAD_NET_PROMOTER_SCORE_UMA_SURVEY(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_LOYAL VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

		var src_wrk_tbl = SRC_WRK_TBL;
		var cnf_schema = C_LOYAL;
		var tgt_wrk_tbl = `${CNF_DB}.${C_STAGE}.Net_Promoter_Score_Uma_Survey_WRK`;
		var tgt_tbl = `${CNF_DB}.${cnf_schema}.Net_Promoter_Score_Uma_Survey`;
		var src_rerun_tbl = `${CNF_DB}.${C_STAGE}.GETNPSUMASURVEY_FLAT_RERUN`;
		var temp_wrk_tbl = `${CNF_DB}.${C_STAGE}.Net_Promoter_Score_Uma_Survey_tmp_WRK`;

		
				 // **************        Truncate and Reload the work table *****************

    var truncate_tgt_wrk_table = `DELETE from ${tgt_wrk_tbl}`;
try {
snowflake.execute ({ sqlText: truncate_tgt_wrk_table });
	}
    catch (err) {
        throw `Deletion of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
				}
				
// Empty the rerun queue table
	var sql_empty_rerun_tbl = `TRUNCATE TABLE ${src_rerun_tbl}`;

//query to load rerun queue table when encountered a failure

	var sql_ins_rerun_tbl = `INSERT INTO ${src_rerun_tbl} SELECT * FROM ${temp_wrk_tbl}`;
	
// persist stream data in work table for the current transaction, includes data from previous failed run
			var sql_crt_src_wrk_tbl = `INSERT INTO  ${temp_wrk_tbl}
										SELECT * FROM `+ src_wrk_tbl +` 
										UNION ALL
										SELECT * FROM `+ src_rerun_tbl+``;
	try {
			snowflake.execute ({sqlText: sql_crt_src_wrk_tbl });
			}
			catch (err) {
			throw `Creation of Source Work table + ${temp_wrk_tbl} + Failed with error:  + ${err}`; // Return a error message.
			}


                       
    // **************        Load for Net_Promoter_Score_Uma_Survey table BEGIN *****************
    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
	var create_tgt_wrk_table = `INSERT INTO ${tgt_wrk_tbl} 
								SELECT DISTINCT
								 src.Survey_Id             
								 ,src.Response_ts             
								 ,src.Digital_Survey_Nm       
								 ,src.Digital_Unit_Nm        
								 ,src.Region_Nm 
								 ,src.City_Nm                 
								 ,src.Device_Vendor_Nm        
								 ,src.Device_Model_Nm        
								 ,src.Device_Screen_Resolution_Txt    
								 ,src.Device_Os_Nm            
								 ,src.Device_Os_Version_Txt    
								 ,src.Retail_Customer_Uuid    
								 ,src.Trigger_Entity_Type_Txt    
								 ,src.Mobile_App_Version_Txt    
								 ,src.Mobile_App_Id           
								 ,src.Mobile_Device_Id        
								 ,src.Dug_Score_Nbr           
								 ,src.Dug_Reason_For_Score_Comment_Txt    
								 ,src.Dug_Item_Availability_Score_Nbr    
								 ,src.Dug_Safety_Precautions_Taken_Score_Nbr    
								 ,src.Dug_Order_Timeliness_Score_Nbr    
								 ,src.Dug_Order_Process_Ease_Score_Nbr    
								 ,src.Dug_Quality_Score_Nbr    
								 ,src.Dug_Freshness_Score_Nbr   
								 ,src.Dug_Order_Accuracy_Score_Nbr    
								 ,src.Dug_Associate_Friendliness_Score_Nbr    
								 ,src.Dug_Give_More_Feedback_Ind    
								 ,src.Dug_Substitution_Satisfaction_Score_Nbr    
								 ,src.Dug_Substitution_Dissatisfaction_Reason_Txt    
								 ,src.Dug_Substitution_Dissatisfaction_Reason_Other_Txt    
								 ,src.Delivery_Score_Nbr      
								 ,src.Delivery_Reason_For_Score_Comment_Txt    
								 ,src.Delivery_Item_Availability_Score_Nbr    
								 ,src.Delivery_Safety_Precautions_Taken_Score_Nbr    
								 ,src.Delivery_Order_Timeliness_Score_Nbr    
								 ,src.Delivery_Order_Process_Ease_Score_Nbr    
								 ,src.Delivery_Quality_Score_Nbr    
								 ,src.Delivery_Freshness_Score_Nbr    
								 ,src.Delivery_Order_Accuracy_Score_Nbr    
								 ,src.Delivery_Associate_Friendliness_Score_Nbr    
								 ,src.Delivery_Give_More_Feedback_Ind   
								 ,src.Delivery_Substitution_Satisfaction_Score_Nbr    
								 ,src.Delivery_Substitution_Dissatisfaction_Reason_Txt    
								 ,src.Delivery_Substitution_Dissatisfaction_Reason_Other_Txt    
								 ,src.Banner_Nm               
								 ,src.Order_Nbr               
								 ,src.Retail_Store_Id
								,src.filename
								,src.DW_LOGICAL_DELETE_IND
                                ,CASE 
								    WHEN (
										     tgt.Survey_Id IS NULL 
								         ) 
									THEN 'I' 
									ELSE 'U' 
								END AS DML_Type
								,CASE   
									WHEN tgt.DW_First_Effective_dt = CURRENT_DATE 
									THEN 1 
									Else 0 
								END as Sameday_chg_ind
								FROM (   SELECT
											Survey_Id             
											 ,Response_ts             
											 ,Digital_Survey_Nm       
											 ,Digital_Unit_Nm        
											 ,Region_Nm 
											 ,City_Nm                 
											 ,Device_Vendor_Nm        
											 ,Device_Model_Nm        
											 ,Device_Screen_Resolution_Txt    
											 ,Device_Os_Nm            
											 ,Device_Os_Version_Txt    
											 ,Retail_Customer_Uuid    
											 ,Trigger_Entity_Type_Txt    
											 ,Mobile_App_Version_Txt    
											 ,Mobile_App_Id           
											 ,Mobile_Device_Id        
											 ,Dug_Score_Nbr           
											 ,Dug_Reason_For_Score_Comment_Txt    
											 ,Dug_Item_Availability_Score_Nbr    
											 ,Dug_Safety_Precautions_Taken_Score_Nbr    
											 ,Dug_Order_Timeliness_Score_Nbr    
											 ,Dug_Order_Process_Ease_Score_Nbr    
											 ,Dug_Quality_Score_Nbr    
											 ,Dug_Freshness_Score_Nbr   
											 ,Dug_Order_Accuracy_Score_Nbr    
											 ,Dug_Associate_Friendliness_Score_Nbr    
											 ,Dug_Give_More_Feedback_Ind    
											 ,Dug_Substitution_Satisfaction_Score_Nbr    
											 ,Dug_Substitution_Dissatisfaction_Reason_Txt    
											 ,Dug_Substitution_Dissatisfaction_Reason_Other_Txt    
											 ,Delivery_Score_Nbr      
											 ,Delivery_Reason_For_Score_Comment_Txt    
											 ,Delivery_Item_Availability_Score_Nbr    
											 ,Delivery_Safety_Precautions_Taken_Score_Nbr    
											 ,Delivery_Order_Timeliness_Score_Nbr    
											 ,Delivery_Order_Process_Ease_Score_Nbr    
											 ,Delivery_Quality_Score_Nbr    
											 ,Delivery_Freshness_Score_Nbr    
											 ,Delivery_Order_Accuracy_Score_Nbr    
											 ,Delivery_Associate_Friendliness_Score_Nbr    
											 ,Delivery_Give_More_Feedback_Ind    
											 ,Delivery_Substitution_Satisfaction_Score_Nbr    
											 ,Delivery_Substitution_Dissatisfaction_Reason_Txt    
											 ,Delivery_Substitution_Dissatisfaction_Reason_Other_Txt    
											 ,Banner_Nm               
											 ,Order_Nbr               
											 ,Retail_Store_Id 
											,filename
											,DW_LOGICAL_DELETE_IND
										
										FROM ( 
											   SELECT
												 Survey_Id             
												 ,Response_ts             
												 ,Digital_Survey_Nm       
												 ,Digital_Unit_Nm        
												 ,Region_Nm 
												 ,City_Nm                 
												 ,Device_Vendor_Nm        
												 ,Device_Model_Nm        
												 ,Device_Screen_Resolution_Txt    
												 ,Device_Os_Nm            
												 ,Device_Os_Version_Txt    
												 ,Retail_Customer_Uuid    
												 ,Trigger_Entity_Type_Txt    
												 ,Mobile_App_Version_Txt    
												 ,Mobile_App_Id           
												 ,Mobile_Device_Id        
												 ,Dug_Score_Nbr           
												 ,Dug_Reason_For_Score_Comment_Txt    
												 ,Dug_Item_Availability_Score_Nbr    
												 ,Dug_Safety_Precautions_Taken_Score_Nbr    
												 ,Dug_Order_Timeliness_Score_Nbr    
												 ,Dug_Order_Process_Ease_Score_Nbr    
												 ,Dug_Quality_Score_Nbr    
												 ,Dug_Freshness_Score_Nbr   
												 ,Dug_Order_Accuracy_Score_Nbr    
												 ,Dug_Associate_Friendliness_Score_Nbr    
												 ,Dug_Give_More_Feedback_Ind    
												 ,Dug_Substitution_Satisfaction_Score_Nbr    
												 ,Dug_Substitution_Dissatisfaction_Reason_Txt    
												 ,Dug_Substitution_Dissatisfaction_Reason_Other_Txt    
												 ,Delivery_Score_Nbr      
												 ,Delivery_Reason_For_Score_Comment_Txt    
												 ,Delivery_Item_Availability_Score_Nbr    
												 ,Delivery_Safety_Precautions_Taken_Score_Nbr    
												 ,Delivery_Order_Timeliness_Score_Nbr    
												 ,Delivery_Order_Process_Ease_Score_Nbr    
												 ,Delivery_Quality_Score_Nbr    
												 ,Delivery_Freshness_Score_Nbr    
												 ,Delivery_Order_Accuracy_Score_Nbr    
												 ,Delivery_Associate_Friendliness_Score_Nbr    
												 ,Delivery_Give_More_Feedback_Ind   
												 ,Delivery_Substitution_Satisfaction_Score_Nbr    
												 ,Delivery_Substitution_Dissatisfaction_Reason_Txt    
												 ,Delivery_Substitution_Dissatisfaction_Reason_Other_Txt    
												 ,Banner_Nm               
												 ,Order_Nbr               
												 ,Retail_Store_Id
												 ,filename
												 ,DW_CREATE_TS
												 ,false as  DW_LOGICAL_DELETE_IND
											,Row_number() OVER (
											 PARTITION BY Survey_Id
											  order by(Response_ts) DESC) as rn
											  FROM(
                                                    SELECT
													 Survey_Id             
													 ,Response_ts             
													 ,Digital_Survey_Nm       
													 ,Digital_Unit_Nm        
													 ,Region_Nm 
													 ,City_Nm                 
													 ,Device_Vendor_Nm        
													 ,Device_Model_Nm        
													 ,Device_Screen_Resolution_Txt    
													 ,Device_Os_Nm            
													 ,Device_Os_Version_Txt    
													 ,Retail_Customer_Uuid    
													 ,Trigger_Entity_Type_Txt    
													 ,Mobile_App_Version_Txt    
													 ,Mobile_App_Id           
													 ,Mobile_Device_Id        
													 ,Dug_Score_Nbr           
													 ,Dug_Reason_For_Score_Comment_Txt    
													 ,Dug_Item_Availability_Score_Nbr    
													 ,Dug_Safety_Precautions_Taken_Score_Nbr    
													 ,Dug_Order_Timeliness_Score_Nbr    
													 ,Dug_Order_Process_Ease_Score_Nbr    
													 ,Dug_Quality_Score_Nbr    
													 ,Dug_Freshness_Score_Nbr   
													 ,Dug_Order_Accuracy_Score_Nbr    
													 ,Dug_Associate_Friendliness_Score_Nbr    
													 ,Dug_Give_More_Feedback_Ind    
													 ,Dug_Substitution_Satisfaction_Score_Nbr    
													 ,Dug_Substitution_Dissatisfaction_Reason_Txt    
													 ,Dug_Substitution_Dissatisfaction_Reason_Other_Txt    
													 ,Delivery_Score_Nbr      
													 ,Delivery_Reason_For_Score_Comment_Txt    
													 ,Delivery_Item_Availability_Score_Nbr    
													 ,Delivery_Safety_Precautions_Taken_Score_Nbr    
													 ,Delivery_Order_Timeliness_Score_Nbr    
													 ,Delivery_Order_Process_Ease_Score_Nbr    
													 ,Delivery_Quality_Score_Nbr    
													 ,Delivery_Freshness_Score_Nbr    
													 ,Delivery_Order_Accuracy_Score_Nbr    
													 ,Delivery_Associate_Friendliness_Score_Nbr    
													 ,Delivery_Give_More_Feedback_Ind    
													 ,Delivery_Substitution_Satisfaction_Score_Nbr    
													 ,Delivery_Substitution_Dissatisfaction_Reason_Txt    
													 ,Delivery_Substitution_Dissatisfaction_Reason_Other_Txt    
													 ,Banner_Nm               
													 ,Order_Nbr               
													 ,Retail_Store_Id 
													 ,filename
													 ,DW_CREATE_TS
													FROM
													  (
													  SELECT  
													    surveyid AS Survey_Id
														,to_timestamp_ltz(responsets) AS Response_ts
														,bp_digital_survey_alt AS Digital_Survey_Nm
														,bp_digital_unit AS Digital_Unit_Nm
														,bp_digital_region_auto AS Region_Nm
														,bp_digital_city_auto AS City_Nm
														,bp_digital_device_vendor_auto AS Device_Vendor_Nm
														,bp_digital_device_model_auto AS Device_Model_Nm
														,bp_digital_device_screen_resolution_auto AS Device_Screen_Resolution_Txt
														,bp_digital_device_os_name_auto AS Device_Os_Nm
														,bp_digital_device_os_version_auto AS Device_Os_Version_Txt
														,bp_digital_uuid_txt AS Retail_Customer_Uuid
														,bp_digital_trigger_entity_type_auto AS Trigger_Entity_Type_Txt
														,bp_digital_mobile_app_version_auto AS Mobile_App_Version_Txt
														,bp_digital_mobile_app_id_auto AS Mobile_App_Id
														,bp_digital_mobile_device_id_auto AS Mobile_Device_Id
														,abs_dug_ltr_scale11 AS Dug_Score_Nbr
														,abs_dug_ltr_filtered_cmt AS Dug_Reason_For_Score_Comment_Txt
														,abs_dug_item_availability_scale11 AS Dug_Item_Availability_Score_Nbr
														,abs_dug_safety_precautions_scale11 AS Dug_Safety_Precautions_Taken_Score_Nbr
														,abs_dug_order_timeliness_scale11 AS Dug_Order_Timeliness_Score_Nbr
														,abs_dug_order_process_ease_scale11 AS Dug_Order_Process_Ease_Score_Nbr
														,abs_dug_quality_scale11 AS Dug_Quality_Score_Nbr
														,abs_dug_freshness_scale11 AS Dug_Freshness_Score_Nbr
														,abs_dug_order_accuracy_scale11 AS Dug_Order_Accuracy_Score_Nbr
														,abs_dug_associate_friendliness_scale11 AS Dug_Associate_Friendliness_Score_Nbr
														,abs_dug_give_more_feedback_yn AS Dug_Give_More_Feedback_Ind
														,abs_dug_substitution_osat_scale11 AS Dug_Substitution_Satisfaction_Score_Nbr
														,abs_dug_substitution_dissatisfaction_reason_mvalue AS Dug_Substitution_Dissatisfaction_Reason_Txt
														,abs_dug_sub_dissatisfaction_reason_other_filtered_txt AS Dug_Substitution_Dissatisfaction_Reason_Other_Txt
														,abs_delivery_ltr_scale11 AS Delivery_Score_Nbr
														,abs_delivery_ltr_filtered_cmt AS Delivery_Reason_For_Score_Comment_Txt
														,abs_delivery_item_availability_scale11 AS Delivery_Item_Availability_Score_Nbr
														,abs_delivery_freshness_scale11 AS Delivery_Safety_Precautions_Taken_Score_Nbr
														,abs_delivery_order_timeliness_scale11 AS Delivery_Order_Timeliness_Score_Nbr
														,abs_delivery_order_accuracy_scale11 AS Delivery_Order_Process_Ease_Score_Nbr
														,abs_delivery_quality_scale11 AS Delivery_Quality_Score_Nbr
														,abs_delivery_order_process_ease_scale11 AS Delivery_Freshness_Score_Nbr
														,abs_delivery_associate_friendliness_scale11 AS Delivery_Order_Accuracy_Score_Nbr
														,abs_delivery_safety_precautions_scale11 AS Delivery_Associate_Friendliness_Score_Nbr
														,abs_delivery_give_more_feedback_yn AS Delivery_Give_More_Feedback_Ind
														,abs_delivery_substitution_osat_scale11 AS Delivery_Substitution_Satisfaction_Score_Nbr
														,abs_delivery_substitution_dissatisfaction_reason_mvalue AS Delivery_Substitution_Dissatisfaction_Reason_Txt
														,abs_delivery_sub_dissatisfaction_reason_other_filtered_txt AS Delivery_Substitution_Dissatisfaction_Reason_Other_Txt
														,md_text_custom_parameter_field_2181 AS Banner_Nm
														,md_text_custom_parameter_field_2180 AS Order_Nbr
														,md_text_custom_parameter_field_3015 AS Retail_Store_Id
														,Filename AS Filename
														,DW_CreateTs AS Dw_Create_Ts
													  FROM 
													   ${temp_wrk_tbl} S
													  )
                                                )
											)  where rn=1	AND Survey_Id is NOT NULL
															
									) src
									LEFT JOIN
									( 
									SELECT  DISTINCT
											 Survey_Id             
											 ,Response_ts             
											 ,Digital_Survey_Nm       
											 ,Digital_Unit_Nm        
											 ,Region_Nm 
											 ,City_Nm                 
											 ,Device_Vendor_Nm        
											 ,Device_Model_Nm        
											 ,Device_Screen_Resolution_Txt    
											 ,Device_Os_Nm            
											 ,Device_Os_Version_Txt    
											 ,Retail_Customer_Uuid    
											 ,Trigger_Entity_Type_Txt    
											 ,Mobile_App_Version_Txt    
											 ,Mobile_App_Id           
											 ,Mobile_Device_Id        
											 ,Dug_Score_Nbr           
											 ,Dug_Reason_For_Score_Comment_Txt    
											 ,Dug_Item_Availability_Score_Nbr    
											 ,Dug_Safety_Precautions_Taken_Score_Nbr    
											 ,Dug_Order_Timeliness_Score_Nbr    
											 ,Dug_Order_Process_Ease_Score_Nbr    
											 ,Dug_Quality_Score_Nbr    
											 ,Dug_Freshness_Score_Nbr   
											 ,Dug_Order_Accuracy_Score_Nbr    
											 ,Dug_Associate_Friendliness_Score_Nbr    
											 ,Dug_Give_More_Feedback_Ind    
											 ,Dug_Substitution_Satisfaction_Score_Nbr    
											 ,Dug_Substitution_Dissatisfaction_Reason_Txt    
											 ,Dug_Substitution_Dissatisfaction_Reason_Other_Txt    
											 ,Delivery_Score_Nbr      
											 ,Delivery_Reason_For_Score_Comment_Txt    
											 ,Delivery_Item_Availability_Score_Nbr    
											 ,Delivery_Safety_Precautions_Taken_Score_Nbr    
											 ,Delivery_Order_Timeliness_Score_Nbr    
											 ,Delivery_Order_Process_Ease_Score_Nbr    
											 ,Delivery_Quality_Score_Nbr    
											 ,Delivery_Freshness_Score_Nbr    
											 ,Delivery_Order_Accuracy_Score_Nbr    
											 ,Delivery_Associate_Friendliness_Score_Nbr    
											 ,Delivery_Give_More_Feedback_Ind  
											 ,Delivery_Substitution_Satisfaction_Score_Nbr    
											 ,Delivery_Substitution_Dissatisfaction_Reason_Txt    
											 ,Delivery_Substitution_Dissatisfaction_Reason_Other_Txt    
											 ,Banner_Nm               
											 ,Order_Nbr               
											 ,Retail_Store_Id
											,DW_First_Effective_dt
											,DW_LOGICAL_DELETE_IND
									FROM
									${tgt_tbl} tgt
									WHERE DW_CURRENT_VERSION_IND = TRUE
									)as tgt 
									ON
										 nvl(src.Survey_Id ,'-1') = nvl(tgt.Survey_Id ,'-1')
									WHERE  (
									         tgt.Survey_Id IS  NULL									
									 )
									OR
									(
									    NVL(to_timestamp(src.Response_ts),'9999-12-31 00:00:00.000') <> NVL(tgt.Response_ts ,'9999-12-31 00:00:00.000')
									 OR NVL(src.Digital_Survey_Nm,'-1') <> NVL(tgt.Digital_Survey_Nm,'-1')  
									 OR NVL(src.Digital_Unit_Nm,'-1') <> NVL(tgt.Digital_Unit_Nm,'-1')
									 OR NVL(src.Region_Nm,'-1') <> NVL(tgt.Region_Nm,'-1')
									 OR NVL(src.City_Nm,'-1') <> NVL(tgt.City_Nm,'-1')
								     OR NVL(src.Device_Vendor_Nm,'-1') <>NVL(tgt.Device_Vendor_Nm,'-1')
									 OR NVL(src.Device_Model_Nm,'-1') <> NVL(tgt.Device_Model_Nm,'-1')
									 OR NVL(src.Device_Screen_Resolution_Txt,'-1') <> NVL(tgt.Device_Screen_Resolution_Txt,'-1')
									 OR NVL(src.Device_Os_Nm,'-1') <> NVL(tgt.Device_Os_Nm,'-1')
									 OR NVL(src.Device_Os_Version_Txt ,'-1') <> NVL(tgt.Device_Os_Version_Txt ,'-1')
									 OR NVL(src.Retail_Customer_Uuid,'-1') <> NVL(tgt.Retail_Customer_Uuid,'-1')  
									 OR NVL(src.Trigger_Entity_Type_Txt,'-1') <> NVL(tgt.Trigger_Entity_Type_Txt,'-1')
									 OR NVL(src.Mobile_App_Version_Txt,'-1') <> NVL(tgt.Mobile_App_Version_Txt,'-1')
									 OR NVL(src.Mobile_App_Id,'-1') <> NVL(tgt.Mobile_App_Id,'-1')
									 OR NVL(src.Mobile_Device_Id,'-1') <>NVL(tgt.Mobile_Device_Id,'-1')
									 OR NVL(to_number(src.Dug_Score_Nbr),'-1') <> NVL(tgt.Dug_Score_Nbr,'-1')
									 OR NVL(src.Dug_Reason_For_Score_Comment_Txt,'-1') <> NVL(tgt.Dug_Reason_For_Score_Comment_Txt,'-1')
									 OR NVL(to_number(src.Dug_Item_Availability_Score_Nbr),'-1') <> NVL(tgt.Dug_Item_Availability_Score_Nbr,'-1')
									 OR NVL(to_number(src.Dug_Safety_Precautions_Taken_Score_Nbr),'-1') <> NVL(tgt.Dug_Safety_Precautions_Taken_Score_Nbr ,'-1')
									 OR NVL(to_number(src.Dug_Order_Timeliness_Score_Nbr),'-1') <> NVL(tgt.Dug_Order_Timeliness_Score_Nbr,'-1')  
									 OR NVL(to_number(src.Dug_Order_Process_Ease_Score_Nbr),'-1') <> NVL(tgt.Dug_Order_Process_Ease_Score_Nbr,'-1')
									 OR NVL(to_number(src.Dug_Quality_Score_Nbr),'-1') <> NVL(tgt.Dug_Quality_Score_Nbr,'-1')
									 OR NVL(to_number(src.Dug_Freshness_Score_Nbr),'-1') <> NVL(tgt.Dug_Freshness_Score_Nbr,'-1')
									 OR NVL(to_number(src.Dug_Order_Accuracy_Score_Nbr),'-1') <>NVL(tgt.Dug_Order_Accuracy_Score_Nbr,'-1')
									 OR NVL(to_number(src.Dug_Associate_Friendliness_Score_Nbr),'-1') <> NVL(tgt.Dug_Associate_Friendliness_Score_Nbr,'-1')
									 OR NVL(to_boolean(src.Dug_Give_More_Feedback_Ind),-1) <> NVL(tgt.Dug_Give_More_Feedback_Ind,-1)
									 OR NVL(to_number(src.Dug_Substitution_Satisfaction_Score_Nbr),'-1') <> NVL(tgt.Dug_Substitution_Satisfaction_Score_Nbr,'-1')
									 OR NVL(src.Dug_Substitution_Dissatisfaction_Reason_Txt ,'-1') <> NVL(tgt.Dug_Substitution_Dissatisfaction_Reason_Txt ,'-1')
									 OR NVL(src.Dug_Substitution_Dissatisfaction_Reason_Other_Txt,'-1') <> NVL(tgt.Dug_Substitution_Dissatisfaction_Reason_Other_Txt,'-1')  
									 OR NVL(to_number(src.Delivery_Score_Nbr),'-1') <> NVL(tgt.Delivery_Score_Nbr,'-1')
									 OR NVL(src.Delivery_Reason_For_Score_Comment_Txt,'-1') <> NVL(tgt.Delivery_Reason_For_Score_Comment_Txt,'-1')
									 OR NVL(to_number(src.Delivery_Item_Availability_Score_Nbr),'-1') <> NVL(tgt.Delivery_Item_Availability_Score_Nbr,'-1')
									 OR NVL(to_number(src.Delivery_Safety_Precautions_Taken_Score_Nbr),'-1') <>NVL(tgt.Delivery_Safety_Precautions_Taken_Score_Nbr,'-1')
									 OR NVL(to_number(src.Delivery_Order_Timeliness_Score_Nbr),'-1') <> NVL(tgt.Delivery_Order_Timeliness_Score_Nbr,'-1')
									 OR NVL(to_number(src.Delivery_Order_Process_Ease_Score_Nbr),'-1') <> NVL(tgt.Delivery_Order_Process_Ease_Score_Nbr,'-1')
									 OR NVL(to_number(src.Delivery_Quality_Score_Nbr),'-1') <> NVL(tgt.Delivery_Quality_Score_Nbr,'-1')
									 OR NVL(to_number(src.Delivery_Freshness_Score_Nbr),'-1') <> NVL(tgt.Delivery_Freshness_Score_Nbr ,'-1')
									 OR NVL(to_number(src.Delivery_Order_Accuracy_Score_Nbr),'-1') <> NVL(tgt.Delivery_Order_Accuracy_Score_Nbr,'-1')  
									 OR NVL(to_number(src.Delivery_Associate_Friendliness_Score_Nbr),'-1') <> NVL(tgt.Delivery_Associate_Friendliness_Score_Nbr,'-1')
									 OR NVL(to_boolean(src.Delivery_Give_More_Feedback_Ind),-1) <> NVL(tgt.Delivery_Give_More_Feedback_Ind,-1)
									 OR NVL(to_number(src.Delivery_Substitution_Satisfaction_Score_Nbr),'-1') <> NVL(tgt.Delivery_Substitution_Satisfaction_Score_Nbr,'-1')
									 OR NVL(src.Delivery_Substitution_Dissatisfaction_Reason_Txt,'-1') <>NVL(tgt.Delivery_Substitution_Dissatisfaction_Reason_Txt,'-1')
									 OR NVL(src.Delivery_Substitution_Dissatisfaction_Reason_Other_Txt,'-1') <> NVL(tgt.Delivery_Substitution_Dissatisfaction_Reason_Other_Txt,'-1')
									 OR NVL(src.Banner_Nm,'-1') <> NVL(tgt.Banner_Nm,'-1')
									 OR NVL(src.Order_Nbr,'-1') <> NVL(tgt.Order_Nbr,'-1')
									 OR NVL(src.Retail_Store_Id ,'-1') <> NVL(tgt.Retail_Store_Id ,'-1')
									 OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND  
									 )`;   

try {
snowflake.execute ({ sqlText: create_tgt_wrk_table });
	}
    catch (err) {

snowflake.execute ({ sqlText: sql_ins_rerun_tbl});


       throw `Creation of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
				}   
				
// Transaction for Insert begins           
    var sql_begin = "BEGIN"	


// SCD Type1 and 2 - Processing updates
var sql_updates = `UPDATE ${tgt_tbl} as tgt
					SET 
					DW_Last_Effective_dt = CURRENT_DATE - 1,
					DW_CURRENT_VERSION_IND = FALSE,
					DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
					DW_SOURCE_UPDATE_NM = FILENAME
				
					FROM ( 
							SELECT 
								 Survey_Id
								,FILENAME
							FROM ${tgt_wrk_tbl}
							WHERE 
								DML_Type = 'U' 
							// AND Sameday_chg_ind = 0
					) src
					WHERE
						nvl(src.Survey_Id,'-1') = nvl(tgt.Survey_Id,'-1')				
					AND tgt.DW_CURRENT_VERSION_IND = TRUE`;

	
            
 // Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl} 
					(
					 Survey_Id             
					 ,Response_ts             
					 ,Digital_Survey_Nm       
					 ,Digital_Unit_Nm        
					 ,Region_Nm 
					 ,City_Nm                 
					 ,Device_Vendor_Nm        
					 ,Device_Model_Nm        
					 ,Device_Screen_Resolution_Txt    
					 ,Device_Os_Nm            
					 ,Device_Os_Version_Txt    
					 ,Retail_Customer_Uuid    
					 ,Trigger_Entity_Type_Txt    
					 ,Mobile_App_Version_Txt    
					 ,Mobile_App_Id           
					 ,Mobile_Device_Id        
					 ,Dug_Score_Nbr           
					 ,Dug_Reason_For_Score_Comment_Txt    
					 ,Dug_Item_Availability_Score_Nbr    
					 ,Dug_Safety_Precautions_Taken_Score_Nbr    
					 ,Dug_Order_Timeliness_Score_Nbr    
					 ,Dug_Order_Process_Ease_Score_Nbr    
					 ,Dug_Quality_Score_Nbr    
					 ,Dug_Freshness_Score_Nbr   
					 ,Dug_Order_Accuracy_Score_Nbr    
					 ,Dug_Associate_Friendliness_Score_Nbr    
					 ,Dug_Give_More_Feedback_Ind    
					 ,Dug_Substitution_Satisfaction_Score_Nbr    
					 ,Dug_Substitution_Dissatisfaction_Reason_Txt    
					 ,Dug_Substitution_Dissatisfaction_Reason_Other_Txt    
					 ,Delivery_Score_Nbr      
					 ,Delivery_Reason_For_Score_Comment_Txt    
					 ,Delivery_Item_Availability_Score_Nbr    
					 ,Delivery_Safety_Precautions_Taken_Score_Nbr    
					 ,Delivery_Order_Timeliness_Score_Nbr    
					 ,Delivery_Order_Process_Ease_Score_Nbr    
					 ,Delivery_Quality_Score_Nbr    
					 ,Delivery_Freshness_Score_Nbr    
					 ,Delivery_Order_Accuracy_Score_Nbr    
					 ,Delivery_Associate_Friendliness_Score_Nbr    
					 ,Delivery_Give_More_Feedback_Ind   
					 ,Delivery_Substitution_Satisfaction_Score_Nbr    
					 ,Delivery_Substitution_Dissatisfaction_Reason_Txt    
					 ,Delivery_Substitution_Dissatisfaction_Reason_Other_Txt    
					 ,Banner_Nm               
					 ,Order_Nbr               
					 ,Retail_Store_Id 
					 ,DW_CREATE_TS
					 ,DW_LOGICAL_DELETE_IND
					 ,DW_SOURCE_CREATE_NM
					 ,DW_CURRENT_VERSION_IND
					 ,Dw_First_Effective_Dt  
					 ,Dw_Last_Effective_Dt  
					)
					SELECT
					 Survey_Id             
					 ,Response_ts             
					 ,Digital_Survey_Nm       
					 ,Digital_Unit_Nm        
					 ,Region_Nm 
					 ,City_Nm                 
					 ,Device_Vendor_Nm        
					 ,Device_Model_Nm        
					 ,Device_Screen_Resolution_Txt    
					 ,Device_Os_Nm            
					 ,Device_Os_Version_Txt    
					 ,Retail_Customer_Uuid    
					 ,Trigger_Entity_Type_Txt    
					 ,Mobile_App_Version_Txt    
					 ,Mobile_App_Id           
					 ,Mobile_Device_Id        
					 ,Dug_Score_Nbr           
					 ,Dug_Reason_For_Score_Comment_Txt    
					 ,Dug_Item_Availability_Score_Nbr    
					 ,Dug_Safety_Precautions_Taken_Score_Nbr    
					 ,Dug_Order_Timeliness_Score_Nbr    
					 ,Dug_Order_Process_Ease_Score_Nbr    
					 ,Dug_Quality_Score_Nbr    
					 ,Dug_Freshness_Score_Nbr   
					 ,Dug_Order_Accuracy_Score_Nbr    
					 ,Dug_Associate_Friendliness_Score_Nbr    
					 ,Dug_Give_More_Feedback_Ind    
					 ,Dug_Substitution_Satisfaction_Score_Nbr    
					 ,Dug_Substitution_Dissatisfaction_Reason_Txt    
					 ,Dug_Substitution_Dissatisfaction_Reason_Other_Txt    
					 ,Delivery_Score_Nbr      
					 ,Delivery_Reason_For_Score_Comment_Txt    
					 ,Delivery_Item_Availability_Score_Nbr    
					 ,Delivery_Safety_Precautions_Taken_Score_Nbr    
					 ,Delivery_Order_Timeliness_Score_Nbr    
					 ,Delivery_Order_Process_Ease_Score_Nbr    
					 ,Delivery_Quality_Score_Nbr    
					 ,Delivery_Freshness_Score_Nbr    
					 ,Delivery_Order_Accuracy_Score_Nbr    
					 ,Delivery_Associate_Friendliness_Score_Nbr    
					 ,Delivery_Give_More_Feedback_Ind    
					 ,Delivery_Substitution_Satisfaction_Score_Nbr    
					 ,Delivery_Substitution_Dissatisfaction_Reason_Txt    
					 ,Delivery_Substitution_Dissatisfaction_Reason_Other_Txt    
					 ,Banner_Nm               
					 ,Order_Nbr               
					 ,Retail_Store_Id 
					 ,CURRENT_TIMESTAMP
					 ,DW_LOGICAL_DELETE_IND
					 ,filename
					 ,TRUE 
					 ,CURRENT_DATE
					 ,'31-DEC-9999'
					FROM ${tgt_wrk_tbl}
					WHERE 
					//  Sameday_chg_ind = 0 AND
					Survey_Id IS NOT NULL`;

    
var sql_commit = "COMMIT";
var sql_rollback = "ROLLBACK";
    
try {
        snowflake.execute ({ sqlText: sql_begin });
		snowflake.execute({ sqlText: sql_empty_rerun_tbl });
        snowflake.execute ({ sqlText: sql_updates });
        snowflake.execute ({ sqlText: sql_inserts });
        snowflake.execute ({ sqlText: sql_commit });
	} catch (err) {
        snowflake.execute ({ sqlText: sql_rollback });
		
		snowflake.execute ({ sqlText: sql_ins_rerun_tbl});
		
		throw `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
					}		
                               
                // **************        Load for Net_Promoter_Score_Uma_Survey Table ENDs *****************
				

$$;

--liquibase formatted sql
--changeset SYSTEM:NET_PROMOTER_SCORE_UMA_SURVEY runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view NET_PROMOTER_SCORE_UMA_SURVEY(
	SURVEY_ID COMMENT 'Unique Identifier of the Survey from Medallia',
	RESPONSE_TS COMMENT 'Timestamp of the Survey response received',
	DIGITAL_SURVEY_NM COMMENT 'Name of the survey being taken. Example Dug/Delivery',
	DIGITAL_UNIT_NM COMMENT 'Digital Unit for which the Survey is being taken',
	REGION_NM COMMENT 'Region where the survey is being taken',
	CITY_NM COMMENT 'City where the survey is being taken',
	DEVICE_VENDOR_NM COMMENT 'Vendor name of the Device',
	DEVICE_MODEL_NM COMMENT 'Model Name of the Device',
	DEVICE_SCREEN_RESOLUTION_TXT COMMENT 'Screen resolution of the Device used for survey',
	DEVICE_OS_NM COMMENT 'Operating System name of the Device used for survey',
	DEVICE_OS_VERSION_TXT COMMENT 'Operating System version of the Device used for Survey',
	RETAIL_CUSTOMER_UUID COMMENT 'Unique Identifier of the Customer',
	TRIGGER_ENTITY_TYPE_TXT COMMENT 'Survey Trigger type',
	MOBILE_APP_VERSION_TXT COMMENT 'Version of the Application',
	MOBILE_APP_ID COMMENT 'Identifier of the Mobile Application',
	MOBILE_DEVICE_ID COMMENT 'Identifier of the Mobile Device',
	DUG_SCORE_NBR COMMENT 'Net Promoter Score for DUG given by the customer',
	DUG_REASON_FOR_SCORE_COMMENT_TXT COMMENT 'Reason provided by customer for the score given',
	DUG_ITEM_AVAILABILITY_SCORE_NBR COMMENT 'Score for Item Availability',
	DUG_SAFETY_PRECAUTIONS_TAKEN_SCORE_NBR COMMENT 'Score for Safety Precautions',
	DUG_ORDER_TIMELINESS_SCORE_NBR COMMENT 'Order TImeliness Score',
	DUG_ORDER_PROCESS_EASE_SCORE_NBR COMMENT 'Order Process Ease Score',
	DUG_QUALITY_SCORE_NBR COMMENT 'Item Quality Score',
	DUG_FRESHNESS_SCORE_NBR COMMENT 'Item Freshness Score',
	DUG_ORDER_ACCURACY_SCORE_NBR COMMENT 'Order Accuracy Score',
	DUG_ASSOCIATE_FRIENDLINESS_SCORE_NBR COMMENT 'Assosciate Friendliness Score',
	DUG_GIVE_MORE_FEEDBACK_IND COMMENT 'Indicator if the customer is willing to provide more feedback',
	DUG_SUBSTITUTION_SATISFACTION_SCORE_NBR COMMENT 'Rating of satisfication with a subsitution if their order had one',
	DUG_SUBSTITUTION_DISSATISFACTION_REASON_TXT COMMENT 'Reason for dissatisfaction with subsitution',
	DUG_SUBSTITUTION_DISSATISFACTION_REASON_OTHER_TXT COMMENT 'Reason for dissatisfaction with subsitution (other open end)',
	DELIVERY_SCORE_NBR COMMENT 'Net Promoter Score for Delivery',
	DELIVERY_REASON_FOR_SCORE_COMMENT_TXT COMMENT 'Reason provided by customer for the score given',
	DELIVERY_ITEM_AVAILABILITY_SCORE_NBR COMMENT 'Score for Item Availability',
	DELIVERY_SAFETY_PRECAUTIONS_TAKEN_SCORE_NBR COMMENT 'Score for Safety Precautions',
	DELIVERY_ORDER_TIMELINESS_SCORE_NBR COMMENT 'Order Timeliness Score',
	DELIVERY_ORDER_PROCESS_EASE_SCORE_NBR COMMENT 'Order Process Ease Score',
	DELIVERY_QUALITY_SCORE_NBR COMMENT 'Item Quality Score',
	DELIVERY_FRESHNESS_SCORE_NBR COMMENT 'Freshness Score',
	DELIVERY_ORDER_ACCURACY_SCORE_NBR COMMENT 'Order Accuracy Score',
	DELIVERY_ASSOCIATE_FRIENDLINESS_SCORE_NBR COMMENT 'Assosciate Friendliness Score',
	DELIVERY_GIVE_MORE_FEEDBACK_IND COMMENT 'Indicator if the customer is willing to provide more feedback',
	DELIVERY_SUBSTITUTION_SATISFACTION_SCORE_NBR COMMENT 'Rating of satisfication with a subsitution if their order had one',
	DELIVERY_SUBSTITUTION_DISSATISFACTION_REASON_TXT COMMENT 'Reason for dissatisfaction with subsitution',
	DELIVERY_SUBSTITUTION_DISSATISFACTION_REASON_OTHER_TXT COMMENT 'Reason for dissatisfaction with subsitution (other open end)',
	BANNER_NM COMMENT 'Customer parameter to include banner with survey data',
	ORDER_NBR COMMENT 'Customer parameter to include Fulfillment Order Number with survey data',
	RETAIL_STORE_ID COMMENT 'Customer parameter to include store # with survey data',
	DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d',
	DW_FIRST_EFFECTIVE_DT COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day'
) COMMENT='VIEW for Net_Promoter_Score_Uma_Survey'
 as
select

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
 ,Dw_Create_Ts            
 ,Dw_Last_Update_Ts       
 ,Dw_Logical_Delete_Ind   
 ,Dw_Source_Create_Nm    
 ,Dw_Source_Update_Nm    
 ,Dw_Current_Version_Ind    
 ,Dw_First_Effective_Dt   
 ,Dw_Last_Effective_Dt
 
 from EDM_CONFIRMED_PRD.DW_C_LOYALTY.Net_Promoter_Score_Uma_Survey;

--liquibase formatted sql
--changeset SYSTEM:SP_GETOFFEROMS_TO_BIM_LOAD_OMS_OFFER runOnChange:true splitStatements:false OBJECT_TYPE:SP

USE DATABASE <<EDM_DB_NAME>>;
USE SCHEMA DW_APPL;

CREATE OR REPLACE PROCEDURE SP_GETOFFEROMS_TO_BIM_LOAD_OMS_OFFER("SRC_WRK_TBL" VARCHAR(16777216), "CNF_DB" VARCHAR(16777216), "C_PRODUCT" VARCHAR(16777216), "C_STAGE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS 
$$
	
	var src_wrk_tbl = SRC_WRK_TBL;
	var cnf_schema = C_PRODUCT;
	var ref_schema = "DW_R_PRODUCT";
	var ref_db = "<<EDM_DB_NAME_R>>";
	var tgt_wrk_tbl = `${CNF_DB}.${C_STAGE}.OMS_Offer_wrk`;
    var tgt_tbl = `${CNF_DB}.${cnf_schema}.OMS_Offer`;
	var flat_tbl = `${ref_db}.${ref_schema}.OFFEROMS_FLAT`;

// ************** Load for OMS_Offer table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.

var sql_truncate_wrk_tbl = `Truncate table  ${tgt_wrk_tbl}`;

var sql_command = `INSERT INTO ${tgt_wrk_tbl} 
                            WITH src_wrk_tbl_recs as 
                            (					
							with cte1
							as (SELECT DISTINCT 
                             FLT.payload_id as OMS_Offer_Id
                            ,FLT.payload_externalOfferId as External_Offer_Id
                            ,FLT.payload_offerRequestId as Offer_Request_Id
                            ,FLT.payload_aggregatorOfferId as Aggregator_Offer_Id
                            ,FLT.payload_manufacturerId as Manufacturer_Id
                            ,FLT.payload_manufacturerOfferRefCd as Manufacturer_Offer_Reference_Cd
                            ,FLT.payload_providerName as Provider_Nm
                            ,FLT.payload_programCode as Program_Cd
                            ,FLT.payload_programCodeDesc as Program_Code_Dsc
                            ,FLT.payload_subProgram as Subprogram_Nm
                            ,FLT.payload_subProgramDesc as Subprogram_Dsc
                            ,FLT.payload_deliveryChannel as Delivery_Channel_Cd
                            ,FLT.payload_deliveryChannelDesc as Delivery_Channel_Dsc
                            ,FLT.payload_status as Offer_Status_Cd
                            ,FLT.payload_statusDesc as Offer_Status_Dsc
                            ,FLT.payload_priceTitle as Price_Title_Txt
                            ,FLT.payload_priceValue as Price_Value_Txt
                            ,FLT.payload_savingsValueText as Savings_Value_Txt
                            ,FLT.payload_titleDescription_additionalProperties as Title_Dsc
							,FLT.PAYLOAD_TITLE_DESCTITLEDSC1 as Title_Dsc1
							,FLT.PAYLOAD_TITLE_DESCTITLEDSC2 as Title_Dsc2
							,FLT.PAYLOAD_TITLE_DESCTITLEDSC3 as Title_Dsc3
                            ,FLT.payload_productDescription_additionalProperties as Product_Dsc
							,FLT.PAYLOAD_PRODUCT_DESCPRODDSC1 as Product_Dsc1
							,FLT.PAYLOAD_PRODUCT_DESCPRODDSC2 as Product_Dsc2
							,FLT.PAYLOAD_PRODUCT_DESCPRODDSC3 as Product_Dsc3
                            ,FLT.payload_disclaimerText as Disclaimer_Txt
                            ,FLT.payload_description as Description_Txt
                            ,FLT.payload_printTags::boolean as Print_Tags_Ind
                            ,FLT.payload_productImageId as Product_Image_Id
                            ,FLT.payload_priceCode as Price_Cd
                            ,FLT.payload_time as Time_Txt
                            ,FLT.payload_year as Year_Txt
                            ,FLT.payload_productCd as Product_Cd
                            ,FLT.payload_isEmployeeOffer::boolean as Is_Employee_Offer_Ind
                            ,FLT.payload_isDefaultAllocationOffer::boolean as Is_Default_Allocation_Offer_Ind
                            ,FLT.payload_programType as Program_Type_Cd
                            ,FLT.payload_shouldReportRedemptions as Should_Report_Redeptions_Ind
                            ,FLT.payload_createdTs::Timestamp as Created_Ts
                            ,FLT.payload_createdApplicationId as Created_Application_Id
                            ,FLT.payload_createdUserId as Created_User_Id
                            ,FLT.payload_lastUpdatedApplicationId as Last_Updated_Application_Id
                            ,FLT.payload_lastUpdatedUserId as Last_Updated_User_Id
                            ,FLT.payload_lastUpdatedTs::Timestamp as Last_Updated_Ts
                            ,date(FLT.payload_displayEffectiveStartDate) as Display_Effective_Start_Dt
                            ,date(FLT.payload_displayEffectiveEndDate) as Display_Effective_End_Dt
                            ,date(FLT.payload_effectiveStartDate) as Effective_Start_Dt
                            ,date(FLT.payload_effectiveEndDate) as Effective_End_Dt
                            ,date(FLT.payload_testEffectiveStartDate) as Test_Effective_Start_Dt
                            ,date(FLT.payload_testEffectiveEndDate) as Test_Effective_End_Dt
                            ,FLT.payload_qualificationUnitType as Qualification_Unit_Type_Dsc
                            ,FLT.payload_qualificationUnitSubType as Qualification_Unite_Subtype_Dsc
                            ,FLT.payload_benefitValueType as Beneifit_Value_Type_Dsc
                            ,FLT.payload_usageLimitTypePerUser as Usage_Limit_Type_Per_User_Dsc
                            ,FLT.payload_pluTriggerBarcode as PLU_Trigger_Barcode_Txt
                            ,FLT.payload_copientCategory as Copient_Category_Dsc
                            ,FLT.payload_engine as Engine_Dsc
                            ,FLT.payload_priority as Priority_Cd
                            ,FLT.payload_tiers as Tiers_Cd
                            ,FLT.payload_sendOutboundData as Send_Outbound_Data_Dsc
                            ,FLT.payload_chargebackVendor as Chargeback_Vendor_Nm
                            ,FLT.payload_autoTransferable::boolean as Auto_Transferable_Ind
                            ,FLT.payload_enableIssuance::boolean as Enable_Issuance_Ind
                            ,FLT.payload_deferEvaluationUntilEOS::boolean as Defer_Evaluation_Until_EOS_Ind
                            ,FLT.payload_enableImpressionReporting::boolean as Enable_Impression_Reporting_Ind
                            ,FLT.payload_limitEligibilityFrequency as Limit_Eligibility_Frequency_Txt
                            ,FLT.payload_isApplicableToJ4U::boolean as Is_Appliable_To_J4U_Ind
                            ,FLT.payload_customerSegment as Customer_Segment_Dsc
                            ,FLT.payload_assignment_userId  as Assignment_User_Id
                            ,FLT.payload_assignment_firstName  as Assignment_First_Nm
                            ,FLT.payload_assignment_lastName as Assignment_Last_Nm
                            ,FLT.payload_qualificationProductDisQualifier as Qualification_Product_Disqualifier_Txt
                            ,FLT.payload_qualificationDay_monday::boolean  as Qualification_Day_Monday_Ind  
                            ,FLT.payload_qualificationDay_tuesday::boolean  as Qualification_Day_Tuesday_Ind  
                            ,FLT.payload_qualificationDay_wednesday::boolean  as Qualification_Day_Wednesday_Ind
                            ,FLT.payload_qualificationDay_thursday::boolean  as Qualification_Day_Thursday_Ind 
                            ,FLT.payload_qualificationDay_friday::boolean  as Qualification_Day_Friday_Ind           
							,FLT.payload_qualificationDay_saturday::boolean  as Qualification_Day_Saturday_Ind 
							,FLT.payload_qualificationDay_sunday::boolean  as Qualification_Day_Sunday_Ind      
							,FLT.payload_qualificationTime_start  as Qualification_Start_Time_Txt           
							,FLT.payload_qualificationTime_end as Qualification_End_Time_Txt               
							,FLT.payload_qualificationEnterpriseInstantWin_numberOfPrizes as Qualification_Enterprise_Instant_Win_Number_Of_Prizes_Qty
							,FLT.payload_qualificationEnterpriseInstantWin_frequency as Qualification_Enterprise_Instant_Win_Frequency_Txt
							,FLT.payload_offerName as Offer_Nm
							,FLT.payload_adType as Ad_Type_Cd
							,FLT.payload_offerProtoType as Offer_Prototype_Cd
							,FLT.payload_offerPrototypeDesc as Offer_Prototype_Dsc
							,FLT.payload_storeGroupVersionId as Store_Group_Version_Id
							,FLT.payload_storeTag_printJ4uTagEnabled::boolean as Store_Tag_Print_J4U_Tag_Enabled_Ind
							,FLT.payload_storeTag_multiple as Store_Tag_Multiple_Nbr
							,FLT.payload_storeTag_amount as Store_Tag_Amt
							,FLT.payload_storeTag_comments as Store_Tag_Comments_Txt
							,FLT.payload_requestedRemovalForAll::boolean as Requested_Removal_For_All_Ind
							,FLT.payload_removedOn::Timestamp as Removed_On_Ts
							,FLT.payload_removedUnclippedOn::Timestamp as Removed_Unclipped_On_Ts
							,FLT.payload_removedForAllOn::Timestamp as Removal_For_All_On_Ts
							,FLT.payload_brandNSize as Brand_Size_Dsc
							,FLT.payload_createdUser_userId as Created_User_User_Id
							,FLT.payload_createdUser_firstName as Created_User_First_Nm
							,FLT.payload_createdUser_lastName as Created_User_Last_Nm
							,FLT.payload_updatedUser_userId as Updated_User_User_Id
							,FLT.payload_updatedUser_firstName as Updated_User_First_Nm
							,FLT.payload_updatedUser_lastName as Updated_User_Last_Nm
							,FLT.payload_firstUpdateToRedemptionEngine::Timestamp as First_Update_To_Redemption_Engine_Ts
							,FLT.payload_lastUpdateToRedemptionEngine::Timestamp as Last_Update_To_Redemption_Engine_Ts
							,FLT.payload_firstUpdateToJ4U::Timestamp as First_Update_To_J4U_Ts
							,FLT.payload_lastUpdateToJ4U::Timestamp as Last_Update_To_J4U_Ts
							,FLT.payload_offerRequestorGroup as Offer_Requestor_Group_Cd
							,FLT.payload_headLine as Headline_Txt
							,FLT.payload_isPODApproved::boolean as Is_POD_Approved_Ind
							,FLT.payload_podUsageLimitTypePerUser as POD_Usage_Limit_Type_Per_User_Dsc
							,FLT.payload_podReferenceOfferId as POD_Reference_Offer_Id
							,FLT.payload_ivieImageId as IVIE_Image_Id
							,FLT.payload_vehicleNm as Vehicle_Name_Txt
							,FLT.payload_adPageNbr as Ad_Page_Number_Txt
							,FLT.payload_adModNbr as Ad_Mod_Nbr
							,FLT.payload_ecomDesc as ECom_Dsc
							,FLT.payload_requestedUser_userId as Requested_User_User_Id
							,FLT.payload_requestedUser_firstName as Requested_User_First_Nm
							,FLT.payload_requestedUser_lastName as Requested_User_Last_Nm
							,FLT.payload_isPrimaryPODOffer::boolean as Is_Primary_POD_Offer_Ind
							,FLT.payload_inEmail as In_Email_Ind
							,date (FLT.payload_submittedDate) as Submitted_Dt
							,FLT.payload_redemptionSystemId as Redemption_System_Id
							,FLT.payload_adBugText as Adbug_Txt
							,FLT.lastUpdateTS
							,FLT.Filename
							,FLT.payload_allocationCode as Allocation_Cd
							,FLT.payload_allocationCodeName as Allocation_Nm
							,FLT.payload_benefit_printedMessage_isApplicableForNotifications as Printed_Message_Notification_Ind
							,FLT.payload_benefit_cashierMessage_isApplicableForNotifications as Cashier_Message_Notification_Ind
							,FLT.payload_categories_additionalProperties as payload_categories_additionalProperties
							,FLT.payload_primaryCategory_additionalProperties as payload_primaryCategory_additionalProperties
							,FLT.payload_usageLimitPerUser as CUSTOM_OFFER_LIMIT_NBR
							,FLT.payload_customPeriod as CUSTOM_PERIOD_NBR
							,FLT.payload_customType as CUSTOM_TYPE_DSC
							,FLT.QUALIFICATIONPRODUCTDISQUALIFIERNAME as QUALIFICATION_PRODUCT_DISQUALIFIER_NM
							,FLT.ISDISPLAYIMMEDIATE as IS_DISPLAY_IMMEDIATE_IND
							,FLT.payload_ecommPromoCode as Ecomm_Promo_Type_Cd
							,FLT.payload_order as Promotion_Order_Nbr
							,FLT.PAYLOAD_HEADLINE2 as Headline2_Txt
							,FLT.PAYLOAD_usageLimitPerOffer as Usage_Limit_Per_Offer_Cnt
							,FLT.payload_refundableRewards as REFUNDABLE_REWARDS_IND
							,FLT.payload_multiClipLimit as Multi_Clip_Limit_Cnt
							,FLT.payload_points as payload_points
							,FLT.payload_programSubType as payload_programSubType
							,FLT.payload_ecommPromoType AS Ecomm_Promo_Type_Nm
							,FLT.payload_autoApplyPromoCode::BOOLEAN AS Auto_Apply_Promo_Ind
							,FLT.payload_validWithOtherOffer::BOOLEAN AS Valid_With_Other_Offers_Ind
							,FLT.payload_orderCount AS Offer_Eligible_Order_Cnt
							,FLT.payload_firstTimeCustomerOnly::BOOLEAN AS Valid_For_First_Time_Customer_Ind
							,case when FLT.payload_land = '' then NULL else FLT.payload_land end AS Merkle_Game_Land_Nm
							,case when FLT.payload_space = '' then NULL else FLT.payload_space end AS Merkle_Game_Land_Space_Nm
							,case when FLT.payload_slot = '' then NULL else FLT.payload_slot end AS Merkle_Game_Land_Space_Slot_Nm
							,FLT.payload_subProgramCode AS Promotion_Subprogram_Type_Cd
							,FLT.PAYLOAD_QUALIFICATIONBEHAVIOR AS Offer_Qualification_Behavior_Cd
							,FLT.payload_initialSubscriptionOffer::BOOLEAN as Initial_Subscription_Offer_Ind
							,FLT.payload_isDynamicOffer::BOOLEAN as Dynamic_Offer_Ind
							,FLT.payload_DaysToRedeem as Days_To_Redeem_Offer_Cnt
							,FLT.payload_ad_clippable::BOOLEAN as Offer_Clippable_Ind
							,FLT.payload_ad_applicableOnline::BOOLEAN as Offer_Applicable_Online_Ind
							,FLT.payload_ad_displayable::BOOLEAN as Offer_Displayable_Ind
                          FROM ${src_wrk_tbl} SRC
						    


						  INNER JOIN ${flat_tbl} FLT ON SRC.payload_id = FLT.payload_id
						  )
						  , cte2 as(
						  Select distinct OMS_OFF_ID,	FILENM			
								,listagg(distinct Cat_Txt, ', ') within group (order by Cat_Txt ) as Categories_Txt
								, listagg(distinct Pri_Category_Txt,', ' ) within group (order by Pri_Category_Txt ) as Primary_Category_Txt
							
								
								
							  from  (
						  SELECT DISTINCT OMS_Offer_Id as OMS_OFF_ID, FILENAME as FILENM,
						  CAT_DATA.VALUE as Cat_Txt,
						  pricat_data.value as Pri_Category_Txt
						  
						  
							FROM cte1
							,LATERAL FLATTEN(input => payload_categories_additionalProperties, outer => TRUE ) as cat_data
							,LATERAL FLATTEN(input => payload_primaryCategory_additionalProperties, outer => TRUE ) as pricat_data
							
							)z
							group by OMS_OFF_ID,	FILENM )						
							
							SELECT DISTINCT 
                             cte1.OMS_Offer_Id
                            ,cte1.External_Offer_Id
                            ,cte1.Offer_Request_Id
                            ,cte1.Aggregator_Offer_Id
                            ,cte1.Manufacturer_Id
                            ,cte1.Manufacturer_Offer_Reference_Cd                                                  
                            ,cte1.Provider_Nm
							,cte2.Categories_Txt
                            ,cte2.Primary_Category_Txt
                            ,cte1.Program_Cd                                
                            ,cte1.Program_Code_Dsc         
                            ,cte1.Subprogram_Nm              
                            ,cte1.Subprogram_Dsc                               
                            ,cte1.Delivery_Channel_Cd                        
                            ,cte1.Delivery_Channel_Dsc
                            ,cte1.Offer_Status_Cd                                 
                            ,cte1.Offer_Status_Dsc                               
                            ,cte1.Price_Title_Txt
							,cte1.Price_Value_Txt
							,cte1.Savings_Value_Txt                            
                            ,cte1.Title_Dsc
							,cte1.Title_Dsc1
							,cte1.Title_Dsc2
							,cte1.Title_Dsc3
                            ,cte1.Product_Dsc
							,cte1.Product_Dsc1
							,cte1.Product_Dsc2
							,cte1.Product_Dsc3
                            ,cte1.Disclaimer_Txt                                  
                            ,cte1.Description_Txt                                                  
                            ,cte1.Print_Tags_Ind                                                   
                            ,cte1.Product_Image_Id                                                            
                            ,cte1.Price_Cd                                                                                                
                            ,cte1.Time_Txt                                                 
                            ,cte1.Year_Txt                                                 
                            ,cte1.Product_Cd                                                        
                            ,cte1.Is_Employee_Offer_Ind                                         
                            ,cte1.Is_Default_Allocation_Offer_Ind                                          
                            ,cte1.Program_Type_Cd                                                                           
                            ,cte1.Should_Report_Redeptions_Ind
                            ,cte1.Created_Ts                                                                           
                            ,cte1.Created_Application_Id                                                             
                            ,cte1.Created_User_Id                                                                                  
                            ,cte1.Last_Updated_Application_Id
                            ,cte1.Last_Updated_User_Id                                    
                            ,cte1.Last_Updated_Ts                                                       
                            ,cte1.Display_Effective_Start_Dt
                            ,cte1.Display_Effective_End_Dt                                            
                            ,cte1.Effective_Start_Dt                                                                                  
                            ,cte1.Effective_End_Dt                                                                       
                            ,cte1.Test_Effective_Start_Dt
                            ,cte1.Test_Effective_End_Dt                        
                            ,cte1.Qualification_Unit_Type_Dsc                                              
                            ,cte1.Qualification_Unite_Subtype_Dsc                                                                         
                            ,cte1.Beneifit_Value_Type_Dsc
							,cte1.Usage_Limit_Type_Per_User_Dsc
                            ,cte1.PLU_Trigger_Barcode_Txt                
                            ,cte1.Copient_Category_Dsc
							,cte1.Engine_Dsc                                               
                            ,cte1.Priority_Cd                                              
                            ,cte1.Tiers_Cd
							,cte1.Send_Outbound_Data_Dsc             
                            ,cte1.Chargeback_Vendor_Nm                                                       
                            ,cte1.Auto_Transferable_Ind                                                                  
                            ,cte1.Enable_Issuance_Ind                         
                            ,cte1.Defer_Evaluation_Until_EOS_Ind
                            ,cte1.Enable_Impression_Reporting_Ind                   
                            ,cte1.Limit_Eligibility_Frequency_Txt
                            ,cte1.Is_Appliable_To_J4U_Ind                                                  
                            ,cte1.Customer_Segment_Dsc
                            ,cte1.Assignment_User_Id                                                                             
                            ,cte1.Assignment_First_Nm                           
                            ,cte1.Assignment_Last_Nm                                                            
                            ,cte1.Qualification_Product_Disqualifier_Txt                                         
                            ,cte1.Qualification_Day_Monday_Ind           
                            ,cte1.Qualification_Day_Tuesday_Ind
                            ,cte1.Qualification_Day_Wednesday_Ind                              
                            ,cte1.Qualification_Day_Thursday_Ind                  
                            ,cte1.Qualification_Day_Friday_Ind                                                 
                            ,cte1.Qualification_Day_Saturday_Ind
                            ,cte1.Qualification_Day_Sunday_Ind       
                            ,cte1.Qualification_Start_Time_Txt
                            ,cte1.Qualification_End_Time_Txt
                            ,cte1.Qualification_Enterprise_Instant_Win_Number_Of_Prizes_Qty  
                            ,cte1.Qualification_Enterprise_Instant_Win_Frequency_Txt                  
                            ,cte1.Offer_Nm                                                 
                            ,cte1.Ad_Type_Cd                                               
                            ,cte1.Offer_Prototype_Cd                           
                            ,cte1.Offer_Prototype_Dsc                         
                            ,cte1.Store_Group_Version_Id                  
                            ,cte1.Store_Tag_Print_J4U_Tag_Enabled_Ind
                            ,cte1.Store_Tag_Multiple_Nbr                   
                            ,cte1.Store_Tag_Amt                                   
                            ,cte1.Store_Tag_Comments_Txt                
                            ,cte1.Requested_Removal_For_All_Ind   
                            ,cte1.Removed_On_Ts                                 
                            ,cte1.Removed_Unclipped_On_Ts            
                            ,cte1.Removal_For_All_On_Ts                   
                            ,cte1.Brand_Size_Dsc
                            ,cte1.Created_User_User_Id                                    
                            ,cte1.Created_User_First_Nm                                  
                            ,cte1.Created_User_Last_Nm                                  
                            ,cte1.Updated_User_User_Id                                   
                            ,cte1.Updated_User_First_Nm                                
                            ,cte1.Updated_User_Last_Nm                                 
                            ,cte1.First_Update_To_Redemption_Engine_Ts
                            ,cte1.Last_Update_To_Redemption_Engine_Ts
                            ,cte1.First_Update_To_J4U_Ts                  
                            ,cte1.Last_Update_To_J4U_Ts                   
                            ,cte1.Offer_Requestor_Group_Cd             
                            ,cte1.Headline_Txt                                             
                            ,cte1.Is_POD_Approved_Ind                                     
                            ,cte1.POD_Usage_Limit_Type_Per_User_Dsc
                            ,cte1.POD_Reference_Offer_Id                 
                            ,cte1.IVIE_Image_Id                                     
                            ,cte1.Vehicle_Name_Txt                             
                            ,cte1.Ad_Page_Number_Txt                                     
                            ,cte1.Ad_Mod_Nbr                                               
                            ,cte1.ECom_Dsc                                                 
                            ,cte1.Requested_User_User_Id                 
                            ,cte1.Requested_User_First_Nm               
                            ,cte1.Requested_User_Last_Nm               
                            ,cte1.Is_Primary_POD_Offer_Ind             
                            ,cte1.In_Email_Ind                                 
                            ,cte1.Submitted_Dt
							,cte1.Redemption_System_Id
							,cte1.Adbug_Txt
                            ,cte1.lastUpdateTS
							,cte1.FileName
							,cte1.Allocation_Cd
							,cte1.Allocation_Nm
							,cte1.Printed_Message_Notification_Ind
							,cte1.Cashier_Message_Notification_Ind					
							,cte1.CUSTOM_OFFER_LIMIT_NBR
							,cte1.CUSTOM_PERIOD_NBR
							,cte1.CUSTOM_TYPE_DSC
							,cte1.QUALIFICATION_PRODUCT_DISQUALIFIER_NM
							,cte1.IS_DISPLAY_IMMEDIATE_IND
							,cte1.Ecomm_Promo_Type_Cd
							,cte1.Promotion_Order_Nbr
							,cte1.Headline2_Txt
							,cte1.Usage_Limit_Per_Offer_Cnt
							,cte1.REFUNDABLE_REWARDS_IND                    
							,cte1.Multi_Clip_Limit_Cnt
							,cte1.payload_points
							,cte1.payload_programSubType
							,cte1.Ecomm_Promo_Type_Nm
							,cte1.Auto_Apply_Promo_Ind
							,cte1.Valid_With_Other_Offers_Ind
							,cte1.Offer_Eligible_Order_Cnt
							,cte1.Valid_For_First_Time_Customer_Ind
							,cte1.Merkle_Game_Land_Nm     
							,cte1.Merkle_Game_Land_Space_Nm
							,cte1.Merkle_Game_Land_Space_Slot_Nm
							,cte1.Promotion_Subprogram_Type_Cd
							,cte1.Offer_Qualification_Behavior_Cd
							,cte1.Initial_Subscription_Offer_Ind
							,cte1.Dynamic_Offer_Ind
							,cte1.Days_To_Redeem_Offer_Cnt
							,cte1.Offer_Clippable_Ind  
							,cte1.Offer_Applicable_Online_Ind
							,cte1.Offer_Displayable_Ind
                            ,Row_number() OVER ( partition BY cte1.OMS_Offer_Id ORDER BY To_timestamp_ntz(cte1.lastUpdateTS) DESC) AS rn
                            from
							cte1 inner join cte2 on cte1.OMS_Offer_Id = cte2.OMS_OFF_ID 
						    and cte1.FileName = cte2.FILENM
							)     
                          SELECT
                          src.OMS_Offer_Id
                          ,src.External_Offer_Id
                          ,src.Offer_Request_Id
                          ,src.Aggregator_Offer_Id
                          ,src.Manufacturer_Id
                          ,src.Manufacturer_Offer_Reference_Cd                                                          
                          ,src.Provider_Nm           
                          ,src.Categories_Txt                            
                          ,src.Primary_Category_Txt                              
                          ,src.Program_Cd                                 
                          ,src.Program_Code_Dsc                                  
                          ,src.Subprogram_Nm                                      
                          ,src.Subprogram_Dsc                                      
                          ,src.Delivery_Channel_Cd                               
                          ,src.Delivery_Channel_Dsc                             
                          ,src.Offer_Status_Cd                                       
                          ,src.Offer_Status_Dsc                                      
                          ,src.Price_Title_Txt                            
                          ,src.Price_Value_Txt                                        
                          ,src.Savings_Value_Txt                                    
                          ,src.Title_Dsc
						  ,src.Title_Dsc1
						  ,src.Title_Dsc2
						  ,src.Title_Dsc3
                          ,src.Product_Dsc
						  ,src.Product_Dsc1
						  ,src.Product_Dsc2
						  ,src.Product_Dsc3
                          ,src.Disclaimer_Txt                            
                          ,src.Description_Txt                           
                          ,src.Print_Tags_Ind                            
                          ,src.Product_Image_Id                                    
                          ,src.Price_Cd                                               
                          ,src.Time_Txt                                               
                          ,src.Year_Txt                                               
                          ,src.Product_Cd                                  
                          ,src.Is_Employee_Offer_Ind            
                          ,src.Is_Default_Allocation_Offer_Ind
                          ,src.Program_Type_Cd                                    
                          ,src.Should_Report_Redeptions_Ind
                          ,src.Created_Ts                                   
                          ,src.Created_Application_Id            
                          ,src.Created_User_Id                          
                          ,src.Last_Updated_Application_Id  
                          ,src.Last_Updated_User_Id                            
                          ,src.Last_Updated_Ts                                      
                          ,src.Display_Effective_Start_Dt
                          ,src.Display_Effective_End_Dt         
                          ,src.Effective_Start_Dt                                    
                          ,src.Effective_End_Dt                                      
                          ,src.Test_Effective_Start_Dt            
                          ,src.Test_Effective_End_Dt                            
                          ,src.Qualification_Unit_Type_Dsc   
                          ,src.Qualification_Unite_Subtype_Dsc   
                          ,src.Beneifit_Value_Type_Dsc                                                
                          ,src.Usage_Limit_Type_Per_User_Dsc                                                                   
                          ,src.PLU_Trigger_Barcode_Txt
                          ,src.Copient_Category_Dsc                                                        
                          ,src.Engine_Dsc                                                                                       
                          ,src.Priority_Cd
                          ,src.Tiers_Cd
						  ,src.Send_Outbound_Data_Dsc                                                                                                                                   
                          ,src.Chargeback_Vendor_Nm                                                                                                                                       
                          ,src.Auto_Transferable_Ind                                                                                                                            
                          ,src.Enable_Issuance_Ind                                                                                                                                              
                          ,src.Defer_Evaluation_Until_EOS_Ind                                                                                            
                          ,src.Enable_Impression_Reporting_Ind                                                                                         
                          ,src.Limit_Eligibility_Frequency_Txt                                                                                               
                          ,src.Is_Appliable_To_J4U_Ind                                                                                                                         
                          ,src.Customer_Segment_Dsc                                                                                                                                        
                          ,src.Assignment_User_Id                                                                                                                                               
                          ,src.Assignment_First_Nm                                                                                                                                            
                          ,src.Assignment_Last_Nm                                                                                                                                             
                          ,src.Qualification_Product_Disqualifier_Txt                                                                   
                          ,src.Qualification_Day_Monday_Ind                                                                                                             
                          ,src.Qualification_Day_Tuesday_Ind                                                                                              
                          ,src.Qualification_Day_Wednesday_Ind                                                                                                       
                          ,src.Qualification_Day_Thursday_Ind                                                                                            
                          ,src.Qualification_Day_Friday_Ind                                                                                                                 
                          ,src.Qualification_Day_Saturday_Ind                                                                                             
                          ,src.Qualification_Day_Sunday_Ind                                                                                                               
                          ,src.Qualification_Start_Time_Txt                                                                                                                 
                          ,src.Qualification_End_Time_Txt                                                                                                                   
                          ,src.Qualification_Enterprise_Instant_Win_Number_Of_Prizes_Qty  
                          ,src.Qualification_Enterprise_Instant_Win_Frequency_Txt                         
                          ,src.Offer_Nm                                                                                                                                                              
                          ,src.Ad_Type_Cd                                                                                                                                               
                          ,src.Offer_Prototype_Cd                                                                                                                                                
                          ,src.Offer_Prototype_Dsc                                                                                                                                              
                          ,src.Store_Group_Version_Id                                                                                                                         
                          ,src.Store_Tag_Print_J4U_Tag_Enabled_Ind                                                                                
                          ,src.Store_Tag_Multiple_Nbr                                                                                                                         
                          ,src.Store_Tag_Amt                                                                                                                                                        
                          ,src.Store_Tag_Comments_Txt                                                                                                                      
                          ,src.Requested_Removal_For_All_Ind                                                                                                          
                          ,src.Removed_On_Ts                                                                                                                                                     
                          ,src.Removed_Unclipped_On_Ts                                                                                                                                 
                          ,src.Removal_For_All_On_Ts                                                                                                                          
                          ,src.Brand_Size_Dsc                                                                                                                                                        
                          ,src.Created_User_User_Id                                                                                                                                           
                          ,src.Created_User_First_Nm                                                                                                                                         
                          ,src.Created_User_Last_Nm                                                                                                                                         
                          ,src.Updated_User_User_Id                                                                                                                                          
                          ,src.Updated_User_First_Nm                                                                                                                                       
                          ,src.Updated_User_Last_Nm                                                                                                                                        
                          ,src.First_Update_To_Redemption_Engine_Ts                                                                            
                          ,src.Last_Update_To_Redemption_Engine_Ts                                                                             
                          ,src.First_Update_To_J4U_Ts                                                                                                                         
                          ,src.Last_Update_To_J4U_Ts                                                                                                                          
                          ,src.Offer_Requestor_Group_Cd                                                                                                                                 
                          ,src.Headline_Txt                                                                                                                                              
                          ,src.Is_POD_Approved_Ind                                                                                                                                           
                          ,src.POD_Usage_Limit_Type_Per_User_Dsc                                                                                 
                          ,src.POD_Reference_Offer_Id                                                                                                                        
                          ,src.IVIE_Image_Id                                                                                                                                            
                          ,src.Vehicle_Name_Txt                                                                                                                                                  
                          ,src.Ad_Page_Number_Txt                                                                                                                                           
                          ,src.Ad_Mod_Nbr                                                                                                                                                            
                          ,src.ECom_Dsc                                                                                                                                                              
                          ,src.Requested_User_User_Id                                                                                                                        
                          ,src.Requested_User_First_Nm                                                                                                                      
                          ,src.Requested_User_Last_Nm                                                                                                                      
                          ,src.Is_Primary_POD_Offer_Ind                                                                                                                                   
                                                                                                                                                 
                          ,src.In_Email_Ind                                                                                                                                        
                          ,src.Submitted_Dt
						  ,src.Redemption_System_Id
						  ,src.Adbug_Txt
                          ,src.DW_Logical_delete_ind
                          ,src.lastUpdateTS
						  ,src.Filename
						  ,src.Allocation_Cd
						  ,src.Allocation_Nm
						  ,src.Printed_Message_Notification_Ind
						  ,src.Cashier_Message_Notification_Ind	
						  ,src.CUSTOM_OFFER_LIMIT_NBR
						  ,src.CUSTOM_PERIOD_NBR
						  ,src.CUSTOM_TYPE_DSC
						  ,src.QUALIFICATION_PRODUCT_DISQUALIFIER_NM
						  ,src.IS_DISPLAY_IMMEDIATE_IND
						  ,src.Ecomm_Promo_Type_Cd
						  ,src.Promotion_Order_Nbr
						  ,src.Headline2_Txt
						  ,src.Usage_Limit_Per_Offer_Cnt
						  ,SRC.REFUNDABLE_REWARDS_IND                     
						  ,SRC.Multi_Clip_Limit_Cnt
                          ,CASE WHEN (tgt.OMS_Offer_Id IS NULL ) THEN 'I' ELSE 'U' END AS DML_Type
                          ,CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1  ELSE 0 END AS Sameday_chg_ind  
						  ,SRC.payload_points
						  ,src.payload_programSubType
						  ,src.Ecomm_Promo_Type_Nm
						  ,src.Auto_Apply_Promo_Ind
						  ,src.Valid_With_Other_Offers_Ind
						  ,src.Offer_Eligible_Order_Cnt
						  ,src.Valid_For_First_Time_Customer_Ind
						  ,src.Merkle_Game_Land_Nm 
                          ,src.Merkle_Game_Land_Space_Nm 
                          ,src.Merkle_Game_Land_Space_Slot_Nm 
						  ,src.Promotion_Subprogram_Type_Cd
						  ,src.Offer_Qualification_Behavior_Cd
						  ,src.Initial_Subscription_Offer_Ind
						  ,src.Dynamic_Offer_Ind
						  ,src.Days_To_Redeem_Offer_Cnt
						  ,src.Offer_Clippable_Ind  
							,src.Offer_Applicable_Online_Ind
							,src.Offer_Displayable_Ind
                          from
                          (SELECT
                          OMS_Offer_Id                                                                                                         
                          ,External_Offer_Id                                                                                                
                          ,Offer_Request_Id                                                                                                
                          ,Aggregator_Offer_Id                                                                                           
                          ,Manufacturer_Id                                                                                                  
                          ,Manufacturer_Offer_Reference_Cd                                                  
                          ,Provider_Nm                                                                                                                                                           
                          ,case when Categories_Txt = '' Then Null Else  Categories_Txt End as Categories_Txt                                                                                                                                               
                          ,case when Primary_Category_Txt  = '' Then Null Else   Primary_Category_Txt End as Primary_Category_Txt                                                                                                                                
                          ,Program_Cd                                                                                                                                                            
                          ,Program_Code_Dsc                                                                                                                                         
                          ,Subprogram_Nm                                                                                                                                             
                          ,Subprogram_Dsc                                                                                                                                              
                          ,Delivery_Channel_Cd                                                                                                                                      
                          ,Delivery_Channel_Dsc                                                                                                                                    
                          ,Offer_Status_Cd                                                                                                                                               
                          ,Offer_Status_Dsc                                                                                                                                             
                          ,Price_Title_Txt                                                                                                                                                 
                          ,Price_Value_Txt                                                                                                                                               
                          ,Savings_Value_Txt                                                                                                                                           
                          ,Title_Dsc
						  ,Title_Dsc1
						  ,Title_Dsc2
						  ,Title_Dsc3
                          ,Product_Dsc 
						  ,Product_Dsc1
						  ,Product_Dsc2
						  ,Product_Dsc3
                          ,Disclaimer_Txt                                                                                                                                                  
                          ,Description_Txt                                                                                                                                                
                          ,Print_Tags_Ind                                                                                                                                                 
                          ,Product_Image_Id                                                                                                                                           
                          ,Price_Cd                                                                                                                                                              
                          ,Time_Txt                                                                                                                                                              
                          ,Year_Txt                                                                                                                                                              
                          ,Product_Cd                                                                                                                                                            
                          ,Is_Employee_Offer_Ind                                                                                                                                  
                          ,Is_Default_Allocation_Offer_Ind                                                                                                   
                          ,Program_Type_Cd                                                                                                                                           
                          ,Should_Report_Redeptions_Ind                                                                                                                   
                          ,Created_Ts                                                                                                                                                            
                          ,Created_Application_Id                                                                                                                                  
                          ,Created_User_Id                                                                                                                                
                          ,Last_Updated_Application_Id                                                                                                        
                          ,Last_Updated_User_Id                                                                                                                                                 
                          ,Last_Updated_Ts                                                                                                                                             
                          ,Display_Effective_Start_Dt                                                                                                             
                          ,Display_Effective_End_Dt                                                                                                                              
                          ,Effective_Start_Dt                                                                                                                                           
                          ,Effective_End_Dt                                                                                                                                             
                          ,Test_Effective_Start_Dt                                                                                                                                  
                          ,Test_Effective_End_Dt                                                                                                                                   
                          ,Qualification_Unit_Type_Dsc                                                                                                         
                          ,Qualification_Unite_Subtype_Dsc                                                                                                 
                          ,Beneifit_Value_Type_Dsc                                                                                                                              
                          ,Usage_Limit_Type_Per_User_Dsc                                                                                                 
                          ,PLU_Trigger_Barcode_Txt                                                                                                                              
                          ,Copient_Category_Dsc                                                                                                                                                 
                          ,Engine_Dsc                                                                                                                                                            
                          ,Priority_Cd                                                                                                                                                           
                          ,Tiers_Cd                                                                                                                                                              
                          ,Send_Outbound_Data_Dsc                                                                                                                            
                          ,Chargeback_Vendor_Nm                                                                                                                                             
                          ,Auto_Transferable_Ind                                                                                                                                   
                          ,Enable_Issuance_Ind                                                                                                                                      
                          ,Defer_Evaluation_Until_EOS_Ind                                                                                                  
                          ,Enable_Impression_Reporting_Ind                                                                                               
                          ,Limit_Eligibility_Frequency_Txt                                                                                                     
                          ,Is_Appliable_To_J4U_Ind                                                                                                                               
                          ,Customer_Segment_Dsc                                                                                                                                              
                          ,Assignment_User_Id                                                                                                                                       
                          ,Assignment_First_Nm                                                                                                                                                   
                          ,Assignment_Last_Nm                                                                                                                                                   
                          ,Qualification_Product_Disqualifier_Txt                                                                          
                          ,Qualification_Day_Monday_Ind                                                                                                                   
                          ,Qualification_Day_Tuesday_Ind                                                                                                    
                          ,Qualification_Day_Wednesday_Ind                                                                                              
                          ,Qualification_Day_Thursday_Ind                                                                                                  
                          ,Qualification_Day_Friday_Ind                                                                                                        
                          ,Qualification_Day_Saturday_Ind                                                                                                   
                          ,Qualification_Day_Sunday_Ind                                                                                                                     
                          ,Qualification_Start_Time_Txt                                                                                                        
                          ,Qualification_End_Time_Txt                                                                                                          
                          ,Qualification_Enterprise_Instant_Win_Number_Of_Prizes_Qty  
                          ,Qualification_Enterprise_Instant_Win_Frequency_Txt                  
                          ,Offer_Nm                                                                                                                                                              
                          ,Ad_Type_Cd                                                                                                                                                            
                          ,Offer_Prototype_Cd                                                                                                                                        
                          ,Offer_Prototype_Dsc                                                                                                                                      
                          ,Store_Group_Version_Id                                                                                                                               
                          ,Store_Tag_Print_J4U_Tag_Enabled_Ind                                                                                      
                          ,Store_Tag_Multiple_Nbr                                                                                                                                
                          ,Store_Tag_Amt                                                                                                                                                
                          ,Store_Tag_Comments_Txt                                                                                                                             
                          ,Requested_Removal_For_All_Ind                                                                                                                
                          ,Removed_On_Ts                                                                                                                                              
                          ,Removed_Unclipped_On_Ts                                                                                                                         
                          ,Removal_For_All_On_Ts                                                                                                                                
                          ,Brand_Size_Dsc                                                                                                                                                
                          ,Created_User_User_Id                                                                                                                                                 
                          ,Created_User_First_Nm                                                                                                                                               
                          ,Created_User_Last_Nm                                                                                                                                               
                          ,Updated_User_User_Id                                                                                                                                                
                          ,Updated_User_First_Nm                                                                                                                                             
                          ,Updated_User_Last_Nm                                                                                                                                              
                          ,First_Update_To_Redemption_Engine_Ts                                                                                  
                          ,Last_Update_To_Redemption_Engine_Ts                                                                                   
                          ,First_Update_To_J4U_Ts                                                                                                                                
                          ,Last_Update_To_J4U_Ts                                                                                                                                
                          ,Offer_Requestor_Group_Cd                                                                                                                          
                          ,Headline_Txt                                                                                                                                                          
                          ,Is_POD_Approved_Ind                                                                                                                                                  
                          ,POD_Usage_Limit_Type_Per_User_Dsc                                                                                       
                          ,POD_Reference_Offer_Id                                                                                                                              
                          ,IVIE_Image_Id                                                                                                                                                  
                          ,Vehicle_Name_Txt                                                                                                                                          
                          ,Ad_Page_Number_Txt                                                                                                                                                  
                          ,Ad_Mod_Nbr                                                                                                                                                            
                          ,ECom_Dsc                                                                                                                                                              
                          ,Requested_User_User_Id                                                                                                                              
                          ,Requested_User_First_Nm                                                                                                                            
                          ,Requested_User_Last_Nm                                                                                                                            
                          ,Is_Primary_POD_Offer_Ind                                                                                                                           
                                                                                                                                             
                          ,In_Email_Ind                                                                                                                                               
                          ,Submitted_Dt
						  ,Redemption_System_Id
						  ,Adbug_Txt
                          ,false AS DW_Logical_delete_ind                                                                                                                                                                                                                                                                                                               
                          ,lastUpdateTS
						  ,Filename
						  ,Allocation_Cd
						  ,Allocation_Nm
						  ,Printed_Message_Notification_Ind
						  ,Cashier_Message_Notification_Ind
						  ,CUSTOM_OFFER_LIMIT_NBR
						  ,CUSTOM_PERIOD_NBR
						  ,CUSTOM_TYPE_DSC
						  ,QUALIFICATION_PRODUCT_DISQUALIFIER_NM
						  ,IS_DISPLAY_IMMEDIATE_IND
						  ,Ecomm_Promo_Type_Cd
						  ,Promotion_Order_Nbr
						  ,Headline2_Txt
						  ,Usage_Limit_Per_Offer_Cnt
						  ,REFUNDABLE_REWARDS_IND
						  ,Multi_Clip_Limit_Cnt
						  ,payload_points
						  ,payload_programSubType
						  ,Ecomm_Promo_Type_Nm
						  ,Auto_Apply_Promo_Ind
						  ,Valid_With_Other_Offers_Ind
						  ,Offer_Eligible_Order_Cnt
						  ,Valid_For_First_Time_Customer_Ind
						  ,Merkle_Game_Land_Nm 
						  ,Merkle_Game_Land_Space_Nm 
						  ,Merkle_Game_Land_Space_Slot_Nm 
						  ,Promotion_Subprogram_Type_Cd
						  ,Offer_Qualification_Behavior_Cd
						  ,Initial_Subscription_Offer_Ind
						  ,Dynamic_Offer_Ind
						  ,Days_To_Redeem_Offer_Cnt
						  ,Offer_Clippable_Ind  
							,Offer_Applicable_Online_Ind
							,Offer_Displayable_Ind
                          FROM src_wrk_tbl_recs 
                          WHERE rn = 1 
                          AND OMS_Offer_Id is not null
						  						  
                          ) src 
                          LEFT JOIN 
                          (SELECT  DISTINCT
                          tgt.OMS_Offer_Id                                                                                                   
                          ,tgt.External_Offer_Id                                                                                                         
                          ,tgt.Offer_Request_Id                                                                                                         
                          ,tgt.Aggregator_Offer_Id                                                                                                    
                          ,tgt.Manufacturer_Id                                                                                                          
                          ,tgt.Manufacturer_Offer_Reference_Cd                                                          
                          ,tgt.Provider_Nm                                                                                                                                              
                          ,tgt.Categories_Txt                                                                                                                                           
                          ,tgt.Primary_Category_Txt                                                                                                                                             
                          ,tgt.Program_Cd                                                                                                                                                
                          ,tgt.Program_Code_Dsc                                                                                                                                                 
                          ,tgt.Subprogram_Nm                                                                                                                                                     
                          ,tgt.Subprogram_Dsc                                                                                                                                                     
                          ,tgt.Delivery_Channel_Cd                                                                                                                                              
                          ,tgt.Delivery_Channel_Dsc                                                                                                                                            
                          ,tgt.Offer_Status_Cd                                                                                                                                                      
                          ,tgt.Offer_Status_Dsc                                                                                                                                                     
                          ,tgt.Price_Title_Txt                                                                                                                                           
                          ,tgt.Price_Value_Txt                                                                                                                                                       
                          ,tgt.Savings_Value_Txt                                                                                                                                                   
                          ,tgt.Title_Dsc  
						  ,tgt.Title_Dsc1
						  ,tgt.Title_Dsc2
						  ,tgt.Title_Dsc3
                          ,tgt.Product_Dsc 
						  ,tgt.Product_Dsc1
						  ,tgt.Product_Dsc2
						  ,tgt.Product_Dsc3
                          ,tgt.Disclaimer_Txt                                                                                                                                            
                          ,tgt.Description_Txt                                                                                                                                          
                          ,tgt.Print_Tags_Ind                                                                                                                                           
                          ,tgt.Product_Image_Id                                                                                                                                                   
                          ,tgt.Price_Cd                                                                                                                                                              
                          ,tgt.Time_Txt                                                                                                                                                              
                          ,tgt.Year_Txt                                                                                                                                                              
                          ,tgt.Product_Cd                                                                                                                                                 
                          ,tgt.Is_Employee_Offer_Ind                                                                                                                            
                          ,tgt.Is_Default_Allocation_Offer_Ind                                                                                             
                          ,tgt.Program_Type_Cd                                                                                                                                                   
                          ,tgt.Should_Report_Redeptions_Ind                                                                                                             
                          ,tgt.Created_Ts                                                                                                                                                  
                          ,tgt.Created_Application_Id                                                                                                                           
                          ,tgt.Created_User_Id                                                                                                                                         
                          ,tgt.Last_Updated_Application_Id                                                                                                                 
                          ,tgt.Last_Updated_User_Id                                                                                                                                           
                          ,tgt.Last_Updated_Ts                                                                                                                                                     
                          ,tgt.Display_Effective_Start_Dt                                                                                                       
                          ,tgt.Display_Effective_End_Dt                                                                                                                        
                          ,tgt.Effective_Start_Dt                                                                                                                                                   
                          ,tgt.Effective_End_Dt                                                                                                                                                     
                          ,tgt.Test_Effective_Start_Dt                                                                                                                           
                          ,tgt.Test_Effective_End_Dt                                                                                                                                           
                          ,tgt.Qualification_Unit_Type_Dsc                                                                                                                  
                          ,tgt.Qualification_Unite_Subtype_Dsc                                                                                           
                          ,tgt.Beneifit_Value_Type_Dsc                                                                                                                        
                          ,tgt.Usage_Limit_Type_Per_User_Dsc                                                                                           
                          ,tgt.PLU_Trigger_Barcode_Txt                                                                                                                        
                          ,tgt.Copient_Category_Dsc                                                                                                                                           
                          ,tgt.Engine_Dsc                                                                                                                                                 
                          ,tgt.Priority_Cd                                                                                                                                                  
                          ,tgt.Tiers_Cd                                                                                                                                                              
                          ,tgt.Send_Outbound_Data_Dsc                                                                                                                                   
                          ,tgt.Chargeback_Vendor_Nm                                                                                                                                       
                          ,tgt.Auto_Transferable_Ind                                                                                                                            
                          ,tgt.Enable_Issuance_Ind                                                                                                                                              
                          ,tgt.Defer_Evaluation_Until_EOS_Ind                                                                                            
                          ,tgt.Enable_Impression_Reporting_Ind                                                                                         
                          ,tgt.Limit_Eligibility_Frequency_Txt                                                                                               
                          ,tgt.Is_Appliable_To_J4U_Ind                                                                                                                         
                          ,tgt.Customer_Segment_Dsc                                                                                                                                        
                          ,tgt.Assignment_User_Id                                                                                                                                               
                          ,tgt.Assignment_First_Nm                                                                                                                                            
                          ,tgt.Assignment_Last_Nm                                                                                                                                             
                          ,tgt.Qualification_Product_Disqualifier_Txt                                                                   
                          ,tgt.Qualification_Day_Monday_Ind                                                                                                             
                          ,tgt.Qualification_Day_Tuesday_Ind                                                                                              
                          ,tgt.Qualification_Day_Wednesday_Ind                                                                                                       
                          ,tgt.Qualification_Day_Thursday_Ind                                                                                            
                          ,tgt.Qualification_Day_Friday_Ind                                                                                                                 
                          ,tgt.Qualification_Day_Saturday_Ind                                                                                             
                          ,tgt.Qualification_Day_Sunday_Ind                                                                                                               
                          ,tgt.Qualification_Start_Time_Txt                                                                                                                 
                          ,tgt.Qualification_End_Time_Txt                                                                                                                   
                          ,tgt.Qualification_Enterprise_Instant_Win_Number_Of_Prizes_Qty  
                          ,tgt.Qualification_Enterprise_Instant_Win_Frequency_Txt                         
                          ,tgt.Offer_Nm                                                                                                                                                              
                          ,tgt.Ad_Type_Cd                                                                                                                                               
                          ,tgt.Offer_Prototype_Cd                                                                                                                                                
                          ,tgt.Offer_Prototype_Dsc                                                                                                                                              
                          ,tgt.Store_Group_Version_Id                                                                                                                         
                          ,tgt.Store_Tag_Print_J4U_Tag_Enabled_Ind                                                                                
                          ,tgt.Store_Tag_Multiple_Nbr                                                                                                                          
                          ,tgt.Store_Tag_Amt                                                                                                                                                        
                          ,tgt.Store_Tag_Comments_Txt                                                                                                                      
                          ,tgt.Requested_Removal_For_All_Ind                                                                                                          
                          ,tgt.Removed_On_Ts                                                                                                                                                      
                          ,tgt.Removed_Unclipped_On_Ts                                                                                                                                 
                          ,tgt.Removal_For_All_On_Ts                                                                                                                          
                          ,tgt.Brand_Size_Dsc                                                                                                                                                        
                          ,tgt.Created_User_User_Id                                                                                                                                           
                          ,tgt.Created_User_First_Nm                                                                                                                                         
                          ,tgt.Created_User_Last_Nm                                                                                                                                         
                          ,tgt.Updated_User_User_Id                                                                                                                                          
                          ,tgt.Updated_User_First_Nm                                                                                                                                       
                          ,tgt.Updated_User_Last_Nm                                                                                                                                        
                          ,tgt.First_Update_To_Redemption_Engine_Ts                                                                            
                          ,tgt.Last_Update_To_Redemption_Engine_Ts                                                                             
                          ,tgt.First_Update_To_J4U_Ts                                                                                                                         
                          ,tgt.Last_Update_To_J4U_Ts                                                                                                                          
                          ,tgt.Offer_Requestor_Group_Cd                                                                                                                                 
                          ,tgt.Headline_Txt                                                                                                                                              
                          ,tgt.Is_POD_Approved_Ind                                                                                                                                           
                          ,tgt.POD_Usage_Limit_Type_Per_User_Dsc                                                                                 
                          ,tgt.POD_Reference_Offer_Id                                                                                                                        
                          ,tgt.IVIE_Image_Id                                                                                                                                            
                          ,tgt.Vehicle_Name_Txt                                                                                                                                                  
                          ,tgt.Ad_Page_Number_Txt                                                                                                                                            
                          ,tgt.Ad_Mod_Nbr                                                                                                                                                            
                          ,tgt.ECom_Dsc                                                                                                                                                              
                          ,tgt.Requested_User_User_Id                                                                                                                        
                          ,tgt.Requested_User_First_Nm                                                                                                                      
                          ,tgt.Requested_User_Last_Nm                                                                                                                      
                          ,tgt.Is_Primary_POD_Offer_Ind                                                                                                                                   
                                                                                                                                                    
                          ,tgt.In_Email_Ind                                                                                                                                  
                          ,tgt.Submitted_Dt
						  ,tgt.Redemption_System_Id
						  ,tgt.Adbug_Txt
						  ,tgt.Allocation_Cd
						  ,tgt.Allocation_Nm
                          ,tgt.dw_logical_delete_ind
                          ,tgt.dw_first_effective_dt
						  ,tgt.Printed_Message_Notification_Ind
						  ,tgt.Cashier_Message_Notification_Ind
						  ,tgt.CUSTOM_OFFER_LIMIT_NBR
						  ,tgt.CUSTOM_PERIOD_NBR
						  ,tgt.CUSTOM_TYPE_DSC
						  ,tgt.QUALIFICATION_PRODUCT_DISQUALIFIER_NM
						  ,tgt.IS_DISPLAY_IMMEDIATE_IND
						  ,tgt.Ecomm_Promo_Type_Cd
						  ,tgt.Promotion_Order_Nbr
						  ,tgt.Headline2_Txt
						  ,tgt.Usage_Limit_Per_Offer_Cnt
						  ,tgt.REFUNDABLE_REWARDS_IND
						  ,tgt.Multi_Clip_Limit_Cnt
						  ,tgt.points
						  ,tgt.programSubType
						  ,tgt.Ecomm_Promo_Type_Nm
						  ,tgt.Auto_Apply_Promo_Ind
						  ,tgt.Valid_With_Other_Offers_Ind
						  ,tgt.Offer_Eligible_Order_Cnt
						  ,tgt.Valid_For_First_Time_Customer_Ind
						  ,tgt.Merkle_Game_Land_Nm 
                          ,tgt.Merkle_Game_Land_Space_Nm 
                          ,tgt.Merkle_Game_Land_Space_Slot_Nm
						  ,tgt.Promotion_Subprogram_Type_Cd
						  ,tgt.Offer_Qualification_Behavior_Cd  
						  ,tgt.Initial_Subscription_Offer_Ind
						  ,tgt.Dynamic_Offer_Ind
						  ,tgt.Days_To_Redeem_Offer_Cnt
						  ,TGT.Offer_Clippable_Ind  
							,tgt.Offer_Applicable_Online_Ind
							,tgt.Offer_Displayable_Ind
                          FROM ${tgt_tbl} tgt 
                          WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
                          ) tgt 
                          ON tgt.OMS_Offer_Id = src.OMS_Offer_Id   						  
                          WHERE  (tgt.OMS_Offer_Id is null)  
                          or(
                           NVL(src.External_Offer_Id,'-1') <> NVL(tgt.External_Offer_Id,'-1')                                                                                                                   
                          OR NVL(src.Offer_Request_Id,'-1') <> NVL(tgt.Offer_Request_Id,'-1')                                                                                                    
                          OR NVL(src.Aggregator_Offer_Id,'-1') <> NVL(tgt.Aggregator_Offer_Id,'-1')   
                          OR NVL(src.Manufacturer_Id,'-1') <> NVL(tgt.Manufacturer_Id,'-1')  
                          OR NVL(src.Manufacturer_Offer_Reference_Cd,'-1') <> NVL(tgt.Manufacturer_Offer_Reference_Cd,'-1')              
                          OR NVL(src.Provider_Nm,'-1') <> NVL(tgt.Provider_Nm,'-1')
                          OR NVL(src.Categories_Txt,'-1') <> NVL(tgt.Categories_Txt,'-1')
                          OR NVL(src.Primary_Category_Txt,'-1') <> NVL(tgt.Primary_Category_Txt,'-1')
                          OR NVL(src.Program_Cd,'-1') <> NVL(tgt.Program_Cd,'-1')
                          OR NVL(src.Program_Code_Dsc,'-1') <> NVL(tgt.Program_Code_Dsc,'-1')
                          OR NVL(src.Subprogram_Nm,'-1') <> NVL(tgt.Subprogram_Nm,'-1')
                          OR NVL(src.Subprogram_Dsc,'-1') <> NVL(tgt.Subprogram_Dsc,'-1')
                          OR NVL(src.Delivery_Channel_Cd,'-1') <> NVL(tgt.Delivery_Channel_Cd,'-1')
                          OR NVL(src.Delivery_Channel_Dsc,'-1') <> NVL(tgt.Delivery_Channel_Dsc,'-1')
                          OR NVL(src.Offer_Status_Cd,'-1') <> NVL(tgt.Offer_Status_Cd,'-1')
                          OR NVL(src.Offer_Status_Dsc,'-1') <> NVL(tgt.Offer_Status_Dsc,'-1')
                          OR NVL(src.Price_Title_Txt,'-1') <> NVL(tgt.Price_Title_Txt,'-1')
                          OR NVL(src.Price_Value_Txt,'-1') <> NVL(tgt.Price_Value_Txt,'-1')
                          OR NVL(src.Savings_Value_Txt,'-1') <> NVL(tgt.Savings_Value_Txt,'-1')
                          OR NVL(src.Title_Dsc,'-1') <> NVL(tgt.Title_Dsc,'-1')
						  OR NVL(src.Title_Dsc1,'-1') <> NVL(tgt.Title_Dsc1,'-1')
						  OR NVL(src.Title_Dsc2,'-1') <> NVL(tgt.Title_Dsc2,'-1')
						  OR NVL(src.Title_Dsc3,'-1') <> NVL(tgt.Title_Dsc3,'-1')
                          OR NVL(src.Product_Dsc,'-1') <> NVL(tgt.Product_Dsc,'-1')
						  OR NVL(src.Product_Dsc1,'-1') <> NVL(tgt.Product_Dsc1,'-1')
						  OR NVL(src.Product_Dsc2,'-1') <> NVL(tgt.Product_Dsc2,'-1')
						  OR NVL(src.Product_Dsc3,'-1') <> NVL(tgt.Product_Dsc3,'-1')
                          OR NVL(src.Disclaimer_Txt,'-1') <> NVL(tgt.Disclaimer_Txt,'-1')
                          OR NVL(src.Description_Txt,'-1') <> NVL(tgt.Description_Txt,'-1')
                          OR NVL(src.Print_Tags_Ind,-1) <> NVL(tgt.Print_Tags_Ind,-1)
                          OR NVL(src.Product_Image_Id,'-1') <> NVL(tgt.Product_Image_Id,'-1')
                          OR NVL(src.Price_Cd,'-1') <> NVL(tgt.Price_Cd,'-1')
                          OR NVL(src.Time_Txt,'-1') <> NVL(tgt.Time_Txt,'-1')
                          OR NVL(src.Year_Txt,'-1') <> NVL(tgt.Year_Txt,'-1')
                          OR NVL(src.Product_Cd,'-1') <> NVL(tgt.Product_Cd,'-1')
                          OR NVL(src.Is_Employee_Offer_Ind,-1) <> NVL(tgt.Is_Employee_Offer_Ind,-1)
                          OR NVL(src.Is_Default_Allocation_Offer_Ind,-1) <> NVL(tgt.Is_Default_Allocation_Offer_Ind,-1)
                          OR NVL(src.Program_Type_Cd,'-1') <> NVL(tgt.Program_Type_Cd,'-1')
                          OR NVL(src.Should_Report_Redeptions_Ind,'-1') <> NVL(tgt.Should_Report_Redeptions_Ind,'-1')
                          OR NVL(src.Created_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Created_Ts,'9999-12-31 00:00:00.000')
                          OR NVL(src.Created_Application_Id,'-1') <> NVL(tgt.Created_Application_Id,'-1')
                          OR NVL(src.Created_User_Id,'-1') <> NVL(tgt.Created_User_Id,'-1')
                          OR NVL(src.Last_Updated_Application_Id,'-1') <> NVL(tgt.Last_Updated_Application_Id,'-1')
                          OR NVL(src.Last_Updated_User_Id,'-1') <> NVL(tgt.Last_Updated_User_Id,'-1')
                          OR NVL(src.Last_Updated_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Last_Updated_Ts,'9999-12-31 00:00:00.000')
                          OR NVL(src.Display_Effective_Start_Dt,'9999-12-31') <> NVL(tgt.Display_Effective_Start_Dt,'9999-12-31')
                          OR NVL(src.Display_Effective_End_Dt,'9999-12-31') <> NVL(tgt.Display_Effective_End_Dt,'9999-12-31')
                          OR NVL(src.Effective_Start_Dt,'9999-12-31') <> NVL(tgt.Effective_Start_Dt,'9999-12-31')
                          OR NVL(src.Effective_End_Dt,'9999-12-31') <> NVL(tgt.Effective_End_Dt,'9999-12-31')
                          OR NVL(src.Test_Effective_Start_Dt,'-1') <> NVL(tgt.Test_Effective_Start_Dt,'-1')
                          OR NVL(src.Test_Effective_End_Dt,'-1') <> NVL(tgt.Test_Effective_End_Dt,'-1')
                          OR NVL(src.Qualification_Unit_Type_Dsc,'-1') <> NVL(tgt.Qualification_Unit_Type_Dsc,'-1')
                          OR NVL(src.Qualification_Unite_Subtype_Dsc,'-1') <> NVL(tgt.Qualification_Unite_Subtype_Dsc,'-1')
                          OR NVL(src.Beneifit_Value_Type_Dsc,'-1') <> NVL(tgt.Beneifit_Value_Type_Dsc,'-1')
                          OR NVL(src.Usage_Limit_Type_Per_User_Dsc,'-1') <> NVL(tgt.Usage_Limit_Type_Per_User_Dsc,'-1')
                          OR NVL(src.PLU_Trigger_Barcode_Txt,'-1') <> NVL(tgt.PLU_Trigger_Barcode_Txt,'-1')
                          OR NVL(src.Copient_Category_Dsc,'-1') <> NVL(tgt.Copient_Category_Dsc,'-1')
                          OR NVL(src.Engine_Dsc,'-1') <> NVL(tgt.Engine_Dsc,'-1')
                          OR NVL(src.Priority_Cd,'-1') <> NVL(tgt.Priority_Cd,'-1')
                          OR NVL(src.Tiers_Cd,'-1') <> NVL(tgt.Tiers_Cd,'-1')
                          OR NVL(src.Send_Outbound_Data_Dsc,'-1') <> NVL(tgt.Send_Outbound_Data_Dsc,'-1')
                          OR NVL(src.Chargeback_Vendor_Nm,'-1') <> NVL(tgt.Chargeback_Vendor_Nm,'-1')
                          OR NVL(src.Auto_Transferable_Ind,-1) <> NVL(tgt.Auto_Transferable_Ind,-1)
                          OR NVL(src.Enable_Issuance_Ind,-1) <> NVL(tgt.Enable_Issuance_Ind,-1)
                          OR NVL(src.Defer_Evaluation_Until_EOS_Ind,-1) <> NVL(tgt.Defer_Evaluation_Until_EOS_Ind,-1)
                          OR NVL(src.Enable_Impression_Reporting_Ind,-1) <> NVL(tgt.Enable_Impression_Reporting_Ind,-1)
                          OR NVL(src.Limit_Eligibility_Frequency_Txt,'-1') <> NVL(tgt.Limit_Eligibility_Frequency_Txt,'-1')
                          OR NVL(src.Is_Appliable_To_J4U_Ind,-1) <> NVL(tgt.Is_Appliable_To_J4U_Ind,-1)
                          OR NVL(src.Customer_Segment_Dsc,'-1') <> NVL(tgt.Customer_Segment_Dsc,'-1')
                          OR NVL(src.Assignment_User_Id,'-1') <> NVL(tgt.Assignment_User_Id,'-1')
                          OR NVL(src.Assignment_First_Nm,'-1') <> NVL(tgt.Assignment_First_Nm,'-1')
                          OR NVL(src.Assignment_Last_Nm,'-1') <> NVL(tgt.Assignment_Last_Nm,'-1')
                          OR NVL(src.Qualification_Product_Disqualifier_Txt,'-1') <> NVL(tgt.Qualification_Product_Disqualifier_Txt,'-1')
                          OR NVL(src.Qualification_Day_Monday_Ind,-1) <> NVL(tgt.Qualification_Day_Monday_Ind,-1)
                          OR NVL(src.Qualification_Day_Tuesday_Ind,-1) <> NVL(tgt.Qualification_Day_Tuesday_Ind,-1)
                          OR NVL(src.Qualification_Day_Wednesday_Ind,-1) <> NVL(tgt.Qualification_Day_Wednesday_Ind,-1)
                          OR NVL(src.Qualification_Day_Thursday_Ind,-1) <> NVL(tgt.Qualification_Day_Thursday_Ind,-1)
                          OR NVL(src.Qualification_Day_Friday_Ind,-1) <> NVL(tgt.Qualification_Day_Friday_Ind,-1)
                          OR NVL(src.Qualification_Day_Saturday_Ind,-1) <> NVL(tgt.Qualification_Day_Saturday_Ind,-1)
                          OR NVL(src.Qualification_Day_Sunday_Ind,-1) <> NVL(tgt.Qualification_Day_Sunday_Ind,-1)
                          OR NVL(src.Qualification_Start_Time_Txt,'-1') <> NVL(tgt.Qualification_Start_Time_Txt,'-1')
                          OR NVL(src.Qualification_End_Time_Txt,'-1') <> NVL(tgt.Qualification_End_Time_Txt,'-1')
                          OR NVL(src.Qualification_Enterprise_Instant_Win_Number_Of_Prizes_Qty,'-1') <> NVL(tgt.Qualification_Enterprise_Instant_Win_Number_Of_Prizes_Qty,'-1') 
                          OR NVL(src.Qualification_Enterprise_Instant_Win_Frequency_Txt,'-1') <> NVL(tgt.Qualification_Enterprise_Instant_Win_Frequency_Txt,'-1')
                          OR NVL(src.Offer_Nm,'-1') <> NVL(tgt.Offer_Nm,'-1')
                          OR NVL(src.Ad_Type_Cd,'-1') <> NVL(tgt.Ad_Type_Cd,'-1')
                          OR NVL(src.Offer_Prototype_Cd,'-1') <> NVL(tgt.Offer_Prototype_Cd,'-1')
                          OR NVL(src.Store_Group_Version_Id,'-1') <> NVL(tgt.Store_Group_Version_Id,'-1')
                          OR NVL(src.Store_Tag_Print_J4U_Tag_Enabled_Ind,-1) <> NVL(tgt.Store_Tag_Print_J4U_Tag_Enabled_Ind,-1)
                          OR NVL(src.Store_Tag_Multiple_Nbr,'-1') <> NVL(tgt.Store_Tag_Multiple_Nbr,'-1') 
                          OR NVL(src.Store_Tag_Amt,'-1') <> NVL(tgt.Store_Tag_Amt,'-1') 
                          OR NVL(src.Store_Tag_Comments_Txt,'-1') <> NVL(tgt.Store_Tag_Comments_Txt,'-1')
                          OR NVL(src.Requested_Removal_For_All_Ind,-1) <> NVL(tgt.Requested_Removal_For_All_Ind,-1)
                          OR NVL(src.Removed_On_Ts,'-1') <> NVL(tgt.Removed_On_Ts,'-1')
                          OR NVL(src.Removed_Unclipped_On_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Removed_Unclipped_On_Ts,'9999-12-31 00:00:00.000')
                          OR NVL(src.Removal_For_All_On_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Removal_For_All_On_Ts,'9999-12-31 00:00:00.000')
                          OR NVL(src.Brand_Size_Dsc,'-1') <> NVL(tgt.Brand_Size_Dsc,'-1')
                          OR NVL(src.Created_User_User_Id,'-1') <> NVL(tgt.Created_User_User_Id,'-1')
                          OR NVL(src.Created_User_First_Nm,'-1') <> NVL(tgt.Created_User_First_Nm,'-1')
                          OR NVL(src.Created_User_Last_Nm,'-1') <> NVL(tgt.Created_User_Last_Nm,'-1')
                          OR NVL(src.Updated_User_User_Id,'-1') <> NVL(tgt.Updated_User_User_Id,'-1')
                          OR NVL(src.Updated_User_First_Nm,'-1') <> NVL(tgt.Updated_User_First_Nm,'-1')
                          OR NVL(src.Updated_User_Last_Nm,'-1') <> NVL(tgt.Updated_User_Last_Nm,'-1')
                          OR NVL(src.First_Update_To_Redemption_Engine_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.First_Update_To_Redemption_Engine_Ts,'9999-12-31 00:00:00.000')
                          OR NVL(src.Last_Update_To_Redemption_Engine_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Last_Update_To_Redemption_Engine_Ts,'9999-12-31 00:00:00.000')
                          OR NVL(src.First_Update_To_J4U_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.First_Update_To_J4U_Ts,'9999-12-31 00:00:00.000')
                          OR NVL(src.Last_Update_To_J4U_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Last_Update_To_J4U_Ts,'9999-12-31 00:00:00.000')
                          OR NVL(src.Offer_Requestor_Group_Cd,'-1') <> NVL(tgt.Offer_Requestor_Group_Cd,'-1')
                          OR NVL(src.Headline_Txt,'-1') <> NVL(tgt.Headline_Txt,'-1')
                          OR NVL(src.Is_POD_Approved_Ind,-1) <> NVL(tgt.Is_POD_Approved_Ind,-1)
                          OR NVL(src.POD_Usage_Limit_Type_Per_User_Dsc,'-1') <> NVL(tgt.POD_Usage_Limit_Type_Per_User_Dsc,'-1')
                          OR NVL(src.POD_Reference_Offer_Id,'-1') <> NVL(tgt.POD_Reference_Offer_Id,'-1')
                          OR NVL(src.IVIE_Image_Id,'-1') <> NVL(tgt.IVIE_Image_Id,'-1')
                          OR NVL(src.Vehicle_Name_Txt,'-1') <> NVL(tgt.Vehicle_Name_Txt,'-1')
                          OR NVL(src.Ad_Page_Number_Txt,'-1') <> NVL(tgt.Ad_Page_Number_Txt,'-1')
                          OR NVL(src.Ad_Mod_Nbr,'-1') <> NVL(tgt.Ad_Mod_Nbr,'-1') 
                          OR NVL(src.ECom_Dsc,'-1') <> NVL(tgt.ECom_Dsc,'-1')
                          OR NVL(src.Requested_User_User_Id,'-1') <> NVL(tgt.Requested_User_User_Id,'-1')
                          OR NVL(src.Requested_User_First_Nm,'-1') <> NVL(tgt.Requested_User_First_Nm,'-1')
                          OR NVL(src.Requested_User_Last_Nm,'-1') <> NVL(tgt.Requested_User_Last_Nm,'-1')
                          OR NVL(src.Is_Primary_POD_Offer_Ind,-1) <> NVL(tgt.Is_Primary_POD_Offer_Ind,-1)    
                          OR NVL(src.In_Email_Ind,'-1') <> NVL(tgt.In_Email_Ind,'-1')
                          OR NVL(src.Submitted_Dt,'9999-12-31') <> NVL(tgt.Submitted_Dt,'9999-12-31')
						  OR NVL(src.Redemption_System_Id,'-1') <> NVL(tgt.Redemption_System_Id,'-1')
						  OR NVL(src.Adbug_Txt,'-1') <> NVL(tgt.Adbug_Txt,'-1')
						  OR NVL(src.Allocation_Cd,'-1') <> NVL(tgt.Allocation_Cd,'-1')
						  OR NVL(src.Allocation_Nm,'-1') <> NVL(tgt.Allocation_Nm,'-1')
						  OR NVL(src.Printed_Message_Notification_Ind,'1') <> NVL(tgt.Printed_Message_Notification_Ind,'1')
						  OR NVL(src.Cashier_Message_Notification_Ind,'1') <> NVL(tgt.Cashier_Message_Notification_Ind,'1')
						  OR NVL(src.CUSTOM_OFFER_LIMIT_NBR,'-1') <> NVL(tgt.CUSTOM_OFFER_LIMIT_NBR,'-1')
						  OR NVL(src.CUSTOM_PERIOD_NBR,'-1') <> NVL(tgt.CUSTOM_PERIOD_NBR,'-1')
						  OR NVL(src.CUSTOM_TYPE_DSC,'-1') <> NVL(tgt.CUSTOM_TYPE_DSC,'-1')
						  OR NVL(src.QUALIFICATION_PRODUCT_DISQUALIFIER_NM,'-1') <> NVL(tgt.QUALIFICATION_PRODUCT_DISQUALIFIER_NM,'-1')
						  OR NVL(src.IS_DISPLAY_IMMEDIATE_IND,1) <> NVL(tgt.IS_DISPLAY_IMMEDIATE_IND,1)
						  OR NVL(src.Ecomm_Promo_Type_Cd , '-1')<> NVL(tgt.Ecomm_Promo_Type_Cd,'-1')
						  OR NVL(src.Promotion_Order_Nbr,'-1') <> NVL(tgt.Promotion_Order_Nbr,'-1')
						  OR NVL(src.Headline2_Txt,'-1') <> NVL(tgt.Headline2_Txt,'-1')
						  OR NVL(src.Usage_Limit_Per_Offer_Cnt,'-1') <> NVL(tgt.Usage_Limit_Per_Offer_Cnt,'-1')
						  OR NVL(src.REFUNDABLE_REWARDS_IND,1) <> NVL(tgt.REFUNDABLE_REWARDS_IND,1)     
						  OR NVL(src.Multi_Clip_Limit_Cnt,'-1') <> NVL(tgt.Multi_Clip_Limit_Cnt,'-1')
						  OR NVL(src.payload_points,'-1') <> NVL(tgt.points,'-1')
						  OR NVL(src.payload_programSubType,'-1') <> NVL(tgt.programSubType,'-1')
						  OR NVL(src.Ecomm_Promo_Type_Nm,'-1') <> NVL(tgt.Ecomm_Promo_Type_Nm,'-1')
						  OR NVL(src.Auto_Apply_Promo_Ind,-1) <> NVL(tgt.Auto_Apply_Promo_Ind,-1)
						  OR NVL(src.Valid_With_Other_Offers_Ind,-1) <> NVL(tgt.Valid_With_Other_Offers_Ind,-1)
						  OR NVL(src.Offer_Eligible_Order_Cnt,'-1') <> NVL(tgt.Offer_Eligible_Order_Cnt,'-1')
						  OR NVL(src.Valid_For_First_Time_Customer_Ind,-1) <> NVL(tgt.Valid_For_First_Time_Customer_Ind,-1)
						  OR NVL(src.Merkle_Game_Land_Nm,'-1') <> NVL(tgt.Merkle_Game_Land_Nm,'-1')
						  OR NVL(src.Merkle_Game_Land_Space_Nm,'-1') <> NVL(tgt.Merkle_Game_Land_Space_Nm,'-1')
						  OR NVL(src.Merkle_Game_Land_Space_Slot_Nm,'-1') <> NVL(tgt.Merkle_Game_Land_Space_Slot_Nm,'-1')
						  OR NVL(src.Promotion_Subprogram_Type_Cd,'-1') <> NVL(tgt.Promotion_Subprogram_Type_Cd,'-1')
						  OR NVL(src.Offer_Qualification_Behavior_Cd,'-1') <> NVL(tgt.Offer_Qualification_Behavior_Cd,'-1')
						  OR NVL(src.Initial_Subscription_Offer_Ind,-1) <> NVL(tgt.Initial_Subscription_Offer_Ind,-1)
						  OR NVL(src.Dynamic_Offer_Ind,-1) <> NVL(tgt.Dynamic_Offer_Ind,-1)		
						  OR NVL(src.Days_To_Redeem_Offer_Cnt,'-1') <> NVL(tgt.Days_To_Redeem_Offer_Cnt,'-1')
						  OR NVL(src.Offer_Clippable_Ind,-1) <> NVL(tgt.Offer_Clippable_Ind,-1)
						  OR NVL(src.Offer_Applicable_Online_Ind,-1) <> NVL(tgt.Offer_Applicable_Online_Ind,-1)
						  OR NVL(src.Offer_Displayable_Ind,-1) <> NVL(tgt.Offer_Displayable_Ind,-1)
						  OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
                          )
						  `                      
						 

try {
        snowflake.execute ({ sqlText: sql_truncate_wrk_tbl});
        snowflake.execute ({sqlText: sql_command  });
        }
    catch (err)  {
        return "Creation of OMS_Offer work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }

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
                                           OMS_Offer_Id,                              
                                           filename
                             FROM ${tgt_wrk_tbl}
                             WHERE DML_Type = 'U' 
                             AND Sameday_chg_ind = 0                                      
                             AND OMS_Offer_Id is not NULL                              
                             ) src
                             WHERE tgt.OMS_Offer_Id = src.OMS_Offer_Id 
							 AND tgt.DW_CURRENT_VERSION_IND = TRUE 
							 AND  tgt.DW_LOGICAL_DELETE_IND = FALSE`;

// Processing Sameday updates
var sql_sameday = ` UPDATE ${tgt_tbl} as tgt
					SET  External_Offer_Id = src.External_Offer_Id                                                
						,Offer_Request_Id = src.Offer_Request_Id                                                               
						,Aggregator_Offer_Id = src.Aggregator_Offer_Id                                                         
						,Manufacturer_Id = src.Manufacturer_Id                                                                
						,Manufacturer_Offer_Reference_Cd = src.Manufacturer_Offer_Reference_Cd  
						,Provider_Nm     = src.Provider_Nm             
						,Categories_Txt  = src.Categories_Txt          
						,Primary_Category_Txt = src.Primary_Category_Txt 
						,Program_Cd          = src.Program_Cd               
						,Program_Code_Dsc    = src.Program_Code_Dsc                
						,Subprogram_Nm       = src.Subprogram_Nm                     
						,Subprogram_Dsc      = src.Subprogram_Dsc      
						,Delivery_Channel_Cd = src.Delivery_Channel_Cd             
						,Delivery_Channel_Dsc = src.Delivery_Channel_Dsc        
						,Offer_Status_Cd  = src.Offer_Status_Cd       
						,Offer_Status_Dsc = src.Offer_Status_Dsc      
						,Price_Title_Txt  = src.Price_Title_Txt          
						,Price_Value_Txt  = src.Price_Value_Txt        
						,Savings_Value_Txt= src.Savings_Value_Txt   
						,Title_Dsc        = src.Title_Dsc
						,Title_Dsc1        = src.Title_Dsc1 
						,Title_Dsc2        = src.Title_Dsc2
						,Title_Dsc3        = src.Title_Dsc3
						,Product_Dsc      = src.Product_Dsc 
						,Product_Dsc1      = src.Product_Dsc1
						,Product_Dsc2      = src.Product_Dsc2
						,Product_Dsc3      = src.Product_Dsc3
						,Disclaimer_Txt   = src.Disclaimer_Txt           
						,Description_Txt  = src.Description_Txt         
						,Print_Tags_Ind   = src.Print_Tags_Ind          
						,Product_Image_Id = src.Product_Image_Id    
						,Price_Cd         = src.Price_Cd                     
						,Time_Txt         = src.Time_Txt                    
						,Year_Txt         = src.Year_Txt                     
						,Product_Cd       = src.Product_Cd                                                                          
						,Is_Employee_Offer_Ind = src.Is_Employee_Offer_Ind                                                 
						,Is_Default_Allocation_Offer_Ind = src.Is_Default_Allocation_Offer_Ind                   
						,Program_Type_Cd = src.Program_Type_Cd                                                                            
						,Should_Report_Redeptions_Ind = src.Should_Report_Redeptions_Ind                                  
						,Created_Ts = src.Created_Ts                                                                          
						,Created_Application_Id = src.Created_Application_Id                                                
						,Created_User_Id = src.Created_User_Id                                                  
						,Last_Updated_Application_Id = src.Last_Updated_Application_Id                                      
						,Last_Updated_User_Id = src.Last_Updated_User_Id                                                                
						,Last_Updated_Ts = src.Last_Updated_Ts                                                                              
						,Display_Effective_Start_Dt = src.Display_Effective_Start_Dt                                           
						,Display_Effective_End_Dt = src.Display_Effective_End_Dt                                             
						,Effective_Start_Dt = src.Effective_Start_Dt                                                              
						,Effective_End_Dt = src.Effective_End_Dt                                                                
						,Test_Effective_Start_Dt = src.Test_Effective_Start_Dt                                                
						,Test_Effective_End_Dt = src.Test_Effective_End_Dt                                                                 
						,Qualification_Unit_Type_Dsc = src.Qualification_Unit_Type_Dsc                                       
						,Qualification_Unite_Subtype_Dsc = src.Qualification_Unite_Subtype_Dsc                 
						,Beneifit_Value_Type_Dsc = src.Beneifit_Value_Type_Dsc                                             
						,Usage_Limit_Type_Per_User_Dsc = src.Usage_Limit_Type_Per_User_Dsc                 
						,PLU_Trigger_Barcode_Txt = src.PLU_Trigger_Barcode_Txt                                                            
						,Copient_Category_Dsc = src.Copient_Category_Dsc                                                                 
						,Engine_Dsc = src.Engine_Dsc                                                                          
						,Priority_Cd = src.Priority_Cd                                                                           
						,Tiers_Cd = src.Tiers_Cd                                                                               
						,Send_Outbound_Data_Dsc = src.Send_Outbound_Data_Dsc                                                         
						,Chargeback_Vendor_Nm = src.Chargeback_Vendor_Nm                                                             
						,Auto_Transferable_Ind = src.Auto_Transferable_Ind                                                 
						,Enable_Issuance_Ind = src.Enable_Issuance_Ind                                                                       
						,Defer_Evaluation_Until_EOS_Ind = src.Defer_Evaluation_Until_EOS_Ind                  
						,Enable_Impression_Reporting_Ind = src.Enable_Impression_Reporting_Ind                             
						,Limit_Eligibility_Frequency_Txt = src.Limit_Eligibility_Frequency_Txt                     
						,Is_Appliable_To_J4U_Ind = src.Is_Appliable_To_J4U_Ind                                               
						,Customer_Segment_Dsc = src.Customer_Segment_Dsc                                                             
						,Assignment_User_Id = src.Assignment_User_Id                                                                        
						,Assignment_First_Nm = src.Assignment_First_Nm                                                                      
						,Assignment_Last_Nm = src.Assignment_Last_Nm                                                                      
						,Qualification_Product_Disqualifier_Txt = src.Qualification_Product_Disqualifier_Txt 
						,Qualification_Day_Monday_Ind = src.Qualification_Day_Monday_Ind                                  
						,Qualification_Day_Tuesday_Ind = src.Qualification_Day_Tuesday_Ind                    
						,Qualification_Day_Wednesday_Ind = src.Qualification_Day_Wednesday_Ind                            
						,Qualification_Day_Thursday_Ind = src.Qualification_Day_Thursday_Ind                  
						,Qualification_Day_Friday_Ind = src.Qualification_Day_Friday_Ind                                      
						,Qualification_Day_Saturday_Ind = src.Qualification_Day_Saturday_Ind                   
						,Qualification_Day_Sunday_Ind = src.Qualification_Day_Sunday_Ind                                    
						,Qualification_Start_Time_Txt = src.Qualification_Start_Time_Txt                                       
						,Qualification_End_Time_Txt = src.Qualification_End_Time_Txt                                         
						,Qualification_Enterprise_Instant_Win_Number_Of_Prizes_Qty = src.Qualification_Enterprise_Instant_Win_Number_Of_Prizes_Qty
						,Qualification_Enterprise_Instant_Win_Frequency_Txt  = src.Qualification_Enterprise_Instant_Win_Frequency_Txt
						,Offer_Nm = src.Offer_Nm                                                                                           
						,Ad_Type_Cd = src.Ad_Type_Cd                                                                                      
						,Offer_Prototype_Cd = src.Offer_Prototype_Cd                                                                                       
						,Offer_Prototype_Dsc = src.Offer_Prototype_Dsc                                                                                     
						,Store_Group_Version_Id = src.Store_Group_Version_Id                                                             
						,Store_Tag_Print_J4U_Tag_Enabled_Ind = src.Store_Tag_Print_J4U_Tag_Enabled_Ind                     
						,Store_Tag_Multiple_Nbr = src.Store_Tag_Multiple_Nbr                                                              
						,Store_Tag_Amt = src.Store_Tag_Amt                                                                                 
						,Store_Tag_Comments_Txt = src.Store_Tag_Comments_Txt                                                                        
						,Requested_Removal_For_All_Ind = src.Requested_Removal_For_All_Ind                                              
						,Removed_On_Ts = src.Removed_On_Ts                                                                                             
						,Removed_Unclipped_On_Ts = src.Removed_Unclipped_On_Ts                                                                     
						,Removal_For_All_On_Ts = src.Removal_For_All_On_Ts                                                              
						,Brand_Size_Dsc = src.Brand_Size_Dsc                                                                                 
						,Created_User_User_Id = src.Created_User_User_Id                                                                                
						,Created_User_First_Nm = src.Created_User_First_Nm                                                                             
						,Created_User_Last_Nm = src.Created_User_Last_Nm                                                                              
						,Updated_User_User_Id = src.Updated_User_User_Id                                                                              
						,Updated_User_First_Nm = src.Updated_User_First_Nm                                                                            
						,Updated_User_Last_Nm = src.Updated_User_Last_Nm                                                                            
						,First_Update_To_Redemption_Engine_Ts = src.First_Update_To_Redemption_Engine_Ts                 
						,Last_Update_To_Redemption_Engine_Ts = src.Last_Update_To_Redemption_Engine_Ts                 
						,First_Update_To_J4U_Ts = src.First_Update_To_J4U_Ts                                                             
						,Last_Update_To_J4U_Ts = src.Last_Update_To_J4U_Ts                                                              
						,Offer_Requestor_Group_Cd = src.Offer_Requestor_Group_Cd                                                                     
						,Headline_Txt = src.Headline_Txt                                                                                     
						,Is_POD_Approved_Ind = src.Is_POD_Approved_Ind                                                                                
						,POD_Usage_Limit_Type_Per_User_Dsc = src.POD_Usage_Limit_Type_Per_User_Dsc                     
						,POD_Reference_Offer_Id = src.POD_Reference_Offer_Id                                                            
						,IVIE_Image_Id = src.IVIE_Image_Id                                                                                   
						,Vehicle_Name_Txt = src.Vehicle_Name_Txt                                
						,Ad_Page_Number_Txt = src.Ad_Page_Number_Txt                      
						,Ad_Mod_Nbr = src.Ad_Mod_Nbr                           
						,ECom_Dsc = src.ECom_Dsc                                 
						,Requested_User_User_Id = src.Requested_User_User_Id  
						,Requested_User_First_Nm = src.Requested_User_First_Nm              
						,Requested_User_Last_Nm = src.Requested_User_Last_Nm               
						,Is_Primary_POD_Offer_Ind = src.Is_Primary_POD_Offer_Ind              
						  
						,In_Email_Ind = src.In_Email_Ind                   
						,Submitted_Dt = src.Submitted_Dt
						,Redemption_System_Id = src.Redemption_System_Id 
						,Adbug_Txt = src.Adbug_Txt
						,Allocation_Cd = src.Allocation_Cd
						,Allocation_Nm = src.Allocation_Nm
						,DW_Logical_delete_ind = src.DW_Logical_delete_ind
						,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
						,DW_SOURCE_UPDATE_NM = FileName
						,Printed_Message_Notification_Ind = src.Printed_Message_Notification_Ind
						,Cashier_Message_Notification_Ind = src.Cashier_Message_Notification_Ind						
						,CUSTOM_OFFER_LIMIT_NBR                  = src.CUSTOM_OFFER_LIMIT_NBR
						,CUSTOM_PERIOD_NBR                       = src.CUSTOM_PERIOD_NBR
						,CUSTOM_TYPE_DSC                         = src.CUSTOM_TYPE_DSC
						,QUALIFICATION_PRODUCT_DISQUALIFIER_NM   = src.QUALIFICATION_PRODUCT_DISQUALIFIER_NM
						,IS_DISPLAY_IMMEDIATE_IND                = src.IS_DISPLAY_IMMEDIATE_IND
						,Ecomm_Promo_Type_Cd                     = src.Ecomm_Promo_Type_Cd
						,Promotion_Order_Nbr                     = src.Promotion_Order_Nbr
						,Headline2_Txt                          = src.Headline2_Txt
						,Usage_Limit_Per_Offer_Cnt              = src.Usage_Limit_Per_Offer_Cnt
						,REFUNDABLE_REWARDS_IND                 = src.REFUNDABLE_REWARDS_IND            
						,Multi_Clip_Limit_Cnt                   =src.Multi_Clip_Limit_Cnt
						,points							= src.payload_points
						,programSubType							= src.payload_programSubType
						,Ecomm_Promo_Type_Nm                = src.Ecomm_Promo_Type_Nm
						,Auto_Apply_Promo_Ind               = src.Auto_Apply_Promo_Ind
						,Valid_With_Other_Offers_Ind        = src.Valid_With_Other_Offers_Ind
						,Offer_Eligible_Order_Cnt           = src.Offer_Eligible_Order_Cnt
						,Valid_For_First_Time_Customer_Ind  = src.Valid_For_First_Time_Customer_Ind
						,Merkle_Game_Land_Nm  = src.Merkle_Game_Land_Nm
						,Merkle_Game_Land_Space_Nm  = src.Merkle_Game_Land_Space_Nm
						,Merkle_Game_Land_Space_Slot_Nm  = src.Merkle_Game_Land_Space_Slot_Nm
						,Promotion_Subprogram_Type_Cd = src.Promotion_Subprogram_Type_Cd
						,Offer_Qualification_Behavior_Cd = src.Offer_Qualification_Behavior_Cd
						,Initial_Subscription_Offer_Ind = src.Initial_Subscription_Offer_Ind
						,Dynamic_Offer_Ind = src.Dynamic_Offer_Ind
						,Days_To_Redeem_Offer_Cnt = src.Days_To_Redeem_Offer_Cnt
						,Offer_Clippable_Ind = src.Offer_Clippable_Ind
						,Offer_Applicable_Online_Ind = src.Offer_Applicable_Online_Ind
						,Offer_Displayable_Ind = src.Offer_Displayable_Ind
						FROM ( SELECT 
						OMS_Offer_Id                                                                                                         
									,External_Offer_Id                                                                                                
									,Offer_Request_Id                                                                                                
									,Aggregator_Offer_Id                                                                                           
									,Manufacturer_Id                                                                                                  
									,Manufacturer_Offer_Reference_Cd                                                  
									,Provider_Nm                                                                                                                                                           
									,Categories_Txt                                                                                                                                                 
									,Primary_Category_Txt                                                                                                                                     
									,Program_Cd                                                                                                                                                            
									,Program_Code_Dsc                                                                                                                                         
									,Subprogram_Nm                                                                                                                                             
									,Subprogram_Dsc                                                                                                                                              
									,Delivery_Channel_Cd                                                                                                                                      
									,Delivery_Channel_Dsc                                                                                                                                    
									,Offer_Status_Cd                                                                                                                                              
									,Offer_Status_Dsc                                                                                                                                             
									,Price_Title_Txt                                                                                                                                                 
									,Price_Value_Txt                                                                                                                                               
									,Savings_Value_Txt                                                                                                                                           
									,Title_Dsc
									,Title_Dsc1
									,Title_Dsc2
									,Title_Dsc3
									,Product_Dsc
									,Product_Dsc1
									,Product_Dsc2
									,Product_Dsc3
									,Disclaimer_Txt                                                                                                                                                  
									,Description_Txt                                                                                                                                                
									,Print_Tags_Ind                                                                                                                                                 
									,Product_Image_Id                                                                                                                                           
									,Price_Cd                                                                                                                                                              
									,Time_Txt                                                                                                                                                              
									,Year_Txt                                                                                                                                                              
									,Product_Cd                                                                                                                                                            
									,Is_Employee_Offer_Ind                                                                                                                                  
									,Is_Default_Allocation_Offer_Ind                                                                                                   
									,Program_Type_Cd                                                                                                                                           
									,Should_Report_Redeptions_Ind                                                                                                                   
									,Created_Ts                                                                                                                                                            
									,Created_Application_Id                                                                                                                                  
									,Created_User_Id                                                                                                                                
									,Last_Updated_Application_Id                                                                                                        
									,Last_Updated_User_Id                                                                                                                                                 
									,Last_Updated_Ts                                                                                                                                             
									,Display_Effective_Start_Dt                                                                                                             
									,Display_Effective_End_Dt                                                                                                                              
									,Effective_Start_Dt                                                                                                                                           
									,Effective_End_Dt                                                                                                                                             
									,Test_Effective_Start_Dt                                                                                                                                  
									,Test_Effective_End_Dt                                                                                                                                   
									,Qualification_Unit_Type_Dsc                                                                                                         
									,Qualification_Unite_Subtype_Dsc                                                                                                 
									,Beneifit_Value_Type_Dsc                                                                                                                              
									,Usage_Limit_Type_Per_User_Dsc                                                                                                 
									,PLU_Trigger_Barcode_Txt                                                                                                                              
									,Copient_Category_Dsc                                                                                                                                                 
									,Engine_Dsc                                                                                                                                                            
									,Priority_Cd                                                                                                                                                           
									,Tiers_Cd                                                                                                                                                              
									,Send_Outbound_Data_Dsc                                                                                                                            
									,Chargeback_Vendor_Nm                                                                                                                                             
									,Auto_Transferable_Ind                                                                                                                                   
									,Enable_Issuance_Ind                                                                                                                                      
									,Defer_Evaluation_Until_EOS_Ind                                                                                                  
									,Enable_Impression_Reporting_Ind                                                                                               
									,Limit_Eligibility_Frequency_Txt                                                                                                     
									,Is_Appliable_To_J4U_Ind                                                                                                                               
									,Customer_Segment_Dsc                                                                                                                                              
									,Assignment_User_Id                                                                                                                                       
									,Assignment_First_Nm                                                                                                                                                   
									,Assignment_Last_Nm                                                                                                                                                   
									,Qualification_Product_Disqualifier_Txt                                                                          
									,Qualification_Day_Monday_Ind                                                                                                                   
									,Qualification_Day_Tuesday_Ind                                                                                                    
									,Qualification_Day_Wednesday_Ind                                                                                              
									,Qualification_Day_Thursday_Ind                                                                                                  
									,Qualification_Day_Friday_Ind                                                                                                        
									,Qualification_Day_Saturday_Ind                                                                                                   
									,Qualification_Day_Sunday_Ind                                                                                                                     
									,Qualification_Start_Time_Txt                                                                                                        
									,Qualification_End_Time_Txt                                                                                                          
									,Qualification_Enterprise_Instant_Win_Number_Of_Prizes_Qty  
									,Qualification_Enterprise_Instant_Win_Frequency_Txt                  
									,Offer_Nm                                                                                                                                                              
									,Ad_Type_Cd                                                                                                                                                            
									,Offer_Prototype_Cd                                                                                                                                        
									,Offer_Prototype_Dsc                                                                                                                                      
									,Store_Group_Version_Id                                                                                                                               
									,Store_Tag_Print_J4U_Tag_Enabled_Ind                                                                                      
									,Store_Tag_Multiple_Nbr                                                                                                                                
									,Store_Tag_Amt                                                                                                                                                
									,Store_Tag_Comments_Txt                                                                                                                             
									,Requested_Removal_For_All_Ind                                                                                                                
									,Removed_On_Ts                                                                                                                                              
									,Removed_Unclipped_On_Ts                                                                                                                         
									,Removal_For_All_On_Ts                                                                                                                                
									,Brand_Size_Dsc                                                                                                                                                
									,Created_User_User_Id                                                                                                                                                 
									,Created_User_First_Nm                                                                                                                                               
									,Created_User_Last_Nm                                                                                                                                               
									,Updated_User_User_Id                                                                                                                                                
									,Updated_User_First_Nm                                                                                                                                             
									,Updated_User_Last_Nm                                                                                                                                              
									,First_Update_To_Redemption_Engine_Ts                                                                                  
									,Last_Update_To_Redemption_Engine_Ts                                                                                   
									,First_Update_To_J4U_Ts                                                                                                                               
									,Last_Update_To_J4U_Ts                                                                                                                                
									,Offer_Requestor_Group_Cd                                                                                                                          
									,Headline_Txt                                                                                                                                                          
									,Is_POD_Approved_Ind                                                                                                                                                  
									,POD_Usage_Limit_Type_Per_User_Dsc                                                                                       
									,POD_Reference_Offer_Id                                                                                                                              
									,IVIE_Image_Id                                                                                                                                                  
									,Vehicle_Name_Txt                                                                                                                                          
									,Ad_Page_Number_Txt                                                                                                                                                  
									,Ad_Mod_Nbr                                                                                                                                                            
									,ECom_Dsc                                                                                                                                                              
									,Requested_User_User_Id                                                                                                                              
									,Requested_User_First_Nm                                                                                                                            
									,Requested_User_Last_Nm                                                                                                                            
									,Is_Primary_POD_Offer_Ind                                                                                                                            
									                                                                                                                     
									,In_Email_Ind                                                                                                                                             
									,Submitted_Dt
									,Redemption_System_Id
									,Adbug_Txt
									,DW_Logical_delete_ind
									,FileName
									,Allocation_Cd
									,Allocation_Nm
									,Printed_Message_Notification_Ind
									,Cashier_Message_Notification_Ind
									,CUSTOM_OFFER_LIMIT_NBR
									,CUSTOM_PERIOD_NBR
									,CUSTOM_TYPE_DSC
									,QUALIFICATION_PRODUCT_DISQUALIFIER_NM
									,IS_DISPLAY_IMMEDIATE_IND	
									,Ecomm_Promo_Type_Cd
									,Promotion_Order_Nbr
									,Headline2_Txt
									,Usage_Limit_Per_Offer_Cnt
									,REFUNDABLE_REWARDS_IND
									,Multi_Clip_Limit_Cnt
									,payload_points
									,payload_programSubType
									,Ecomm_Promo_Type_Nm
									,Auto_Apply_Promo_Ind
									,Valid_With_Other_Offers_Ind
									,Offer_Eligible_Order_Cnt
									,Valid_For_First_Time_Customer_Ind
									,Merkle_Game_Land_Nm
									,Merkle_Game_Land_Space_Nm
									,Merkle_Game_Land_Space_Slot_Nm
									,Promotion_Subprogram_Type_Cd
									,Offer_Qualification_Behavior_Cd
									,Initial_Subscription_Offer_Ind
									,Dynamic_Offer_Ind
									,Days_To_Redeem_Offer_Cnt
									,Offer_Clippable_Ind  
									,Offer_Applicable_Online_Ind
									,Offer_Displayable_Ind
									FROM ${tgt_wrk_tbl}
									WHERE DML_Type = 'U'
									AND Sameday_chg_ind = 1
									AND OMS_Offer_Id IS NOT NULL									
									) src
							WHERE tgt.OMS_Offer_Id = src.OMS_Offer_Id  
							AND tgt.DW_CURRENT_VERSION_IND = TRUE`;


// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl}
                   (
                     OMS_Offer_Id  
                    ,External_Offer_Id                                                                                               
                    ,Offer_Request_Id                                                                                               
                    ,Aggregator_Offer_Id                                                                                                        
                    ,Manufacturer_Id                                                                                                
                    ,Manufacturer_Offer_Reference_Cd                                                
                    ,Provider_Nm                                                                                                                                                  
                    ,Categories_Txt                                                                                                                                               
                    ,Primary_Category_Txt                                                                                                                                   
                    ,Program_Cd                                                                                                                                                    
                    ,Program_Code_Dsc                                                                                                                                                     
                    ,Subprogram_Nm                                                                                                                                           
                    ,Subprogram_Dsc                                                                                                                                           
                    ,Delivery_Channel_Cd                                                                                                                                                  
                    ,Delivery_Channel_Dsc                                                                                                                                               
                    ,Offer_Status_Cd                                                                                                                                            
                    ,Offer_Status_Dsc                                                                                                                                           
                    ,Price_Title_Txt                                                                                                                                               
                    ,Price_Value_Txt                                                                                                                                             
                    ,Savings_Value_Txt                                                                                                                                         
                    ,Title_Dsc
					,Title_Dsc1
					,Title_Dsc2
					,Title_Dsc3
                    ,Product_Dsc
					,Product_Dsc1
					,Product_Dsc2
					,Product_Dsc3
                    ,Disclaimer_Txt                                                                                                                                               
                    ,Description_Txt                                                                                                                                              
                    ,Print_Tags_Ind                                                                                                                                               
                    ,Product_Image_Id                                                                                                                                         
                    ,Price_Cd                                                                                                                                                          
                    ,Time_Txt                                                                                                                                                         
                    ,Year_Txt                                                                                                                                                          
                    ,Product_Cd                                                                                                                                                     
                    ,Is_Employee_Offer_Ind                                                                                                                               
                    ,Is_Default_Allocation_Offer_Ind                                                                                                 
                    ,Program_Type_Cd                                                                                                                                         
                    ,Should_Report_Redeptions_Ind                                                                                                                 
                    ,Created_Ts                                                                                                                                                      
                    ,Created_Application_Id                                                                                                                               
                    ,Created_User_Id                                                                                                                              
                    ,Last_Updated_Application_Id                                                                                                                     
                    ,Last_Updated_User_Id                                                                                                                                               
                    ,Last_Updated_Ts                                                                                                                                           
                    ,Display_Effective_Start_Dt                                                                                                           
                    ,Display_Effective_End_Dt                                                                                                                            
                    ,Effective_Start_Dt                                                                                                                                         
                    ,Effective_End_Dt                                                                                                                                           
                    ,Test_Effective_Start_Dt                                                                                                                               
                    ,Test_Effective_End_Dt                                                                                                                                               
                    ,Qualification_Unit_Type_Dsc                                                                                                       
                    ,Qualification_Unite_Subtype_Dsc                                                                                               
                    ,Beneifit_Value_Type_Dsc                                                                                                                            
                    ,Usage_Limit_Type_Per_User_Dsc                                                                                               
                    ,PLU_Trigger_Barcode_Txt                                                                                                                           
                    ,Copient_Category_Dsc                                                                                                                                               
                    ,Engine_Dsc                                                                                                                                                     
                    ,Priority_Cd                                                                                                                                                      
                    ,Tiers_Cd                                                                                                                                                          
                    ,Send_Outbound_Data_Dsc                                                                                                                         
                    ,Chargeback_Vendor_Nm                                                                                                                                           
                    ,Auto_Transferable_Ind                                                                                                                                
                    ,Enable_Issuance_Ind                                                                                                                                    
                    ,Defer_Evaluation_Until_EOS_Ind                                                                                                
                    ,Enable_Impression_Reporting_Ind                                                                                             
                    ,Limit_Eligibility_Frequency_Txt                                                                                                   
                    ,Is_Appliable_To_J4U_Ind                                                                                                                            
                    ,Customer_Segment_Dsc                                                                                                                                            
                    ,Assignment_User_Id                                                                                                                                                   
                    ,Assignment_First_Nm                                                                                                                                                
                    ,Assignment_Last_Nm                                                                                                                                                 
                    ,Qualification_Product_Disqualifier_Txt                                                                          
                    ,Qualification_Day_Monday_Ind                                                                                                                 
                    ,Qualification_Day_Tuesday_Ind                                                                                                  
                    ,Qualification_Day_Wednesday_Ind                                                                                            
                    ,Qualification_Day_Thursday_Ind                                                                                                
                    ,Qualification_Day_Friday_Ind                                                                                                                     
                    ,Qualification_Day_Saturday_Ind                                                                                                 
                    ,Qualification_Day_Sunday_Ind                                                                                                                   
                    ,Qualification_Start_Time_Txt                                                                                                                     
                    ,Qualification_End_Time_Txt                                                                                                        
                    ,Qualification_Enterprise_Instant_Win_Number_Of_Prizes_Qty  
                    ,Qualification_Enterprise_Instant_Win_Frequency_Txt                  
                    ,Offer_Nm                                                                                                                                                        
                    ,Ad_Type_Cd                                                                                                                                                   
                    ,Offer_Prototype_Cd                                                                                                                                                    
                    ,Offer_Prototype_Dsc                                                                                                                                                  
                    ,Store_Group_Version_Id                                                                                                                             
                    ,Store_Tag_Print_J4U_Tag_Enabled_Ind                                                                                        
                    ,Store_Tag_Multiple_Nbr                                                                                                                              
                    ,Store_Tag_Amt                                                                                                                                              
                    ,Store_Tag_Comments_Txt                                                                                                                          
                    ,Requested_Removal_For_All_Ind                                                                                                              
                    ,Removed_On_Ts                                                                                                                                           
                    ,Removed_Unclipped_On_Ts                                                                                                                       
                    ,Removal_For_All_On_Ts                                                                                                                              
                    ,Brand_Size_Dsc                                                                                                                                              
                    ,Created_User_User_Id                                                                                                                                               
                    ,Created_User_First_Nm                                                                                                                                             
                    ,Created_User_Last_Nm                                                                                                                                             
                    ,Updated_User_User_Id                                                                                                                                              
                    ,Updated_User_First_Nm                                                                                                                                           
                    ,Updated_User_Last_Nm                                                                                                                                            
                    ,First_Update_To_Redemption_Engine_Ts                                                                                        
                    ,Last_Update_To_Redemption_Engine_Ts                                                                                        
                    ,First_Update_To_J4U_Ts                                                                                                                             
                    ,Last_Update_To_J4U_Ts                                                                                                                              
                    ,Offer_Requestor_Group_Cd                                                                                                                       
                    ,Headline_Txt                                                                                                                                                  
                    ,Is_POD_Approved_Ind                                                                                                                                               
                    ,POD_Usage_Limit_Type_Per_User_Dsc                                                                                        
                    ,POD_Reference_Offer_Id                                                                                                                            
                    ,IVIE_Image_Id                                                                                                                                                
                    ,Vehicle_Name_Txt                                                                                                                                        
                    ,Ad_Page_Number_Txt                                                                                                                                                
                    ,Ad_Mod_Nbr                                                                                                                                                  
                    ,ECom_Dsc                                                                                                                                                       
                    ,Requested_User_User_Id                                                                                                                            
                    ,Requested_User_First_Nm                                                                                                                          
                    ,Requested_User_Last_Nm                                                                                                                          
                    ,Is_Primary_POD_Offer_Ind                                                                                                                         
                                                                                                                                     
                    ,In_Email_Ind                                                                                                                                           
                    ,Submitted_Dt
					,Redemption_System_Id
					,Adbug_Txt
					,Allocation_Cd
					,Allocation_Nm
                    ,DW_First_Effective_Dt 
                    ,DW_Last_Effective_Dt              
                    ,DW_CREATE_TS
                    ,DW_LOGICAL_DELETE_IND
                    ,DW_SOURCE_CREATE_NM
                    ,DW_CURRENT_VERSION_IND
					,Printed_Message_Notification_Ind
					,Cashier_Message_Notification_Ind
					,CUSTOM_OFFER_LIMIT_NBR
                    ,CUSTOM_PERIOD_NBR
                    ,CUSTOM_TYPE_DSC
                    ,QUALIFICATION_PRODUCT_DISQUALIFIER_NM
					,IS_DISPLAY_IMMEDIATE_IND
					,Ecomm_Promo_Type_Cd
					,Promotion_Order_Nbr
					,Headline2_Txt
					,Usage_Limit_Per_Offer_Cnt
					,REFUNDABLE_REWARDS_IND
					,Multi_Clip_Limit_Cnt
					,points
					,programSubType
					,Ecomm_Promo_Type_Nm
					,Auto_Apply_Promo_Ind
					,Valid_With_Other_Offers_Ind
					,Offer_Eligible_Order_Cnt
					,Valid_For_First_Time_Customer_Ind
					,Merkle_Game_Land_Nm  
					,Merkle_Game_Land_Space_Nm					
					,Merkle_Game_Land_Space_Slot_Nm
					,Promotion_Subprogram_Type_Cd
					,Offer_Qualification_Behavior_Cd
					,Initial_Subscription_Offer_Ind
					,Dynamic_Offer_Ind
					,Days_To_Redeem_Offer_Cnt
					,Offer_Clippable_Ind  
									,Offer_Applicable_Online_Ind
									,Offer_Displayable_Ind
                   )
                   SELECT distinct
                      OMS_Offer_Id                                                                                                         
                     ,External_Offer_Id                                                                                               
                     ,Offer_Request_Id                                                                                               
                     ,Aggregator_Offer_Id                                                                                                        
                     ,Manufacturer_Id                                                                                                
                     ,Manufacturer_Offer_Reference_Cd                                                
                     ,Provider_Nm                                                                                                                                                  
                     ,Categories_Txt                                                                                                                                               
                     ,Primary_Category_Txt                                                                                                                                  
                     ,Program_Cd                                                                                                                                                    
                     ,Program_Code_Dsc                                                                                                                                                     
                     ,Subprogram_Nm                                                                                                                                           
                     ,Subprogram_Dsc                                                                                                                                           
                     ,Delivery_Channel_Cd                                                                                                                                                  
                     ,Delivery_Channel_Dsc                                                                                                                                                
                     ,Offer_Status_Cd                                                                                                                                            
                     ,Offer_Status_Dsc                                                                                                                                           
                     ,Price_Title_Txt                                                                                                                                               
                     ,Price_Value_Txt                                                                                                                                             
                     ,Savings_Value_Txt                                                                                                                                         
                     ,Title_Dsc 
					 ,Title_Dsc1
					 ,Title_Dsc2
					 ,Title_Dsc3
                     ,Product_Dsc
					 ,Product_Dsc1
					 ,Product_Dsc2
					 ,Product_Dsc3
                     ,Disclaimer_Txt                                                                                                                                               
                     ,Description_Txt                                                                                                                                              
                     ,Print_Tags_Ind                                                                                                                                               
                     ,Product_Image_Id                                                                                                                                         
                     ,Price_Cd                                                                                                                                                          
                     ,Time_Txt                                                                                                                                                         
                     ,Year_Txt                                                                                                                                                          
                     ,Product_Cd                                                                                                                                                     
                     ,Is_Employee_Offer_Ind                                                                                                                               
                     ,Is_Default_Allocation_Offer_Ind                                                                                                 
                     ,Program_Type_Cd                                                                                                                                         
                     ,Should_Report_Redeptions_Ind                                                                                                                 
                     ,Created_Ts                                                                                                                                                      
                     ,Created_Application_Id                                                                                                                               
                     ,Created_User_Id                                                                                                                              
                     ,Last_Updated_Application_Id                                                                                                                     
                     ,Last_Updated_User_Id                                                                                                                                               
                     ,Last_Updated_Ts                                                                                                                                           
                     ,Display_Effective_Start_Dt                                                                                                           
                     ,Display_Effective_End_Dt                                                                                                                            
                     ,Effective_Start_Dt                                                                                                                                         
                     ,Effective_End_Dt                                                                                                                                           
                     ,Test_Effective_Start_Dt                                                                                                                               
                     ,Test_Effective_End_Dt                                                                                                                                               
                     ,Qualification_Unit_Type_Dsc                                                                                                       
                     ,Qualification_Unite_Subtype_Dsc                                                                                               
                     ,Beneifit_Value_Type_Dsc                                                                                                                            
                     ,Usage_Limit_Type_Per_User_Dsc                                                                                               
                     ,PLU_Trigger_Barcode_Txt                                                                                                                           
                     ,Copient_Category_Dsc                                                                                                                                               
                     ,Engine_Dsc                                                                                                                                                     
                     ,Priority_Cd                                                                                                                                                      
                     ,Tiers_Cd                                                                                                                                                          
                     ,Send_Outbound_Data_Dsc                                                                                                                         
                     ,Chargeback_Vendor_Nm                                                                                                                                           
                     ,Auto_Transferable_Ind                                                                                                                                
                     ,Enable_Issuance_Ind                                                                                                                                    
                     ,Defer_Evaluation_Until_EOS_Ind                                                                                                
                     ,Enable_Impression_Reporting_Ind                                                                                             
                     ,Limit_Eligibility_Frequency_Txt                                                                                                   
                     ,Is_Appliable_To_J4U_Ind                                                                                                                             
                     ,Customer_Segment_Dsc                                                                                                                                            
                     ,Assignment_User_Id                                                                                                                                                   
                     ,Assignment_First_Nm                                                                                                                                                
                     ,Assignment_Last_Nm                                                                                                                                                 
                     ,Qualification_Product_Disqualifier_Txt                                                                          
                     ,Qualification_Day_Monday_Ind                                                                                                                 
                     ,Qualification_Day_Tuesday_Ind                                                                                                  
                     ,Qualification_Day_Wednesday_Ind                                                                                            
                     ,Qualification_Day_Thursday_Ind                                                                                                
                     ,Qualification_Day_Friday_Ind                                                                                                                     
                     ,Qualification_Day_Saturday_Ind                                                                                                 
                     ,Qualification_Day_Sunday_Ind                                                                                                                   
                     ,Qualification_Start_Time_Txt                                                                                                                     
                     ,Qualification_End_Time_Txt                                                                                                        
                     ,Qualification_Enterprise_Instant_Win_Number_Of_Prizes_Qty  
                     ,Qualification_Enterprise_Instant_Win_Frequency_Txt                  
                     ,Offer_Nm                                                                                                                                                        
                     ,Ad_Type_Cd                                                                                                                                                   
                     ,Offer_Prototype_Cd                                                                                                                                                    
                     ,Offer_Prototype_Dsc                                                                                                                                                  
                     ,Store_Group_Version_Id                                                                                                                             
                     ,Store_Tag_Print_J4U_Tag_Enabled_Ind                                                                                        
                     ,Store_Tag_Multiple_Nbr                                                                                                                              
                     ,Store_Tag_Amt                                                                                                                                              
                     ,Store_Tag_Comments_Txt                                                                                                                          
                     ,Requested_Removal_For_All_Ind                                                                                                              
                     ,Removed_On_Ts                                                                                                                                           
                     ,Removed_Unclipped_On_Ts                                                                                                                       
                     ,Removal_For_All_On_Ts                                                                                                                              
                     ,Brand_Size_Dsc                                                                                                                                              
                     ,Created_User_User_Id                                                                                                                                               
                     ,Created_User_First_Nm                                                                                                                                             
                     ,Created_User_Last_Nm                                                                                                                                             
                     ,Updated_User_User_Id                                                                                                                                              
                     ,Updated_User_First_Nm                                                                                                                                           
                     ,Updated_User_Last_Nm                                                                                                                                            
                     ,First_Update_To_Redemption_Engine_Ts                                                                                        
                     ,Last_Update_To_Redemption_Engine_Ts                                                                                        
                     ,First_Update_To_J4U_Ts                                                                                                                             
                     ,Last_Update_To_J4U_Ts                                                                                                                              
                     ,Offer_Requestor_Group_Cd                                                                                                                       
                     ,Headline_Txt                                                                                                                                                  
                     ,Is_POD_Approved_Ind                                                                                                                                               
                     ,POD_Usage_Limit_Type_Per_User_Dsc                                                                                        
                     ,POD_Reference_Offer_Id                                                                                                                            
                     ,IVIE_Image_Id                                                                                                                                                
                     ,Vehicle_Name_Txt                                                                                                                                        
                     ,Ad_Page_Number_Txt                                                                                                                                                
                     ,Ad_Mod_Nbr                                                                                                                                                  
                     ,ECom_Dsc                                                                                                                                                       
                     ,Requested_User_User_Id                                                                                                                            
                     ,Requested_User_First_Nm                                                                                                                          
                     ,Requested_User_Last_Nm                                                                                                                          
                     ,Is_Primary_POD_Offer_Ind                                                                                                                         
                                                                                                                                         
                     ,In_Email_Ind                                                                                                                                           
                     ,Submitted_Dt
					 ,Redemption_System_Id
					 ,Adbug_Txt
					 ,Allocation_Cd
					 ,Allocation_Nm
                     ,CURRENT_DATE as DW_First_Effective_dt
					 ,'31-DEC-9999'                     
					 ,CURRENT_TIMESTAMP
                     ,DW_Logical_delete_ind
                     ,FileName
                     ,TRUE
					 ,Printed_Message_Notification_Ind
					 ,Cashier_Message_Notification_Ind
					 ,CUSTOM_OFFER_LIMIT_NBR
					 ,CUSTOM_PERIOD_NBR
					 ,CUSTOM_TYPE_DSC
					 ,QUALIFICATION_PRODUCT_DISQUALIFIER_NM
					 ,IS_DISPLAY_IMMEDIATE_IND
					 ,Ecomm_Promo_Type_Cd
					,Promotion_Order_Nbr
					,Headline2_Txt
					,Usage_Limit_Per_Offer_Cnt
					,REFUNDABLE_REWARDS_IND
					,Multi_Clip_Limit_Cnt
					,payload_points
					,payload_programSubType
					,Ecomm_Promo_Type_Nm
					,Auto_Apply_Promo_Ind
					,Valid_With_Other_Offers_Ind
					,Offer_Eligible_Order_Cnt
					,Valid_For_First_Time_Customer_Ind
					,Merkle_Game_Land_Nm  
					,Merkle_Game_Land_Space_Nm					
					,Merkle_Game_Land_Space_Slot_Nm
					,Promotion_Subprogram_Type_Cd
					,Offer_Qualification_Behavior_Cd
					,Initial_Subscription_Offer_Ind
					,Dynamic_Offer_Ind
					,Days_To_Redeem_Offer_Cnt
					,Offer_Clippable_Ind  
									,Offer_Applicable_Online_Ind
									,Offer_Displayable_Ind
				FROM ${tgt_wrk_tbl}
                where OMS_Offer_Id is not null		
               
				and Sameday_chg_ind = 0`;

var sql_commit = "COMMIT"
var sql_rollback = "ROLLBACK"
try {
        snowflake.execute (
            {sqlText: sql_begin  }
            );
snowflake.execute (
            {sqlText: sql_updates  }
            );
        snowflake.execute (
            {sqlText: sql_sameday  }
            );
        snowflake.execute (
            {sqlText: sql_inserts  }
            );
        snowflake.execute (
            {sqlText: sql_commit  }
            ); 

                             }             
    catch (err)  {
        snowflake.execute (
            {sqlText: sql_rollback  }
            );

        return `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
        }

// ************** Load for OMS_Offer table ENDs *****************

$$;

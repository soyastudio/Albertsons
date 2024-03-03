--liquibase formatted sql
--changeset SYSTEM:OMS_OFFER runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view OMS_OFFER(
	OMS_OFFER_ID COMMENT 'Internal Offer Id',
	DW_FIRST_EFFECTIVE_DT COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT COMMENT 'for the current record this is 12/31/9999.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	EXTERNAL_OFFER_ID COMMENT 'External Ofer Id. Ex: Offers from TOGM, OMS and EMOM.',
	OFFER_REQUEST_ID COMMENT 'Identifier of the offer request',
	AGGREGATOR_OFFER_ID COMMENT 'Offer Aggregate Identifier',
	MANUFACTURER_ID COMMENT 'Offer Manufacturer Identifier',
	MANUFACTURER_OFFER_REFERENCE_CD COMMENT 'Offer reference code of the manufacturer',
	PROVIDER_NM COMMENT 'Name of the Offer Provider',
	CATEGORIES_TXT COMMENT 'Category of the offer. Defines the higher categorization of an offer',
	PRIMARY_CATEGORY_TXT COMMENT 'Primary Category of the offer',
	PROGRAM_CD COMMENT 'Offer Program Code',
	PROGRAM_CODE_DSC COMMENT 'Offer Program Code Description',
	SUBPROGRAM_NM COMMENT 'Subprogram Name of the offer',
	SUBPROGRAM_DSC COMMENT 'Subprogram description of the offer',
	DELIVERY_CHANNEL_CD COMMENT 'Offer Delivery Channel Code',
	DELIVERY_CHANNEL_DSC COMMENT 'Offer Delivery Channel Description',
	OFFER_STATUS_CD COMMENT 'Status code of the offer',
	OFFER_STATUS_DSC COMMENT 'Status of the offer. Defines the different stages of an offer and its effective date.',
	PRICE_TITLE_TXT COMMENT 'Offer Price Title text',
	PRICE_VALUE_TXT COMMENT 'Offer Price Value Text',
	SAVINGS_VALUE_TXT COMMENT 'Savings value of the offer when used',
	TITLE_DSC COMMENT 'Description of the Offer Title',
	TITLE_DSC1 COMMENT 'Additional Description 1 of the offer title',
	TITLE_DSC2 COMMENT 'Additional Description 2 of the offer title',
	TITLE_DSC3 COMMENT 'Additional Description 3 of the offer title',
	PRODUCT_DSC COMMENT 'Description of the product on the offer',
	PRODUCT_DSC1 COMMENT 'Additional description 1 of the product on the offer',
	PRODUCT_DSC2 COMMENT 'Additional description 2 of the product on the offer',
	PRODUCT_DSC3 COMMENT 'Additional description 3 of the product on the offer',
	DISCLAIMER_TXT COMMENT 'Offer Disclaimer Text',
	DESCRIPTION_TXT COMMENT 'long description of the offer',
	PRINT_TAGS_IND COMMENT 'Indicator if the offer tags can be printable',
	PRODUCT_IMAGE_ID COMMENT 'Identifier of the Product Image on the Offer',
	PRICE_CD COMMENT 'Offer Price Code',
	TIME_TXT COMMENT 'Offer Time Text',
	YEAR_TXT COMMENT ' Offer Year Text',
	PRODUCT_CD COMMENT 'Product code of the product on the offer',
	IS_EMPLOYEE_OFFER_IND COMMENT 'Indicator if the offer is for employees',
	IS_DEFAULT_ALLOCATION_OFFER_IND COMMENT 'Indicator if the offer has been allocated by default',
	PROGRAM_TYPE_CD COMMENT 'Program Type of the Offer',
	SHOULD_REPORT_REDEPTIONS_IND COMMENT 'Indicator if the offer redemptions has to be reported for the offer',
	CREATED_TS COMMENT 'Offer Created Timestamp',
	CREATED_APPLICATION_ID COMMENT 'Created Application Identifier',
	CREATED_USER_ID COMMENT 'Identifier of the person who created the offer',
	LAST_UPDATED_APPLICATION_ID COMMENT 'Identifier of the application which last updated the offer',
	LAST_UPDATED_USER_ID COMMENT 'Offer last updated by User Identifier',
	LAST_UPDATED_TS COMMENT 'Offer last updated timestamp',
	DISPLAY_EFFECTIVE_START_DT COMMENT 'Date from when the offer will be displayed',
	DISPLAY_EFFECTIVE_END_DT COMMENT 'Date till when the offer will be displayed',
	EFFECTIVE_START_DT COMMENT 'Offer Effective Start Date',
	EFFECTIVE_END_DT COMMENT 'Offer Effective end date',
	TEST_EFFECTIVE_START_DT COMMENT 'Date from when the offer test is effective',
	TEST_EFFECTIVE_END_DT COMMENT 'Date till when the offers test is effective',
	QUALIFICATION_UNIT_TYPE_DSC COMMENT 'Offer Qualification Unit Type description',
	QUALIFICATION_UNITE_SUBTYPE_DSC COMMENT 'Offer Qualification Unit subtype description',
	BENEIFIT_VALUE_TYPE_DSC COMMENT 'Type of the benefit value of offer',
	USAGE_LIMIT_TYPE_PER_USER_DSC COMMENT 'Usage Limit for the Offer',
	PLU_TRIGGER_BARCODE_TXT COMMENT 'Barcode Value of the PLU Trigger',
	COPIENT_CATEGORY_DSC COMMENT 'Copient Category Description',
	ENGINE_DSC COMMENT 'Name of the engine that the offer was originated from',
	PRIORITY_CD COMMENT 'Priority of the offer. Used for receipt message ordering.',
	TIERS_CD COMMENT 'Offer Tier Code',
	SEND_OUTBOUND_DATA_DSC COMMENT 'Outbound data description of the offer',
	CHARGEBACK_VENDOR_NM COMMENT 'Chargeback Vendor Name',
	AUTO_TRANSFERABLE_IND COMMENT 'Indicator if the offer is auto transferrable',
	ENABLE_ISSUANCE_IND COMMENT 'Indicator if issuance is enabled on the offer',
	DEFER_EVALUATION_UNTIL_EOS_IND COMMENT 'Indicator if the Offer is Deferred for Evaluation',
	ENABLE_IMPRESSION_REPORTING_IND COMMENT 'Indicator if the impression reporting is enabled on the offer',
	LIMIT_ELIGIBILITY_FREQUENCY_TXT COMMENT 'Eligibility frequency limit of the offer',
	IS_APPLIABLE_TO_J4U_IND COMMENT 'Indicator if the offer is applicable to J4U',
	CUSTOMER_SEGMENT_DSC COMMENT 'Offer Customer Segment Description',
	ASSIGNMENT_USER_ID COMMENT 'User Identifier of the Person who assigned the offer',
	ASSIGNMENT_FIRST_NM COMMENT 'First Name of the person who assigned the offer',
	ASSIGNMENT_LAST_NM COMMENT 'Last Name of the person who assigned the offer',
	QUALIFICATION_PRODUCT_DISQUALIFIER_TXT COMMENT 'Qualifiation product disqualifier text',
	QUALIFICATION_DAY_MONDAY_IND COMMENT 'Indicator if the qualification day of the offer is Monday',
	QUALIFICATION_DAY_TUESDAY_IND COMMENT 'Indicator if the qualification day of the offer is Tuesday',
	QUALIFICATION_DAY_WEDNESDAY_IND COMMENT 'Indicator if the qualification day of the offer is Wednesday',
	QUALIFICATION_DAY_THURSDAY_IND COMMENT 'Indicator if the qualification day of the offer is Thursday',
	QUALIFICATION_DAY_FRIDAY_IND COMMENT 'Indicator if the qualification day of the offer is Friday',
	QUALIFICATION_DAY_SATURDAY_IND COMMENT 'Indicator if the qualification day of the offer is Saturday',
	QUALIFICATION_DAY_SUNDAY_IND COMMENT 'Indicator if the qualification day of the offer is Sunday',
	QUALIFICATION_START_TIME_TXT COMMENT 'Offer Qualificaiton start time text',
	QUALIFICATION_END_TIME_TXT COMMENT 'Offer Qualification end time',
	QUALIFICATION_ENTERPRISE_INSTANT_WIN_NUMBER_OF_PRIZES_QTY COMMENT 'Offer Qualification instant win prizes quantity',
	QUALIFICATION_ENTERPRISE_INSTANT_WIN_FREQUENCY_TXT COMMENT 'Offer Qualification Instant Win frequency',
	OFFER_NM COMMENT 'Name of the Offer',
	AD_TYPE_CD COMMENT 'Type of the Advertisement',
	OFFER_PROTOTYPE_CD COMMENT 'Prototype code of the offer',
	OFFER_PROTOTYPE_DSC COMMENT 'Prototype description of the offer',
	STORE_GROUP_VERSION_ID COMMENT 'Offer Version Identifier at the store group level',
	STORE_TAG_PRINT_J4U_TAG_ENABLED_IND COMMENT 'Indicator if the J4U tag is printed on the store tag',
	STORE_TAG_MULTIPLE_NBR COMMENT 'Store tag number',
	STORE_TAG_AMT COMMENT 'Offer Amount on the store tag of the offer',
	STORE_TAG_COMMENTS_TXT COMMENT 'Comments on the store tag',
	REQUESTED_REMOVAL_FOR_ALL_IND COMMENT 'Indicator if there is a request for the offer has to be removed for all',
	REMOVED_ON_TS COMMENT 'Timestamp when the offer is removed',
	REMOVED_UNCLIPPED_ON_TS COMMENT 'Timestamp when the offer has been unclipped',
	REMOVAL_FOR_ALL_ON_TS COMMENT 'Timestamp when the offer has to be removed for all ',
	BRAND_SIZE_DSC COMMENT 'Offer Brand size description',
	CREATED_USER_USER_ID COMMENT 'User Id of the person who created the offer',
	CREATED_USER_FIRST_NM COMMENT 'First Name of the person who created the offer',
	CREATED_USER_LAST_NM COMMENT 'last name of the person who created the offer',
	UPDATED_USER_USER_ID COMMENT 'Offer last updated by User Identifier',
	UPDATED_USER_FIRST_NM COMMENT 'First name of the user who updated the offer',
	UPDATED_USER_LAST_NM COMMENT 'Last name of the user who updated the offer',
	FIRST_UPDATE_TO_REDEMPTION_ENGINE_TS COMMENT 'Timestamp when the offer was first updated to redemption engine',
	LAST_UPDATE_TO_REDEMPTION_ENGINE_TS COMMENT 'Timestamp when the offer was last updated to redemption engine',
	FIRST_UPDATE_TO_J4U_TS COMMENT 'Timestamp when the offer was first updated to J4U',
	LAST_UPDATE_TO_J4U_TS COMMENT 'Timestamp when the offer was last updated to J4U',
	OFFER_REQUESTOR_GROUP_CD COMMENT 'Group code of the offer requestor',
	HEADLINE_TXT COMMENT 'Offer Headline text',
	IS_POD_APPROVED_IND COMMENT 'Indicator if the offer is approved by POD',
	POD_USAGE_LIMIT_TYPE_PER_USER_DSC COMMENT 'Usage Limit of the Of the Offer per User at POD level',
	POD_REFERENCE_OFFER_ID COMMENT 'Offer Identifier of the POD reference',
	IVIE_IMAGE_ID COMMENT 'Identifier of the Coupon Image',
	VEHICLE_NAME_TXT COMMENT 'Vehicle Name text of offer',
	AD_PAGE_NUMBER_TXT COMMENT 'Page Number of the Offer Advertisement',
	AD_MOD_NBR COMMENT 'Advertisement MOD Number',
	ECOM_DSC COMMENT 'Online Description of the offer',
	REQUESTED_USER_USER_ID COMMENT 'User Identifier of the user who requested the offer',
	REQUESTED_USER_FIRST_NM COMMENT 'First name of the user who requested the offer',
	REQUESTED_USER_LAST_NM COMMENT 'Last name of the user who requested the offer',
	IS_PRIMARY_POD_OFFER_IND COMMENT 'Indicator if the offer is a Primary POD Offer',
	IN_EMAIL_IND COMMENT 'Is Offer sent in email Indicator',
	SUBMITTED_DT COMMENT 'Offer Submitted date',
	REDEMPTION_SYSTEM_ID COMMENT 'Identifier of the system which redeemed the offer',
	ADBUG_TXT COMMENT 'Advertisement Text',
	PRINTED_MESSAGE_NOTIFICATION_IND COMMENT 'Indicator if the printed message notifications are enabled for the offer',
	CASHIER_MESSAGE_NOTIFICATION_IND COMMENT 'Indicator for the cashiers message notification',
	ALLOCATION_CD COMMENT 'Offer Allocation code',
	ALLOCATION_NM COMMENT 'Offer Allocation Name oms offer',
	CUSTOM_OFFER_LIMIT_NBR COMMENT 'Usage Limit for the Offer',
	CUSTOM_TYPE_DSC COMMENT 'Custom Usage type of the offer',
	CUSTOM_PERIOD_NBR COMMENT 'Usage custom period of the offer',
	IS_DISPLAY_IMMEDIATE_IND COMMENT 'used for cashier message, if true, then Display Immediately; if false Display at EOS',
	QUALIFICATION_PRODUCT_DISQUALIFIER_NM COMMENT 'Disqualified product group name (there was already a property qualificationProductDisQualifier which is id of the disqualified product group)',
	DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS COMMENT 'The timestamp the record was inserted.',
	DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',
	DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d',
	DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
	ECOMM_PROMO_TYPE_CD COMMENT 'this field calls out the type of ecommerce promotion that is being done through the offer.',
	PROMOTION_ORDER_NBR COMMENT 'order number is associated to that promotion that the customer is placing.',
	HEADLINE2_TXT COMMENT 'Coupon Headline Text from second line',
	USAGE_LIMIT_PER_OFFER_CNT COMMENT 'Stores the overall usage limit of the coupon',
	REFUNDABLE_REWARDS_IND COMMENT 'defines if the rewards are Refundable or Non Refundable for particular type of requirement',
	MULTI_CLIP_LIMIT_CNT COMMENT 'Indicates the number of times offer can be clippable per user when the usage type is multi-clip',
	POINTS COMMENT 'The point/$ value associated with the offer',
	PROGRAMSUBTYPE COMMENT 'This is either \"point\" or \"usd\".  This determines how user will buy these offers.',
	ECOMM_PROMO_TYPE_NM COMMENT 'Name of a Ecomm promotion program type',
	AUTO_APPLY_PROMO_IND COMMENT 'Promo code can be auto applied without the customer entering it on the site.',
	VALID_WITH_OTHER_OFFERS_IND COMMENT 'Promo code can be used (is combinable) with other promo codes',
	OFFER_ELIGIBLE_ORDER_CNT COMMENT 'Count of number of Orderson which promotion is valid/Number of orders or the order number on which the promo ',
	VALID_FOR_FIRST_TIME_CUSTOMER_IND COMMENT 'Promo code can only apply on a customer''s first order.',
	MERKLE_GAME_LAND_NM COMMENT 'Merkle Game Land Name',
	MERKLE_GAME_LAND_SPACE_NM COMMENT 'Merkle Game Land Space Name',
	MERKLE_GAME_LAND_SPACE_SLOT_NM COMMENT 'Merkle Game Land Space Slot Name',
	PROMOTION_SUBPROGRAM_TYPE_CD COMMENT 'Subprogram Type Code',
	OFFER_QUALIFICATION_BEHAVIOR_CD COMMENT 'Behavior Code',
	INITIAL_SUBSCRIPTION_OFFER_IND,
	DYNAMIC_OFFER_IND,
	DAYS_TO_REDEEM_OFFER_CNT,
	OFFER_CLIPPABLE_IND,
	OFFER_APPLICABLE_ONLINE_IND,
	OFFER_DISPLAYABLE_IND
) COMMENT='VIEW for OMS_Offer'
 as 
SELECT
 OMS_Offer_Id           ,
 DW_First_Effective_Dt  ,
 DW_Last_Effective_Dt  ,
 External_Offer_Id       ,
 Offer_Request_Id        ,
 Aggregator_Offer_Id     ,
 Manufacturer_Id         ,
 Manufacturer_Offer_Reference_Cd    ,
 Provider_Nm             ,
 Categories_Txt          ,
 Primary_Category_Txt    ,
 Program_Cd              ,
 Program_Code_Dsc        ,
 Subprogram_Nm           ,
 Subprogram_Dsc          ,
 Delivery_Channel_Cd     ,
 Delivery_Channel_Dsc    ,
 Offer_Status_Cd         ,
 Offer_Status_Dsc        ,
 Price_Title_Txt         ,
 Price_Value_Txt         ,
 Savings_Value_Txt       ,
 Title_Dsc               ,
 Title_Dsc1              ,
 Title_Dsc2              ,
 Title_Dsc3              ,
 Product_Dsc             ,
 Product_Dsc1            ,
 Product_Dsc2            ,
 Product_Dsc3            ,
 Disclaimer_Txt          ,
 Description_Txt         ,
 Print_Tags_Ind          ,
 Product_Image_Id        ,
 Price_Cd                ,
 Time_Txt                ,
 Year_Txt                ,
 Product_Cd              ,
 Is_Employee_Offer_Ind    ,
 Is_Default_Allocation_Offer_Ind    ,
 Program_Type_Cd         ,
 Should_Report_Redeptions_Ind    ,
 Created_Ts              ,
 Created_Application_Id    ,
 Created_User_Id         ,
 Last_Updated_Application_Id    ,
 Last_Updated_User_Id    ,
 Last_Updated_Ts         ,
 Display_Effective_Start_Dt  ,
 Display_Effective_End_Dt  ,
 Effective_Start_Dt    ,
 Effective_End_Dt      ,
 Test_Effective_Start_Dt  ,
 Test_Effective_End_Dt  ,
 Qualification_Unit_Type_Dsc    ,
 Qualification_Unite_Subtype_Dsc    ,
 Beneifit_Value_Type_Dsc    ,
 Usage_Limit_Type_Per_User_Dsc    ,
 PLU_Trigger_Barcode_Txt    ,
 Copient_Category_Dsc    ,
 Engine_Dsc              ,
 Priority_Cd             ,
 Tiers_Cd                ,
 Send_Outbound_Data_Dsc    ,
 Chargeback_Vendor_Nm    ,
 Auto_Transferable_Ind    ,
 Enable_Issuance_Ind     ,
 Defer_Evaluation_Until_EOS_Ind    ,
 Enable_Impression_Reporting_Ind    ,
 Limit_Eligibility_Frequency_Txt    ,
 Is_Appliable_To_J4U_Ind    ,
 Customer_Segment_Dsc    ,
 Assignment_User_Id      ,
 Assignment_First_Nm     ,
 Assignment_Last_Nm      ,
 Qualification_Product_Disqualifier_Txt    ,
 Qualification_Day_Monday_Ind    ,
 Qualification_Day_Tuesday_Ind    ,
 Qualification_Day_Wednesday_Ind    ,
 Qualification_Day_Thursday_Ind    ,
 Qualification_Day_Friday_Ind    ,
 Qualification_Day_Saturday_Ind    ,
 Qualification_Day_Sunday_Ind    ,
 Qualification_Start_Time_Txt    ,
 Qualification_End_Time_Txt    ,
 Qualification_Enterprise_Instant_Win_Number_Of_Prizes_Qty    ,
 Qualification_Enterprise_Instant_Win_Frequency_Txt    ,
 Offer_Nm                ,
 Ad_Type_Cd              ,
 Offer_Prototype_Cd      ,
 Offer_Prototype_Dsc     ,
 Store_Group_Version_Id    ,
 Store_Tag_Print_J4U_Tag_Enabled_Ind    ,
 Store_Tag_Multiple_Nbr    ,
 Store_Tag_Amt         ,
 Store_Tag_Comments_Txt    ,
 Requested_Removal_For_All_Ind    ,
 Removed_On_Ts           ,
 Removed_Unclipped_On_Ts    ,
 Removal_For_All_On_Ts    ,
 Brand_Size_Dsc          ,
 Created_User_User_Id    ,
 Created_User_First_Nm    ,
 Created_User_Last_Nm    ,
 Updated_User_User_Id    ,
 Updated_User_First_Nm    ,
 Updated_User_Last_Nm    ,
 First_Update_To_Redemption_Engine_Ts    ,
 Last_Update_To_Redemption_Engine_Ts    ,
 First_Update_To_J4U_Ts    ,
 Last_Update_To_J4U_Ts    ,
 Offer_Requestor_Group_Cd    ,
 Headline_Txt            ,
 Is_POD_Approved_Ind     ,
 POD_Usage_Limit_Type_Per_User_Dsc    ,
 POD_Reference_Offer_Id    ,
 IVIE_Image_Id           ,
 Vehicle_Name_Txt        ,
 Ad_Page_Number_Txt      ,
 Ad_Mod_Nbr              ,
 ECom_Dsc                ,
 Requested_User_User_Id    ,
 Requested_User_First_Nm    ,
 Requested_User_Last_Nm    ,
 Is_Primary_POD_Offer_Ind    ,
 In_Email_Ind            ,
 Submitted_Dt          ,
 Redemption_System_Id,
 Adbug_Txt,
 Printed_Message_Notification_Ind,
 Cashier_Message_Notification_Ind,
 Allocation_Cd,
 Allocation_Nm, 
 CUSTOM_OFFER_LIMIT_NBR,
 CUSTOM_TYPE_DSC, 
 CUSTOM_PERIOD_NBR ,
 IS_DISPLAY_IMMEDIATE_IND ,
 QUALIFICATION_PRODUCT_DISQUALIFIER_NM ,
 DW_CREATE_TS            ,
 DW_LAST_UPDATE_TS       ,
 DW_SOURCE_CREATE_NM    ,
 DW_LOGICAL_DELETE_IND    ,
 DW_CURRENT_VERSION_IND    ,
 DW_SOURCE_UPDATE_NM ,
 Ecomm_Promo_Type_Cd ,
 Promotion_Order_Nbr,
 Headline2_Txt,
 Usage_Limit_Per_Offer_Cnt,
 REFUNDABLE_REWARDS_IND,
 Multi_Clip_Limit_Cnt,
 POINTS,
 programSubType ,
 Ecomm_Promo_Type_Nm,
 Auto_Apply_Promo_Ind,
 Valid_With_Other_Offers_Ind,
 Offer_Eligible_Order_Cnt,
 Valid_For_First_Time_Customer_Ind,
 Merkle_Game_Land_Nm,
 Merkle_Game_Land_Space_Nm,
 Merkle_Game_Land_Space_Slot_Nm,
 Promotion_Subprogram_Type_Cd,
 Offer_Qualification_Behavior_Cd,
 Initial_Subscription_Offer_Ind,
 Dynamic_Offer_Ind,
 Days_To_Redeem_Offer_Cnt,
 Offer_Clippable_Ind , 
 Offer_Applicable_Online_Ind ,
 Offer_Displayable_Ind 

FROM  <<EDM_DB_NAME>>.DW_C_PRODUCT.OMS_Offer;

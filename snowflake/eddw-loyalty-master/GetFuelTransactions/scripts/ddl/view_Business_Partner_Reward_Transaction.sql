--liquibase formatted sql
--changeset SYSTEM:Business_Partner_Reward_Transaction runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view BUSINESS_PARTNER_REWARD_TRANSACTION(
	TRANSACTION_ID COMMENT 'Transaction_Id',
	TRANSACTION_TS COMMENT 'Transaction_Ts',
	TRANSACTION_TYPE_CD COMMENT 'Transaction_Type_Cd',
	TRANSACTION_TYPE_DSC COMMENT 'Transaction_Type_Dsc',
	TRANSACTION_TYPE_SHORT_DSC COMMENT 'Transaction_Type_Short_Dsc',
	REFERENCE_NBR COMMENT 'Reference_Nbr',
	ALT_TRANSACTION_ID COMMENT 'Alt_Transaction_Id',
	ALT_TRANSACTION_TYPE_CD COMMENT 'Alt_Transaction_Type_Cd',
	ALT_TRANSACTION_TYPE_DSC COMMENT 'Alt_Transaction_Type_Dsc',
	ALT_TRANSACTION_TYPE_SHORT_DSC COMMENT 'Alt_Transaction_Type_Short_Dsc',
	PARTNER_DIVISION_ID COMMENT 'Partner_Division_Id',
	POSTAL_ZONE_CD COMMENT 'Postal_Zone_Cd',
	CUSTOMER_DIVISION_ID COMMENT 'Customer_Division_Id',
	STATUS_TYPE_DSC COMMENT 'Status_Type_Dsc',
	STATUS_TYPE_EFFECTIVE_TS COMMENT 'Status_Type_Effective_Ts',
	FUEL_PUMP_ID COMMENT 'Fuel_Pump_Id',
	REGISTER_ID COMMENT 'Register_Id',
	FUEL_GRADE_CD COMMENT 'Fuel_Grade_Cd',
	FUEL_GRADE_DSC COMMENT 'Fuel_Grade_Dsc',
	FUEL_GRADE_SHORT_DSC COMMENT 'Fuel_Grade_Short_Dsc',
	TENDER_TYPE_CD COMMENT 'Tender_Type_Cd',
	TENDER_TYPE_DSC COMMENT 'Tender_Type_Dsc',
	TENDER_TYPE_SHORT_DSC COMMENT 'Tender_Type_Short_Dsc',
	REWARD_MESSAGE_ID COMMENT 'Reward_Message_Id',
	REWARD_TOKEN_OFFERED_QTY COMMENT 'Reward_Token_Offered_Qty',
	TOTAL_PURCHASE_QTY COMMENT 'Total_Purchase_Qty',
	PURCHASE_UOM_CD COMMENT 'Purchase_UOM_Cd',
	PURCHASE_UOM_NM COMMENT 'Purchase_UOM_Nm',
	PURCHASE_DISCOUNT_LIMIT_QTY COMMENT 'Purchase_Discount_Limit_Qty',
	PURCHASE_DISCOUNT_AMT COMMENT 'Purchase_Discount_Amt',
	TOTAL_FUEL_PURCHASE_AMT COMMENT 'Total_Fuel_Purchase_Amt',
	NONFUEL_PURCHASE_AMT COMMENT 'Nonfuel_Purchase_Amt',
	TOTAL_PURCHASE_AMT COMMENT 'Total_Purchase_Amt',
	DISCOUNT_AMT COMMENT 'Discount_Amt',
	EXCEPTION_TYPE_DSC COMMENT 'Exception_Type_Dsc',
	EXCEPTION_TYPE_SHORT_DSC COMMENT 'Exception_Type_Short_Dsc',
	EXCEPTION_TRANSACTION_TS COMMENT 'Exception_Transaction_Ts',
	CREATE_TS COMMENT 'Create_Ts',
	CREATE_USER_ID COMMENT 'Create_User_Id',
	UPDATE_TS COMMENT 'Update_Ts',
	UPDATE_USER_ID COMMENT 'Update_User_Id',
	DW_FIRST_EFFECTIVE_DT COMMENT 'DW_First_Effective_Dt',
	DW_LAST_EFFECTIVE_DT COMMENT 'DW_Last_Effective_Dt',
	BUSINESS_PARTNER_INTEGRATION_ID COMMENT 'Business_Partner_Integration_Id',
	RETAIL_CUSTOMER_UUID COMMENT 'Retail_Customer_UUID',
	LAST_UPDATE_TS COMMENT 'Last_Update_Ts',
	DW_CREATE_TS COMMENT 'DW_CREATE_TS',
	DW_LAST_UPDATE_TS COMMENT 'DW_LAST_UPDATE_TS',
	DW_LOGICAL_DELETE_IND COMMENT 'DW_LOGICAL_DELETE_IND',
	DW_SOURCE_CREATE_NM COMMENT 'DW_SOURCE_CREATE_NM',
	DW_SOURCE_UPDATE_NM COMMENT 'DW_SOURCE_UPDATE_NM',
	DW_CURRENT_VERSION_IND COMMENT 'DW_CURRENT_VERSION_IND',
	STATUS_TYPE_CD COMMENT 'Status_Type_Cd',
	OLD_CLUB_CARD_NBR COMMENT 'Old_Club_Card_Nbr',
	CLUB_CARD_NBR COMMENT 'Club_Card_Nbr',
	HOUSEHOLD_ID COMMENT 'House_Hold_Id',
	CUSTOMER_PHONE_NBR COMMENT 'Customer_Phone_Nbr',
	TOTAL_SAVINGS_VALUE_AMT COMMENT 'Total Savings Value Amt',
	Alt_Transaction_Ts COMMENT 'Alternate TRansaction Timestamp'
) COMMENT='VIEW for Business_Partner_Reward_Transaction'
 as
SELECT
	 Transaction_Id        			    
    ,Transaction_Ts        			    
    ,Transaction_Type_Cd   			    
    ,Transaction_Type_Dsc  			    
    ,Transaction_Type_Short_Dsc 	    
    ,Reference_Nbr         			    
    ,Alt_Transaction_Id    			    
    ,Alt_Transaction_Type_Cd  		    
    ,Alt_Transaction_Type_Dsc 		    
    ,Alt_Transaction_Type_Short_Dsc     
    ,Partner_Division_Id   			    
    ,Postal_Zone_Cd        			    
    ,Customer_Division_Id  			    
    ,Status_Type_Dsc       			    
    ,Status_Type_Effective_Ts  		    
    ,Fuel_Pump_Id          			    
    ,Register_Id           			    
    ,Fuel_Grade_Cd         			    
    ,Fuel_Grade_Dsc        			    
    ,Fuel_Grade_Short_Dsc  			    
    ,Tender_Type_Cd        			    
    ,Tender_Type_Dsc       			    
    ,Tender_Type_Short_Dsc 			    
    ,Reward_Message_Id     			    
    ,Reward_Token_Offered_Qty  		    
    ,Total_Purchase_Qty    			    
    ,Purchase_UOM_Cd       			    
    ,Purchase_UOM_Nm       			    
    ,Purchase_Discount_Limit_Qty  	    
    ,Purchase_Discount_Amt  		    
    ,Total_Fuel_Purchase_Amt  		    
    ,Nonfuel_Purchase_Amt  			    
    ,Total_Purchase_Amt    			    
    ,Discount_Amt          			    
    ,Exception_Type_Dsc    			    
    ,Exception_Type_Short_Dsc  		    
    ,Exception_Transaction_Ts  		    
    ,Create_Ts             			    
    ,Create_User_Id        			    
    ,Update_Ts             			    
    ,Update_User_Id        			    
    ,DW_First_Effective_Dt 			    
    ,DW_Last_Effective_Dt  			    
    ,Business_Partner_Integration_Id    
    ,Retail_Customer_UUID  				
    ,Last_Update_Ts        				
    ,DW_CREATE_TS          				
    ,DW_LAST_UPDATE_TS     				
    ,DW_LOGICAL_DELETE_IND 				
    ,DW_SOURCE_CREATE_NM   				
    ,DW_SOURCE_UPDATE_NM   				
    ,DW_CURRENT_VERSION_IND				
    ,Status_Type_Cd        				
    ,Old_Club_Card_Nbr  
	,Club_Card_Nbr                         
    ,HouseHold_Id                         
    ,Customer_Phone_Nbr
    ,Total_Savings_Value_Amt
    ,Alt_Transaction_Ts
FROM  <<EDM_DB_NAME>>.DW_C_Loyalty.Business_Partner_Reward_Transaction ;

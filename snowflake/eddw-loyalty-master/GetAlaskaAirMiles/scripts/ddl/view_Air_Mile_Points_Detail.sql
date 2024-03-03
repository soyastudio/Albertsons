--liquibase formatted sql
--changeset SYSTEM:Air_Mile_Points_Detail runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view AIR_MILE_POINTS_DETAIL(
	BATCH_ID COMMENT 'BatchId also known as External BatchId. This represents identifier to the batch file containing the summary of AirMiles records.',
	HOUSEHOLD_ID COMMENT 'Unique identifier of the Household. The aggregateId in CHMS service is the HHID unlike the other services where the aggegatedid we map to the CustomerId.',
	TRANSACTION_ID COMMENT 'Transaction Id for AirMiles.',
	TRANSACTION_TS COMMENT 'This is the timestamp when the transaction was updated.',
	DW_FIRST_EFFECTIVE_DT COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT COMMENT 'for the current record this is 12/31/9999.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	TRANSACTION_DATE_TXT COMMENT 'This is the date when the transaction was updated.',
	TRANSACTION_TYPE_CD COMMENT 'Transaction type code for the transaction.',
	TRANSACTION_TYPE_DSC COMMENT 'Transaction type Description for the transaction.',
	TRANSACTION_TYPE_SHORT_DSC COMMENT 'Transaction type Short Description for the transaction.',
	TRANSACTION_REASON_CD COMMENT 'Transaction Reason code for the transaction.',
	TRANSACTION_REASON_DSC COMMENT 'Transaction Reason Description for the transaction.',
	TRANSACTION_REASON_SHORT_DSC COMMENT 'Transaction Reason Short Description for the transaction.',
	TRANSACTION_REFERENCE_NBR COMMENT 'Can be used as a cross reference nbr from external systems for the internal transaction id.',
	ALTERNATE_TRANSACTION_ID COMMENT 'Alternate transaction Ids linked to the partner transaction.',
	ALTERNATE_TRANSACTION_TYPE_CD COMMENT 'Alternate transaction Code linked to the partner transaction.',
	ALTERNATE_TRANSACTION_TYPE_DSC COMMENT 'Alternate transaction Description linked to the partner transaction.',
	ALTERNATE_TRANSACTION_TYPE_SHORT_DSC COMMENT 'Alternate transaction Short Description linked to the partner transaction.',
	ALTERNATE_TRANSACTION_TS COMMENT 'This is the timestamp when the transaction was updated.',
	RECORD_TYPE_CD COMMENT 'Code that Can be used to specify type of record.',
	RECORD_TYPE_DSC COMMENT 'Description that Can be used to specify type of record.',
	RECORD_TYPE_SHORT_DSC COMMENT 'Short Description that Can be used to specify type of record.',
	AIR_MILE_PROGRAM_ID COMMENT 'AirMilePoints program Unique Identifier',
	AIR_MILE_PROGRAM_NM COMMENT 'AirMilePoints program name',
	AIR_MILE_TIER_NM COMMENT 'AirMilePoints Tier program name',
	AIR_MILE_POINT_QTY COMMENT 'Number of AirMiles in the Transaction file. Note that AirMiles are calculated from  customers rewards/points they have earned.',
	CUSTOMER_FORMATTED_NM COMMENT 'Customer formatted name invloved with the transaction.',
	CUSTOMER_PREFERRED_SALUTION_CD COMMENT 'Customer preferred Salution Code invloved with the transaction.',
	CUSTOMER_TITLE_CD COMMENT 'Customer Title Code invloved with the transaction.',
	CUSTOMER_GIVEN_NM COMMENT 'Customer Given name invloved with the transaction.',
	CUSTOMER_NICK_NM COMMENT 'Customer Nick name invloved with the transaction.',
	CUSTOMER_MIDDLE_NM COMMENT 'Middle Name of the Customer involved with the Transaction',
	CUSTOMER_FAMILY_NM COMMENT 'Family Name of the Customer involved with the Transaction',
	CUSTOMER_MAIDEN_NM COMMENT 'Maiden Name of the Customer involved with the Transaction',
	CUSTOMER_GENERATION_AFFIX_CD COMMENT 'Customer Generation Affix code involved with the Transaction',
	CUSTOMER_QUALIFICATION_AFFIX_CD COMMENT 'Customer Qualification Affix Code involved with the transaction',
	CREATE_TS COMMENT 'Date and time when the record was created in the source system.',
	CREATE_USER_ID COMMENT 'User Id of the record created in the source system record',
	UPDATE_TS COMMENT 'Last updated timestamp of the source sytem record.',
	UPDATE_USER_ID COMMENT 'User Id of the last updated in the source system record',
	SOURCE_EXTRACT_TS COMMENT 'Source Extract Create Timestamp',
	SOURCE_TYPE_CD COMMENT 'Indicates if the AirMilePoints are SUMMARY or DETAIL payload.',
	DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
) COMMENT='VIEW for Air_Mile_Points_DETAIL'
 as
select
 Batch_Id                             
,Household_Id                         
,Transaction_Id                     
,Transaction_Ts                       
,DW_First_Effective_Dt                
,DW_Last_Effective_Dt                 
,Transaction_Date_Txt                       
,Transaction_Type_Cd                  
,Transaction_Type_Dsc                 
,Transaction_Type_Short_Dsc           
,Transaction_Reason_Cd                
,Transaction_Reason_Dsc               
,Transaction_Reason_Short_Dsc         
,Transaction_Reference_Nbr            
,Alternate_Transaction_Id             
,Alternate_Transaction_Type_Cd        
,Alternate_Transaction_Type_Dsc       
,Alternate_Transaction_Type_Short_Dsc 
,Alternate_Transaction_Ts             
,Record_Type_Cd                       
,Record_Type_Dsc                      
,Record_Type_Short_Dsc                
,Air_Mile_Program_Id                  
,Air_Mile_Program_Nm                  
,Air_Mile_Tier_Nm                     
,Air_Mile_Point_Qty                   
,Customer_Formatted_Nm                
,Customer_Preferred_Salution_Cd       
,Customer_Title_Cd                    
,Customer_Given_Nm                    
,Customer_Nick_Nm                     
,Customer_Middle_Nm                   
,Customer_Family_Nm                   
,Customer_Maiden_Nm                   
,Customer_Generation_Affix_Cd         
,Customer_Qualification_Affix_Cd      
,Create_Ts                            
,Create_User_Id                       
,Update_Ts                            
,Update_User_Id 
,Source_Extract_Ts                      
,Source_Type_cd                       
,DW_CREATE_TS                         
,DW_LAST_UPDATE_TS                    
,DW_LOGICAL_DELETE_IND                
,DW_SOURCE_CREATE_NM                  
,DW_SOURCE_UPDATE_NM                  
,DW_CURRENT_VERSION_IND 
from  <<EDM_DB_NAME>>.DW_C_LOYALTY.Air_Mile_Points_DETAIL;

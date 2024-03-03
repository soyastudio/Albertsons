--liquibase formatted sql
--changeset SYSTEM:OMS_Offer_Printed_Message runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view OMS_OFFER_PRINTED_MESSAGE(
	OMS_OFFER_ID COMMENT 'Offer Id from OMS Offer',
	PRINTED_MESSAGE_LEVEL_NBR COMMENT 'Message level number in printed message',
	DW_FIRST_EFFECTIVE_DT COMMENT 'Record first inserted date',
	DW_LAST_EFFECTIVE_DT COMMENT 'Record last updated date',
	PRINTED_MESSAGE_CD COMMENT 'Message code in printed message',
	DW_CREATE_TS COMMENT 'Record inserted timestamp',
	DW_LAST_UPDATE_TS COMMENT 'Record updated timestamp',
	DW_SOURCE_CREATE_NM COMMENT 'Source Filename',
	DW_LOGICAL_DELETE_IND COMMENT 'Delete scenario indicator',
	DW_CURRENT_VERSION_IND COMMENT 'To find the latest record',
	DW_SOURCE_UPDATE_NM COMMENT 'Source filename based on SCD Types'
) COMMENT='VIEW for OMS_Offer_Printed_Message'
 as
SELECT
	  OMS_Offer_Id          
	 ,Printed_Message_Level_Nbr
	 ,DW_First_Effective_Dt 
	 ,DW_Last_Effective_Dt  
	 ,Printed_Message_Cd    
	 ,DW_CREATE_TS          
	 ,DW_LAST_UPDATE_TS     
	 ,DW_SOURCE_CREATE_NM   
	 ,DW_LOGICAL_DELETE_IND 
	 ,DW_CURRENT_VERSION_IND
	 ,DW_SOURCE_UPDATE_NM   
FROM  <<EDM_DB_NAME>>.DW_C_PRODUCT.OMS_Offer_Printed_Message;

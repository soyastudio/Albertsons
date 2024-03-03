--liquibase formatted sql
--changeset SYSTEM:OMS_Offer_Cashier_Message_Tier runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view OMS_OFFER_CASHIER_MESSAGE_TIER(
	OMS_OFFER_ID COMMENT 'Offer Id from OMS Offer',
	CASHIER_MESSAGE_LEVEL_NBR COMMENT 'Message level cashier number',
	DW_FIRST_EFFECTIVE_DT COMMENT 'Record first inserted date',
	DW_LAST_EFFECTIVE_DT COMMENT 'Record last updated date',
	CASHIER_MESSAGE_BEEP_TYPE_TXT COMMENT 'Type text of cashier message',
	CASHIER_MESSAGE_BEEP_DURATION_NBR COMMENT 'Duration number of cashier message',
	CASHIER_MESSAGE_LINE1_TXT COMMENT 'line 1 text of cashier message',
	CASHIER_MESSAGE_LINE2_TXT COMMENT 'line 2 text of cashier message',
	DW_CREATE_TS COMMENT 'Record inserted timestamp',
	DW_LAST_UPDATE_TS COMMENT 'Record updated timestamp',
	DW_SOURCE_CREATE_NM COMMENT 'Source Filename',
	DW_LOGICAL_DELETE_IND COMMENT 'Delete scenario indicator',
	DW_CURRENT_VERSION_IND COMMENT 'To find the latest record',
	DW_SOURCE_UPDATE_NM COMMENT 'Source filename based on SCD Types'
) COMMENT='VIEW for OMS_Offer_Cashier_Message_Tier'
 as
SELECT
	 OMS_Offer_Id,
	 Cashier_Message_Level_Nbr,
	 DW_First_Effective_Dt,
	 DW_Last_Effective_Dt,
	 Cashier_Message_Beep_Type_Txt,
	 Cashier_Message_Beep_Duration_Nbr,
	 Cashier_Message_Line1_Txt,
	 Cashier_Message_Line2_Txt,
	 DW_CREATE_TS,
	 DW_LAST_UPDATE_TS,
	 DW_SOURCE_CREATE_NM,
	 DW_LOGICAL_DELETE_IND,
	 DW_CURRENT_VERSION_IND,
	 DW_SOURCE_UPDATE_NM
FROM  <<EDM_DB_NAME>>.DW_C_PRODUCT.OMS_Offer_Cashier_Message_Tier;

--liquibase formatted sql
--changeset SYSTEM:Business_Partner_Reward_Reconciliation_Error runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view BUSINESS_PARTNER_REWARD_RECONCILIATION_ERROR(
	RECONCILATION_ERROR_TYPE_CD COMMENT 'Reconcilation Error Type Cd',
	RECONCILATION_ERROR_TYPE_DSC COMMENT 'Reconcilation Error Type Dsc',
	RECONCILATION_ERROR_TYPE_SHORT_DSC COMMENT 'Reconcilation Error Type Short Dsc',
	TRANSACTION_ID COMMENT 'Transaction Id',
	DW_FIRST_EFFECTIVE_DT COMMENT 'DW First Effective Dt',
	DW_LAST_EFFECTIVE_DT COMMENT 'DW First Effective Dt',
	BUSINESS_PARTNER_INTEGRATION_ID COMMENT 'Business Partner Integration Id',
	TRANSACTION_TS COMMENT 'Transaction Ts',
	SEQUENCE_NBR COMMENT ' Sequence_Nbr',
	DW_CREATE_TS COMMENT 'DW CREATE TS',
	DW_LAST_UPDATE_TS COMMENT 'DW LAST UPDATE TS',
	DW_LOGICAL_DELETE_IND COMMENT 'DW LOGICAL DELETE IND',
	DW_SOURCE_CREATE_NM COMMENT 'DW SOURCE CREATE NM',
	DW_SOURCE_UPDATE_NM COMMENT 'DW SOURCE UPDATE NM',
	DW_CURRENT_VERSION_IND COMMENT 'DW CURRENT VERSION IND'
) COMMENT='VIEW for Business_Partner_Reward_Reconciliation_Error'
 as
select
 Reconcilation_Error_Type_Cd           ,
 Reconcilation_Error_Type_Dsc          ,
 Reconcilation_Error_Type_Short_Dsc    ,
 Transaction_Id                        ,
 DW_First_Effective_Dt                 ,
 DW_Last_Effective_Dt                  ,
 Business_Partner_Integration_Id       ,
 Transaction_Ts                        ,
 Sequence_Nbr                          ,
 DW_CREATE_TS               ,
 DW_LAST_UPDATE_TS             ,
 DW_LOGICAL_DELETE_IND      ,
 DW_SOURCE_CREATE_NM     ,    
 DW_SOURCE_UPDATE_NM      ,   
 DW_CURRENT_VERSION_IND
FROM  <EDM_DB_NAME>>.DW_C_Loyalty.Business_Partner_Reward_Reconciliation_Error ;

--liquibase formatted sql
--changeset SYSTEM:Oms_Excluded_Promotion runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database EDM_VIEWS_PRD;
use schema EDM_VIEWS_PRD.DW_VIEWS;

create or replace view Oms_Excluded_Promotion(
Oms_Offer_Id      COMMENT 'Entity to define Promotion excluded to combine with the main offer in OMS_OFFER table'    
,Promo_Cd               COMMENT 'Promo code cannot be used (not combinable) with other promo codes'
,Dw_First_Effective_Dt  COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key'
,Dw_Last_Effective_Dt  COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day'
,Dw_Create_Ts          COMMENT 'The timestamp the record was inserted.'
,Dw_Last_Update_Ts     COMMENT 'When a record is updated  this would be the current timestamp'
,Dw_Logical_Delete_Ind  COMMENT 'Set to True when we receive a delete record for the primary key, else False'
,Dw_Source_Create_Nm   COMMENT 'The Bod (data source) name of this insert.'
,Dw_Source_Update_Nm   COMMENT 'The Bod (data source) name of this update or delete.'
,Dw_Current_Version_Ind COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
) COMMENT='VIEW for Oms_Excluded_Promotion'
 as 
SELECT
Oms_Offer_Id          
,Promo_Cd              
,Dw_First_Effective_Dt 
,Dw_Last_Effective_Dt  
,Dw_Create_Ts          
,Dw_Last_Update_Ts     
,Dw_Logical_Delete_Ind 
,Dw_Source_Create_Nm   
,Dw_Source_Update_Nm   
,Dw_Current_Version_Ind
FROM EDM_CONFIRMED_PRD.DW_C_PRODUCT.Oms_Excluded_Promotion;

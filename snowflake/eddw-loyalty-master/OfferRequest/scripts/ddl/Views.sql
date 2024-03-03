--liquibase formatted sql
--changeset SYSTEM:Views runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view OFFER_REQUEST_BUY_PRODUCT_GROUP(
	OFFER_REQUEST_ID,
	USER_INTERFACE_UNIQUE_ID,
	PRODUCT_GROUP_NM,
	PRODUCT_GROUP_VERSION_ID,
	STORE_GROUP_VERSION_ID,
	DW_FIRST_EFFECTIVE_DT,
	DW_LAST_EFFECTIVE_DT,
	PRODUCT_GROUP_ID,
	PRODUCT_GROUP_DSC,
	DISPLAY_ORDER_NBR,
	ITEM_QTY,
	UNIT_OF_MEASURE_CD,
	UNIT_OF_MEASURE_NM,
	UNIT_OF_MEASURE_DSC,
	GIFT_CARD_IND,
	ANY_PRODUCT_IND,
	UNIQUE_ITEM_IND,
	CONJUNCTION_DSC,
	MINIMUM_PURCHASE_AMT,
	MAXIMUM_PURCHASE_AMT,
	INHERITED_IND,
	EXCLUDED_PRODUCT_GROUP_ID,
	EXCLUDED_PRODUCT_GROUP_NM,
	DW_CREATE_TS,
	DW_LAST_UPDATE_TS,
	DW_LOGICAL_DELETE_IND,
	DW_SOURCE_CREATE_NM,
	DW_SOURCE_UPDATE_NM,
	DW_CURRENT_VERSION_IND
) COMMENT='View For OFFER_REQUEST_BUY_PRODUCT_GROUP'
 as 
Select 
OFFER_REQUEST_ID
,USER_INTERFACE_UNIQUE_ID
,PRODUCT_GROUP_NM
,PRODUCT_GROUP_VERSION_ID
,STORE_GROUP_VERSION_ID
,DW_FIRST_EFFECTIVE_DT
,DW_LAST_EFFECTIVE_DT
,PRODUCT_GROUP_ID
,PRODUCT_GROUP_DSC
,DISPLAY_ORDER_NBR
,ITEM_QTY
,UNIT_OF_MEASURE_CD
,UNIT_OF_MEASURE_NM
,UNIT_OF_MEASURE_DSC
,GIFT_CARD_IND
,ANY_PRODUCT_IND
,UNIQUE_ITEM_IND
,CONJUNCTION_DSC
,MINIMUM_PURCHASE_AMT
,MAXIMUM_PURCHASE_AMT
,INHERITED_IND
,EXCLUDED_PRODUCT_GROUP_ID
,EXCLUDED_PRODUCT_GROUP_NM
,DW_CREATE_TS
,DW_LAST_UPDATE_TS
,DW_LOGICAL_DELETE_IND
,DW_SOURCE_CREATE_NM
,DW_SOURCE_UPDATE_NM
,DW_CURRENT_VERSION_IND
From <<EDM_DB_NAME>>.DW_C_PURCHASING.OFFER_REQUEST_BUY_PRODUCT_GROUP;

create or replace view OFFER_REQUEST_EXCLUDED_PROMOTION(
	OFFER_REQUEST_ID COMMENT 'Unique identifer for each offer request created in source system',
	PROMO_CD COMMENT 'Promo code cannot be used (not combinable) with other promo codes',
	DW_FIRST_EFFECTIVE_DT COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT COMMENT 'for the current record this is ''12/31/9999''.for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
) COMMENT='VIEW for Offer_Request_Excluded_Promotion'
 as
SELECT
Offer_Request_Id ,
Promo_Cd  ,     
Dw_First_Effective_Dt  ,
Dw_Last_Effective_Dt ,  
Dw_Create_Ts  ,      
Dw_Last_Update_Ts ,    
Dw_Logical_Delete_Ind , 
Dw_Source_Create_Nm  , 
Dw_Source_Update_Nm  , 
Dw_Current_Version_Ind
FROM <<EDM_DB_NAME>>.DW_C_PURCHASING.Offer_Request_Excluded_Promotion;

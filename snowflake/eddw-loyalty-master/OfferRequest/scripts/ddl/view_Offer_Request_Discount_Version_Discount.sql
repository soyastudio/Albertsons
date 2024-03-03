--liquibase formatted sql
--changeset SYSTEM:Offer_Request_Discount_Version_Discount runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database EDM_VIEWS_PRD;
use schema EDM_VIEWS_PRD.DW_VIEWS;

create or replace view OFFER_REQUEST_DISCOUNT_VERSION_DISCOUNT(
	OFFER_REQUEST_ID COMMENT 'Offer_Request_Id',
	USER_INTERFACE_UNIQUE_ID COMMENT 'User_Interface_Unique_Id',
	DISCOUNT_VERSION_ID COMMENT 'Discount_Version_Id ',
	DISCOUNT_ID COMMENT 'Discount_Id ',
	DW_FIRST_EFFECTIVE_DT COMMENT 'DW_First_Effective_Dt ',
	DW_LAST_EFFECTIVE_DT COMMENT 'DW_Last_Effective_Dt',
	PRODUCT_GROUP_ID COMMENT 'Product_Group_Id ',
	DISCOUNT_TYPE_CD COMMENT 'Discount_Type_Cd ',
	DISCOUNT_TYPE_DSC COMMENT 'Discount_Type_Dsc',
	DISCOUNT_TYPE_SHORT_DSC COMMENT 'Discount_Type_Short_Dsc ',
	BENEFIT_VALUE_TYPE_CODE COMMENT 'Benefit_Value_Type_Code ',
	BENEFIT_VALUE_TYPE_DSC COMMENT 'Benefit_Value_Type_Dsc',
	BENEFIT_VALUE_TYPE_SHORT_DSC COMMENT 'Benefit_Value_Type_Short_Dsc',
	BENEFIT_VALUE_QTY COMMENT 'Benefit_Value_Qty',
	INCLUDED_PRODUCT_GROUP_ID COMMENT 'Included_Product_Group_Id ',
	INCLUDED_PRODUCT_GROUP_NM COMMENT 'Included_Product_Group_Nm',
	EXCLUDED_PRODUCT_GROUP_ID COMMENT 'Excluded_Product_Group_Id',
	EXCLUDED_PRODUCT_GROUP_NM COMMENT 'Excluded_Product_Group_Nm',
	CHARGEBACK_DEPARTMENT_ID COMMENT 'Chargeback_Department_Id',
	CHARGEBACK_DEPARTMENT_NM COMMENT 'Chargeback_Department_Nm',
	DISPLAY_ORDER_NBR COMMENT 'Display_Order_Nbr',
	DW_CREATE_TS COMMENT 'DW_CREATE_TS',
	DW_LAST_UPDATE_TS COMMENT 'DW_LAST_UPDATE_TS ',
	DW_LOGICAL_DELETE_IND COMMENT 'DW_LOGICAL_DELETE_IND',
	DW_SOURCE_CREATE_NM COMMENT 'DW_SOURCE_CREATE_NM ',
	DW_SOURCE_UPDATE_NM COMMENT 'DW_SOURCE_UPDATE_NM ',
	DW_CURRENT_VERSION_IND COMMENT 'DW_CURRENT_VERSION_IND'
) COMMENT='View For Offer_Request_Discount_Version_Discount'
 as 
Select
Offer_Request_Id      ,
User_Interface_Unique_Id  ,
Discount_Version_Id   ,
Discount_Id           ,
DW_First_Effective_Dt  ,
DW_Last_Effective_Dt  ,
Product_Group_Id      ,
Discount_Type_Cd      ,
Discount_Type_Dsc     ,
Discount_Type_Short_Dsc  ,
Benefit_Value_Type_Code  ,
Benefit_Value_Type_Dsc  ,
Benefit_Value_Type_Short_Dsc  ,
Benefit_Value_Qty     ,
Included_Product_Group_Id  ,
Included_Product_Group_Nm  ,
Excluded_Product_Group_Id  ,
Excluded_Product_Group_Nm  ,
Chargeback_Department_Id  ,
Chargeback_Department_Nm  ,
Display_Order_Nbr     ,
DW_CREATE_TS          ,
DW_LAST_UPDATE_TS     ,
DW_LOGICAL_DELETE_IND  ,
DW_SOURCE_CREATE_NM   ,
DW_SOURCE_UPDATE_NM   ,
DW_CURRENT_VERSION_IND  
From EDM_CONFIRMED_PRD.DW_C_PURCHASING.Offer_Request_Discount_Version_Discount;
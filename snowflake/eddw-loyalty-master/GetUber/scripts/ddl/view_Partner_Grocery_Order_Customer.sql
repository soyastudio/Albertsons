--liquibase formatted sql
--changeset SYSTEM:view_PARTNER_GROCERY_ORDER_CUSTOMER runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view PARTNER_GROCERY_ORDER_CUSTOMER(
	PARTNER_GROCERY_ORDER_CUSTOMER_INTEGRATION_ID COMMENT 'Partner_Grocery_Order_Customer_Integration_Id',
	DW_FIRST_EFFECTIVE_DT COMMENT 'DW_First_Effective_Dt',
	DW_LAST_EFFECTIVE_DT COMMENT 'DW_Last_Effective_Dt',
	SOURCE_CUSTOMER_ID COMMENT 'Source_Customer_Id',
	LOYALTY_PHONE_NBR COMMENT 'Loyalty_Phone_Nbr',
	FIRST_NM COMMENT 'First_Nm',
	LAST_NM COMMENT 'Last_Nm',
	EMAIL_ADDRESS_TXT COMMENT 'Email_Address_Txt',
	CONTACT_PHONE_NBR COMMENT 'Contact_Phone_Nbr',
	RETAIL_CUSTOMER_UUID COMMENT 'Retail_Customer_UUID',
	DW_CREATE_TS COMMENT 'DW_CREATE_TS',
	DW_LAST_UPDATE_TS COMMENT 'DW_LAST_UPDATE_TS',
	DW_SOURCE_CREATE_NM COMMENT 'DW_SOURCE_CREATE_NM',
	DW_SOURCE_UPDATE_NM COMMENT 'DW_SOURCE_UPDATE_NM',
	DW_LOGICAL_DELETE_IND COMMENT 'DW_LOGICAL_DELETE_IND',
	DW_CURRENT_VERSION_IND COMMENT 'DW_CURRENT_VERSION_IND'
) COMMENT='VIEW for Partner_Grocery_Order_Customer'
 as
select
Partner_Grocery_Order_Customer_Integration_Id   ,  
DW_First_Effective_Dt	,
DW_Last_Effective_Dt	,
Source_Customer_Id      ,
 Loyalty_Phone_Nbr     ,
 First_Nm                ,
 Last_Nm                ,
 Email_Address_Txt       ,
 Contact_Phone_Nbr      ,
 Retail_Customer_UUID    , 
DW_CREATE_TS	,
DW_LAST_UPDATE_TS	,
DW_SOURCE_CREATE_NM	,
DW_SOURCE_UPDATE_NM,
DW_LOGICAL_DELETE_IND,
DW_CURRENT_VERSION_IND
from  <<EDM_DB_NAME>>.DW_C_Loyalty.Partner_Grocery_Order_Customer;

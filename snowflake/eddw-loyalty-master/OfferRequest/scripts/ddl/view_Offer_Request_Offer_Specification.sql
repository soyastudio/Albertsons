--liquibase formatted sql
--changeset SYSTEM:Offer_Request_Offer_Specification runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database EDM_VIEWS_PRD;
use schema EDM_VIEWS_PRD.DW_VIEWS;

create or replace view OFFER_REQUEST_OFFER_SPECIFICATION(
	OFFER_REQUEST_ID COMMENT 'Offer_Request_Id',
	USER_INTERFACE_UNIQUE_ID COMMENT 'User_Interface_Unique_Id',
	DW_FIRST_EFFECTIVE_DT COMMENT 'DW_First_Effective_Dt',
	DW_LAST_EFFECTIVE_DT COMMENT 'DW_Last_Effective_Dt',
	STORE_GROUP_VERSION_ID COMMENT 'Store_Group_Version_Id',
	DISPLAY_ORDER_NBR COMMENT 'Display_Order_Nbr',
	PROTO_TYPE_CD COMMENT 'Proto_Type_Cd',
	PROTO_TYPE_DSC COMMENT 'Proto_Type_Dsc',
	PROTO_TYPE_SHORT_DSC COMMENT 'Proto_Type_Short_Dsc',
	STORE_TAG_NBR COMMENT 'Store_Tag_Nbr ',
	STORE_TAG_AMT COMMENT 'Store_Tag_Amt',
	LOYALTY_PROGRAM_TAG_IND COMMENT 'Loyalty_Program_Tag_Ind',
	STORE_TAG_DSC COMMENT 'Store_Tag_Dsc',
	POD_HEADLINE_TXT COMMENT 'POD_Headline_Txt',
	POD_HEADLINE_SUB_TXT COMMENT 'POD_Headline_Sub_Txt',
	POD_OFFER_DSC COMMENT 'POD_Offer_Dsc',
	POD_OFFER_DETAIL_CD COMMENT 'POD_Offer_Detail_Cd ',
	POD_OFFER_DETAIL_DSC COMMENT 'POD_Offer_Detail_Dsc',
	POD_OFFER_DETAIL_SHORT_DSC COMMENT 'POD_Offer_Detail_Short_Dsc',
	POD_PRICE_INFO_TXT COMMENT 'POD_Price_Info_Txt',
	POD_ITEM_QTY COMMENT 'POD_Item_Qty',
	POD_UNIT_OF_MEASURE_CD COMMENT 'POD_Unit_Of_Measure_Cd ',
	POD_UNIT_OF_MEASURE_NM COMMENT 'POD_Unit_Of_Measure_Nm ',
	USAGE_LIMIT_TYPE_TXT COMMENT 'Usage_Limit_Type_Txt',
	POD_DISCLAIMER_TXT COMMENT 'POD_Disclaimer_Txt',
	POD_DISPLAY_START_DT COMMENT 'POD_Display_Start_Dt',
	POD_DISPLAY_END_DT COMMENT 'POD_Display_End_Dt ',
	POD_CUSTOMER_FRIENDLY_CATEGORY_CD COMMENT 'POD_Customer_Friendly_Category_Cd',
	POD_CUSTOMER_FRIENDLY_CATEGORY_DSC COMMENT 'POD_Customer_Friendly_Category_Dsc',
	POD_CUSTOMER_FRIENDLY_CATEGORY_SHORT_DSC COMMENT 'POD_Customer_Friendly_Category_Short_Dsc',
	INSTANT_WIN_PROGRAM_ID COMMENT 'Instant_Win_Program_Id',
	INSTANT_WIN_VERSION_ID COMMENT 'Instant_Win_Version_Id',
	INSTANT_WIN_PRIZE_ITEM_QTY COMMENT 'Instant_Win_Prize_Item_Qty',
	INSTANT_WIN_FREQUENCY_DSC COMMENT 'Instant_Win_Frequency_Dsc',
	DW_CREATE_TS COMMENT 'DW_CREATE_TS',
	DW_LAST_UPDATE_TS COMMENT 'DW_LAST_UPDATE_TS',
	DW_LOGICAL_DELETE_IND COMMENT 'DW_LOGICAL_DELETE_IND',
	DW_SOURCE_CREATE_NM COMMENT 'DW_SOURCE_CREATE_NM ',
	DW_SOURCE_UPDATE_NM COMMENT 'DW_SOURCE_UPDATE_NM',
	DW_CURRENT_VERSION_IND COMMENT 'DW_CURRENT_VERSION_IND'
) COMMENT='View For Offer_Request_Offer_Specification'
 as
select
Offer_Request_Id	,
User_Interface_Unique_Id	,
DW_First_Effective_Dt	,
DW_Last_Effective_Dt	,
Store_Group_Version_Id	,
Display_Order_Nbr	,
Proto_Type_Cd	,
Proto_Type_Dsc	,
Proto_Type_Short_Dsc	,
Store_Tag_Nbr	,
Store_Tag_Amt	,
Loyalty_Program_Tag_Ind	,
Store_Tag_Dsc	,
POD_Headline_Txt	,
POD_Headline_Sub_Txt	,
POD_Offer_Dsc	,
POD_Offer_Detail_Cd	,
POD_Offer_Detail_Dsc	,
POD_Offer_Detail_Short_Dsc	,
POD_Price_Info_Txt	,
POD_Item_Qty	,
POD_Unit_Of_Measure_Cd	,
POD_Unit_Of_Measure_Nm	,
Usage_Limit_Type_Txt	,
POD_Disclaimer_Txt	,
POD_Display_Start_Dt	,
POD_Display_End_Dt	,
POD_Customer_Friendly_Category_Cd	,
POD_Customer_Friendly_Category_Dsc	,
POD_Customer_Friendly_Category_Short_Dsc	,
Instant_Win_Program_Id	,
Instant_Win_Version_Id	,
Instant_Win_Prize_Item_Qty	,
Instant_Win_Frequency_Dsc	,
DW_CREATE_TS	,
DW_LAST_UPDATE_TS	,
DW_LOGICAL_DELETE_IND	,
DW_SOURCE_CREATE_NM	,
DW_SOURCE_UPDATE_NM	,
DW_CURRENT_VERSION_IND	
From EDM_CONFIRMED_PRD.DW_C_PURCHASING.Offer_Request_Offer_Specification;
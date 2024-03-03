--liquibase formatted sql
--changeset SYSTEM:Offer_Request_Restriction_Type runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database EDM_VIEWS_PRD;
use schema EDM_VIEWS_PRD.DW_VIEWS;

create or replace view OFFER_REQUEST_RESTRICTION_TYPE(
	OFFER_REQUEST_ID COMMENT 'Unique identifer for each offer request created in source system',
	USAGE_LIMIT_TYPE_TXT COMMENT 'Customer limit types like one per transaction ,one …',
	DW_FIRST_EFFECTIVE_DT COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	LIMIT_QTY COMMENT 'Item Quantity Limit Eligible for the offer',
	LIMIT_WT COMMENT 'Item weight limit eligible for the offer',
	LIMIT_VOL COMMENT 'Item Volume limit eligible for the offer',
	UNIT_OF_MEASURE_CD COMMENT 'Unit of Measure code for the item on the offer',
	UNIT_OF_MEASURE_NM COMMENT 'Unit of Measure Name for the item on the offer',
	LIMIT_AMT COMMENT 'Item dollar value limit eligible for off…',
	RESTRICTION_TYPE_CD COMMENT 'Restrictions or limits applicable to an offer. lIke one per user,…',
	RESTRICTION_TYPE_DSC COMMENT 'Description of the Restrictions or limits applicable to an offer. lIke one per user,...',
	RESTRICTION_TYPE_SHORT_DSC COMMENT 'Short description for the restriction type on the offer',
	USAGE_LIMIT_NBR COMMENT 'number of offers customer can get or com',
	USAGE_LIMIT_PERIOD_NBR COMMENT 'Describes the usage limit period time for an offer request.',
	DW_CREATE_TS COMMENT 'The timestamp the record was inserted',
	DW_LAST_UPDATE_TS COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert',
	DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete',
	DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
) COMMENT='View For Offer_Request_Restriction_Type'
 as
select
Offer_Request_Id    ,
Usage_limit_Type_Txt   ,
DW_First_Effective_Dt  ,
DW_Last_Effective_Dt  ,
Limit_Qty ,
Limit_Wt  ,
Limit_Vol ,
Unit_Of_Measure_Cd ,
Unit_Of_Measure_Nm ,
Limit_Amt ,
Restriction_Type_Cd ,
Restriction_Type_Dsc ,
Restriction_Type_Short_Dsc  ,
Usage_Limit_Nbr  ,
USAGE_LIMIT_PERIOD_NBR,
DW_CREATE_TS  ,
DW_LAST_UPDATE_TS  ,
DW_LOGICAL_DELETE_IND  ,
DW_SOURCE_CREATE_NM  ,
DW_SOURCE_UPDATE_NM  ,
DW_CURRENT_VERSION_IND 
From EDM_CONFIRMED_PRD.DW_C_PURCHASING.Offer_Request_Restriction_Type;
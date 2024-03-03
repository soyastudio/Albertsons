--liquibase formatted sql
--changeset SYSTEM:EPE_Transaction_Header runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view EPE_TRANSACTION_HEADER(
	TRANSACTION_INTEGRATION_ID COMMENT 'Generated Integration Identifier of each transaction',
	DW_FIRST_EFFECTIVE_DT COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	STORE_NBR COMMENT 'Store Number where the transaction took place',
	REGISTER_TRANSACTION_SEQUENCE_NBR COMMENT 'Transaction sequence number on the register',
	TERMINAL_NBR COMMENT 'The register at which the transaction took place',
	TRANSACTION_ID COMMENT 'Unique Transaction Identifier for each transaction made',
	TRANSACTION_TS COMMENT 'The real time of the transaction. Date and Time as printed on the receipt which is coimg from POS',
	ORDER_ID COMMENT 'Order Identifier of the transaction',
	HOUSEHOLD_ID COMMENT 'Unique identifier of the Household. The ''aggregateId'' in CHMS service is the HHID unlike the other services where the ''aggegatedid'' we map to the CustomerId',
	STATUS_CD COMMENT 'Status code for the transaction',
	CREATE_DT COMMENT 'Transaction Created Date',
	SOURCE_SYSTEM_CD COMMENT 'source system code \" Number\"',
	TOTAL_CARD_SAVINGS_AMT COMMENT 'Total Savings Amount for the transaction',
	TRANSACTION_TOTAL_AMT COMMENT 'Total Transaction Amount',
	FULFILLMENT_STORE_NBR COMMENT 'Store Number where the order has been fulfilled from.',
	DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
) COMMENT='VIEW for EPE_Transaction_Header'
 as
select
Transaction_Integration_Id ,
DW_First_Effective_Dt,
DW_Last_Effective_Dt,
Store_Nbr,	
Register_Transaction_Sequence_Nbr,
Terminal_Nbr,
Transaction_Id,
Transaction_Ts,
Order_Id ,
Household_Id,
Status_Cd,
Create_Dt,
Source_System_Cd ,
Total_Card_Savings_Amt,
Transaction_Total_Amt,
FULFILLMENT_STORE_NBR,
DW_CREATE_TS,
DW_LAST_UPDATE_TS,
DW_LOGICAL_DELETE_IND,
DW_SOURCE_CREATE_NM,
DW_SOURCE_UPDATE_NM,
DW_CURRENT_VERSION_IND  

from  <<EDM_DB_NAME>>.DW_C_RETAILSALE.EPE_Transaction_Header;

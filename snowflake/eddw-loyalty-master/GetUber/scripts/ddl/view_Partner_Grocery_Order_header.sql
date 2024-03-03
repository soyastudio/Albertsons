--liquibase formatted sql
--changeset SYSTEM:Partner_Grocery_Order_header runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view PARTNER_GROCERY_ORDER_HEADER(
	ORDER_ID COMMENT 'Partner created Order ID, Must be numeric (not alphanumeric).This is Unique for each Grocery Order placed by the  Customers through Partner channels.',
	PARTNER_GROCERY_ORDER_CUSTOMER_INTEGRATION_ID COMMENT 'Partner created id for user/Customer.  This is Unique ID of the Customer. This is Partner created id for the user, unique per each Customer.',
	DW_FIRST_EFFECTIVE_DT COMMENT 'The date that this division instance became effective.',
	DW_LAST_EFFECTIVE_DT COMMENT 'The last date that this division instance was effective. This date for the current instance will be 9999/12/31.',
	TRANSACTION_DT COMMENT 'Grocery Order Create Date',
	STORE_TRANSACTION_TS COMMENT 'Grocery Order Create Timestamp.',
	NET_AMT COMMENT 'Total amount of the transaction (total sale less club card discount) - Will include tax if partner does not perform tax exemption properly.',
	PARTNER_ID COMMENT 'Unique Identifier of the Parter. ',
	PARTNER_NM COMMENT 'Name of the Partner.',
	FACILITY_INTEGRATION_ID COMMENT 'Uniquely identifies a facility, it is a surrogate key that is needed because there are 2 systems of record for facilities and this identifier maybe for a facility from both systems or for just one of them',
	STORE_ID COMMENT 'Unique identifier of the Store',
	ALCOHOLIC_IND COMMENT 'Indicator to indicate whether or not the Order has the Alocoholic Items.  TRUE = Has Alcoholic Items, FALSE = No Alcoholic Items',
	SNAP_IND COMMENT ' Was SNAP/EBT applied to the purchase.  Is the total was even  $0.01 the response should be true.',
	DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
) COMMENT='VIEW for Partner_Grocery_Order_Header '
 as
select
Order_Id,
Partner_Grocery_Order_Customer_Integration_Id,
DW_First_Effective_Dt,
DW_Last_Effective_Dt,
Transaction_Dt,
Store_Transaction_Ts,
Net_Amt,
Partner_Id,
Partner_Nm,
Facility_Integration_ID,
Store_Id,
Alcoholic_Ind,
Snap_Ind,
DW_CREATE_TS,
DW_LAST_UPDATE_TS,
DW_LOGICAL_DELETE_IND,
DW_SOURCE_CREATE_NM,
DW_SOURCE_UPDATE_NM,
DW_CURRENT_VERSION_IND   
from  <<EDM_DB_NAME>>.DW_C_Loyalty.Partner_Grocery_Order_Header;

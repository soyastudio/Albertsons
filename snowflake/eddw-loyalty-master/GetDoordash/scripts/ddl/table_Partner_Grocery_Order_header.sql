--liquibase formatted sql
--changeset SYSTEM:Partner_Grocery_Order_header runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_LOYALTY;

create or replace TABLE PARTNER_GROCERY_ORDER_HEADER (
	ORDER_ID VARCHAR(50) NOT NULL COMMENT 'Partner created Order ID, Must be numeric (not alphanumeric).  This is Unique for each Grocery Order placed by the  Customers through Partner channels.',
	PARTNER_GROCERY_ORDER_CUSTOMER_INTEGRATION_ID NUMBER(38,0) NOT NULL COMMENT 'Partner_Grocery_Order_Customer_Integration_Id',
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL COMMENT 'The date that this division instance became effective.',
	DW_LAST_EFFECTIVE_DT DATE NOT NULL COMMENT 'The last date that this division instance was effective. Thid date for the current instance will be 9999/12/31.',
	TRANSACTION_DT DATE COMMENT 'Grocery Order Create Date.',
	STORE_TRANSACTION_TS TIMESTAMP_LTZ(9) COMMENT 'Grocery Order Create Timestamp.',
	NET_AMT NUMBER(16,2) COMMENT 'Total amount of the transaction (total sale less club card discount) - Will include tax if partner does not perform tax exemption properly.',
	PARTNER_ID VARCHAR(20) COMMENT 'Unique Identifier of the Parter.',
	PARTNER_NM VARCHAR(100) COMMENT 'Name of the Partner.',
	FACILITY_INTEGRATION_ID NUMBER(38,0) COMMENT 'Uniquely identifies a facility, it is a surrogate key that is needed because there are 2 systems of record for facilities and this identifier maybe for a facility from both systems or for just one of them',
	STORE_ID VARCHAR(20) COMMENT 'Store_Id',
	DW_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'DW_CREATE_TS',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'DW_LAST_UPDATE_TS',
	DW_LOGICAL_DELETE_IND BOOLEAN COMMENT 'DW_LOGICAL_DELETE_IND',
	DW_SOURCE_CREATE_NM VARCHAR(255) COMMENT 'DW_SOURCE_CREATE_NM',
	DW_SOURCE_UPDATE_NM VARCHAR(255) COMMENT 'DW_SOURCE_UPDATE_NM',
	DW_CURRENT_VERSION_IND BOOLEAN COMMENT 'DW_CURRENT_VERSION_IND',
	ALCOHOLIC_IND BOOLEAN COMMENT 'Indicator to indicate whether or not the Order has the Alocoholic Items.  TRUE = Has Alcoholic Items, FALSE = No Alcoholic Items',
	SNAP_IND BOOLEAN COMMENT 'Was SNAP/EBT applied to the purchase.  Is the total was even  $0.01 the response should be true.',
	primary key (ORDER_ID, PARTNER_GROCERY_ORDER_CUSTOMER_INTEGRATION_ID, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT)
);

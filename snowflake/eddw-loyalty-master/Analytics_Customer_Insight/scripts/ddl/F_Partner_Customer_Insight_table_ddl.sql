--liquibase formatted sql
--changeset SYSTEM:F_Partner_Customer_Insight_table_ddl runOnChange:true splitStatements:false OBJECT_TYPE:TABLE

use database <<EDM_DB_NAME_A>>;
use schema DW_RETAIL_EXP;

CREATE OR Replace TABLE F_Partner_Customer_Insight
(
	Customer_Insight_F_Sk NUMBER autoincrement ,
	DAY_ID NUMBER(10,0) NOT NULL  COMMENT 'Day Identifier;  Format: YYYYMMDD; e.g. 20210101',
	RETAIL_CUSTOMER_HOUSEHOLD_D1_SK NUMBER NOT NULL  COMMENT 'System Generated Column gets next value inserted when record gets inserted.',
	Business_Partner_D1_SK NUMBER NOT NULL  COMMENT 'A Surrogate Key - system-generated unique identifier that is used instead of the natural key.',
	Retail_Store_D1_Sk NUMBER NOT NULL  COMMENT 'A Surrogate Key - system-generated unique identifier that is used instead of the natural key.',
	Banner_D1_Sk NUMBER NOT NULL  COMMENT 'A Surrogate Key - system-generated unique identifier that is used instead of the natural key.',
	Division_D1_Sk NUMBER NOT NULL  COMMENT 'A Surrogate Key - system-generated unique identifier that is used instead of the natural key.',
	Household_Id VARCHAR COMMENT 'Household Identifier',
	TRANSACTION_DT DATE NOT NULL  COMMENT 'Date of the transaction',
	Product_Group_Nm VARCHAR NOT NULL  COMMENT 'Name of the product group to which a UPC belongs to',
	ORDER_ID varchar NOT NULL  COMMENT 'Total count of orders purchansed by a household during the week',
	GMV_Order_Value_Amt NUMBER(12,2) NOT NULL  COMMENT 'Sale value corresponding to the product group for that week',
	Loyalty_Indicator_Cd VARCHAR NOT NULL  COMMENT 'Indicator to specify if the phone number entered at partner channel while placing the order exists in Albertsons Customer base or not',
	Freshpass_Subscribed_Ind BOOLEAN NOT NULL  COMMENT 'Indicator if a household is subscribed to freshpass',
	B4U_Linked_Ind BOOLEAN NOT NULL  COMMENT 'Indicator if a customer is B4U linked',
	DW_Last_Update_Ts TIMESTAMP NOT NULL  COMMENT 'DW Last Update Timestamp',
	DW_Create_Ts TIMESTAMP NOT NULL  COMMENT 'DW Create Timestamp'
);

ALTER TABLE F_Partner_Customer_Insight
	ADD CONSTRAINT XPKF_Customer_Insight PRIMARY KEY (Customer_Insight_F_Sk);

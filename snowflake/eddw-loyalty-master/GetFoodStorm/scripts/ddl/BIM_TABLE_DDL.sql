--liquibase formatted sql
--changeset SYSTEM:BIM_TABLE_DDL runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_LOYALTY;


CREATE OR REPLACE TABLE Foodstorm_Order
(
 Order_Id              NUMBER  NOT NULL ,
 Dw_First_Effective_Dt  DATE  NOT NULL ,
 Dw_Last_Effective_Dt  DATE  NOT NULL ,
 Facility_Integration_Id  NUMBER  ,
 Store_Id              VARCHAR  ,
 Partner_Id            NUMBER  ,
 Partner_Nm            VARCHAR  ,
 Order_Type_Cd         VARCHAR  ,
 Delivery_Dt           DATE  ,
 Delivery_Zip_Cd       VARCHAR  ,
 Total_Amt             NUMBER(8,2)  ,
 Tax_Amt               NUMBER(8,2)  ,
 Payment_Amt           NUMBER(8,2)  ,
 Balance_Due_Amt       NUMBER(8,2)  ,
 Tax_Exempt_Id         VARCHAR  ,
 Tax_Exempt_Ind        BOOLEAN  ,
 House_Account_Ind     BOOLEAN  ,
 Source_Customer_Id    VARCHAR  ,
 Customer_Loyalty_Phone_Nbr  VARCHAR  ,
 Dw_Create_Ts          TIMESTAMP  ,
 Dw_Last_Update_Ts     TIMESTAMP  ,
 Dw_Logical_Delete_Ind  BOOLEAN  ,
 Dw_Source_Create_Nm   VARCHAR(255)  ,
 Dw_Source_Update_Nm   VARCHAR(255)  ,
 Dw_Current_Version_Ind  BOOLEAN  ,
 CONSTRAINT XPKFoodstorm_Order PRIMARY KEY (Order_Id, Dw_First_Effective_Dt, Dw_Last_Effective_Dt)
);

COMMENT ON COLUMN Foodstorm_Order.Order_Id IS 'The FoodStorm order/credit number';

COMMENT ON COLUMN Foodstorm_Order.Order_Type_Cd IS 'Type of the order. Order or Credit';

COMMENT ON COLUMN Foodstorm_Order.Delivery_Dt IS 'The delivery/pickup date of the order in the format YYYY-MM-DD';

COMMENT ON COLUMN Foodstorm_Order.Delivery_Zip_Cd IS 'The delivery zip code of the order';

COMMENT ON COLUMN Foodstorm_Order.Total_Amt IS 'The total cost of the order';

COMMENT ON COLUMN Foodstorm_Order.Tax_Amt IS 'The total tax applied to the order';

COMMENT ON COLUMN Foodstorm_Order.Payment_Amt IS 'The total payments applied to the order';

COMMENT ON COLUMN Foodstorm_Order.Balance_Due_Amt IS 'The balance of the order after payment applied (i.e. zero when paid in full)';

COMMENT ON COLUMN Foodstorm_Order.Tax_Exempt_Id IS 'The tax exempt ID assigned to the customer, if tax exempt';

COMMENT ON COLUMN Foodstorm_Order.House_Account_Ind IS 'true if the order is for a house account customer';

COMMENT ON COLUMN Foodstorm_Order.Source_Customer_Id IS 'Alphanumeric customer identifier, globally unique across FoodStorm system';

COMMENT ON COLUMN Foodstorm_Order.Customer_Loyalty_Phone_Nbr IS 'The loyalty identifier linked to the customer order.  Can be alphanumeric to support IDs, phone number, email addresses';

COMMENT ON COLUMN Foodstorm_Order.Dw_Create_Ts IS 'The timestamp the record was inserted.';

COMMENT ON COLUMN Foodstorm_Order.Dw_Last_Update_Ts IS 'When a record is updated  this would be the current timestamp';

COMMENT ON COLUMN Foodstorm_Order.Dw_Logical_Delete_Ind IS 'Set to True when we receive a delete record for the primary key, else False';

COMMENT ON COLUMN Foodstorm_Order.Dw_Source_Create_Nm IS 'The Bod (data source) name of this insert.';

COMMENT ON COLUMN Foodstorm_Order.Dw_Source_Update_Nm IS 'The Bod (data source) name of this update or delete.';

COMMENT ON COLUMN Foodstorm_Order.Dw_Current_Version_Ind IS 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d';

COMMENT ON COLUMN Foodstorm_Order.Dw_First_Effective_Dt IS 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key';

COMMENT ON COLUMN Foodstorm_Order.Dw_Last_Effective_Dt IS 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day';

COMMENT ON COLUMN Foodstorm_Order.Store_Id IS 'Unique Identifier of the store';

COMMENT ON COLUMN Foodstorm_Order.Partner_Id IS 'Unique Identifier of the partner';

COMMENT ON COLUMN Foodstorm_Order.Partner_Nm IS 'Name of the partner';

CREATE OR REPLACE TABLE Foodstorm_Order_Item
(
 Order_Id              NUMBER  NOT NULL ,
 Upc_Id                NUMBER  NOT NULL ,
 Dw_First_Effective_Dt  DATE  NOT NULL ,
 Dw_Last_Effective_Dt  DATE  NOT NULL ,
 Item_Nm               VARCHAR  ,
 Department_Nm         VARCHAR  ,
 Item_Price_Amt        Number(8,2)  ,
 Item_Qty              NUMBER  ,
 Item_Total_Amt        NUMBER(8,2)  ,
 Tax_Cd                VARCHAR  ,
 Tax_Rate_Pct          NUMBER(8,5)  ,
 Dw_Create_Ts          TIMESTAMP  ,
 Dw_Last_Update_Ts     TIMESTAMP  ,
 Dw_Logical_Delete_Ind  BOOLEAN  ,
 Dw_Source_Create_Nm   VARCHAR(255)  ,
 Dw_Source_Update_Nm   VARCHAR(255)  ,
 Dw_Current_Version_Ind  BOOLEAN  ,
 CONSTRAINT XPKFoodstorm_Order_Item PRIMARY KEY (Order_Id, Upc_Id, Dw_First_Effective_Dt, Dw_Last_Effective_Dt)
);

COMMENT ON COLUMN Foodstorm_Order_Item.Upc_Id IS 'The UPC assigned to the line-item in the order. Note: No price embedding will occur.';

COMMENT ON COLUMN Foodstorm_Order_Item.Item_Nm IS 'The name of the item in FoodStorm';

COMMENT ON COLUMN Foodstorm_Order_Item.Item_Qty IS 'The qty ordered';

COMMENT ON COLUMN Foodstorm_Order_Item.Item_Price_Amt IS 'The unit price of the item (ex-tax)';

COMMENT ON COLUMN Foodstorm_Order_Item.Item_Total_Amt IS 'The line item total (price x qty), ex tax';

COMMENT ON COLUMN Foodstorm_Order_Item.Tax_Cd IS 'The FoodStorm tax code assigned to the line item (e.g. SALES)';

COMMENT ON COLUMN Foodstorm_Order_Item.Tax_Rate_Pct IS 'The tax rate applied to the line item as a decimal, e.g. 0.06 for 6% tax.';

COMMENT ON COLUMN Foodstorm_Order_Item.Order_Id IS 'The FoodStorm order/credit number';

COMMENT ON COLUMN Foodstorm_Order_Item.Dw_First_Effective_Dt IS 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key';

COMMENT ON COLUMN Foodstorm_Order_Item.Dw_Last_Effective_Dt IS 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day';

COMMENT ON COLUMN Foodstorm_Order_Item.Dw_Create_Ts IS 'The timestamp the record was inserted.';

COMMENT ON COLUMN Foodstorm_Order_Item.Dw_Last_Update_Ts IS 'When a record is updated  this would be the current timestamp';

COMMENT ON COLUMN Foodstorm_Order_Item.Dw_Logical_Delete_Ind IS 'Set to True when we receive a delete record for the primary key, else False';

COMMENT ON COLUMN Foodstorm_Order_Item.Dw_Source_Create_Nm IS 'The Bod (data source) name of this insert.';

COMMENT ON COLUMN Foodstorm_Order_Item.Dw_Source_Update_Nm IS 'The Bod (data source) name of this update or delete.';

COMMENT ON COLUMN Foodstorm_Order_Item.Dw_Current_Version_Ind IS 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d';

COMMENT ON COLUMN Foodstorm_Order_Item.Department_Nm IS 'Name of the department';

--liquibase formatted sql
--changeset SYSTEM:BIM_Table_view runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

CREATE or replace view Foodstorm_Order
(
 Order_Id              COMMENT 'The FoodStorm order/credit number' ,
 Dw_First_Effective_Dt  COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key' ,
 Dw_Last_Effective_Dt  COMMENT  'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
 Facility_Integration_Id COMMENT 'Surrogate Key for Facility based on FacilityID & DivisionID' ,
 Store_Id              COMMENT 'Unique Identifier of the store'  ,
 Partner_Id            COMMENT 'Unique Identifier of the partner'  ,
 Partner_Nm            COMMENT 'Name of the partner'  ,
 Order_Type_Cd         COMMENT 'Type of the order. Order or Credit' ,
 Delivery_Dt           COMMENT 'The delivery/pickup date of the order in the format YYYY-MM-DD' ,
 Delivery_Zip_Cd       COMMENT 'The delivery zip code of the order',
 Total_Amt             COMMENT 'The total cost of the order'  ,
 Tax_Amt               COMMENT 'The total tax applied to the order' ,
 Payment_Amt           COMMENT 'The total payments applied to the order' ,
 Balance_Due_Amt       COMMENT 'The balance of the order after payment applied (i.e. zero when paid in full)' ,
 Tax_Exempt_Id         COMMENT 'The tax exempt ID assigned to the customer, if tax exempt' ,
 Tax_Exempt_Ind        COMMENT 'true if the order is tax exempt',
 House_Account_Ind     COMMENT 'true if the order is for a house account customer' ,
 Source_Customer_Id    COMMENT 'Alphanumeric customer identifier, globally unique across FoodStorm system' ,
 Customer_Loyalty_Phone_Nbr  COMMENT 'The loyalty identifier linked to the customer order.  Can be alphanumeric to support IDs, phone number, email addresses'  ,
 Dw_Create_Ts          COMMENT 'The timestamp the record was inserted.'  ,
 Dw_Last_Update_Ts     COMMENT 'When a record is updated  this would be the current timestamp'  ,
 Dw_Logical_Delete_Ind  COMMENT 'Set to True when we receive a delete record for the primary key, else False'  ,
 Dw_Source_Create_Nm  COMMENT 'The Bod (data source) name of this insert.'  ,
 Dw_Source_Update_Nm  COMMENT 'The Bod (data source) name of this update or delete.',
 Dw_Current_Version_Ind COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'  
 ) 
COPY GRANTS
COMMENT = 'VIEW for Foodstorm_Order'
AS
SELECT
 Order_Id ,             
 Dw_First_Effective_Dt ,  
 Dw_Last_Effective_Dt  ,
 Facility_Integration_Id ,
 Store_Id              ,
 Partner_Id             ,
 Partner_Nm             ,
 Order_Type_Cd          ,
 Delivery_Dt            ,
 Delivery_Zip_Cd       ,
 Total_Amt               ,
 Tax_Amt                ,
 Payment_Amt           ,
 Balance_Due_Amt        ,
 Tax_Exempt_Id          ,
 Tax_Exempt_Ind         ,
 House_Account_Ind      ,
 Source_Customer_Id ,
 Customer_Loyalty_Phone_Nbr   ,
 Dw_Create_Ts            ,
 Dw_Last_Update_Ts      ,
 Dw_Logical_Delete_Ind    ,
 Dw_Source_Create_Nm    ,
 Dw_Source_Update_Nm ,
 Dw_Current_Version_Ind
 FROM <<EDM_DB_NAME>>.DW_C_LOYALTY.Foodstorm_Order;
 
------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE View Foodstorm_Order_Item
(
 Order_Id              COMMENT 'The FoodStorm order/credit number',
 Upc_Id                COMMENT 'The UPC assigned to the line-item in the order. Note: No price embedding will occur.',
 Dw_First_Effective_Dt COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
 Dw_Last_Effective_Dt  COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day' ,
 Item_Nm               COMMENT 'The name of the item in FoodStorm',
 Department_Nm         COMMENT 'Name of the department'  ,
 Item_Price_Amt        COMMENT 'The unit price of the item (ex-tax)' ,
 Item_Qty              COMMENT 'The qty ordered',
 Item_Total_Amt        COMMENT 'The line item total (price x qty), ex tax',
 Tax_Cd                COMMENT 'The FoodStorm tax code assigned to the line item (e.g. SALES)',
 Tax_Rate_Pct          COMMENT 'The tax rate applied to the line item as a decimal, e.g. 0.06 for 6% tax.',
 Dw_Create_Ts          COMMENT 'The timestamp the record was inserted.',
 Dw_Last_Update_Ts     COMMENT 'When a record is updated  this would be the current timestamp',
 Dw_Logical_Delete_Ind COMMENT 'Set to True when we receive a delete record for the primary key, else False',
 Dw_Source_Create_Nm   COMMENT 'The Bod (data source) name of this insert.' ,
 Dw_Source_Update_Nm   COMMENT 'The Bod (data source) name of this update or delete.' ,
 Dw_Current_Version_Ind  COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'  
 ) 
COPY GRANTS
COMMENT = 'VIEW for Foodstorm_Order_Item'
AS
SELECT
 Order_Id ,           
 Upc_Id ,              
 Dw_First_Effective_Dt , 
 Dw_Last_Effective_Dt ,  
 Item_Nm ,               
 Department_Nm ,         
 Item_Price_Amt ,        
 Item_Qty ,              
 Item_Total_Amt ,        
 Tax_Cd ,               
 Tax_Rate_Pct ,          
 Dw_Create_Ts ,         
 Dw_Last_Update_Ts ,     
 Dw_Logical_Delete_Ind ,
 Dw_Source_Create_Nm , 
 Dw_Source_Update_Nm ,  
 Dw_Current_Version_Ind  
 FROM <<EDM_DB_NAME>>.DW_C_LOYALTY.Foodstorm_Order_Item;

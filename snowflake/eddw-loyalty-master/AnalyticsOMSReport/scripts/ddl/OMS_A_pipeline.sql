USE WAREHOUSE EDM_ADMIN_WH;
USE DATABASE EDM_ANALYTICS_PRD;
USE SCHEMA RETAIL_EXP;


CREATE TABLE DIM_Channel
(
 Channel_Type_Cd       VARCHAR  NOT NULL,
 Channel_Type_Dsc     VARCHAR,
 DW_CREATE_TS          TIMESTAMP,
 DW_LAST_UPDATE_TS     TIMESTAMP,
 DW_LOGICAL_DELETE_IND  boolean  
);

ALTER TABLE DIM_Channel
 ADD CONSTRAINT XPKchannel PRIMARY KEY (Channel_Type_Cd);

CREATE TABLE DIM_Discount
(
 Discount_Id           NUMBER  NOT NULL,
 Discount_Dsc         VARCHAR,
 DW_CREATE_TS          TIMESTAMP,
 DW_LAST_UPDATE_TS     TIMESTAMP,
 DW_LOGICAL_DELETE_IND  boolean  
);

ALTER TABLE DIM_Discount
 ADD CONSTRAINT XPKdiscount PRIMARY KEY (Discount_Id);

CREATE TABLE DIM_Group
(
 Group_Cd              VARCHAR  NOT NULL,
 Group_Dsc            VARCHAR,
 DW_CREATE_TS          TIMESTAMP,
 DW_LAST_UPDATE_TS     TIMESTAMP,
 DW_LOGICAL_DELETE_IND  boolean  
);

ALTER TABLE DIM_Group
 ADD CONSTRAINT XPKGroup PRIMARY KEY (Group_Cd);

CREATE TABLE DIM_Offer_Type
(
 Offer_Type_Cd         VARCHAR  NOT NULL,
 Offer_Type_Dsc       VARCHAR,
 DW_CREATE_TS          TIMESTAMP,
 DW_LAST_UPDATE_TS     TIMESTAMP,
 DW_LOGICAL_DELETE_IND  boolean  
);

ALTER TABLE DIM_Offer_Type
 ADD CONSTRAINT XPKoffer_type PRIMARY KEY (Offer_Type_Cd);

CREATE TABLE DIM_Product_Group_UPC
(
 Product_Group_Id      NUMBER  NOT NULL,
 Product_Group_Nm      VARCHAR,
 UPC_Nbr               NUMBER(14)  NOT NULL,
 UPC_Dsc               VARCHAR,
 Retail_Item_Dsc       VARCHAR,
 Internet_Item_Dsc     VARCHAR,
 UPC_UOM               VARCHAR,
 UOM_Size_Qty          NUMBER(10,2),
 DW_CREATE_TS          TIMESTAMP,
 DW_LAST_UPDATE_TS     TIMESTAMP,
 DW_LOGICAL_DELETE_IND  boolean  
);

ALTER TABLE DIM_Product_Group_UPC
 ADD CONSTRAINT XPKDIM_PG_UPC PRIMARY KEY (Product_Group_Id);

CREATE TABLE DIM_Program
(
 Program_Cd            VARCHAR  NOT NULL,
 Program_Dsc          VARCHAR,
 DW_CREATE_TS          TIMESTAMP,
 DW_LAST_UPDATE_TS     TIMESTAMP,
 DW_LOGICAL_DELETE_IND  boolean  
);

ALTER TABLE DIM_Program
 ADD CONSTRAINT XPKDIM_Program PRIMARY KEY (Program_Cd);

CREATE TABLE DIM_ROG
(
 Rog_Id                VARCHAR(4)  NOT NULL,
 Division_Id           NUMBER,
 Division_Nm           VARCHAR,
 Rog_Dsc              VARCHAR,
 DW_CREATE_TS          TIMESTAMP,
 DW_LAST_UPDATE_TS     TIMESTAMP,
 DW_LOGICAL_DELETE_IND  boolean  
);

ALTER TABLE DIM_ROG
 ADD CONSTRAINT XPKrog PRIMARY KEY (Rog_Id);

CREATE TABLE DIM_Store
(
 Store_Id              NUMBER  NOT NULL,
 Store_Dsc            VARCHAR,
 DW_CREATE_TS          TIMESTAMP,
 DW_LAST_UPDATE_TS     TIMESTAMP,
 DW_LOGICAL_DELETE_IND  boolean,
 Store_Group_Id        NUMBER  
);

ALTER TABLE DIM_Store
 ADD CONSTRAINT XPKstore_id PRIMARY KEY (Store_Id);

CREATE TABLE DIM_Store_Group
(
 Store_Group_Id        NUMBER  NOT NULL,
 Store_Group_Nm        VARCHAR,
 Store_Group_Category_Cd  VARCHAR,
 Store_Group_Dsc      VARCHAR,
 DW_CREATE_TS          TIMESTAMP,
 DW_LAST_UPDATE_TS     TIMESTAMP,
 DW_LOGICAL_DELETE_IND  boolean  
);

ALTER TABLE DIM_Store_Group
 ADD CONSTRAINT XPKstore_group PRIMARY KEY (Store_Group_Id);


CREATE OR REPLACE TABLE Fact_Offer_Request
(
 Additional_Detail_Dsc  VARCHAR,
 Amount                NUMBER(10,2),
 Brand_Size            VARCHAR,
 Buy_Get               VARCHAR,
 Channel_Type_Cd       VARCHAR,
 Created_Date          TIMESTAMP_NTZ(9),
 Created_By            VARCHAR,
 Department_Nm         VARCHAR,
 Digital_Builder       VARCHAR,
 Digital_Store_Group_List  VARCHAR,
 Discount_Id           NUMBER,
 Discount_Amt          NUMBER(10,2),
 Discount_Type_Dsc    VARCHAR,
 Division_Offer_Request_List  VARCHAR,
 Division_Store_Tag_UPC_List  VARCHAR,
 Dollar_Limit          NUMBER(10,2),
 Gift_Card_Ind         VARCHAR,
 Group_Cd              VARCHAR,
 Group_Offer_Request_List  VARCHAR,
 In_Ad                 VARCHAR,
 Item_Limit            VARCHAR,
 J4U_Store_Group_List  VARCHAR,
 J4U_Tag_Comment       VARCHAR,
 J4U_Tag_Display_Price  VARCHAR,
 Last_Modified_Dt      TIMESTAMP_NTZ(9),
 Last_Modified_By      VARCHAR,
 Min_Amount_To_Buy     NUMBER(10,2),
 Min_Qty_To_Buy        NUMBER,
 Min_Purchase          NUMBER(10,2),
 Non_Digital_Builder   VARCHAR,
 Non_Digital_Store_Group_List  VARCHAR,
 Nopa_Number_Offer_Request_List  VARCHAR,
 Nopa_Billed_Ind       VARCHAR,
 Nopa_Billing_Option   VARCHAR,
 Nopa_Start_Dt         DATE,
 Nopa_End_Dt           DATE,
 Offer_Id              VARCHAR,
 Offer_Limit           VARCHAR,
 Offer_Request_Id      VARCHAR,
 Offer_Start_Dt        DATE,
 Offer_End_Dt          DATE,
 Offer_Status_Cd       VARCHAR,
 Offer_Request_Status_Cd  VARCHAR,
 Offer_Type_Cd         VARCHAR,
 Offer_Version         VARCHAR,
 Point_Group           VARCHAR,
 Points                VARCHAR,
 PLU                   NUMBER,
 Print_J4U_Tag_Ind     VARCHAR,
 Prizes                NUMBER(10,2),
 Program_Cd            VARCHAR,
 Product_Group_Id      NUMBER,
 Product_Group_Nm      VARCHAR,
 Qualification_Day_Time  VARCHAR,
 Quantity              NUMBER(10,2),
 Rog_Id                VARCHAR(4),
 Rog_Offer_Request_List  VARCHAR,
 Rog_Store_Tag_UPC_List  VARCHAR,
 Segment               VARCHAR,
 Store_Id              NUMBER,
 Store_Group_Id        NUMBER,
 Store_Group_Category_Cd  VARCHAR,
 Tag_Display_Price     VARCHAR,
 Tag_Display_Qty       NUMBER,
 Tag_Comment           VARCHAR,
 UOM                   VARCHAR,
 Upto                  NUMBER(10,2),
 Weight_Limit          NUMBER(10,2),
 DW_CREATE_TS          TIMESTAMP,
 DW_LAST_UPDATE_TS     TIMESTAMP,
 DW_LOGICAL_DELETE_IND  boolean,
Store_Tag_J4U_Ind      VARCHAR,
Image_Type_Cd          VARCHAR(50),
COPIENT_ID             VARCHAR(50)
);


USE DATABASE EDM_VIEWS_PRD;
USE SCHEMA DW_VIEWS;


CREATE VIEW DIM_Channel AS 
SELECT Channel_Type_Cd
,Channel_Type_Dsc
,DW_CREATE_TS
,DW_LAST_UPDATE_TS
,DW_LOGICAL_DELETE_IND 
FROM EDM_ANALYTICS_PRD.RETAIL_EXP.DIM_Channel;

CREATE VIEW DIM_Discount AS 
SELECT Discount_Id
,Discount_Dsc
,DW_CREATE_TS
,DW_LAST_UPDATE_TS
,DW_LOGICAL_DELETE_IND 
FROM EDM_ANALYTICS_PRD.RETAIL_EXP.DIM_Discount;

CREATE VIEW DIM_Group AS 
SELECT Group_Cd
,Group_Dsc
,DW_CREATE_TS
,DW_LAST_UPDATE_TS
,DW_LOGICAL_DELETE_IND 
FROM EDM_ANALYTICS_PRD.RETAIL_EXP.DIM_Group;

CREATE VIEW DIM_Offer_Type AS 
SELECT Offer_Type_Cd
,Offer_Type_Dsc
,DW_CREATE_TS
,DW_LAST_UPDATE_TS
,DW_LOGICAL_DELETE_IND
FROM EDM_ANALYTICS_PRD.RETAIL_EXP.DIM_Offer_Type;

CREATE VIEW DIM_Product_Group_UPC AS 
SELECT Product_Group_Id
,Product_Group_Nm
,UPC_Nbr
,UPC_Dsc
,UPC_UOM
,UOM_Size_Qty
,Retail_Item_Dsc
,Internet_Item_Dsc
,DW_CREATE_TS
,DW_LAST_UPDATE_TS
,DW_LOGICAL_DELETE_IND 
FROM EDM_ANALYTICS_PRD.RETAIL_EXP.DIM_Product_Group_UPC;

CREATE VIEW DIM_Program AS 
SELECT Program_Cd
,Program_Dsc
,DW_CREATE_TS
,DW_LAST_UPDATE_TS
,DW_LOGICAL_DELETE_IND 
FROM EDM_ANALYTICS_PRD.RETAIL_EXP.DIM_Program;

CREATE VIEW DIM_ROG AS 
SELECT Rog_Id
,Division_Id
,Division_Nm
,Rog_Dsc
,DW_CREATE_TS
,DW_LAST_UPDATE_TS
,DW_LOGICAL_DELETE_IND 
FROM EDM_ANALYTICS_PRD.RETAIL_EXP.DIM_ROG;

CREATE VIEW  DIM_Store AS 
SELECT Store_Id
,Store_Dsc
,DW_CREATE_TS
,DW_LAST_UPDATE_TS
,DW_LOGICAL_DELETE_IND
,Store_Group_Id 
FROM EDM_ANALYTICS_PRD.RETAIL_EXP.DIM_Store;

CREATE VIEW DIM_Store_Group AS 
SELECT Store_Group_Id
,Store_Group_Nm
,Store_Group_Category_Cd
,Store_Group_Dsc
,DW_CREATE_TS
,DW_LAST_UPDATE_TS
,DW_LOGICAL_DELETE_IND
FROM EDM_ANALYTICS_PRD.RETAIL_EXP.DIM_Store_Group;

CREATE OR REPLACE VIEW Fact_Offer_Request copy grants AS 
SELECT Additional_Detail_Dsc
,Amount
,Brand_Size
,Buy_Get
,Channel_Type_Cd
,Created_Date
,Created_By
,Department_Nm
,Digital_Builder
,Digital_Store_Group_List
,Discount_Id
,Discount_Amt
,Discount_Type_Dsc
,Division_Offer_Request_List
,Division_Store_Tag_UPC_List
,Dollar_Limit
,Gift_Card_Ind
,Group_Cd
,Group_Offer_Request_List
,In_Ad
,Item_Limit
,J4U_Store_Group_List
,J4U_Tag_Comment
,J4U_Tag_Display_Price
,Last_Modified_Dt
,Last_Modified_By
,Min_Amount_To_Buy
,Min_Qty_To_Buy
,Min_Purchase
,Non_Digital_Builder
,Non_Digital_Store_Group_List
,Nopa_Number_Offer_Request_List
,Nopa_Billed_Ind
,Nopa_Billing_Option
,Nopa_Start_Dt
,Nopa_End_Dt
,Offer_Id
,Offer_Limit
,Offer_Request_Id
,Offer_Start_Dt
,Offer_End_Dt
,Offer_Status_Cd
,Offer_Request_Status_Cd
,Offer_Type_Cd
,Offer_Version
,Point_Group
,Points
,PLU
,Print_J4U_Tag_Ind
,Prizes
,Program_Cd
,Product_Group_Id
,Product_Group_Nm
,Qualification_Day_Time
,Quantity
,Rog_Id
,Rog_Offer_Request_List
,Rog_Store_Tag_UPC_List
,Segment
,Store_Id
,Store_Group_Id
,Store_Group_Category_Cd
,Tag_Display_Price
,Tag_Display_Qty
,Tag_Comment
,UOM
,Upto
,Weight_Limit
,Store_Tag_J4U_Ind
,Image_Type_Cd
,Copient_ID
,DW_CREATE_TS
,DW_LAST_UPDATE_TS
,DW_LOGICAL_DELETE_IND
FROM EDM_ANALYTICS_PRD.RETAIL_EXP.Fact_Offer_Request;

CREATE VIEW dim_calendar AS 
select calendar_dt
from EDM_CONFIRMED_PRD.DW_C_MASTERDATA.CALENDAR;

INSERT INTO EDM_ANALYTICS_PRD.RETAIL_EXP.dim_product_group_upc
SELECT 6, 'Any Product',00000000000000,' ',' ',' ',' ',null,current_timestamp(),NULL,FALSE
UNION
SELECT 10, 'OWN Brands Items - Corporate Managed UPC List',00000000000000,' ',' ',' ',' ',null,current_timestamp(),NULL,FALSE;


USE DATABASE EDM_ANALYTICS_PRD;
USE SCHEMA RETAIL_EXP;

//Task
CREATE OR REPLACE task SP_GETOFFERREQUEST_To_Analytics_load_TASK
warehouse = 'EDM_ADMIN_WH'
schedule = '2 minutes'
WHEN SYSTEM$STREAM_HAS_DATA('EDM_CONFIRMED_PRD.DW_C_PRODUCT.GetOfferRequest_Flat_C_Stream')
AS
CALL SP_GETOFFERREQUEST_TO_ANALYTICAL_LOAD(); 


//Task
CREATE OR REPLACE task SP_StoreGroup_To_Analytics_LOAD_TASK
warehouse = 'EDM_ADMIN_WH'
schedule = '2 minutes'
WHEN SYSTEM$STREAM_HAS_DATA('EDM_CONFIRMED_PRD.DW_C_PRODUCT.Storegroup_Flat_C_STREAM')
AS
CALL SP_StoreGroup_To_Analytical_LOAD(); 


//Task
CREATE OR REPLACE task SP_ProductGroup_To_Analytics_LOAD_TASK
warehouse = 'EDM_ADMIN_WH'
schedule = '2 minutes'
WHEN SYSTEM$STREAM_HAS_DATA('EDM_CONFIRMED_PRD.DW_C_PRODUCT.Productgroup_Flat_C_STREAM')
AS
CALL SP_ProductGroup_To_Analytics_LOAD();

//Task
CREATE OR REPLACE TASK SP_OFFEROMS_TO_ANALYTICS_LOAD_TASK
WAREHOUSE = 'EDM_ADMIN_WH'
SCHEDULE = '1 minutes'
WHEN
  SYSTEM$STREAM_HAS_DATA('EDM_CONFIRMED_PRD.DW_C_PRODUCT.OfferOMS_Flat_C_STREAM')
AS
CALL SP_OfferOMS_To_Analytical_LOAD();


alter task SP_GETOFFERREQUEST_To_Analytics_load_TASK resume;
alter task SP_StoreGroup_To_Analytics_LOAD_TASK resume;
alter task SP_ProductGroup_To_Analytics_LOAD_TASK resume;
ALTER task SP_OFFEROMS_TO_ANALYTICS_LOAD_TASK resume;

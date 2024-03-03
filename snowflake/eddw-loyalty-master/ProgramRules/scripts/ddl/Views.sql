--liquibase formatted sql
--changeset SYSTEM:View with division runOnChange:true splitStatements:false OBJECT_TYPE:VIEW

USE Database <<EDM_VIEW_NAME>>;
USE Schema DW_VIEWS;

create or replace view F_PARTNER_ORDER_TRANSACTION(
	PARTNER_ORDER_F_SK COMMENT 'Unique identifier for each partner order transaction.',
	RETAIL_CUSTOMER_D1_SK COMMENT 'Unique identifier for each customer',
	RETAIL_STORE_D1_SK COMMENT 'Unique identifier for each store',
	BANNER_D1_SK COMMENT 'Unique Banner Id of the store where actual transaction happened.',
	DAY_ID COMMENT 'Unique day Id of the transaction date.',
	DIVISION_D1_SK COMMENT 'Unique Division ID of the store where actual transaction happened.',
	BUSINESS_PARTNER_D1_SK COMMENT 'Unique Delivery partner channel id through which order was placed. 1 for Instacart',
	TRANSACTION_DT COMMENT 'Date when a Partner transaction fulfilled at the Albertsons Store.',
	PARTNER_ORDER_USER_IDENTIFIER COMMENT 'Unique ID for customer at the partner channel who made the transaction at third party channel.',
	ORDER_ID COMMENT 'Unique ID for the order placed at partner channel',
	LOYALTY_IND_CD COMMENT 'Indicator to specify if the phone number entered at partner channel while placing the order exists in Albertsons Customer base or not',
	GMV_ORDER_VALUE COMMENT 'Total order amount paid by the customer for the particular order to Partner. It is set to Gross Merch Value of the order',
	DW_LAST_UPDATE_TS COMMENT 'Timestamp representing updation of record in the Table',
	DW_CREATE_TS COMMENT 'Timestamp representing creation of record in the Table',
	ORDER_TAX_AMT COMMENT 'Total Tax amount of the orde',
	ALCOHOL_ORDER_IND COMMENT 'Indicator if the order contains an alcoholic product',
	SNAP_ORDER_IND COMMENT 'Indicator if the order is a SNAP ordrer',
	DUG_ORDER_IND COMMENT 'Indicator if the order is a Drive Up and Go Order',
	DELI_ORDER_IND COMMENT 'Indicator if the order is a Deli Order',
	FFC_ORDER_IND COMMENT 'Indicator if the order is a Flash Delivery Order',
	OWN_BRAND_ITEM_ORDER_IND COMMENT 'Indicator if the order contains atleast one Own brand product'
) COMMENT='This table contains total amount spent by each customer on each order. This table contains transactions made at Partner channel only'
 as
SELECT
PARTNER_ORDER_F_SK,
RETAIL_CUSTOMER_D1_SK,
RETAIL_STORE_D1_SK,
BANNER_D1_SK,
DAY_ID,
DIVISION_D1_SK,
BUSINESS_PARTNER_D1_SK,
TRANSACTION_DT,
PARTNER_ORDER_USER_IDENTIFIER,
ORDER_ID,
LOYALTY_IND_CD,
GMV_ORDER_VALUE,
DW_LAST_UPDATE_TS,
DW_CREATE_TS,
ORDER_TAX_AMT,
Alcohol_Order_Ind,
Snap_Order_Ind,
Dug_Order_Ind,
Deli_Order_Ind,
FFC_Order_Ind,
Own_Brand_Item_Order_Ind
FROM <<EDM_DB_NAME_A>>.DW_RETAIL_EXP.F_PARTNER_ORDER_TRANSACTION;

------------------------------------------


create or replace view FACT_PARTNER_SCORECARD(
	FISCAL_WEEK_ID COMMENT 'Week Id when the transaction was made',
	FISCAL_WEEK_START_DT COMMENT 'Week date when the transaction was made',
	RETAIL_CUSTOMER_D1_SK COMMENT 'Unique identifier for each customer',
	PARTNER_ORDER_USER_IDENTIFIER COMMENT 'Unique ID for customer at the partner channel who made the transaction at third party channel.',
	RETAIL_STORE_D1_SK COMMENT 'Unique identifier for each store',
	LOYALTY_INDICATOR COMMENT 'Indicator to specify if the phone number entered at partner channel while placing the order exists in Albertsons Customer base or not',
	BANNER_D1_SK COMMENT 'Unique Banner Id of the store where actual transaction happened.',
	DIVISION_D1_SK COMMENT 'Unique Division ID of the store where actual transaction happened.',
	BUSINESS_PARTNER_D1_SK COMMENT 'Unique Delivery partner channel id through which order was placed. 1 for Instacart',
	ORDER_CNT COMMENT 'Unique ID for the order placed at partner channel',
	ORDER_WITHTAX_CNT COMMENT 'Unique ID for the order placed at partner channel with Tax',
	GMV_ORDER_VALUE COMMENT 'Total order amount paid by the customer. It is set to Gross Merch Value of the order',
	ORDER_TAX_AMT COMMENT 'Total Tax amount of the order',
	ITEM_QTY COMMENT 'Total count of items purchased in the order',
    Alcohol_Order_Ind  COMMENT 'Indicator if the order contains an alcoholic product',
    Snap_Order_Ind  COMMENT 'Indicator if the order is a SNAP ordrer',
    Dug_Order_Ind  COMMENT 'Indicator if the order is a Drive Up and Go Order',
    Deli_Order_Ind  COMMENT 'Indicator if the order is a Deli Order',
    FFC_Order_Ind  COMMENT 'Indicator if the order is a Flash Delivery Order',
    Own_Brand_Item_Order_Ind COMMENT 'Indicator if the order contains atleast one Own brand product',
	DW_CREATE_TS COMMENT 'Timestamp representing creation of record in the Table'
) COMMENT='This view contains total amount spent by each customer on each order. This table contains transactions made at Partner channel only'
 as
WITH CAS_WEEK_DIVISION AS
(
select distinct division_id,division_nm,DIVISION_D1_SK from
(
select
lsfu.Retail_Store_Facility_Nbr as store_id,
lsfu.Retail_store_D1_sk as Retail_store_D1_sk,
dd.division_D1_Sk,
dd.division_nm,
dd.division_id
from <<EDM_VIEW_NAME>>.DW_VIEWS.D1_RETAIL_STORE lsfu
inner join <<EDM_VIEW_NAME>>.DW_VIEWS.D1_DIVISION dd
on dd.division_id = lsfu.division_id
where dd.corporation_id = '001')
order by division_id desc )

select distinct
DCAL.FISCAL_WEEK_ID,
DCAL.FISCAL_WEEK_START_DT,
RETAIL_CUSTOMER_D1_SK,
PARTNER_ORDER_USER_IDENTIFIER,
RETAIL_STORE_D1_SK,
loyalty_indicator,
BANNER_D1_SK,
DIVISION_D1_SK,
BUSINESS_PARTNER_D1_SK,
COUNT(DISTINCT ORDER_ID) AS ORDER_CNT,
COUNT(DISTINCT ORDER_ID_WithTax) AS ORDER_WITHTAX_CNT,
SUM(GMV_ORDER_VALUE) AS GMV_ORDER_VALUE,
SUM(Order_Tax_Amt) AS Order_Tax_Amt,
SUM(Item_Qty) AS ITEM_QTY ,
Alcohol_Order_Ind,
Snap_Order_Ind,
Dug_Order_Ind,
Deli_Order_Ind,
FFC_Order_Ind,
Own_Brand_Item_Order_Ind,
FACT.DW_CREATE_TS
from
(
select distinct
RETAIL_CUSTOMER_D1_SK,
PARTNER_ORDER_USER_IDENTIFIER,
RETAIL_STORE_D1_SK,
loyalty_indicator,
BANNER_D1_SK,
DIVISION_D1_SK,
BUSINESS_PARTNER_D1_SK,
ORDER_ID,
case when ORDER_TAX_AMT > 0 then ORDER_ID end ORDER_ID_WithTax,
TXN_DATE,
GMV_ORDER_VALUE,
Order_Tax_Amt,
Item_Qty,
Alcohol_Order_Ind,
Snap_Order_Ind,
Dug_Order_Ind,
Deli_Order_Ind,
FFC_Order_Ind,
Own_Brand_Item_Order_Ind,
CURRENT_TIMESTAMP() as DW_CREATE_TS,
row_number() over (PARTITION BY order_id ORDER BY TXN_DATE desc,loyalty_indicator,RETAIL_STORE_D1_SK,BANNER_D1_SK,DIVISION_D1_SK,BUSINESS_PARTNER_D1_SK,
                   PARTNER_ORDER_USER_IDENTIFIER,RETAIL_CUSTOMER_D1_SK,GMV_ORDER_VALUE,Order_Tax_Amt,Item_Qty,Alcohol_Order_Ind,Snap_Order_Ind,
                   Dug_Order_Ind,Deli_Order_Ind,FFC_Order_Ind,Own_Brand_Item_Order_Ind asc) as rn
from
(
SELECT distinct
 FPOT.RETAIL_CUSTOMER_D1_SK,
 FPOT.PARTNER_ORDER_USER_IDENTIFIER,
 FPOT.RETAIL_STORE_D1_SK,
 FPOT.loyalty_indicator,
 FPOT.BANNER_D1_SK,
 FPOT.DIVISION_D1_SK,
 FPOT.BUSINESS_PARTNER_D1_SK,
 FPOT.ORDER_ID,
 FPOT.TXN_DATE,
 FPOT.GMV_ORDER_VALUE,
 FPOT.Order_Tax_Amt,
 FPOT.Item_Qty,
 FPOT.Alcohol_Order_Ind,
 FPOT.Snap_Order_Ind,
 FPOT.Dug_Order_Ind,
 FPOT.Deli_Order_Ind,
 FPOT.FFC_Order_Ind,
 FPOT.Own_Brand_Item_Order_Ind
  from
(SELECT distinct
RETAIL_STORE_D1_SK,
LOYALTY_IND_CD as loyalty_indicator,
BANNER_D1_SK,
DIVISION_D1_SK,
BUSINESS_PARTNER_D1_SK,
ORDER_ID,
Transaction_Dt as TXN_DATE,
SUM(GMV_ORDER_VALUE)  as GMV_ORDER_VALUE,
SUM(Order_Tax_Amt) as Order_Tax_Amt,
SUM(Item_Qty) as Item_Qty,
RETAIL_CUSTOMER_D1_SK,
PARTNER_ORDER_USER_IDENTIFIER,
Alcohol_Order_Ind,
Snap_Order_Ind,
Dug_Order_Ind,
Deli_Order_Ind,
FFC_Order_Ind,
Own_Brand_Item_Order_Ind
 from
(SELECT distinct
RETAIL_STORE_D1_SK,
LOYALTY_IND_CD,
BANNER_D1_SK,
FPOT.DIVISION_D1_SK,
BUSINESS_PARTNER_D1_SK,
ORDER_ID,
Transaction_Dt,
GMV_ORDER_VALUE,
Order_Tax_Amt,
Item_Qty,
RETAIL_CUSTOMER_D1_SK,
PARTNER_ORDER_USER_IDENTIFIER,
Alcohol_Order_Ind,
Snap_Order_Ind,
Dug_Order_Ind,
Deli_Order_Ind,
FFC_Order_Ind,
Own_Brand_Item_Order_Ind
FROM <<EDM_DB_NAME_A>>.DW_RETAIL_EXP.F_PARTNER_ORDER_TRANSACTION FPOT
INNER JOIN CAS_WEEK_DIVISION  ON FPOT.DIVISION_D1_SK = CAS_WEEK_DIVISION.DIVISION_D1_SK
 ) group by 
RETAIL_CUSTOMER_D1_SK,
  PARTNER_ORDER_USER_IDENTIFIER,
  RETAIL_STORE_D1_SK,
  LOYALTY_IND_CD,
  BANNER_D1_SK,
  DIVISION_D1_SK,
  BUSINESS_PARTNER_D1_SK,
  ORDER_ID,RETAIL_CUSTOMER_D1_SK,
  TRANSACTION_DT,
 Alcohol_Order_Ind,
Snap_Order_Ind,
Dug_Order_Ind,
Deli_Order_Ind,
FFC_Order_Ind,
Own_Brand_Item_Order_Ind
  ) FPOT

)) FACT
INNER JOIN <<EDM_VIEW_NAME>>.DW_VIEWS.D0_FISCAL_DAY DCAL ON FACT.TXN_DATE = DCAL.CALENDAR_DT
where rn=1
GROUP BY
DCAL.FISCAL_WEEK_ID,
DCAL.FISCAL_WEEK_START_DT,
RETAIL_CUSTOMER_D1_SK,
PARTNER_ORDER_USER_IDENTIFIER,
RETAIL_STORE_D1_SK,
loyalty_indicator,
BANNER_D1_SK,
DIVISION_D1_SK,
BUSINESS_PARTNER_D1_SK,
Alcohol_Order_Ind,
Snap_Order_Ind,
Dug_Order_Ind,
Deli_Order_Ind,
FFC_Order_Ind,
Own_Brand_Item_Order_Ind,
FACT.DW_CREATE_TS

;

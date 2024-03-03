--liquibase formatted sql
--changeset SYSTEM:Fact_views runOnChange:true splitStatements:false OBJECT_TYPE:VIEW

use database <<EDM_VIEW_NAME>>;
use schema DW_VIEWS;

create or replace view Fact_Partner_Customer_Insight(
DAY_ID,
RETAIL_CUSTOMER_HOUSEHOLD_D1_SK,
Business_Partner_D1_SK,
Retail_Store_D1_Sk,
Banner_D1_Sk,
Division_D1_Sk,
--Product_Group_Nm,
ORDER_ID,
TRANSACTION_DT,
GMV_Order_Value_Amt,
Instacart_Linked_Ind,
Uber_Linked_Ind,
Doordash_Linked_Ind,
Not_Linked_Ind,
Not_ACI_Linked_Ind,
Loyalty_Indicator_Cd,
Freshpass_Subscribed_Ind,
B4U_Linked_Ind)
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
DAY_ID,
RETAIL_CUSTOMER_HOUSEHOLD_D1_SK,
Business_Partner_D1_SK,
Retail_Store_D1_Sk,
Banner_D1_Sk,
Division_D1_Sk,
ORDER_ID,
TRANSACTION_DT,
SUM(GMV_Order_Value_Amt) as GMV_Order_Value_Amt,
Instacart_Linked_Ind,
Uber_Linked_Ind,
Doordash_Linked_Ind,
Not_Linked_Ind,
Not_ACI_Linked_Ind,
Loyalty_Indicator_Cd,
Freshpass_Subscribed_Ind,
B4U_Linked_Ind
from
  
(select distinct
DAY_ID,
RETAIL_CUSTOMER_HOUSEHOLD_D1_SK,
Business_Partner_D1_SK,
Retail_Store_D1_Sk,
Banner_D1_Sk,
Division_D1_Sk,
Product_Group_Nm,
ORDER_ID,
TRANSACTION_DT,
GMV_Order_Value_Amt,
Instacart_Linked_Ind,
Uber_Linked_Ind,
Doordash_Linked_Ind,
Not_Linked_Ind,
Not_ACI_Linked_Ind,
Loyalty_Indicator_Cd,
Freshpass_Subscribed_Ind,
B4U_Linked_Ind
from
  
(
select distinct
DAY_ID,
RETAIL_CUSTOMER_HOUSEHOLD_D1_SK,
Business_Partner_D1_SK,
Retail_Store_D1_Sk,
Banner_D1_Sk,
Division_D1_Sk,
Product_Group_Nm,
ORDER_ID,
TRANSACTION_DT,
GMV_Order_Value_Amt,
Instacart_Linked_Ind,
Uber_Linked_Ind,
Doordash_Linked_Ind,
Not_Linked_Ind,
Not_ACI_Linked_Ind,
Loyalty_Indicator_Cd,
Freshpass_Subscribed_Ind,
B4U_Linked_Ind,
row_number() over (PARTITION BY ORDER_ID,Product_Group_Nm ORDER BY TRANSACTION_DT desc,
                   Loyalty_Indicator_Cd,RETAIL_STORE_D1_SK,BANNER_D1_SK,DIVISION_D1_SK,BUSINESS_PARTNER_D1_SK,RETAIL_CUSTOMER_HOUSEHOLD_D1_SK,
		               DAY_ID,Instacart_Linked_Ind,Uber_Linked_Ind,Doordash_Linked_Ind,Not_Linked_Ind,Not_ACI_Linked_Ind,
		               Freshpass_Subscribed_Ind,B4U_Linked_Ind desc) as rn
from
  
(select 
FPCI.DAY_ID,
FPCI.RETAIL_CUSTOMER_HOUSEHOLD_D1_SK,
FPCI.Business_Partner_D1_SK,
FPCI.Retail_Store_D1_Sk,
FPCI.Banner_D1_Sk,
FPCI.Division_D1_Sk,
FPCI.Product_Group_Nm,
FPCI.ORDER_ID,
FPCI.TRANSACTION_DT,
FPCI.GMV_Order_Value_Amt,
Case when FPCI.Loyalty_Indicator_Cd = 'match' and FPCI.Business_Partner_D1_SK = 1 then TRUE else FALSE end as Instacart_Linked_Ind,
Case when FPCI.Loyalty_Indicator_Cd = 'match' and FPCI.Business_Partner_D1_SK = 2 then TRUE else FALSE end as Uber_Linked_Ind,
Case when FPCI.Loyalty_Indicator_Cd = 'match' and FPCI.Business_Partner_D1_SK = 3 then TRUE else FALSE end as Doordash_Linked_Ind,
Case when FPCI.Loyalty_Indicator_Cd = 'no-match' then TRUE else FALSE end as Not_Linked_Ind,
Case when FPCI.Loyalty_Indicator_Cd = 'no-phone' then TRUE else FALSE end as Not_ACI_Linked_Ind,
FPCI.Loyalty_Indicator_Cd, 
FPCI.Freshpass_Subscribed_Ind,
FPCI.B4U_Linked_Ind
from <<EDM_DB_NAME_A>>.DW_RETAIL_EXP.F_Partner_Customer_Insight FPCI
INNER JOIN CAS_WEEK_DIVISION  ON FPCI.DIVISION_D1_SK = CAS_WEEK_DIVISION.DIVISION_D1_SK
 ) 

)
where rn=1)
group by 
DAY_ID,
RETAIL_CUSTOMER_HOUSEHOLD_D1_SK,
Business_Partner_D1_SK,
Retail_Store_D1_Sk,
Banner_D1_Sk,
Division_D1_Sk,
ORDER_ID,
TRANSACTION_DT,
Instacart_Linked_Ind,
Uber_Linked_Ind,
Doordash_Linked_Ind,
Not_Linked_Ind,
Not_ACI_Linked_Ind,
Loyalty_Indicator_Cd,
Freshpass_Subscribed_Ind,
B4U_Linked_Ind;

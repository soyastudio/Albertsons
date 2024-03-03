--liquibase formatted sql
--changeset SYSTEM:SP_F_Partner_Customer_Insight_QA runOnChange:true splitStatements:false OBJECT_TYPE:SP

use database <<EDM_DB_NAME_A>>;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE SP_F_Partner_Customer_Insight()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS $$

//--------------------------------------------------------------------------------------------------------------------#
// Desc         : Stored proc to Load KPI target table F_Partner_Customer_Insight
//--------------------------------------------------------------------------------------------------------------------#

var db_confirmed = '<<EDM_DB_NAME>>';
var db_analytics = '<<EDM_DB_NAME_A>>';
var db_refined = '<<EDM_DB_NAME_R>>';
var db_views =  '<<EDM_VIEW_NAME>>';
var db_views_BIZOPS = '<<EDM_VIEW_NAME>>';
var results_array = [];
var current_watermark = new Date();
current_watermark = current_watermark.toISOString();
var current_wm_id1=snowflake.createStatement({sqlText: `SELECT MAX(cycle_id) 
FROM ${db_views}.dw_edw_views.txn_hdr_combined txnc WHERE txn_dte >= CURRENT_DATE - 365;`})
var current_wm_id2=current_wm_id1.execute();
current_wm_id2.next();
var current_wm_id = current_wm_id2.getColumnValue(1);
var feed_extract_job_ini1 = snowflake.execute({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Job_Initiate(
                'OCRP',
                'SP_F_Partner_Customer_Insight',
                'FACT_Partner_Customer_Insight',
                null, 
                'I',
                null,
                null,
                null,
                '${db_analytics}.DW_RETAIL_EXP.F_Partner_Customer_Insight',
				null)`
                
     });
	 
//return_value will give job run auto id 
feed_extract_job_ini1.next();
job_run_id1 = feed_extract_job_ini1.getColumnValue(1);
var get_wm_ts1 = snowflake.createStatement({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Get_Watermark_TS(${job_run_id1})`
                
     });
//return_value will give job run auto id 
var ret_wm_ts1 = get_wm_ts1.execute();
ret_wm_ts1.next();
var last_watermark_from_tble1 = ret_wm_ts1.getColumnValue(1);
if (last_watermark_from_tble1 === null) {
  last_watermark1 = '2021-04-01 00:00:00.000';
} else {
  last_watermark1 = last_watermark_from_tble1;
}
//--------------------------------------
var feed_extract_job_ini2 = snowflake.execute({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Job_Initiate(
                'OCRP',
                'SP_F_Partner_Customer_Insight',
                'FACT_Partner_Customer_Insight_ubd',
                null, 
                'I',
                null,
                null,
                null,
                '${db_analytics}.DW_RETAIL_EXP.F_Partner_Customer_Insight',
				null)`
                
     });
	 
//return_value will give job run auto id 
feed_extract_job_ini2.next();
job_run_id2 = feed_extract_job_ini2.getColumnValue(1);
var get_wm_ts2 = snowflake.createStatement({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Get_Watermark_TS(${job_run_id2})`
                
     });
//return_value will give job run auto id 
var ret_wm_ts2 = get_wm_ts2.execute();
ret_wm_ts2.next();
var last_watermark_from_ubd = ret_wm_ts2.getColumnValue(1);
if (last_watermark_from_ubd === null) {
  last_watermark2 = '2021-12-01 00:00:00.000';
} else {
  last_watermark2 = last_watermark_from_ubd;
}
results_array[0]=last_watermark1;
results_array[1]=last_watermark2;
// ************** Load for F_Partner_Customer_Insight table BEGIN *****************	
var adb_ban=snowflake.createStatement({
			sqlText: `CREATE OR REPLACE TEMP TABLE ${db_refined}.DW_R_STAGE.adobe_banner_lkp_new
as
select
banner_d1_sk,
banner_nm, 
case
when banner_nm ='ACME' then 'acmemarkets'
when banner_nm ='JEWEL-OSCO' then 'jewelosco'
when banner_nm ='VONS' then 'vons'
when banner_nm ='RANDALLS' then 'randalls'
when banner_nm ='ALBERTSONS' then 'albertsons'
when banner_nm ='TOM THUMB' then 'tomthumb'
when banner_nm ='STAR MARKET' then 'STAR'
when banner_nm = 'PAK N SAV' then 'PAK N SAV BY SAFEWAY'
when banner_nm ='SHAWS' then 'shaws'
when banner_nm ='PAVILIONS' then 'pavilions'
when banner_nm ='CARRS' then 'carrsqc'
when banner_nm ='SAFEWAY' then 'safeway'
else banner_nm end adobe_banner_nm
from "${db_views}"."DW_VIEWS"."D1_BANNER"
WHERE DW_LOGICAL_DELETE_IND= FALSE;`});
var adb_ban1 = adb_ban.execute();
	var stmt_ban_div = snowflake.createStatement({
			sqlText:`CREATE OR REPLACE TEMP TABLE ${db_refined}.DW_R_STAGE.t_prepare_banner_division_id_new as
					select
					lsfu.Retail_Store_Facility_Nbr as store_id,
					lsfu.Retail_store_D1_sk,
					abl.Banner_D1_Sk,
					dd.division_D1_Sk
					from ${db_views}.DW_VIEWS.D1_RETAIL_STORE lsfu
					inner join ${db_views}.DW_VIEWS.D1_DIVISION dd
					on dd.division_id = lsfu.division_id
					inner join ${db_refined}.DW_R_STAGE.adobe_banner_lkp_new abl
					ON lsfu.banner_nm = abl.ADOBE_BANNER_NM
                    OR (lsfu.banner_nm <> abl.ADOBE_BANNER_NM and upper(lsfu.banner_nm) = upper(abl.BANNER_nm))
						     WHERE lsfu.DW_LOGICAL_DELETE_IND= FALSE and dd.DW_LOGICAL_DELETE_IND= FALSE
					
`});
    var stmt_ban_div1 = stmt_ban_div.execute();
    var stmt_cust_base = snowflake.createStatement({
			sqlText:`CREATE OR REPLACE TEMP TABLE ${db_refined}.DW_R_STAGE.t_prepare_customer_base_new as
					(SELECT
					rc.Retail_Customer_UUID ,
					rc.RETAIL_CUSTOMER_D1_SK,
					clp.loyalty_program_card_nbr as current_card_nbr,
					rch.HOUSEHOLD_ID
					FROM ${db_views}.DW_VIEWS.D1_RETAIL_CUSTOMER rc
					inner join ${db_views}.DW_VIEWS.RETAIL_CUSTOMER_HOUSEHOLD rch
					on rc.Retail_Customer_UUID = rch.Retail_Customer_UUID
					inner join ${db_views}.DW_VIEWS.RETAIL_HOUSEHOLD rh
					on rh.household_id = rch.household_id
					inner JOIN ${db_views}.DW_VIEWS.CUSTOMER_LOYALTY_PROGRAM clp
					on rc.Retail_Customer_UUID = clp.Retail_Customer_UUID
					WHERE rc.DW_LOGICAL_DELETE_IND= FALSE and rc.DW_CURRENT_VERSION_IND = TRUE
                     			and rch.DW_LOGICAL_DELETE_IND= FALSE and rch.DW_CURRENT_VERSION_IND = TRUE
                     			and rh.DW_LOGICAL_DELETE_IND= FALSE and rh.DW_CURRENT_VERSION_IND = TRUE
                     			and clp.DW_LOGICAL_DELETE_IND= FALSE and clp.DW_CURRENT_VERSION_IND = TRUE
					 QUALIFY (ROW_NUMBER() OVER (PARTITION BY current_card_nbr ORDER BY clp.DW_FIRST_EFFECTIVE_TS DESC)=1)
					);
`});
    
	var stmt_cust_base1 = stmt_cust_base.execute();
	var stmt_retail_exp = snowflake.createStatement({
			sqlText:`CREATE OR REPLACE TEMP TABLE ${db_refined}.DW_R_STAGE.t_prepare_customer_phn_new as
					SELECT
					distinct
					a.Retail_Customer_UUID,
					a.RETAIL_CUSTOMER_D1_SK,
					a.current_card_nbr,
					a.HOUSEHOLD_ID,
					cpfc.phone_nbr full_phone_nbr
					FROM ${db_refined}.DW_R_STAGE.t_prepare_customer_base_new a
					inner join ${db_views}.DW_VIEWS.CUSTOMER_PHONE_FAX_CONTACT cpfc
					on a.Retail_Customer_UUID = cpfc.Retail_Customer_UUID
					where cpfc.DW_LOGICAL_DELETE_IND= FALSE and cpfc.DW_CURRENT_VERSION_IND = TRUE
					QUALIFY (ROW_NUMBER() OVER (PARTITION BY full_phone_nbr ORDER BY current_card_nbr asc)=1);
`});	
    var stmt_retail_exp1 = stmt_retail_exp.execute();
    
    var sql_truncates = `Truncate table ${db_confirmed}.DW_C_STAGE.F_PARTNER_CUSTOMER_INSIGHT_work
                   `;

 try {
        snowflake.execute (
            {sqlText: sql_truncates  }
            );
        }   catch (err)  {
        return "Truncate records for ${db_confirmed}.DW_C_STAGE.F_PARTNER_CUSTOMER_INSIGHT_work Failed with error: " + err;   // Return a error message.
        };
	
	var stmt1 = snowflake.createStatement({
			sqlText:`Insert into ${db_confirmed}.DW_C_STAGE.F_PARTNER_CUSTOMER_INSIGHT_work (
    
	Day_Id,
	RETAIL_CUSTOMER_HOUSEHOLD_D1_SK,
	Business_Partner_D1_SK ,
	Retail_Store_D1_Sk ,
	Banner_D1_Sk ,
	Division_D1_Sk,
	Household_Id ,
	Transaction_Dt,
	Product_Group_Nm ,
	Order_Id,
	GMV_Order_Value_Amt ,
	Loyalty_Indicator_Cd,
	Freshpass_Subscribed_Ind,
	B4U_Linked_Ind ,
	DW_Last_Update_Ts,
	DW_Create_Ts 

)
WITH pos as (
select distinct order_id,store_txn_ts,partner_id,store_id,dlvry_id,dw_last_updt_ts,TO_DATE(store_txn_ts) as txn_date //,dw_last_updt_ts
FROM ${db_views}.dw_edw_views.partner_order_store 
QUALIFY (ROW_NUMBER() OVER (PARTITION BY order_id,dlvry_id ORDER BY store_txn_ts desc)=1)
),

pgn as 
(
select distinct sg.SMIC_GROUP_DSC,poi.order_id,poi.gross_merch_val_amt,poi.upc_id,poi.dlvry_id from ${db_views}.dw_edw_views.partner_order_itm poi
join ${db_views}.DW_VIEWS.Corporate_Item_Upc_Reference ciur on ciur.UPC_NBR = poi.upc_id
join ${db_views}.DW_VIEWS.Corporate_Item ci on ci.Corporate_Item_Integration_Id = ciur.Corporate_Item_Integration_Id
join ${db_views}.DW_VIEWS.SMIC_GROUP sg on sg.SMIC_GROUP_CD = ci.SMIC_GROUP_CD 
where ciur.DW_CURRENT_VERSION_IND = 'TRUE' and ci.DW_CURRENT_VERSION_IND = 'TRUE' and sg.DW_CURRENT_VERSION_IND = 'TRUE'
)

select DAY_ID,
RETAIL_CUSTOMER_HOUSEHOLD_D1_SK,
BUSINESS_PARTNER_D1_SK,
RETAIL_STORE_D1_SK,
BANNER_D1_SK,
DIVISION_D1_SK,
HOUSEHOLD_ID,
TRANSACTION_DT,
PRODUCT_GROUP_NM,
ORDER_ID,
sum(GMV_ORDER_VALUE_AMT),
LOYALTY_INDICATOR_CD,
FRESHPASS_SUBSCRIBED_IND,
B4U_LINKED_IND,
DW_LAST_UPDATE_TS,
DW_CREATE_TS


from

(
SELECT distinct
NVL(fd.Day_Id,0) as Day_Id,
NVL(drch.RETAIL_CUSTOMER_HOUSEHOLD_D1_SK,0) as RETAIL_CUSTOMER_HOUSEHOLD_D1_SK,
NVL(dbp.business_partner_d1_sk,0) as business_partner_d1_sk,
NVL(tbd.Retail_store_D1_sk,0) as retail_store_d1_sk,
NVL(tbd.Banner_D1_Sk,0) as Banner_D1_Sk,
NVL(tbd.Division_D1_Sk,0) as Division_D1_Sk,
customer.HOUSEHOLD_ID as HOUSEHOLD_ID,
NVL(pos.txn_date,CURRENT_DATE) as Transaction_Dt,
NVL(pgn.SMIC_GROUP_DSC,'Need') as Product_Group_Nm,
NVL(po.order_id,'0') as order_id,
( pgn.gross_merch_val_amt) as GMV_Order_Value_Amt,
CASE WHEN po.loyalty_phone_nbr IN ('0','0.0','0.') THEN 'no-phone'
   WHEN (po.loyalty_phone_nbr NOT IN ('0','0.0','0.') 
   AND customer.HOUSEHOLD_ID IS NULL) THEN 'no-match'
   ELSE 'match' END as Loyalty_Indicator_Cd,
case when fsa.ACTION_STATUS_ID in (1,5) and (pos.txn_date between fsa.START_DATE and fsa.END_DATE)
   then 'YES' Else 'NO' end as Freshpass_Subscribed_Ind,
case when orh.j4u_first_visit_dt is not null then 'YES' Else 'NO' end as B4U_Linked_Ind,
TO_TIMESTAMP(current_timestamp()) dw_last_update_ts,
TO_TIMESTAMP(current_timestamp()) as dw_create_ts

FROM ${db_views}.dw_edw_views.partner_order po
inner JOIN pos
ON po.order_id = pos.order_id and po.dlvry_id = pos.dlvry_id
join pgn on pgn.order_id = pos.order_id
left join ${db_views}.dw_edw_views.partner_order_itm_tax poit 
on pgn.upc_id = poit.upc_id and pgn.order_id = poit.order_id and poit.dlvry_id = pgn.dlvry_id
inner JOIN ${db_refined}.DW_R_STAGE.t_prepare_banner_division_id_new tbd
ON pos.store_id = try_to_numeric(tbd.store_id)
left JOIN ${db_refined}.DW_R_STAGE.t_prepare_customer_phn_new customer
ON po.loyalty_phone_nbr = customer.full_phone_nbr
left join ${db_views}.dw_views.D1_RETAIL_CUSTOMER_HOUSEHOLD drch on drch.HOUSEHOLD_ID = customer.HOUSEHOLD_ID
left join (select household_id,ACTION_STATUS_ID, MIN(START_DATE) as START_DATE,MAX(END_DATE) as END_DATE 
from "${db_views}"."DW_BIZOPS_VIEWS"."FP_SUBS_ALL" where RK =1
group by 1,2)  fsa on fsa.Household_Id = customer.HOUSEHOLD_ID
left join ${db_views}.DW_BIZOPS_VIEWS.ONLINE_REGISTERED_HHS orh on orh.HOUSEHOLD_ID = customer.HOUSEHOLD_ID and orh.UUID = customer.RETAIL_CUSTOMER_UUID
left join ${db_views}.dw_views.D1_BUSINESS_PARTNER dbp
on pos.partner_id = try_to_numeric(dbp.partner_id)
inner join ${db_views}.dw_views.D0_CALENDAR_DAY fd --------(condn to  join)
on pos.txn_date = fd.calendar_dt 
WHERE pos.dw_last_updt_ts >=  TO_TIMESTAMP('${last_watermark1}') --TO_TIMESTAMP('2023-02-01 00:21:00.157 -0600')
and pos.dw_last_updt_ts <= TO_TIMESTAMP('${current_watermark}'))
  
GROUP BY DAY_ID,
RETAIL_CUSTOMER_HOUSEHOLD_D1_SK,
BUSINESS_PARTNER_D1_SK,
RETAIL_STORE_D1_SK,
BANNER_D1_SK,
DIVISION_D1_SK,
HOUSEHOLD_ID,
TRANSACTION_DT,
PRODUCT_GROUP_NM,
ORDER_ID,
LOYALTY_INDICATOR_CD,
FRESHPASS_SUBSCRIBED_IND,
B4U_LINKED_IND,
DW_LAST_UPDATE_TS,
DW_CREATE_TS
`});
var res1 = stmt1.execute();
	res1.next();
var return_value1 = res1.getColumnValue(1);

var stmt2 = snowflake.createStatement({
			sqlText:`Insert into ${db_confirmed}.DW_C_STAGE.F_PARTNER_CUSTOMER_INSIGHT_work (
        Day_Id,
	RETAIL_CUSTOMER_HOUSEHOLD_D1_SK,
	Business_Partner_D1_SK ,
	Retail_Store_D1_Sk ,
	Banner_D1_Sk ,
	Division_D1_Sk,
	Household_Id ,
	Transaction_Dt,
	Product_Group_Nm ,
	Order_Id,
	GMV_Order_Value_Amt ,
	Loyalty_Indicator_Cd,
	Freshpass_Subscribed_Ind,
	B4U_Linked_Ind ,
	DW_Last_Update_Ts,
	DW_Create_Ts 
	
)
WITH 
pgod as (
select PARTNER_GROCERY_ORDER_CUSTOMER_INTEGRATION_ID,
order_id,date(STORE_TRANSACTION_TS) STORE_TRANSACTION_TS,
LOYALTY_PHONE_NBR,
(COALESCE(REVENUE_AMT,0)) as gmv_order_value,
  sg.SMIC_GROUP_DSC,
sum(item_tax_amt) as order_tax_amt,
sum(item_qty) as Item_qty
FROM ${db_views}.dw_views."PARTNER_GROCERY_ORDER_DETAIL" pgod 
join ${db_views}.DW_VIEWS.Corporate_Item_Upc_Reference ciur on ciur.UPC_NBR = pgod.upc_id
join ${db_views}.DW_VIEWS.Corporate_Item ci on ci.Corporate_Item_Integration_Id = ciur.Corporate_Item_Integration_Id
join ${db_views}.DW_VIEWS.SMIC_GROUP sg on sg.SMIC_GROUP_CD = ci.SMIC_GROUP_CD 
where pgod.DW_CURRENT_VERSION_IND=TRUE --and order_id='cca3c0d4-3c6b-47dd-a16b-44fe831b2ba8'
and ciur.DW_CURRENT_VERSION_IND = 'TRUE' and Ci.DW_CURRENT_VERSION_IND = 'TRUE' and sg.DW_CURRENT_VERSION_IND = 'TRUE'
GROUP BY 1,2,3,4,5,6
),
pgoh as (
select PARTNER_GROCERY_ORDER_CUSTOMER_INTEGRATION_ID,order_id,date(STORE_TRANSACTION_TS) STORE_TRANSACTION_TS,PARTNER_ID,STORE_ID,DW_create_TS,
  sum(NET_AMT) as net_amt //,DW_create_TS
from ${db_views}.dw_views."PARTNER_GROCERY_ORDER_HEADER" pgoh where DW_CURRENT_VERSION_IND=TRUE 
group by 1,2,3,4,5,6
),
pgot as
(
select distinct order_id
FROM ${db_views}.dw_views."PARTNER_GROCERY_ORDER_TENDER" pgot where DW_CURRENT_VERSION_IND=TRUE 
)

select DAY_ID,
RETAIL_CUSTOMER_HOUSEHOLD_D1_SK,
BUSINESS_PARTNER_D1_SK,
RETAIL_STORE_D1_SK,
BANNER_D1_SK,
DIVISION_D1_SK,
HOUSEHOLD_ID,
TRANSACTION_DT,
PRODUCT_GROUP_NM,
ORDER_ID,
sum(GMV_ORDER_VALUE_AMT),
LOYALTY_INDICATOR_CD,
FRESHPASS_SUBSCRIBED_IND,
B4U_LINKED_IND,
DW_LAST_UPDATE_TS,
DW_CREATE_TS


from(
SELECT distinct
NVL(fd.Day_Id,0) as Day_Id,
NVL(drch.RETAIL_CUSTOMER_HOUSEHOLD_D1_SK,0) as RETAIL_CUSTOMER_HOUSEHOLD_D1_SK,
NVL(dbp.business_partner_d1_sk,0) as business_partner_d1_sk,
NVL(tbd.Retail_Store_D1_Sk,0) as retail_store_d1_sk,
NVL(tbd.Banner_D1_Sk,0) as Banner_D1_Sk,
NVL(tbd.Division_D1_Sk,0) as Division_D1_Sk,
customer.HOUSEHOLD_ID as HOUSEHOLD_ID,
nvl(date(pgoh.STORE_TRANSACTION_TS),CURRENT_DATE) as Transaction_Dt,
NVL(pgod.SMIC_GROUP_DSC,'Need') as Product_Group_Nm,
NVL(pgod.order_id,'0') as order_id,
(COALESCE(pgod.gmv_order_value,0)) as GMV_Order_Value_Amt,
CASE WHEN pgod.loyalty_phone_nbr IN ('0','0.0','0.') THEN 'no-phone'
    WHEN (pgod.loyalty_phone_nbr NOT IN ('0','0.0','0.')
    AND customer.HOUSEHOLD_ID IS NULL) THEN 'no-match'
    WHEN pgod.loyalty_phone_nbr=customer.full_phone_nbr THEN 'match'
    ELSE 'no-match' END as Loyalty_Indicator_Cd,
case when fsa.ACTION_STATUS_ID in (1,5) and (pgoh.STORE_TRANSACTION_TS between fsa.START_DATE and fsa.END_DATE)
    then 'YES' Else 'NO' end as Freshpass_Subscribed_Ind,
case when orh.j4u_first_visit_dt is not null then 'YES' Else 'NO' end as B4U_Linked_Ind,
TO_TIMESTAMP(current_timestamp()) as dw_last_update_ts,
TO_TIMESTAMP(current_timestamp()) as dw_create_ts
 
 FROM pgoh
inner JOIN pgod
ON pgoh.order_id = pgod.order_id
inner join pgot
ON pgot.order_id=pgod.order_id
inner JOIN ${db_refined}.DW_R_STAGE.t_prepare_banner_division_id_new tbd 
ON pgoh.store_id = try_to_numeric(tbd.store_id)
left JOIN ${db_refined}.DW_R_STAGE.t_prepare_customer_phn_new customer
ON pgod.loyalty_phone_nbr = customer.full_phone_nbr
left join ${db_views}.dw_views.D1_RETAIL_CUSTOMER_HOUSEHOLD drch on drch.HOUSEHOLD_ID = customer.HOUSEHOLD_ID
left join (select household_id,ACTION_STATUS_ID, MIN(START_DATE) as START_DATE,MAX(END_DATE) as END_DATE 
from "${db_views}"."DW_BIZOPS_VIEWS"."FP_SUBS_ALL" where RK =1
group by 1,2) fsa on fsa.Household_Id = customer.HOUSEHOLD_ID
left join ${db_views}.DW_BIZOPS_VIEWS.ONLINE_REGISTERED_HHS orh on orh.HOUSEHOLD_ID = customer.HOUSEHOLD_ID and orh.UUID = customer.RETAIL_CUSTOMER_UUID
left join ${db_views}.dw_views.D1_BUSINESS_PARTNER dbp
on pgoh.partner_id = try_to_numeric(dbp.partner_id)
  inner join ${db_views}.dw_views.D0_CALENDAR_DAY fd --------(condn to  join)
on pgoh.STORE_TRANSACTION_TS = fd.calendar_dt 
WHERE pgoh.DW_create_TS >=  TO_TIMESTAMP('${last_watermark2}') -- TO_TIMESTAMP('2023-02-01 00:21:00.157 -0600')
and pgoh.DW_create_TS <= TO_TIMESTAMP('${current_watermark}'))
GROUP BY DAY_ID,
RETAIL_CUSTOMER_HOUSEHOLD_D1_SK,
BUSINESS_PARTNER_D1_SK,
RETAIL_STORE_D1_SK,
BANNER_D1_SK,
DIVISION_D1_SK,
HOUSEHOLD_ID,
TRANSACTION_DT,
PRODUCT_GROUP_NM,
ORDER_ID,
LOYALTY_INDICATOR_CD,
FRESHPASS_SUBSCRIBED_IND,
B4U_LINKED_IND,
DW_LAST_UPDATE_TS,
DW_CREATE_TS
`});
//return_value1 will return number of rows inserted		
	var res2 = stmt2.execute();
	res2.next();
var return_value2 = res2.getColumnValue(1);
var return_value = return_value1 + return_value2;

var sql_deletes = `delete from ${db_analytics}.DW_RETAIL_EXP.F_Partner_Customer_Insight where order_id in 
(select distinct ORDER_ID from ${db_confirmed}.DW_C_STAGE.F_PARTNER_CUSTOMER_INSIGHT_work where order_id in 
(select distinct ORDER_ID from ${db_analytics}.DW_RETAIL_EXP.F_Partner_Customer_Insight))
                   `;

 try {
        snowflake.execute (
            {sqlText: sql_deletes  }
            );
        }   catch (err)  {
        return "Delete records for ${db_analytics}.DW_RETAIL_EXP.F_Partner_Customer_Insight Failed with error: " + err;   // Return a error message.
        };
        
        
var sql_inserts = `insert into ${db_analytics}.DW_RETAIL_EXP.F_Partner_Customer_Insight
select * from ${db_confirmed}.DW_C_STAGE.F_PARTNER_CUSTOMER_INSIGHT_work
                   `;

 try {
        snowflake.execute (
            {sqlText: sql_inserts  }
            );
        }   catch (err)  {
        return "Insert records for ${db_analytics}.DW_RETAIL_EXP.F_Partner_Customer_Insight Failed with error: " + err;   // Return a error message.
        };
// *******************END OF LOAD FOR FACT_PARTNER_ORDER TABLE******************************	
var stmt2_fpo = snowflake.createStatement({
    sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Job_Success(
                '${job_run_id1}',
                '${return_value1}',
                null,
                null)`
                
     });
 //return_value2 will give job run auto id
var res2_fpo = stmt2_fpo.execute();
res2_fpo.next();
var stmt3_fpo = snowflake.createStatement({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Set_Watermark_TS(
                ${job_run_id1},
				'${last_watermark1}',
				'${current_watermark}'
				)`
                
     });
 //return_value3 will give job run auto id 
var res3_fpo = stmt3_fpo.execute();
res3_fpo.next();
return_value3_fpo = res3_fpo.getColumnValue(1);
//------------------
var stmt4_fpo = snowflake.createStatement({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Job_Success(
               '${job_run_id2}',
               '${return_value2}',
               null,
               null)`
                
     });
 //return_value2 will give job run auto id
var res4_fpo = stmt4_fpo.execute();
res4_fpo.next();
var stmt5_fpo = snowflake.createStatement({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Set_Watermark_TS(
                ${job_run_id2},
				'${last_watermark2}',
				'${current_watermark}'
				)`
                
     });
 //return_value3 will give job run auto id 
var res5_fpo = stmt5_fpo.execute();
res5_fpo.next();
return_value5_fpo = res5_fpo.getColumnValue(1);
return "F_PARTNER_ORDER TABLE LOADED";

$$;
  

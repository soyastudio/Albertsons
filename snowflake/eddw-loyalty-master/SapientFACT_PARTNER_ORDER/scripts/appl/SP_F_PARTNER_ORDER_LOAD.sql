--liquibase formatted sql
--changeset SYSTEM:SP_F_PARTNER_ORDER_LOAD runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_CONFIRMED_PRD;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE EDM_CONFIRMED_PRD.DW_APPL.SP_F_PARTNER_ORDER_LOAD()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

//--------------------------------------------------------------------------------------------------------------------#
// Desc         : Stored proc to Load KPI target table F_PARTNER_ORDER
//--------------------------------------------------------------------------------------------------------------------#
// Modification : Auth   : Amrita Pandey
//              : Date   : 12/27/2021
//              : Change : Retrofit Release Code change as per new Data model
//--------------------------------------------------------------------------------------------------------------------#

var db_confirmed = 'EDM_CONFIRMED_PRD';
var db_analytics = 'EDM_ANALYTICS_PRD';
var db_refined = 'EDM_REFINED_PRD';
var db_views = 'EDM_VIEWS_PRD';

var results_array = [];

var current_watermark = new Date();
current_watermark = current_watermark.toISOString();

var current_wm_id1=snowflake.createStatement({sqlText: `SELECT MAX(cycle_id) FROM ${db_views}.dw_edw_views.txn_hdr_combined txnc WHERE txn_dte >= CURRENT_DATE - 365;`})
var current_wm_id2=current_wm_id1.execute();
current_wm_id2.next();
var current_wm_id = current_wm_id2.getColumnValue(1);



var feed_extract_job_ini1 = snowflake.execute({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Job_Initiate(
                'OCRP',
                'SP_F_PARTNER_ORDER',
                'FACT_PARTNER_ORDER',
                null, 
                'I',
                null,
                null,
                null,
                '${db_analytics}.DW_RETAIL_EXP.F_PARTNER_ORDER_TRANSACTION',
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
                'SP_F_PARTNER_ORDER',
                'FACT_PARTNER_ORDER_ubd',
                null, 
                'I',
                null,
                null,
                null,
                '${db_analytics}.DW_RETAIL_EXP.F_PARTNER_ORDER_TRANSACTION',
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

// ************** Load for fact_partner_order table BEGIN *****************	
var adb_ban=snowflake.createStatement({
			sqlText: `CREATE OR REPLACE TEMP TABLE ${db_refined}.DW_R_STAGE.adobe_banner_lkp
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
when banner_nm ='SHAW\\'S' then 'shaws'
when banner_nm ='PAVILIONS' then 'pavilions'
when banner_nm ='CARRS' then 'carrsqc'
when banner_nm ='SAFEWAY' then 'safeway'
else banner_nm end adobe_banner_nm
from "${db_views}"."DW_VIEWS"."D1_BANNER";`});

var adb_ban1 = adb_ban.execute();

	var stmt_ban_div = snowflake.createStatement({
			sqlText:`CREATE OR REPLACE TEMP TABLE ${db_refined}.DW_R_STAGE.t_prepare_banner_division_id as
					select
					lsfu.Retail_Store_Facility_Nbr as store_id,
					lsfu.Retail_store_D1_sk,
					abl.Banner_D1_Sk,
					dd.division_D1_Sk
					from ${db_views}.DW_VIEWS.D1_RETAIL_STORE lsfu
					inner join ${db_views}.DW_VIEWS.D1_DIVISION dd
					on dd.division_id = lsfu.division_id
					inner join ${db_refined}.DW_R_STAGE.adobe_banner_lkp abl
					ON lsfu.banner_nm = abl.ADOBE_BANNER_NM
                    OR (lsfu.banner_nm <> abl.ADOBE_BANNER_NM and upper(lsfu.banner_nm) = upper(abl.BANNER_nm));

`});

    var stmt_ban_div1 = stmt_ban_div.execute();

    var stmt_cust_base = snowflake.createStatement({
			sqlText:`CREATE OR REPLACE TEMP TABLE ${db_refined}.DW_R_STAGE.t_prepare_customer_base as
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
					WHERE rc.DW_LOGICAL_DELETE_IND= FALSE
					 QUALIFY (ROW_NUMBER() OVER (PARTITION BY current_card_nbr ORDER BY clp.DW_FIRST_EFFECTIVE_TS DESC)=1)
					);
`});
    
	var stmt_cust_base1 = stmt_cust_base.execute();

	var stmt_retail_exp = snowflake.createStatement({
			sqlText:`CREATE OR REPLACE TEMP TABLE ${db_refined}.DW_R_STAGE.t_prepare_customer_phn as
					SELECT
					distinct
					a.Retail_Customer_UUID,
					a.RETAIL_CUSTOMER_D1_SK,
					a.current_card_nbr,
					a.HOUSEHOLD_ID,
					cpfc.phone_nbr full_phone_nbr
					FROM ${db_refined}.DW_R_STAGE.t_prepare_customer_base a
					inner join ${db_views}.DW_VIEWS.CUSTOMER_PHONE_FAX_CONTACT cpfc
					on a.Retail_Customer_UUID = cpfc.Retail_Customer_UUID
					QUALIFY (ROW_NUMBER() OVER (PARTITION BY full_phone_nbr ORDER BY current_card_nbr asc)=1);
`});	

    var stmt_retail_exp1 = stmt_retail_exp.execute();

	var stmt1 = snowflake.createStatement({
			sqlText:`Insert into "${db_analytics}"."DW_RETAIL_EXP".F_PARTNER_ORDER_TRANSACTION (
    Retail_Customer_D1_Sk ,
    Retail_Store_D1_Sk ,
    Banner_D1_Sk ,
    Day_Id ,
    Division_D1_Sk ,
    Business_Partner_D1_SK ,
	Transaction_Dt ,
    Partner_Order_User_Identifier ,
  	ORDER_ID ,
    loyalty_ind_cd,
	GMV_ORDER_VALUE ,
    DW_Last_Update_Ts ,
    DW_CREATE_TS
)
WITH pos as (
select distinct order_id,store_txn_ts,partner_id,store_id,dlvry_id,dw_last_updt_ts,TO_DATE(store_txn_ts) as txn_date
FROM ${db_views}.dw_edw_views.partner_order_store
QUALIFY (ROW_NUMBER() OVER (PARTITION BY order_id,dlvry_id ORDER BY store_txn_ts desc)=1)
)
SELECT 
coalesce(customer.RETAIL_CUSTOMER_D1_SK,0) as Retail_Customer_D1_sk,
pos.store_id as retail_store_d1_sk,
tbd.Banner_D1_Sk as Banner_D1_Sk,
fd.fiscal_day_id as DAY_ID,
tbd.Division_D1_Sk as Division_D1_Sk,
dbp.business_partner_d1_sk as business_partner_d1_sk,
pos.txn_date as Transaction_Dt,
po.user_id as Partner_Order_User_Identifier,
po.order_id as order_id,
CASE WHEN po.loyalty_phone_nbr IN ('0','0.0','0.') THEN 'no-phone'
WHEN (po.loyalty_phone_nbr NOT IN ('0','0.0','0.') 
AND customer.HOUSEHOLD_ID IS NULL) THEN 'no-match'
ELSE 'match' END as loyalty_ind_cd,
SUM(poi.gross_merch_val_amt) as GMV_order_value,
TO_TIMESTAMP(current_timestamp()) dw_last_update_ts,
TO_TIMESTAMP(current_timestamp()) as dw_create_ts
FROM ${db_views}.dw_edw_views.partner_order po
inner JOIN pos
ON po.order_id = pos.order_id
and po.dlvry_id = pos.dlvry_id
LEFT JOIN ${db_views}.dw_edw_views.partner_order_itm poi
ON po.order_id = poi.order_id
and po.dlvry_id = poi.dlvry_id
inner JOIN ${db_refined}.DW_R_STAGE.t_prepare_banner_division_id tbd
ON pos.store_id = try_to_numeric(tbd.store_id)
left JOIN ${db_refined}.DW_R_STAGE.t_prepare_customer_phn customer
ON po.loyalty_phone_nbr = customer.full_phone_nbr
left join ${db_views}.dw_views.D1_BUSINESS_PARTNER dbp
on pos.partner_id = try_to_numeric(dbp.partner_id)
inner join ${db_views}.dw_views.D0_FISCAL_DAY fd
on pos.txn_date = fd.calendar_dt 
WHERE pos.dw_last_updt_ts > TO_TIMESTAMP('${last_watermark1}')
and pos.dw_last_updt_ts <= TO_TIMESTAMP('${current_watermark}')
GROUP BY 1,2,3,4,5,6,7,8,9,10,12,13;
`});

var res1 = stmt1.execute();
	res1.next();
var return_value1 = res1.getColumnValue(1);

var stmt2 = snowflake.createStatement({
			sqlText:`Insert into "${db_analytics}"."DW_RETAIL_EXP".F_PARTNER_ORDER_TRANSACTION (
    Retail_Customer_D1_Sk ,
    Retail_Store_D1_Sk ,
    Banner_D1_Sk ,
    Day_Id ,
    Division_D1_Sk ,
    Business_Partner_D1_SK ,
	Transaction_Dt ,
    Partner_Order_User_Identifier ,
  	ORDER_ID ,
    loyalty_ind_cd,
	GMV_ORDER_VALUE ,
    DW_Last_Update_Ts ,
    DW_CREATE_TS
)
WITH 
pgod as (
select PARTNER_GROCERY_ORDER_CUSTOMER_INTEGRATION_ID,order_id,date(STORE_TRANSACTION_TS) STORE_TRANSACTION_TS,LOYALTY_PHONE_NBR,sum(COALESCE(REVENUE_AMT,0)) as gmv_order_value
FROM ${db_views}.dw_views."PARTNER_GROCERY_ORDER_DETAIL" pgod where DW_CURRENT_VERSION_IND=TRUE
GROUP BY 1,2,3,4
),
pgoh as (
select PARTNER_GROCERY_ORDER_CUSTOMER_INTEGRATION_ID,order_id,date(STORE_TRANSACTION_TS) STORE_TRANSACTION_TS,PARTNER_ID,STORE_ID,DW_create_TS,sum(NET_AMT) as net_amt
from ${db_views}.dw_views."PARTNER_GROCERY_ORDER_HEADER" pgoh where DW_CURRENT_VERSION_IND=TRUE
group by 1,2,3,4,5,6
),
pgot as
(
select distinct order_id
FROM ${db_views}.dw_views."PARTNER_GROCERY_ORDER_TENDER" pgot where DW_CURRENT_VERSION_IND=TRUE
)
SELECT 
coalesce(customer.RETAIL_CUSTOMER_D1_SK,0) as Retail_Customer_D1_sk,
pgoh.store_id as retail_store_d1_sk,
tbd.Banner_D1_Sk as Banner_D1_Sk,
fd.fiscal_day_id as DAY_ID,
tbd.Division_D1_Sk as Division_D1_Sk,
dbp.business_partner_d1_sk as business_partner_d1_sk,
date(pgoh.STORE_TRANSACTION_TS) as Transaction_Dt,
pgoh.PARTNER_GROCERY_ORDER_CUSTOMER_INTEGRATION_ID as Partner_Order_User_Identifier,
pgod.order_id as order_id,
CASE WHEN pgod.loyalty_phone_nbr IN ('0','0.0','0.') THEN 'no-phone'
WHEN (pgod.loyalty_phone_nbr NOT IN ('0','0.0','0.')
AND customer.HOUSEHOLD_ID IS NULL) THEN 'no-match'
WHEN pgod.loyalty_phone_nbr=customer.full_phone_nbr THEN 'match'
ELSE 'no-match' END as loyalty_ind_cd,
SUM(COALESCE(pgod.gmv_order_value,0)) as GMV_order_value,
TO_TIMESTAMP(current_timestamp()) dw_last_update_ts,
TO_TIMESTAMP(current_timestamp()) as dw_create_ts
FROM pgoh
inner JOIN pgod
ON pgoh.order_id = pgod.order_id
inner join pgot
ON pgot.order_id=pgod.order_id
inner JOIN ${db_refined}.DW_R_STAGE.t_prepare_banner_division_id tbd
ON pgoh.store_id = try_to_numeric(tbd.store_id)
left JOIN ${db_refined}.DW_R_STAGE.t_prepare_customer_phn customer
ON pgod.loyalty_phone_nbr = customer.full_phone_nbr
left join ${db_views}.dw_views.D1_BUSINESS_PARTNER dbp
on pgoh.partner_id = try_to_numeric(dbp.partner_id)
inner join ${db_views}.dw_views.D0_FISCAL_DAY fd
on date(pgoh.STORE_TRANSACTION_TS) = fd.calendar_dt 
WHERE pgoh.DW_create_TS > TO_TIMESTAMP('${last_watermark2}')
and pgoh.DW_create_TS <= TO_TIMESTAMP('${current_watermark}')
GROUP BY 1,2,3,4,5,6,7,8,9,10,12,13;
`});

//return_value1 will return number of rows inserted		
	var res2 = stmt2.execute();
	res2.next();
var return_value2 = res2.getColumnValue(1);

var return_value = return_value1 + return_value2

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

return "F_PARTNER_ORDER TABLE LOADED"

$$;
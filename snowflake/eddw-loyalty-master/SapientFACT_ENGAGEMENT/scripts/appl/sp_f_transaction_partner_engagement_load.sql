--liquibase formatted sql
--changeset SYSTEM:sp_f_transaction_partner_engagement_load runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_CONFIRMED_PRD;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE EDM_CONFIRMED_PRD.DW_APPL.SP_F_TRANSACTION_PARTNER_ENGAGEMENT_LOAD()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$


//--------------------------------------------------------------------------------------------------------------------#
// Desc         : Stored proc to F_Transaction_Partner_Engagement table
//--------------------------------------------------------------------------------------------------------------------#
// Modification : Auth   : Vidushi Jaiswal
//              : Date   : 10/11/2021
//              : Change : initial version
//--------------------------------------------------------------------------------------------------------------------#
//--------------------------------------------------------------------------------------------------------------------#
// Modification : Auth   : Amrita Pandey
//              : Date   : 12/14/2021
//              : Change : uber doordash changes
//--------------------------------------------------------------------------------------------------------------------#
//--------------------------------------------------------------------------------------------------------------------#
// Modification : Auth   : Jitendra DG
//              : Date   : 07/09/2022
//              : Change : 
//--------------------------------------------------------------------------------------------------------------------#


var results_array = [];

var current_watermark = new Date();
current_watermark = current_watermark.toISOString();

var db_confirmed = 'EDM_CONFIRMED_PRD';
var db_analytics = 'EDM_ANALYTICS_PRD';
var db_refined = 'EDM_REFINED_PRD';
var db_views = 'EDM_VIEWS_PRD';

var feed_extract_job_ini = snowflake.execute({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Job_Initiate(
                'OCRP',
                'SP_F_Transaction_Partner_Engagement_LOAD',
                'F_Transaction_Partner_Engagement',
                null, 
                'I',
                null,
                null,
                null,
                '${db_analytics}.DW_RETAIL_EXP.F_Transaction_Partner_Engagement',
				null)`
                
     });
	 
var feed_extract_job_ini1 = snowflake.execute({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Job_Initiate(
                'OCRP',
                'SP_F_Transaction_Partner_Engagement_LOAD',
                'F_Transaction_Partner_Engagement_ubd',
                null,
                'I',
                null,
                null,
                null,
                '${db_analytics}.DW_RETAIL_EXP.F_Transaction_Partner_Engagement',
				null)`
                
     });	


//return_value will give job run auto id 
feed_extract_job_ini.next();
job_run_id = feed_extract_job_ini.getColumnValue(1);

//return_value will give job run auto id 
feed_extract_job_ini1.next();
job_run_id1 = feed_extract_job_ini1.getColumnValue(1);


var get_wm_ts = snowflake.createStatement({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Get_Watermark_TS(${job_run_id})`
                
     });
	 
var get_wm_ts1 = snowflake.createStatement({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Get_Watermark_TS(${job_run_id1})`
                
     });
	 
//return_value will give job run auto id 
var ret_wm_ts = get_wm_ts.execute();
ret_wm_ts.next();
var last_watermark_from_table = ret_wm_ts.getColumnValue(1);

//return_value will give job run auto id 
var ret_wm_ts1 = get_wm_ts1.execute();
ret_wm_ts1.next();
var last_watermark_from_ubd = ret_wm_ts1.getColumnValue(1);




if (last_watermark_from_table === null) {
  last_watermark = '2021-04-01 00:00:00.000';
} else {
  last_watermark = last_watermark_from_table;
}

if (last_watermark_from_ubd === null) {
  last_watermark1 = '2021-12-01 00:00:00.000';
} else {
  last_watermark1 = last_watermark_from_ubd;
}


var current_wm_id1=snowflake.createStatement({sqlText: `SELECT MAX(cycle_id) FROM ${db_views}.dw_edw_views.txn_hdr_combined txnc WHERE txn_dte >= CURRENT_DATE - 365;`})
var current_wm_id2=current_wm_id1.execute();
current_wm_id2.next();
var current_wm_id = current_wm_id2.getColumnValue(1);
var get_wm_id = snowflake.createStatement({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Get_Watermark_ID(${job_run_id})`
     });
	 
var ret_wm_id = get_wm_id.execute();
ret_wm_id.next();
var last_watermarkid_from_tble = ret_wm_id.getColumnValue(1);	 
var last_wm_id1 = snowflake.createStatement({sqlText: `SELECT MIN(cycle_id) FROM ${db_views}.dw_edw_views.txn_hdr_combined txnc WHERE txn_dte>= '2021-04-01'`});
var last_wm_id2=last_wm_id1.execute();
last_wm_id2.next();
var last_wm_id3=last_wm_id2.getColumnValue(1);

if (last_watermarkid_from_tble === null) {
  last_wm_id = last_wm_id3;
} else {
  last_wm_id = last_watermarkid_from_tble;
}

results_array[0]=last_watermark;
results_array[1]=last_watermark1;


var t_banner_division_id_stmt = snowflake.createStatement({
    sqlText: `CREATE OR REPLACE TEMP TABLE ${db_refined}.DW_R_STAGE.t_banner_division_id as
select
lsfu.Retail_Store_Facility_Nbr,
lsfu.Retail_store_D1_sk,
db.Banner_D1_Sk,
dd.division_D1_Sk
from ${db_views}.DW_VIEWS.D1_RETAIL_STORE lsfu
inner join ${db_views}.DW_VIEWS.D1_DIVISION dd
on dd.division_id = lsfu.division_id
inner join ${db_views}.DW_VIEWS.D1_BANNER db
on db.banner_nm = lsfu.banner_nm;`});

var t_banner_division_id_rslt = t_banner_division_id_stmt.execute();

var t_customer_base_stmt = snowflake.createStatement({
    sqlText: `CREATE OR REPLACE TEMP TABLE ${db_refined}.DW_R_STAGE.t_customer_base as
SELECT
  rc.Retail_Customer_UUID ,
  clp.loyalty_program_card_nbr as current_card_nbr,
  rch.HOUSEHOLD_ID
  FROM ${db_views}.DW_VIEWS.RETAIL_CUSTOMER rc
  inner join ${db_views}.DW_VIEWS.RETAIL_CUSTOMER_HOUSEHOLD rch
  on rc.Retail_Customer_UUID = rch.Retail_Customer_UUID
  inner join ${db_views}.DW_VIEWS.RETAIL_HOUSEHOLD rh
  on rh.household_id = rch.household_id
  inner JOIN ${db_views}.DW_VIEWS.CUSTOMER_LOYALTY_PROGRAM clp
  on rc.Retail_Customer_UUID = clp.Retail_Customer_UUID
  WHERE rc.DW_LOGICAL_DELETE_IND='FALSE'
  AND to_date(rc.DW_LAST_EFFECTIVE_TS) ='9999-12-31'
  QUALIFY (ROW_NUMBER() OVER (PARTITION BY current_card_nbr ORDER  BY clp.DW_FIRST_EFFECTIVE_TS DESC)=1);`});
  
var t_customer_base_rslt = t_customer_base_stmt.execute();  
var t_abs_txn_hdr_dtls_stmt = snowflake.createStatement({
    sqlText: `CREATE OR REPLACE TEMP TABLE ${db_refined}.DW_R_STAGE.t_abs_txn_hdr_dtls as
SELECT
customer.Retail_Customer_UUID,
hdr.txn_id,
hdr.txn_dte,
hdr.card_nbr,
customer.household_id,
bdid.Retail_store_D1_sk,
bdid.Banner_D1_Sk,
bdid.division_D1_Sk,
SUM(hdr.total_gross_amt+hdr.total_mkdn_amt) as txn_amt
from ${db_views}.dw_edw_views.txn_hdr_combined hdr
inner join ${db_refined}.DW_R_STAGE.t_customer_base customer
on customer.current_card_nbr = hdr.card_nbr::varchar
inner join ${db_refined}.DW_R_STAGE.t_banner_division_id bdid
on hdr.store_id = try_to_numeric(bdid.Retail_Store_Facility_Nbr)
where 
hdr.txn_dte >= TO_TIMESTAMP('${last_watermark}')
and 
hdr.txn_dte < TO_TIMESTAMP('${current_watermark}')
group by 1,2,3,4,5,6,7,8;`});
var t_abs_txn_hdr_dtls_rslt = t_abs_txn_hdr_dtls_stmt.execute();


var delete_customer_phn = `delete from ${db_refined}.DW_R_STAGE.customer_phn where Retail_Customer_UUID in 
(select Retail_Customer_UUID from 
 (
  
   SELECT
 distinct
  a.Retail_Customer_UUID,
  a.current_card_nbr,
  a.HOUSEHOLD_ID,
  cpfc.phone_nbr full_phone_nbr
  FROM ${db_refined}.DW_R_STAGE.t_customer_base a
  inner join ${db_views}.DW_VIEWS.CUSTOMER_PHONE_FAX_CONTACT cpfc
  on a.Retail_Customer_UUID = cpfc.Retail_Customer_UUID 
where dw_create_ts >= TO_TIMESTAMP('${last_watermark}')  
and dw_create_ts < TO_TIMESTAMP('${current_watermark}')
  QUALIFY (ROW_NUMBER() OVER (PARTITION BY full_phone_nbr ORDER  BY current_card_nbr asc)=1 )
  )
  )`;
  
  
var insert_customer_phn = `insert into ${db_refined}.DW_R_STAGE.customer_phn
  SELECT
 distinct
  a.Retail_Customer_UUID,
  a.current_card_nbr,
  a.HOUSEHOLD_ID,
  cpfc.phone_nbr full_phone_nbr
  FROM ${db_refined}.DW_R_STAGE.t_customer_base a
  inner join ${db_views}.DW_VIEWS.CUSTOMER_PHONE_FAX_CONTACT cpfc
  on a.Retail_Customer_UUID = cpfc.Retail_Customer_UUID 
  where dw_create_ts >= TO_TIMESTAMP('${last_watermark}')
  and dw_create_ts < TO_TIMESTAMP('${current_watermark}')
  QUALIFY (ROW_NUMBER() OVER (PARTITION BY full_phone_nbr ORDER  BY current_card_nbr asc)=1)`;
  
  try {
        snowflake.execute ( 
            {sqlText: delete_customer_phn}
        );
		snowflake .execute (
		{sqlText:  insert_customer_phn }
		);
    }
    catch (err)  { 
    throw "error" + err;   // Return a error message.
    }



var t_partner_order_dtl_stmt = snowflake.createStatement({
    sqlText: `
	CREATE OR REPLACE TEMP TABLE ${db_refined}.DW_R_STAGE.t_partner_order_dtl as
WITH po AS
(
  select partner_id,order_id,dlvry_id,loyalty_phone_nbr
  from ${db_views}.dw_edw_views.partner_order po
  group by partner_id,order_id,dlvry_id,loyalty_phone_nbr
),
pos as (
  select order_id,store_txn_ts,partner_id,store_id,dlvry_id,dw_last_updt_ts
  FROM ${db_views}.dw_edw_views.partner_order_store
  WHERE dw_last_updt_ts >= TO_DATE('${last_watermark}') - 30
  and dw_last_updt_ts < TO_TIMESTAMP('${current_watermark}')
  QUALIFY (ROW_NUMBER() OVER (PARTITION BY order_id,dlvry_id ORDER  BY store_txn_ts desc)=1)
),
poi as (
  select order_id,dlvry_id,sum(gross_merch_val_amt) as gross_merch_val_amt
  from ${db_views}.dw_edw_views.partner_order_itm
  group by 1,2
),
post as
(
select order_id,dlvry_id,txn_id,net_amt,to_date(store_txn_ts)as txn_date
  FROM ${db_views}.dw_edw_views.partner_order_store_tender post
  where txn_id is not null
  QUALIFY (ROW_NUMBER() OVER (PARTITION BY txn_id,order_id,dlvry_id ORDER  BY store_txn_ts desc)=1)
)
SELECT
customer_phn.Retail_Customer_UUID,
po.partner_id,
po.order_id,
bdid.Retail_store_D1_sk,
bdid.Banner_D1_Sk,
bdid.division_D1_Sk,
customer_phn.household_id,
post.txn_id,
post.txn_date,
post.net_amt as txn_amt,
poi.gross_merch_val_amt as gmv_order_value,
pos.dw_last_updt_ts
FROM po
inner JOIN ${db_refined}.DW_R_STAGE.customer_phn customer_phn
ON  po.loyalty_phone_nbr = customer_phn.full_phone_nbr
INNER JOIN poi
ON poi.order_id = po.order_id
and poi.dlvry_id = po.dlvry_id
INNER JOIN pos
ON pos.order_id = po.order_id
AND pos.dlvry_id = po.dlvry_id
INNER JOIN ${db_refined}.DW_R_STAGE.t_banner_division_id bdid
ON pos.store_id = try_to_numeric(bdid.Retail_Store_Facility_Nbr)
inner join post
on po.order_id = post.order_id
and po.dlvry_id = post.dlvry_id
;`});

var t_partner_order_dtl_rslt = t_partner_order_dtl_stmt.execute();  
var j4u_transaction_dtl_stmt = snowflake.createStatement({
    sqlText: `CREATE OR REPLACE TEMP TABLE ${db_refined}.DW_R_STAGE.j4u_transaction_dtl AS
select a.txn_id
from ${db_views}.dw_edw_views.txn_facts as a
join
   
    (select offer_id
    from ${db_views}.dw_views.OFFER
    where offer_id > 0
    and length(offer_external_id) > 1
    and offer_external_id not like '%-ND' --non digital
    and substr(offer_external_id,13,1) <> '_'  --J4U
    group by 1) as b
on a.incentive_id = b.offer_id
where a.txn_dte >= TO_TIMESTAMP('${last_watermark}')
and a.txn_dte < TO_TIMESTAMP('${current_watermark}')
group by 1
;`});
var j4u_transaction_dtl_rslt = j4u_transaction_dtl_stmt.execute();  
var gr_transaction_dtl_stmt = snowflake.createStatement({
    sqlText: `CREATE OR REPLACE TEMP TABLE ${db_refined}.DW_R_STAGE.gr_transaction_dtl AS
select a.txn_id
from ${db_views}.dw_edw_views.txn_facts as a
join
   
    (select offer_id
    from ${db_views}.dw_views.OFFER
    where offer_id > 0
    and length(offer_external_id) > 1
    and offer_external_id not like '%-ND' --non digital
    and substr(offer_external_id,13,1) = '_'  --Grocery Rewards only
    group by 1) as b
on a.incentive_id = b.offer_id
where
a.txn_dte >= TO_TIMESTAMP('${last_watermark}')
and a.txn_dte < TO_TIMESTAMP('${current_watermark}')
group by 1;`});

var gr_transaction_dtl_rslt = gr_transaction_dtl_stmt.execute();  
var fuel_own_transaction_dtl_stmt = snowflake.createStatement({
    sqlText: `
	CREATE OR REPLACE TEMP TABLE ${db_refined}.DW_R_STAGE.fuel_own_transaction_dtl AS
SELECT      a.txn_id
FROM    (  
            select      txn.txn_dte
                ,   txn.txn_id
                ,   cast (str.parent_op_area_cd as smallint) as division_id
                ,   hhs.household_id
            from        ${db_views}.dw_edw_views.txn_facts        txn
                ,   ${db_views}.dw_edw_views.lu_card_account  hhs
                ,   ${db_views}.dw_edw_views.lu_store_finance_om  str
                ,   ${db_views}.dw_edw_views.lu_upc       upc
            where   txn.card_nbr = hhs.card_nbr
                and hhs.household_id > 0
                and txn.store_id = str.store_id
                and txn.upc_id = upc.upc_id
                and upc.category_id = 9801
                and upc.corporation_id = 1
                and txn_dte >= TO_TIMESTAMP('${last_watermark}')
				and txn_dte < TO_TIMESTAMP('${current_watermark}')
            group by        1,2,3,4  )  
           A
            ----        --------
    ,   (   select      txn.txn_dte
                ,   txn.txn_id
                ,   cast (str.parent_op_area_cd as smallint) as division_id
                ,   hhs.household_id
            from        ${db_views}.dw_edw_views.txn_facts        txn
                ,   ${db_views}.dw_edw_views.lu_card_account  hhs
                ,   ${db_views}.dw_edw_views.lu_store_finance_om  str
                ,   ${db_views}.dw_edw_views.lu_upc       upc
            where       txn.mkdn_amt < 0
                and txn.incentive_id in ( 94457, 377074 ,7917754)
                and txn.card_nbr = hhs.card_nbr
                and hhs.household_id > 0
                and txn.store_id = str.store_id
                and txn.upc_id = upc.upc_id
                and upc.category_id = 9801
                and upc.corporation_id = 1
                and txn_dte >= TO_TIMESTAMP('${last_watermark}')
				and txn_dte < TO_TIMESTAMP('${current_watermark}')
            group by        1,2,3,4    
        )   B
WHERE       A.txn_dte = B.txn_dte
    AND A.txn_id = B.txn_id
    AND A.division_id = B.division_id
    AND A.household_id = B.household_id
    and a.txn_dte >= TO_TIMESTAMP('${last_watermark}')
	and a.txn_dte < TO_TIMESTAMP('${current_watermark}')
GROUP BY        1;`});

var fuel_own_transaction_dtl_rslt = fuel_own_transaction_dtl_stmt.execute();  

var REWARD_EARNS_stmt = snowflake.createStatement({
    sqlText: `
CREATE OR REPLACE TEMP TABLE ${db_refined}.DW_R_STAGE.REWARD_EARNS AS 
with earnedpoints as
( select
TRANSACTION_ID,
date(reward_origin_ts) as txn_dt ,
sum(REWARD_DOLLAR_POINTS_QTY) as earnedpoints
from "${db_views}"."DW_VIEWS"."REWARD_TRANSACTION"
where DW_LOGICAL_DELETE_IND = FALSE
and DW_CURRENT_VERSION_IND = TRUE
and status_cd = 'E'
--and date(txn_dt) >= TO_TIMESTAMP('${last_watermark}')
--and date(txn_dt) < TO_TIMESTAMP('${current_watermark}')
group by 1,2),
redeempoints as
(select
TRANSACTION_ID,
date(reward_origin_ts) as txn_dt ,
sum(REWARD_DOLLAR_POINTS_QTY) as redeempoints
from "${db_views}"."DW_VIEWS"."REWARD_TRANSACTION"
where DW_LOGICAL_DELETE_IND = FALSE
and DW_CURRENT_VERSION_IND = TRUE
and status_cd = 'R'
--and date(txn_dt) >= TO_TIMESTAMP('${last_watermark}')
--and date(txn_dt) < TO_TIMESTAMP('${current_watermark}')
group by 1,2 )
select
ep.TRANSACTION_ID as txn_id,
ep.txn_dt as txn_dt,
case when rp.redeempoints is null
then ep.earnedpoints
else (ep.earnedpoints-rp.redeempoints) end
as rwd_earn
from earnedpoints ep left join redeempoints rp
 on ep.TRANSACTION_ID = rp.TRANSACTION_ID
and ep.txn_dt = rp.txn_dt;`});

var REWARD_EARNS_rslt = REWARD_EARNS_stmt.execute();  

var REWARD_EARNS_partner_stmt = snowflake.createStatement({
    sqlText: `CREATE OR REPLACE TEMP TABLE ${db_refined}.DW_R_STAGE.REWARD_EARNS_PARTNER AS
select
TRANSACTION_ID,
substr(alt_transaction_id,charindex('|',alt_transaction_id)+1,len(alt_transaction_id)) as alt_transaction_id,
reward_origin_cd,
date(reward_origin_ts) as txn_dt ,
sum(REWARD_DOLLAR_POINTS_QTY) as earnedpoints
from "${db_views}"."DW_VIEWS"."REWARD_TRANSACTION"
where DW_LOGICAL_DELETE_IND = FALSE
and DW_CURRENT_VERSION_IND = TRUE
and status_cd = 'E'
and date(dw_create_ts) >= TO_TIMESTAMP('${last_watermark}')
and date(dw_create_ts) <  TO_TIMESTAMP('${current_watermark}')
and reward_origin_cd in ('INSTACART')
--and alt_transaction_id is not null
group by 1,2,3,4
UNION
select
TRANSACTION_ID,
substr(alt_transaction_id,charindex('|',alt_transaction_id)+1,len(alt_transaction_id)) as alt_transaction_id,
reward_origin_cd,
date(reward_origin_ts) as txn_dt ,
sum(REWARD_DOLLAR_POINTS_QTY) as earnedpoints
from "${db_views}"."DW_VIEWS"."REWARD_TRANSACTION"
where DW_LOGICAL_DELETE_IND = FALSE
and DW_CURRENT_VERSION_IND = TRUE
and status_cd = 'E'
and date(dw_create_ts) >= TO_TIMESTAMP('${last_watermark1}')
and date(dw_create_ts) <  TO_TIMESTAMP('${current_watermark}')
and reward_origin_cd in ('UBER','DOORDASH')
--and alt_transaction_id is not null
group by 1,2,3,4
;`});

var REWARD_EARNS_partner_rslt = REWARD_EARNS_partner_stmt.execute();  

var ubd_transaction_dtl_stmt = snowflake.createStatement({
    sqlText: `CREATE OR REPLACE TEMP TABLE ${db_refined}.DW_R_STAGE.t_partner_grocery_order_dtl_ubd as
WITH pgod as (
  select order_id,date(STORE_TRANSACTION_TS) STORE_TRANSACTION_TS,LOYALTY_PHONE_NBR,sum(REVENUE_AMT) as gmv_order_value
  FROM "${db_views}"."DW_VIEWS"."PARTNER_GROCERY_ORDER_DETAIL" pgod
  --WHERE dw_last_updt_ts >= TO_TIMESTAMP('${last_watermark}')
  --and dw_last_updt_ts < TO_TIMESTAMP('${current_watermark}')
GROUP BY 1,2,3
  --QUALIFY (ROW_NUMBER() OVER (PARTITION BY order_id ORDER  BY STORE_TRANSACTION_TS desc)=1)
),
pgoh as (
  select order_id,date(STORE_TRANSACTION_TS) STORE_TRANSACTION_TS,PARTNER_ID,STORE_ID,sum(NET_AMT) as net_amt
  from "${db_views}"."DW_VIEWS". "PARTNER_GROCERY_ORDER_HEADER" pgoh
  WHERE dw_create_ts >= TO_TIMESTAMP('${last_watermark1}')
  and dw_create_ts < TO_TIMESTAMP('${current_watermark}')
 group by 1,2,3,4
),
pgot as
(
select distinct order_id
  FROM "${db_views}"."DW_VIEWS"."PARTNER_GROCERY_ORDER_TENDER" pgot
  )
SELECT
customer_phn.Retail_Customer_UUID,
pgoh.partner_id,
pgod.order_id,
bdid.Retail_store_D1_sk,
bdid.Banner_D1_Sk,
bdid.division_D1_Sk,
customer_phn.household_id,
0 as txn_id,
date(pgod.store_transaction_ts) txn_date,
pgoh.net_amt as txn_amt,--net amt(header)
pgod.gmv_order_value as gmv_order_value--revenue amt(detail)
FROM pgod
inner JOIN ${db_refined}.DW_R_STAGE.customer_phn customer_phn
ON  pgod.loyalty_phone_nbr = customer_phn.full_phone_nbr
INNER JOIN pgoh
ON pgoh.order_id = pgod.order_id
--and poi.dlvry_id = po.dlvry_id
INNER JOIN pgot
ON pgot.order_id = pgoh.order_id
--AND pos.dlvry_id = po.dlvry_id
INNER JOIN ${db_refined}.DW_R_STAGE.t_banner_division_id bdid
ON pgoh.store_id = try_to_numeric(bdid.Retail_Store_Facility_Nbr);`});

var ubd_transaction_dtl_rslt = ubd_transaction_dtl_stmt.execute();  

var insert1_F_Trans_Part_Eng_stmt = snowflake.createStatement({
    sqlText: `
	INSERT INTO ${db_analytics}.dw_retail_exp.F_Transaction_Partner_Engagement
select
  abs_txn.txn_id,
  fd.fiscal_day_id ,
  abs_txn.division_D1_Sk AS division_id,
  abs_txn.Retail_store_D1_sk as Retail_store_D1_sk,
  abs_txn.txn_dte,
  drc.Retail_Customer_D1_sk ,
  4 as business_partner_d1_sk,
  CUSTOMER_BUSINESS_PARTNER_REGISTRATION_D1_SK,
  abs_txn.Banner_D1_Sk AS Banner_D1_Sk,
  'N/A' as adobe_banner_nm,
  0 as ORDER_ID,
  abs_txn.txn_amt as TRANSACTION_AMT,
  0 as GROSS_MERCHANT_VALUE_AMT,
  CASE WHEN j4u.txn_id is not null then TRUE
     ELSE FALSE
  END AS J4U_ENGAGEMENT_IND,
  CASE WHEN gr.txn_id is not null then TRUE
      ELSE FALSE
  END AS GROCERY_REWARD_ENGAGEMENT_IND,
  CASE WHEN fr_own.txn_id is not null then TRUE
      ELSE FALSE
  END AS FUEL_REWARD_OWN_ENGAGEMENT_IND,
  FALSE AS FUEL_REWARD_PARTNER_ENGAGEMENT_IND,
  COALESCE(reward.rwd_earn,0) as POINTS_EARNED_NBR,
  current_timestamp() as DW_LAST_UPDATED_TS ,
  current_timestamp() as DW_CREATE_TS
FROM ${db_refined}.DW_R_STAGE.t_abs_txn_hdr_dtls abs_txn
LEFT JOIN ${db_refined}.DW_R_STAGE.t_partner_order_dtl part_txn
on abs_txn.txn_id = part_txn.txn_id
inner join ${db_views}.dw_views.D1_RETAIL_CUSTOMER drc
on abs_txn.Retail_Customer_UUID = drc.Retail_Customer_UUID
inner join ${db_analytics}.dw_retail_exp.D1_CUSTOMER_BUSINESS_PARTNER_REGISTRATION bpr
on abs_txn.Retail_Customer_UUID = bpr.Retail_Customer_UUID
and abs_txn.household_id = bpr.household_id
LEFT JOIN ${db_refined}.DW_R_STAGE.j4u_transaction_dtl j4u
on abs_txn.txn_id = j4u.txn_id
left join ${db_refined}.DW_R_STAGE.gr_transaction_dtl gr
on abs_txn.txn_id = gr.txn_id
left join ${db_refined}.DW_R_STAGE.fuel_own_transaction_dtl fr_own
on abs_txn.txn_id = fr_own.txn_id
left join ${db_refined}.DW_R_STAGE.REWARD_EARNS reward
on abs_txn.txn_id = reward.txn_id
--and abs_txn.household_id = reward.household_id
inner join ${db_views}.dw_views.D0_FISCAL_DAY fd
on abs_txn.txn_dte = fd.calendar_dt
where part_txn.txn_id is null ;`});



var insert1_F_Trans_Part_Eng_rslt = insert1_F_Trans_Part_Eng_stmt.execute();
insert1_F_Trans_Part_Eng_rslt.next();
var return_value1 = insert1_F_Trans_Part_Eng_rslt.getColumnValue(1);

var insert3_F_Trans_Part_Eng_stmt = snowflake.createStatement({
    sqlText: `
	INSERT INTO ${db_analytics}.DW_RETAIL_EXP.F_Transaction_Partner_Engagement
(select
  part_txn.txn_id,
  fd.fiscal_day_id ,
  part_txn.division_D1_Sk AS division_id,
  part_txn.Retail_store_D1_sk as Retail_store_D1_sk,
  part_txn.txn_date as txn_dte,
  drc.Retail_Customer_D1_sk ,
  dbp.business_partner_d1_sk as business_partner_d1_sk,
  CUSTOMER_BUSINESS_PARTNER_REGISTRATION_D1_SK,
  part_txn.Banner_D1_Sk AS Banner_D1_Sk,
  'N/A' as adobe_banner_nm,
   COALESCE(part_txn.order_id,0) as ORDER_ID,
    part_txn.txn_amt as TRANSACTION_AMT,
  COALESCE(part_txn.gmv_order_value,0) as GROSS_MERCHANT_VALUE_AMT,
  FALSE AS J4U_ENGAGEMENT_IND,
  FALSE AS GROCERY_REWARD_ENGAGEMENT_IND,
  FALSE AS FUEL_REWARD_OWN_ENGAGEMENT_IND,
  FALSE AS FUEL_REWARD_PARTNER_ENGAGEMENT_IND,
 -- 0  POINTS_EARNED_NBR,
 COALESCE(reward_insta.earnedpoints,0) as POINTS_EARNED_NBR,
  current_timestamp() as DW_LAST_UPDATED_TS ,
  current_timestamp() as DW_CREATE_TS
 FROM 
${db_refined}.DW_R_STAGE.t_partner_order_dtl part_txn
inner join ${db_views}.dw_views.D1_RETAIL_CUSTOMER drc
on part_txn.Retail_Customer_UUID = drc.Retail_Customer_UUID
left join ${db_views}.dw_views.D1_BUSINESS_PARTNER dbp
on part_txn.partner_id = try_to_numeric(dbp.partner_id)
inner join ${db_analytics}.dw_retail_exp.D1_CUSTOMER_BUSINESS_PARTNER_REGISTRATION bpr
on part_txn.Retail_Customer_UUID = bpr.Retail_Customer_UUID
and part_txn.household_id = bpr.household_id
inner join ${db_views}.dw_views.D0_FISCAL_DAY fd
on part_txn.txn_date = fd.calendar_dt
inner JOIN ${db_refined}.DW_R_STAGE.REWARD_EARNS_PARTNER reward_insta
on reward_insta.reward_origin_cd in ('INSTACART') and
part_txn.order_id = try_to_numeric(reward_insta.alt_transaction_id)
union 
select
  part_txn.txn_id,
  fd.fiscal_day_id ,
  part_txn.division_D1_Sk AS division_id,
  part_txn.Retail_store_D1_sk as Retail_store_D1_sk,
  part_txn.txn_date as txn_dte,
  drc.Retail_Customer_D1_sk ,
  dbp.business_partner_d1_sk as business_partner_d1_sk,
  CUSTOMER_BUSINESS_PARTNER_REGISTRATION_D1_SK,
  part_txn.Banner_D1_Sk AS Banner_D1_Sk,
  'N/A' as adobe_banner_nm,
   COALESCE(part_txn.order_id,0) as ORDER_ID,
    part_txn.txn_amt as TRANSACTION_AMT,
  COALESCE(part_txn.gmv_order_value,0) as GROSS_MERCHANT_VALUE_AMT,
  FALSE AS J4U_ENGAGEMENT_IND,
  FALSE AS GROCERY_REWARD_ENGAGEMENT_IND,
  FALSE AS FUEL_REWARD_OWN_ENGAGEMENT_IND,
  FALSE AS FUEL_REWARD_PARTNER_ENGAGEMENT_IND,
 -- 0  POINTS_EARNED_NBR,
 COALESCE(reward_insta.earnedpoints,0) as POINTS_EARNED_NBR,
  current_timestamp() as DW_LAST_UPDATED_TS ,
  current_timestamp() as DW_CREATE_TS
 FROM 
${db_refined}.DW_R_STAGE.t_partner_order_dtl part_txn
inner join ${db_views}.dw_views.D1_RETAIL_CUSTOMER drc
on part_txn.Retail_Customer_UUID = drc.Retail_Customer_UUID
left join ${db_views}.dw_views.D1_BUSINESS_PARTNER dbp
on part_txn.partner_id = try_to_numeric(dbp.partner_id)
inner join ${db_analytics}.dw_retail_exp.D1_CUSTOMER_BUSINESS_PARTNER_REGISTRATION bpr
on part_txn.Retail_Customer_UUID = bpr.Retail_Customer_UUID
and part_txn.household_id = bpr.household_id
inner join ${db_views}.dw_views.D0_FISCAL_DAY fd
on part_txn.txn_date = fd.calendar_dt
inner JOIN ${db_refined}.DW_R_STAGE.REWARD_EARNS_PARTNER reward_insta
on reward_insta.reward_origin_cd in ('INSTACART') and
part_txn.order_id = reward_insta.transaction_id )
 ;`});

var insert3_F_Trans_Part_Eng_rslt = insert3_F_Trans_Part_Eng_stmt.execute();
insert3_F_Trans_Part_Eng_rslt.next();
var return_value3 = insert3_F_Trans_Part_Eng_rslt.getColumnValue(1);
  
var insert4_F_Trans_Part_Eng_stmt = snowflake.createStatement({
    sqlText: `
	INSERT INTO ${db_analytics}.DW_RETAIL_EXP.F_Transaction_Partner_Engagement 
select
  0 AS TXN_ID,
  fd.fiscal_day_id ,
  part_txn_ubd.division_D1_Sk AS division_id,
  part_txn_ubd.Retail_store_D1_sk as Retail_store_D1_sk,
  part_txn_ubd.txn_date as txn_dte,
  drc.Retail_Customer_D1_sk ,
  dbp.business_partner_d1_sk as business_partner_d1_sk,
  CUSTOMER_BUSINESS_PARTNER_REGISTRATION_D1_SK,
  part_txn_ubd.Banner_D1_Sk AS Banner_D1_Sk,
  'N/A' as adobe_banner_nm,
  COALESCE(part_txn_ubd.order_id,'0') as ORDER_ID,
   part_txn_ubd.txn_amt as TRANSACTION_AMT,
  COALESCE(part_txn_ubd.gmv_order_value,0) as GROSS_MERCHANT_VALUE_AMT,
  FALSE AS J4U_ENGAGEMENT_IND,
  FALSE AS GROCERY_REWARD_ENGAGEMENT_IND,
  FALSE AS FUEL_REWARD_OWN_ENGAGEMENT_IND,
  FALSE AS FUEL_REWARD_PARTNER_ENGAGEMENT_IND,
  COALESCE(reward_ubd.earnedpoints,0) as POINTS_EARNED_NBR,
  current_timestamp() as DW_LAST_UPDATED_TS ,
  current_timestamp() as DW_CREATE_TS
  FROM 
${db_refined}.DW_R_STAGE.t_partner_grocery_order_dtl_ubd part_txn_ubd
inner join ${db_views}.dw_views.D1_RETAIL_CUSTOMER drc
on part_txn_ubd.Retail_Customer_UUID = drc.Retail_Customer_UUID
left join ${db_views}.dw_views.D1_BUSINESS_PARTNER dbp
on part_txn_ubd.partner_id = try_to_numeric(dbp.partner_id)
inner join ${db_analytics}.dw_retail_exp.D1_CUSTOMER_BUSINESS_PARTNER_REGISTRATION bpr
on part_txn_ubd.Retail_Customer_UUID = bpr.Retail_Customer_UUID
and part_txn_ubd.household_id = bpr.household_id
inner join ${db_views}.dw_views.D0_FISCAL_DAY fd
on date(part_txn_ubd.txn_date) = fd.calendar_dt
left JOIN ${db_refined}.DW_R_STAGE.REWARD_EARNS_PARTNER reward_ubd
on part_txn_ubd.order_id=reward_ubd.alt_transaction_id
 ;`});

var insert4_F_Trans_Part_Eng_rslt = insert4_F_Trans_Part_Eng_stmt.execute();
insert4_F_Trans_Part_Eng_rslt.next();
var return_value4 = insert4_F_Trans_Part_Eng_rslt.getColumnValue(1);

  
var insert2_F_Trans_Part_Eng_stmt = snowflake.createStatement({
    sqlText: `
	INSERT INTO ${db_analytics}.dw_retail_exp.F_Transaction_Partner_Engagement
select
    TXN_ID,
    FISCAL_DAY_ID,
    Division_D1_SK,
    0 as Retail_store_D1_SK,
    TXN_DT,
    0 as Retail_customer_D1_SK,
    4 as Business_Partner_D1_SK,
    cbpr.CUSTOMER_BUSINESS_PARTNER_REGISTRATION_D1_SK ,
    0 as BANNER_D1_SK,
    'N/A' as adobe_banner_nm,      
    0 as ORDER_ID,
    TRANSACTION_AMT,
    0 as GROSS_MERCHANT_VALUE_AMT,
    'FALSE' as J4U_ENGAGEMENT_IND,
    'FALSE' as GROCERY_REWARD_ENGAGEMENT_IND,
    'FALSE' as FUEL_REWARD_OWN_ENGAGEMENT_IND,
    FUEL_REWARD_PARTNER_ENGAGEMENT_IND,
    0 as POINTS_EARNED_NBR,
    current_timestamp() as DW_Last_Updated_TS,
    current_timestamp() as DW_CREATE_TS
from
    (
        select
            gas.TXN_ID,
            gas.TXN_DT,
            fd.fiscal_day_id ,
            gas.TXN_REFERENCE_ID,
            gas.CLUB_CARD_NBR as CARD_NBR,
            gas.HOUSEHOLD_ID,
            gas.TXN_NET_PAYMENT_AMT as TRANSACTION_AMT,
            gas.LOCATION_SALE_DIVISION_ID,
            CASE
                WHEN b.txn_id is not null THEN TRUE
                ELSE FALSE
            END AS FUEL_REWARD_PARTNER_ENGAGEMENT_IND
        from
            ${db_views}.dw_edw_views.PARTNER_GAS_TRANSACTION_DTL gas
            inner join ${db_views}.dw_views.D0_FISCAL_DAY fd
                on gas.txn_dt = fd.calendar_dt
            left join(
                select
                    gas.CLUB_CARD_NBR,
                    gas.HOUSEHOLD_ID,
                    gas.TXN_ID,
                    max(gas.TXN_DT) as TXN_DT,
                    sum(gas.TXN_NET_PAYMENT_AMT) as TXN_NET_PAYMENT_AMT
                from
                    ${db_views}.dw_edw_views.PARTNER_GAS_TRANSACTION_DTL gas
                where
                    gas.reward_token_offer_qty < 0
                    and trim(upper(gas.message_type_code)) IN ('REDREQ')
                    and trim(upper(gas.record_activity_status_cd)) IN ('T')
                    and gas.txn_dt >= TO_TIMESTAMP('${last_watermark}')
					and gas.txn_dt < TO_TIMESTAMP('${current_watermark}')
                    and gas.location_sale_division_id in (5, 17, 19, 20, 23, 25, 26, 27, 28, 29, 30, 33, 34, 35, 32)
                    and (
                        gas.HOUSEHOLD_ID is not null
                        or gas.HOUSEHOLD_ID > 0
                    )
                group by
                    1,
                    2,
                    3
            ) B on gas.txn_id = b.txn_id
            and gas.txn_dt = b.txn_dt
    ) fct_eng
    inner join ${db_analytics}.dw_retail_exp.D1_CUSTOMER_BUSINESS_PARTNER_REGISTRATION cbpr
    on fct_eng.household_id = cbpr.household_id
    inner join ${db_views}.DW_VIEWS.D1_DIVISION dd
    on try_to_numeric(dd.division_id) = (CASE WHEN LOCATION_SALE_DIVISION_ID = 23 THEN 20 ELSE LOCATION_SALE_DIVISION_ID END )
where
    fct_eng.HOUSEHOLD_ID > 0
    and fct_eng.LOCATION_SALE_DIVISION_ID > 0
    and fct_eng.HOUSEHOLD_ID is not null
    and fct_eng.txn_dt>= TO_TIMESTAMP('${last_watermark}')
	and fct_eng.txn_dt < TO_TIMESTAMP('${current_watermark}')
    and dd.division_id not in ('N/A','string')
    QUALIFY(row_number() over (partition by txn_id order by txn_dt desc) =1);`});
    
 var insert2_F_Trans_Part_Eng_rslt = insert2_F_Trans_Part_Eng_stmt.execute(); 
 insert2_F_Trans_Part_Eng_rslt.next();
 var return_value2 = insert2_F_Trans_Part_Eng_rslt.getColumnValue(1);
 
 var return_value = return_value1 + return_value2 + return_value3 
 
    
var stmt2_fpo = snowflake.createStatement({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Job_Success(
                '${job_run_id}',
                '${return_value}',
                null,
                null)`
                
     });
 //return_value2 will give job run auto id
var res2_fpo = stmt2_fpo.execute();
res2_fpo.next();
var stmt3_fpo = snowflake.createStatement({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Set_Watermark_TS(
                ${job_run_id},
				'${last_watermark}',
				'${current_watermark}'
				)`
                
     });
 //return_value3 will give job run auto id 
var res3_fpo = stmt3_fpo.execute();
res3_fpo.next();
return_value3_fpo = res3_fpo.getColumnValue(1);

var stmt5_fpo = snowflake.createStatement({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Job_Success(
                '${job_run_id1}',
                '${return_value4}',
                null,
                null)`
                
     });
 //return_value2 will give job run auto id
var res5_fpo = stmt5_fpo.execute();
res5_fpo.next();

var stmt4_fpo = snowflake.createStatement({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Set_Watermark_TS(
                ${job_run_id1},
				'${last_watermark1}',
				'${current_watermark}'
				)`
                
     });
 //return_value3 will give job run auto id 
var res4_fpo = stmt4_fpo.execute();
res4_fpo.next();
return_value4_fpo = res4_fpo.getColumnValue(1);

return "F_Transaction_Partner_Engagement Table loaded"

$$;
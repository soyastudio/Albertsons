--liquibase formatted sql
--changeset SYSTEM:SP_F_PARTNER_ORDER_CHANNEL_METRICS_LOAD runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_CONFIRMED_PRD;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE EDM_CONFIRMED_PRD.DW_APPL.SP_F_PARTNER_ORDER_CHANNEL_METRICS_LOAD()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

//--------------------------------------------------------------------------------------------------------------------#
// Desc         : Stored proc to Load KPI target table F_PARTNER_ORDER_CHANNEL_METRICS
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

 
var feed_extract_job_ini2 = snowflake.execute({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Job_Initiate(
                'OCRP',
                'SP_F_PARTNER_ORDER_CHANNEL_METRICS',
                'FACT_PARTNER_ORDER_CHANNEL_METRICS',
                null,
                'I',
                null,
                null,
                null,
                '${db_analytics}.DW_RETAIL_EXP.F_PARTNER_ORDER_CHANNEL_METRIC',
				null)`
                
     });	 


//return_value will give job run auto id 
feed_extract_job_ini2.next();
job_run_id2 = feed_extract_job_ini2.getColumnValue(1);




var get_wm_ts2 = snowflake.createStatement({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Get_Watermark_TS(${job_run_id2})`
                
     });


var get_wm_id = snowflake.createStatement({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Get_Watermark_ID(${job_run_id2})`
     });
	 
	 
//return_value will give job run auto id 
var ret_wm_ts2 = get_wm_ts2.execute();
ret_wm_ts2.next();
var last_watermark_from_tble2 = ret_wm_ts2.getColumnValue(1);



//return_value will give job run auto id 
var ret_wm_id = get_wm_id.execute();
ret_wm_id.next();
var last_watermarkid_from_tble = ret_wm_id.getColumnValue(1);

if (last_watermark_from_tble2 === null) {
  last_watermark2 = '2021-04-01 00:00:00.000';
} else {
  last_watermark2 = last_watermark_from_tble2;
}


var last_wm_id1 = snowflake.createStatement({sqlText: `SELECT MIN(cycle_id) FROM ${db_views}.dw_edw_views.txn_hdr_combined txnc WHERE txn_dte>= '2021-04-01'`});
var last_wm_id2=last_wm_id1.execute();
last_wm_id2.next();
var last_wm_id3=last_wm_id2.getColumnValue(1);

var default_wm_id1 = snowflake.createStatement({
     sqlText: `SELECT NVL(${last_watermarkid_from_tble},${last_wm_id3})`
     });
var default_wm_id2 = default_wm_id1.execute();
default_wm_id2.next();

var last_wm_id = default_wm_id2.getColumnValue(1);

//---------------------

var feed_extract_job_ini3 = snowflake.execute({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Job_Initiate(
                'OCRP',
                'SP_F_PARTNER_ORDER_CHANNEL_METRICS',
                'FACT_PARTNER_ORDER_CHANNEL_METRICS_ubd',
                null,
                'I',
                null,
                null,
                null,
                '${db_analytics}.DW_RETAIL_EXP.F_PARTNER_ORDER_CHANNEL_METRIC',
				null)`
                
     });	 


//return_value will give job run auto id 
feed_extract_job_ini3.next();
job_run_id3 = feed_extract_job_ini3.getColumnValue(1);




var get_wm_ts3 = snowflake.createStatement({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Get_Watermark_TS(${job_run_id3})`
                
     });
	 
//return_value will give job run auto id 
var ret_wm_ts3 = get_wm_ts3.execute();
ret_wm_ts3.next();
var last_watermark_from_ubd = ret_wm_ts3.getColumnValue(1);

if (last_watermark_from_ubd === null) {
  last_watermark3 = '2021-12-01 00:00:00.000';
} else {
  last_watermark3 = last_watermark_from_ubd;	 

}
results_array[0]=last_watermark2;
results_array[1]=last_watermark3;





// ************** Load for FACT_PARTNER_ORDER_CHANNEL_METRICS table BEGIN *****************

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

var stmt_lsfu=snowflake.createStatement({
			sqlText: `CREATE OR REPLACE TEMP TABLE ${db_refined}.DW_R_STAGE.lsfu as (
    select store.Retail_Store_Facility_Nbr as store_id,
        store.Retail_store_D1_sk,
        abl.Banner_D1_Sk,
        dd.division_D1_Sk
    from ${db_views}.DW_VIEWS.D1_RETAIL_STORE store
    inner join ${db_views}.DW_VIEWS.D1_DIVISION dd 
        on dd.division_id = store.division_id
    inner join ${db_refined}.DW_R_STAGE.adobe_banner_lkp abl 
        ON store.banner_nm = abl.ADOBE_BANNER_NM
OR (store.banner_nm <> abl.ADOBE_BANNER_NM and upper(store.banner_nm) = upper(abl.BANNER_nm))
);`});

var stmt_lsfu1 = stmt_lsfu.execute();

var stmt_cust_channel_phn=snowflake.createStatement({
			sqlText: `CREATE OR REPLACE TEMP TABLE ${db_refined}.DW_R_STAGE.customer_channel_phn as (
    SELECT household_id,
    loyalty_active_id,
    full_phone_nbr,
    current_card_nbr,
    Retail_Customer_UUID,
    RETAIL_CUSTOMER_D1_SK
FROM (
        SELECT rch.household_id,
            (
                CASE
                    WHEN cas.STATUS_TYPE_CD IS NOT NULL THEN 1
                    ELSE 2
                END
            ) loyalty_active_id,
            cpfc.phone_nbr full_phone_nbr,
            clp.Loyalty_Program_Card_Nbr as current_card_nbr,
            d1_rc.Retail_Customer_UUID,
            d1_rc.RETAIL_CUSTOMER_D1_SK
         FROM ${db_views}.DW_VIEWS.D1_RETAIL_CUSTOMER d1_rc 
            INNER join ${db_views}.DW_VIEWS.RETAIL_CUSTOMER_HOUSEHOLD rch on d1_rc.Retail_Customer_UUID = rch.Retail_Customer_UUID
            INNER join ${db_views}.DW_VIEWS.RETAIL_HOUSEHOLD rh on rh.household_id = rch.household_id
            LEFT JOIN ${db_views}.DW_VIEWS.CUSTOMER_LOYALTY_PROGRAM clp on d1_rc.Retail_Customer_UUID = clp.Retail_Customer_UUID
            Left join ${db_views}.DW_VIEWS.CUSTOMER_PHONE_FAX_CONTACT cpfc on d1_rc.Retail_Customer_UUID = cpfc.Retail_Customer_UUID
			and cpfc.PHONE_PURPOSE_DSC='PRIMARY'
            left join ${db_views}.DW_VIEWS.CUSTOMER_ACCOUNT_STATUS cas ON d1_rc.Retail_Customer_UUID = cas.Retail_Customer_UUID
            and cas.STATUS_TYPE_CD = 'ONLINE_ENROLLMENT'
            and cas.dw_current_Version_ind = TRUE
            AND cas.dw_logical_delete_ind = FALSE
        WHERE d1_rc.DW_LOGICAL_DELETE_IND = FALSE

    ) a
where household_id is not null QUALIFY (
        ROW_NUMBER() OVER (
            PARTITION BY full_phone_nbr
            ORDER BY a.loyalty_active_id asc
        ) = 1
    )
);`});

var stmt_cust_channel_phn1 = stmt_cust_channel_phn.execute();

var stmt4 = snowflake.createStatement({
			sqlText: `INSERT INTO "${db_analytics}"."DW_RETAIL_EXP".F_PARTNER_ORDER_CHANNEL_METRIC
                       (TRANSACTION_DT,
						RETAIL_CUSTOMER_D1_SK,
						Retail_store_D1_sk,
						BUSINESS_PARTNER_D1_SK,
						banner_d1_sk,
						ORDER_CHARGE_AMT,
						division_d1_sk,
						day_id,
						dw_create_ts,
						dw_last_update_ts)
WITH po AS (
    select po.partner_id,
        BUSINESS_PARTNER_D1_SK,
        order_id,
        dlvry_id,
        loyalty_phone_nbr
    from ${db_views}.dw_edw_views.partner_order po
    left join ${db_views}.DW_VIEWS.D1_BUSINESS_PARTNER d1_bp 
    on po.partner_id::varchar = d1_bp.partner_id 
    group by po.partner_id,
        BUSINESS_PARTNER_D1_SK,
        order_id,
        dlvry_id,
        loyalty_phone_nbr
),
pos as (
    select order_id,
        store_txn_ts,
        partner_id,
        store_id,
        dlvry_id,
        dw_last_updt_ts
    FROM ${db_views}.dw_edw_views.partner_order_store QUALIFY (
            ROW_NUMBER() OVER (
                PARTITION BY order_id,
                dlvry_id
                ORDER BY store_txn_ts desc
            ) = 1
        )
),
post as (
    select order_id,
        dlvry_id,
        txn_id
    from ${db_views}.dw_edw_views.partner_order_store_tender
    group by order_id,
        dlvry_id,
        txn_id
),
poi as (
    select order_id,
        dlvry_id,
        gross_merch_val_amt
    from ${db_views}.dw_edw_views.partner_order_itm
),
src_rec AS (
SELECT DISTINCT
clp.Loyalty_Program_Card_Nbr as current_card_nbr,
d1_rc.RETAIL_CUSTOMER_D1_SK
FROM
${db_views}.DW_VIEWS.D1_RETAIL_CUSTOMER d1_rc
LEFT JOIN ${db_views}.DW_VIEWS.CUSTOMER_LOYALTY_PROGRAM clp on d1_rc.Retail_Customer_UUID = clp.Retail_Customer_UUID
Left join ${db_views}.DW_VIEWS.CUSTOMER_PHONE_FAX_CONTACT cpfc on d1_rc.Retail_Customer_UUID = cpfc.Retail_Customer_UUID
left join ${db_views}.DW_VIEWS.CUSTOMER_ACCOUNT_STATUS cas ON d1_rc.Retail_Customer_UUID = cas.Retail_Customer_UUID
and cas.STATUS_TYPE_CD = 'ONLINE_ENROLLMENT'
and cas.dw_current_Version_ind = TRUE
AND cas.dw_logical_delete_ind = FALSE
inner join po
ON po.loyalty_phone_nbr = cpfc.phone_nbr
INNER JOIN post ON po.order_id = post.order_id
AND po.dlvry_id = post.dlvry_id
INNER JOIN poi ON post.order_id = poi.order_id
AND post.dlvry_id = poi.dlvry_id
INNER JOIN pos ON po.order_id = pos.order_id
AND po.dlvry_id = pos.dlvry_id
INNER JOIN ${db_refined}.DW_R_STAGE.lsfu lsfu ON pos.store_id = try_to_numeric(lsfu.store_id)
WHERE d1_rc.DW_LOGICAL_DELETE_IND = FALSE

),
final_rec AS (
    SELECT thc.txn_dte,
        s.RETAIL_CUSTOMER_D1_SK,
        4 as BUSINESS_PARTNER_D1_SK,
        lsfu.division_D1_Sk,
        lsfu.banner_d1_sk,
        lsfu.Retail_store_D1_sk,
        cal.fiscal_day_id,
        SUM(thc.total_gross_amt + thc.total_mkdn_amt) as amt_spent
    FROM src_rec s
        INNER JOIN ${db_views}.dw_edw_views.txn_hdr_combined thc ON s.current_card_nbr = thc.card_nbr::varchar
        INNER JOIN ${db_refined}.DW_R_STAGE.lsfu lsfu ON thc.store_id = try_to_numeric(lsfu.store_id)
        INNER JOIN "${db_views}"."DW_VIEWS"."D0_FISCAL_DAY" cal
        ON thc.txn_dte=cal.calendar_dt
        LEFT JOIN post ON(thc.txn_id = post.txn_id)
    WHERE thc.cycle_id >= ${last_wm_id}
    AND thc.cycle_id < ${current_wm_id}
	AND post.txn_id IS NULL
    GROUP BY 1,2,3,4,5,6,7
            UNION
   SELECT TO_DATE(pos.store_txn_ts) txn_dte,
          customer_channel_phn.RETAIL_CUSTOMER_D1_SK,
          po.BUSINESS_PARTNER_D1_SK,
          lsfu.division_d1_sk,
          lsfu.banner_d1_sk,
          lsfu.Retail_store_D1_sk,
          cal.fiscal_day_id,
          SUM(poi.gross_merch_val_amt) as amt_spent
   FROM po
          INNER JOIN ${db_refined}.DW_R_STAGE.customer_channel_phn customer_channel_phn 
		  ON po.loyalty_phone_nbr = customer_channel_phn.full_phone_nbr
          INNER JOIN poi ON poi.order_id = po.order_id and poi.dlvry_id = po.dlvry_id
          INNER JOIN pos ON pos.order_id = po.order_id AND pos.dlvry_id = po.dlvry_id
          INNER JOIN ${db_refined}.DW_R_STAGE.lsfu lsfu ON pos.store_id = try_to_numeric(lsfu.store_id)
          INNER JOIN "${db_views}"."DW_VIEWS"."D0_FISCAL_DAY" cal
          ON TO_DATE(pos.store_txn_ts)=cal.calendar_dt
		  WHERE pos.dw_last_updt_ts > TO_TIMESTAMP('${last_watermark2}')
    AND pos.dw_last_updt_ts <= TO_TIMESTAMP('${current_watermark}')
   GROUP BY 1,2,3,4,5,6,7)
   SELECT txn_dte,
    RETAIL_CUSTOMER_D1_SK,
    Retail_store_D1_sk,
    BUSINESS_PARTNER_D1_SK,
    banner_d1_sk,
    amt_spent,
    division_d1_sk,
    fiscal_day_id,
    TO_TIMESTAMP(current_timestamp()) as dw_create_ts,
	TO_TIMESTAMP(current_timestamp()) as dw_last_update_ts
FROM final_rec;`});
   
   var res4 = stmt4.execute();
	res4.next();
    return_value1 = res4.getColumnValue(1);
 
var stmt5 = snowflake.createStatement({
			sqlText: `INSERT INTO "${db_analytics}"."DW_RETAIL_EXP".F_PARTNER_ORDER_CHANNEL_METRIC
                       (TRANSACTION_DT,
						RETAIL_CUSTOMER_D1_SK,
						Retail_store_D1_sk,
						BUSINESS_PARTNER_D1_SK,
						banner_d1_sk,
						ORDER_CHARGE_AMT,
						division_d1_sk,
						day_id,
						dw_create_ts,
						dw_last_update_ts) 
 WITH pgod as (
  select order_id,date(STORE_TRANSACTION_TS) STORE_TRANSACTION_TS,LOYALTY_PHONE_NBR,sum(COALESCE(REVENUE_AMT,0)) as gmv_order_value
  FROM "${db_views}"."DW_VIEWS"."PARTNER_GROCERY_ORDER_DETAIL" pgod
  GROUP BY 1,2,3
  ),
pgoh as (
  select order_id,date(STORE_TRANSACTION_TS) STORE_TRANSACTION_TS,PARTNER_ID,STORE_ID,DW_create_TS,sum(NET_AMT) as net_amt
  from "${db_views}"."DW_VIEWS". "PARTNER_GROCERY_ORDER_HEADER" pgoh
 group by 1,2,3,4,5
),
pgot as
(
select distinct order_id
  FROM "${db_views}"."DW_VIEWS"."PARTNER_GROCERY_ORDER_TENDER" pgot
  )
   SELECT TO_DATE(pgoh.STORE_TRANSACTION_TS) txn_dte,
          customer_channel_phn.RETAIL_CUSTOMER_D1_SK,
		  lsfu.Retail_store_D1_sk,
          dbp.BUSINESS_PARTNER_D1_SK,
          lsfu.banner_d1_sk,
		  SUM(COALESCE(pgod.gmv_order_value,0)) as amt_spent,
		  lsfu.division_d1_sk,
          cal.fiscal_day_id,
		  TO_TIMESTAMP(current_timestamp()) as dw_create_ts,
	      TO_TIMESTAMP(current_timestamp()) as dw_last_update_ts
   FROM pgod
          INNER JOIN ${db_refined}.dw_r_stage.customer_channel_phn customer_channel_phn 
  ON pgod.loyalty_phone_nbr = customer_channel_phn.full_phone_nbr
          INNER JOIN pgoh ON pgoh.order_id = pgod.order_id 
          INNER JOIN pgot ON pgot.order_id = pgod.order_id 
          INNER JOIN ${db_refined}.dw_r_stage.lsfu lsfu ON pgoh.store_id = try_to_numeric(lsfu.store_id)
		  INNER JOIN ${db_views}.dw_views.D1_BUSINESS_PARTNER dbp
          ON pgoh.partner_id = try_to_numeric(dbp.partner_id)
          INNER JOIN "${db_views}"."DW_VIEWS"."D0_FISCAL_DAY" cal
          ON TO_DATE(pgoh.STORE_TRANSACTION_TS)=cal.calendar_dt
		  WHERE pgoh.DW_create_TS > TO_TIMESTAMP('${last_watermark3}')
and pgoh.DW_create_TS <= TO_TIMESTAMP('${current_watermark}')
   GROUP BY 1,2,3,4,5,7,8,9,10;`});

		
	var res5 = stmt5.execute();
	res5.next();
    return_value2 = res5.getColumnValue(1);
	
	var return_value = return_value1 + return_value2
	


// *******************END OF LOAD FOR FACT_PARTNER_ORDER_CHANNEL_METRICS TABLE******************************

var stmt2_fpocm = snowflake.createStatement({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Job_Success(
                '${job_run_id2}',
                '${return_value1}',
                null,
                null)`
                
     });
 
var res2_fpocm = stmt2_fpocm.execute();
res2_fpocm.next();

var stmt3_fpocm = snowflake.createStatement({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Set_Watermark_TS(
                ${job_run_id2},
				'${last_watermark2}',
				'${current_watermark}'
				)`
                
     });
 //return_value3 will give job run auto id 
var res3_fpocm = stmt3_fpocm.execute();
res3_fpocm.next();
return_value3_fpocm = res3_fpocm.getColumnValue(1);



var stmt4_fpocm = snowflake.createStatement({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Set_Watermark_ID(
                ${job_run_id2},
				'${last_wm_id}',
				'${current_wm_id}'
				)`
                
     });
	 
//return_value3 will give job run auto id 
var res4_fpocm = stmt4_fpocm.execute();
res4_fpocm.next();
return_value4_fpocm = res4_fpocm.getColumnValue(1);

//-----------------------------
var stmt5_fpocm = snowflake.createStatement({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Job_Success(
                '${job_run_id3}',
                '${return_value2}',
                null,
                null)`
                
     });
 
var res5_fpocm = stmt5_fpocm.execute();
res5_fpocm.next();

var stmt6_fpocm = snowflake.createStatement({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Set_Watermark_TS(
                ${job_run_id3},
				'${last_watermark3}',
				'${current_watermark}'
				)`
                
     });
 //return_value3 will give job run auto id 
var res6_fpocm = stmt6_fpocm.execute();
res6_fpocm.next();
return_value6_fpocm = res6_fpocm.getColumnValue(1);

return "F_PARTNER_CHANNEL_METRICS TABLE LOADED"

$$;
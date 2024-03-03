--liquibase formatted sql
--changeset SYSTEM:sp_dimesion_household_load runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_CONFIRMED_PRD;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE EDM_CONFIRMED_PRD.DW_APPL.SP_DIMESION_HOUSEHOLD_LOAD()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

//--------------------------------------------------------------------------------------------------------------------#
// Desc         : Stored proc to DIMESION_HOUSEHOLD_LOAD table
//--------------------------------------------------------------------------------------------------------------------#
// Modification : Auth   : Amrita Pandey
//              : Date   : 12/13/2021
//              : Change : uber/doordash changes
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
                'PBI',
                'SP_DIMESION_HOUSEHOLD_LOAD',
                'D1_Customer_Business_Partner_Registration',
                null, 
                'I',
                null,
                null,
                null,
                '${db_analytics}.DW_RETAIL_EXP.D1_Customer_Business_Partner_Registration',
				null)`
                
     });
	 
var feed_extract_job_ini1 = snowflake.execute({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Job_Initiate(
                'PBI',
                'SP_DIMESION_HOUSEHOLD_LOAD',
                'D1_Customer_Business_Partner_Registration_ubd',
                null, 
                'I',
                null,
                null,
                null,
                '${db_analytics}.DW_RETAIL_EXP.D1_Customer_Business_Partner_Registration',
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
	 
//return_value will give job run auto id 
var ret_wm_ts = get_wm_ts.execute();
ret_wm_ts.next();
var last_watermark_from_table = ret_wm_ts.getColumnValue(1);

if (last_watermark_from_table === null) {
  last_watermark = '2021-04-01 00:00:00.000';
} else {
  last_watermark = last_watermark_from_table;
}

var get_wm_ts1 = snowflake.createStatement({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Get_Watermark_TS(${job_run_id1})`
                
     });
	 
//return_value will give job run auto id 
var ret_wm_ts1 = get_wm_ts1.execute();
ret_wm_ts1.next();
var last_watermark_from_ubd = ret_wm_ts1.getColumnValue(1);

if (last_watermark_from_ubd === null) {
  last_watermark1 = '2021-12-01 00:00:00.000';
} else {
  last_watermark1 = last_watermark_from_ubd;
}

results_array[0]=last_watermark;
results_array[1]=last_watermark1;


var T_OCRP_INSTACART_REG_HHS_stmt = snowflake.createStatement({
    sqlText: `CREATE OR REPLACE TEMP TABLE ${db_refined}.DW_R_STAGE.T_OCRP_INSTACART_REG_HHS AS
(select
    REG_DATE,
    Banner as BANNER_NM,
    hhid as HOUSEHOLD_ID,
    household_tag
from
    (
        select
            cast(date_time as timestamp) AS REG_DATE,
            'Insta' household_tag,
            post_evar4 as Banner,
            last_value(post_evar47) over(
                partition by concat(
                    post_visid_high,
                    post_visid_low,
                    visit_num,
                    visit_start_time_gmt
                )
                order by
                    post_evar47 desc nulls first
            ) as HHID,
            concat(
                post_visid_high,
                post_visid_low,
                visit_num,
                visit_start_time_gmt
            ) as Visit,
            CONCAT(',', post_event_list, ',') as Event_List
        from
            ${db_views}.dw_views.click_hit_data
        where
            dw_createts >= TO_TIMESTAMP('${last_watermark}')
            and dw_createts < TO_TIMESTAMP('${current_watermark}')
            and banner != 'albertsonscompanies'
    )
where
    visit in (
        select
            concat(
                post_visid_high,
                post_visid_low,
                visit_num,
                visit_start_time_gmt
            )
        from
            ${db_views}.dw_views.click_hit_data
        where
            contains(post_prop19, 'account/short-registration') --url
            and hit_source not in (5, 7, 8, 9)
            AND exclude_hit = 0
            and dw_createts >= TO_TIMESTAMP('${last_watermark}')
            and dw_createts < TO_TIMESTAMP('${current_watermark}')
            and post_evar2 in ('allb_ist_web_j4uc_ih')
    )
and event_list like '%,20333,%'
AND try_to_numeric(HHID) IS NOT NULL
UNION
select
    REG_DATE,
    Banner as BANNER_NM,
    hhid as HOUSEHOLD_ID,
    household_tag
from
    (
        select
            cast(date_time as timestamp) AS REG_DATE,
            case when post_evar2='allb_reg_dd_j4uc_ih' then 'Doordash'
		         when post_evar2='allb_reg_uber_j4uc_ih' then 'Uber'
	        else post_evar2 end household_tag,
            post_evar4 as Banner,
            last_value(post_evar47) over(
                partition by concat(
                    post_visid_high,
                    post_visid_low,
                    visit_num,
                    visit_start_time_gmt
                )
                order by
                    post_evar47 desc nulls first
            ) as HHID,
            concat(
                post_visid_high,
                post_visid_low,
                visit_num,
                visit_start_time_gmt
            ) as Visit,
            CONCAT(',', post_event_list, ',') as Event_List
        from
            ${db_views}.dw_views.click_hit_data
        where
            dw_createts >= TO_TIMESTAMP('${last_watermark1}')
            and dw_createts < TO_TIMESTAMP('${current_watermark}')
            and banner != 'albertsonscompanies'
    )
where
    visit in (
        select
            concat(
                post_visid_high,
                post_visid_low,
                visit_num,
                visit_start_time_gmt
            )
        from
            ${db_views}.dw_views.click_hit_data
        where
            contains(post_prop19, 'account/short-registration') --url
            and hit_source not in (5, 7, 8, 9)
            AND exclude_hit = 0
            and dw_createts >= TO_TIMESTAMP('${last_watermark1}')
            and dw_createts < TO_TIMESTAMP('${current_watermark}')
            and post_evar2 in ('allb_reg_dd_j4uc_ih','allb_reg_uber_j4uc_ih')
    )
and event_list like '%,20333,%'
AND try_to_numeric(HHID) IS NOT NULL)
;`});


var T_OCRP_INSTACART_REG_HHS_rslt = T_OCRP_INSTACART_REG_HHS_stmt.execute();



var T_OCRP_CUSTOMER_HOUSEHOLDS_stmt = snowflake.createStatement({
    sqlText: `
CREATE OR REPLACE TEMP TABLE ${db_refined}.DW_R_STAGE.T_OCRP_CUSTOMER_HOUSEHOLDS AS
SELECT household_id, loyalty_active_id,Retail_Customer_UUID
FROM (
    SELECT 
    rch.household_id,
    (CASE WHEN cas.STATUS_TYPE_CD IS NOT NULL THEN 1
                    ELSE 2 END) loyalty_active_id,
	rc.Retail_Customer_UUID
    FROM ${db_views}.DW_VIEWS.RETAIL_CUSTOMER rc
    Left join ${db_views}.DW_VIEWS.RETAIL_CUSTOMER_HOUSEHOLD rch
    on rc.Retail_Customer_UUID = rch.Retail_Customer_UUID
	left join ${db_views}.DW_VIEWS.RETAIL_HOUSEHOLD rh
	on rh.household_id = rch.household_id
    LEFT JOIN ${db_views}.DW_VIEWS.CUSTOMER_LOYALTY_PROGRAM clp
    on rc.Retail_Customer_UUID = clp.Retail_Customer_UUID 
    left join ${db_views}.DW_VIEWS.CUSTOMER_ACCOUNT_STATUS cas
    ON rc.Retail_Customer_UUID = cas.Retail_Customer_UUID
    and cas.STATUS_TYPE_CD='ONLINE_ENROLLMENT'
    and cas.dw_current_Version_ind = TRUE
    AND cas.dw_logical_delete_ind = FALSE
    WHERE rc.DW_LOGICAL_DELETE_IND=FALSE 
    AND to_date(rc.DW_LAST_EFFECTIVE_TS) ='9999-12-31'
) a
QUALIFY (ROW_NUMBER() OVER (PARTITION BY a.household_id ORDER BY a.loyalty_active_id asc)=1);`});



var T_OCRP_CUSTOMER_HOUSEHOLDS_rslt = T_OCRP_CUSTOMER_HOUSEHOLDS_stmt.execute();



var t_ocrp_dim_household_stmt = snowflake.createStatement({
    sqlText: `
CREATE OR REPLACE TEMP TABLE ${db_refined}.DW_R_STAGE.t_ocrp_dim_household AS 
SELECT
DISTINCT 
customer.household_id,
customer.Retail_Customer_UUID,
customer.loyalty_active_id,
CASE WHEN irh.household_id is not null then 'PARTNER' 
    ELSE 'EXTERNAL' 
END as registration_channel_dsc,
CASE WHEN irh.household_id is not null and irh.household_tag='Insta' then 1
ELSE 0 END AS instacart_registration_ind,
irh.REG_DATE AS instacart_registration_ts,
CASE WHEN irh.household_id is not null and irh.household_tag='Uber' then 1
ELSE 0 END  AS UBER_REGISTRATION_IND,
irh.REG_DATE UBER_REGISTRATION_TS,
CASE WHEN irh.household_id is not null and irh.household_tag='Doordash' then 1
ELSE 0 END AS DOORDASH_REGISTRATION_IND,
irh.REG_DATE AS DOORDASH_REGISTRATION_TS,
current_timestamp() as DW_CREATE_TS,
current_timestamp() as DW_UPDATE_TS
FROM
${db_refined}.DW_R_STAGE.t_ocrp_customer_households customer
LEFT JOIN ${db_refined}.DW_R_STAGE.t_ocrp_instacart_reg_hhs irh 
ON customer.household_id = irh.household_id;`});


var t_ocrp_dim_household_rslt = t_ocrp_dim_household_stmt.execute();


var t_ocrp_partner_transact_household_stmt = snowflake.createStatement({
    sqlText: `
CREATE OR REPLACE TEMP TABLE ${db_refined}.DW_R_STAGE.t_ocrp_partner_transact_household AS 
WITH po AS
(
    select partner_id,order_id,dlvry_id,loyalty_phone_nbr
    from ${db_views}.dw_edw_views.partner_order po
    group by partner_id,order_id,dlvry_id,loyalty_phone_nbr
),
pos as (
    select order_id,store_txn_ts,partner_id,store_id,dlvry_id,dw_last_updt_ts
    FROM ${db_views}.dw_edw_views.partner_order_store
    WHERE dw_last_updt_ts >= TO_TIMESTAMP('${last_watermark}')
    and dw_last_updt_ts < TO_TIMESTAMP('${current_watermark}')
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY order_id,dlvry_id ORDER  BY store_txn_ts desc)=1)
),
post as (
    select order_id,dlvry_id,txn_id
    from ${db_views}.dw_edw_views.partner_order_store_tender 
    group by order_id,dlvry_id,txn_id
),
poi as (
    select order_id,dlvry_id,gross_merch_val_amt
    from ${db_views}.dw_edw_views.partner_order_itm
),
pgod as (
  select order_id,STORE_TRANSACTION_TS,LOYALTY_PHONE_NBR,sum(REVENUE_AMT) as gmv_order_value
  FROM ${db_views}."DW_VIEWS"."PARTNER_GROCERY_ORDER_DETAIL" 
  --WHERE dw_last_updt_ts >= TO_TIMESTAMP(''${last_watermark}'')
  --and dw_last_updt_ts < TO_TIMESTAMP(''${current_watermark}'')
GROUP BY 1,2,3
  --QUALIFY (ROW_NUMBER() OVER (PARTITION BY order_id ORDER  BY STORE_TRANSACTION_TS desc)=1)
),
pgoh as (
  select order_id,STORE_TRANSACTION_TS,PARTNER_ID,STORE_ID,sum(NET_AMT)
  from ${db_views}."DW_VIEWS". "PARTNER_GROCERY_ORDER_HEADER" 
  WHERE dw_create_ts >= TO_TIMESTAMP('${last_watermark1}')
  and dw_create_ts < TO_TIMESTAMP('${current_watermark}')
 group by 1,2,3,4
),
pgot as
(
select order_id
  FROM ${db_views}."DW_VIEWS"."PARTNER_GROCERY_ORDER_TENDER" 
  )
SELECT DISTINCT
    rch.household_id
FROM ${db_views}.DW_VIEWS.CUSTOMER_PHONE_FAX_CONTACT lc
INNER JOIN ${db_views}.DW_VIEWS.RETAIL_CUSTOMER_HOUSEHOLD rch
on lc.Retail_Customer_UUID = rch.Retail_Customer_UUID
INNER JOIN ${db_views}.DW_VIEWS.RETAIL_HOUSEHOLD rh
on rh.household_id = rch.household_id
inner join po
ON po.loyalty_phone_nbr = lc.phone_nbr
INNER JOIN post
ON po.order_id = post.order_id
AND po.dlvry_id = post.dlvry_id
INNER JOIN poi
ON post.order_id = poi.order_id
AND post.dlvry_id = poi.dlvry_id
INNER JOIN pos
ON po.order_id = pos.order_id
AND po.dlvry_id = pos.dlvry_id
INNER JOIN ${db_views}.DW_VIEWS.D1_RETAIL_STORE lsfu
ON pos.store_id = try_to_numeric(lsfu.RETAIL_STORE_FACILITY_NBR)
WHERE lc.DW_LOGICAL_DELETE_IND='FALSE' 
AND to_date(lc.DW_LAST_EFFECTIVE_TS) ='9999-12-31'
union
SELECT DISTINCT
    rch.household_id
FROM ${db_views}.DW_VIEWS.CUSTOMER_PHONE_FAX_CONTACT lc
INNER JOIN ${db_views}.DW_VIEWS.RETAIL_CUSTOMER_HOUSEHOLD rch
on lc.Retail_Customer_UUID = rch.Retail_Customer_UUID
inner join pgod
ON  pgod.loyalty_phone_nbr = lc.phone_nbr
INNER JOIN pgoh
ON pgoh.order_id = pgod.order_id
--and poi.dlvry_id = po.dlvry_id
INNER JOIN pgot
ON pgot.order_id = pgoh.order_id
--AND pos.dlvry_id = po.dlvry_id
INNER JOIN ${db_views}.DW_VIEWS.D1_RETAIL_STORE lsfu
ON pgoh.store_id = try_to_numeric(lsfu.Retail_Store_Facility_Nbr)
WHERE lc.DW_LOGICAL_DELETE_IND='FALSE' 
AND to_date(lc.DW_LAST_EFFECTIVE_TS) ='9999-12-31';`});

var t_ocrp_partner_transact_household_rslt = t_ocrp_partner_transact_household_stmt.execute();


var update1_Cust_Bus_Par_Reg_stmt = snowflake.createStatement({
    sqlText: `
UPDATE ${db_analytics}.DW_RETAIL_EXP.D1_Customer_Business_Partner_Registration dim
SET Registration_Channel_Desc='PARTNER', 
    instacart_registration_ind=1,
    instacart_registration_ts=irh.REG_DATE,
    DW_LAST_UPDATE_TS = current_timestamp()
FROM ${db_refined}.DW_R_STAGE.t_ocrp_instacart_reg_hhs irh
WHERE dim.household_id = irh.household_id
and irh.household_tag = 'Insta'
AND dim.Registration_Channel_Desc='EXTERNAL';`});

var update1_Cust_Bus_Par_Reg_rslt = update1_Cust_Bus_Par_Reg_stmt.execute();
  
  var update1_Cust_Bus_Par_Reg_uber_stmt = snowflake.createStatement({
    sqlText: `
UPDATE ${db_analytics}.DW_RETAIL_EXP.D1_Customer_Business_Partner_Registration dim
SET Registration_Channel_Desc='PARTNER', 
    uber_registration_ind=1,
    uber_registration_ts=irh.REG_DATE,
    DW_LAST_UPDATE_TS = current_timestamp()
FROM ${db_refined}.DW_R_STAGE.t_ocrp_instacart_reg_hhs irh
WHERE dim.household_id = irh.household_id
and irh.household_tag = 'Uber'
AND dim.Registration_Channel_Desc='EXTERNAL';`});

var update1_Cust_Bus_Par_Reg_uber_rslt = update1_Cust_Bus_Par_Reg_uber_stmt.execute();
  
  var update1_Cust_Bus_Par_Reg_doordash_stmt = snowflake.createStatement({
    sqlText: `
UPDATE ${db_analytics}.DW_RETAIL_EXP.D1_Customer_Business_Partner_Registration dim
SET Registration_Channel_Desc='PARTNER', 
    doordash_registration_ind=1,
    doordash_registration_ts=irh.REG_DATE,
    DW_LAST_UPDATE_TS = current_timestamp()
FROM ${db_refined}.DW_R_STAGE.t_ocrp_instacart_reg_hhs irh
WHERE dim.household_id = irh.household_id
and irh.household_tag = 'Doordash'
AND dim.Registration_Channel_Desc='EXTERNAL';`});

var update1_Cust_Bus_Par_Reg_doordash_rslt = update1_Cust_Bus_Par_Reg_doordash_stmt.execute();


var insert_D1_Cust_Bus_Par_Reg_stmt = snowflake.createStatement({
    sqlText: `
INSERT INTO ${db_analytics}.DW_RETAIL_EXP.D1_Customer_Business_Partner_Registration(  
  --Customer_Business_Partner_Registration_D1_Sk,
  household_id,
  Retail_Customer_UUID,
  loyalty_active_id,
  Registration_Channel_Desc,
  instacart_registration_ind,
  instacart_registration_ts,
  Uber_Registration_Ind ,
    Uber_Registration_Ts,
    Doordash_Registration_Ind ,
    Doordash_Registration_Ts,
  partner_transaction_ind,
  dw_create_ts,
  DW_LAST_UPDATE_TS,
  DW_LOGICAL_DELETE_IND,
  DW_CURRENT_VERSION_IND)
SELECT household_id,
Retail_Customer_UUID,
loyalty_active_id,
registration_channel_dsc,
instacart_registration_ind,
instacart_registration_ts,
Uber_Registration_Ind ,
Uber_Registration_Ts ,
Doordash_Registration_Ind ,
Doordash_Registration_Ts,
0,
dw_create_ts,
dw_update_ts,'N','Y'
FROM ${db_refined}.DW_R_STAGE.t_ocrp_dim_household 
WHERE household_id NOT IN (SELECT household_id 
                           FROM ${db_analytics}.dw_retail_exp.D1_Customer_Business_Partner_Registration)
                           and household_id is not null;`});
						   

var insert_D1_Cust_Bus_Par_Reg_rslt = insert_D1_Cust_Bus_Par_Reg_stmt.execute();
insert_D1_Cust_Bus_Par_Reg_rslt.next();


					  
var update2_Cust_Bus_Par_Reg_stmt = snowflake.createStatement({
    sqlText: `UPDATE ${db_analytics}.dw_retail_exp.D1_Customer_Business_Partner_Registration dim
SET PARTNER_TRANSACTION_IND = 1,
    DW_LAST_UPDATE_TS = current_timestamp()
FROM ${db_refined}.DW_R_STAGE.t_ocrp_partner_transact_household pth
WHERE pth.household_id = dim.household_id;`});


var update2_Cust_Bus_Par_Reg_rslt = update2_Cust_Bus_Par_Reg_stmt.execute();
update2_Cust_Bus_Par_Reg_rslt.next();

var return_value = insert_D1_Cust_Bus_Par_Reg_rslt.getColumnValue(1);


var log_num_rec = snowflake.createStatement({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Job_Success(
                '${job_run_id}',
                '${return_value}',
                null,
                null)`
                
     });

 //return_value2 will give job run auto id
var log_num_rec_exe = log_num_rec.execute();
log_num_rec_exe.next();

var log_ts = snowflake.createStatement({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Set_Watermark_TS(
                ${job_run_id},
				'${last_watermark}',
				'${current_watermark}'
				)`
                
     });
 //return_value3 will give job run auto id 
var log_ts_exe = log_ts.execute();
log_ts_exe.next();
var log_ts_res = log_ts_exe.getColumnValue(1);

var log_num_rec1 = snowflake.createStatement({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Job_Success(
                '${job_run_id1}',
                '${return_value}',
                null,
                null)`
                
     });

 //return_value2 will give job run auto id
var log_num_rec_exe1 = log_num_rec1.execute();
log_num_rec_exe1.next();

var log_ts1 = snowflake.createStatement({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Set_Watermark_TS(
                ${job_run_id1},
				'${last_watermark1}',
				'${current_watermark}'
				)`
                
     });
 //return_value3 will give job run auto id 
var log_ts_exe1 = log_ts1.execute();
log_ts_exe1.next();
var log_ts_res1 = log_ts_exe1.getColumnValue(1);


return "D1_Customer_Business_Partner_Registration Table loaded"

$$;
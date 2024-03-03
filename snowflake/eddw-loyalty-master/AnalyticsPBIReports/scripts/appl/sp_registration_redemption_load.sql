--liquibase formatted sql
--changeset SYSTEM:sp_registration_redemption_load runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_CONFIRMED_PRD;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE EDM_CONFIRMED_PRD.DW_APPL.SP_REGISTRATION_REDEMPTION_LOAD()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

//--------------------------------------------------------------------------------------------------------------------#
// Desc         : Stored proc to DIMESION_HOUSEHOLD_LOAD table
//--------------------------------------------------------------------------------------------------------------------#
// Modification : Auth   : Amrita Pandey
//              : Date   : 12/16/2021
//              : Change : add filter for banner name
//--------------------------------------------------------------------------------------------------------------------#

var current_watermark = new Date();
current_watermark = current_watermark.toISOString();

var db_confirmed = 'EDM_CONFIRMED_PRD';
var db_analytics = 'EDM_ANALYTICS_PRD';
var db_refined = 'EDM_REFINED_PRD';
var db_views = 'EDM_VIEWS_PRD';

var instacart_watermark_name = 'SP_registration_redemption_LOAD';
var uber_watermark_name = 'SP_registration_redemption_UBER_LOAD';
var doordash_watermark_name = 'SP_registration_redemption_DD_LOAD';
var no_of_partner = 3;


function prepare_lastwatermark_attribute(watermark_name,partner_sk)
{
	var feed_extract_job_ini = snowflake.execute({
		sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Job_Initiate(
					'PBI',
					'${watermark_name}',
					'hh_partner_registration_redemption',
					null, 
					'I',
					null,
					null,
					null,
					'${db_analytics}.DW_RETAIL_EXP.hh_partner_registration_redemption',
					null)`
					
		 });

	//return_value will give job run auto id 
	feed_extract_job_ini.next();
	job_run_id = feed_extract_job_ini.getColumnValue(1);

	var get_wm_ts = snowflake.createStatement({
        sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Get_Watermark_TS(${job_run_id})`});

	//return_value will give job run auto id 
	var ret_wm_ts = get_wm_ts.execute();
	ret_wm_ts.next();
	var last_watermark_from_table = ret_wm_ts.getColumnValue(1);

	if (last_watermark_from_table === null) {
		if (partner_sk == 1){last_watermark = '2021-04-01 00:00:00.000';}
			else{last_watermark = '2021-12-01 00:00:00.000';}
	} else {
	  last_watermark = last_watermark_from_table;
	}
	return last_watermark
}

for (let partner_sk = 1;partner_sk <=no_of_partner;partner_sk++)
{
	 if (partner_sk == 1)
	 {
	 last_watermark = prepare_lastwatermark_attribute(instacart_watermark_name,partner_sk);
	 partner_reg_ts = 'instacart_registration_ts';
	 registration_ind = 'instacart_registration_ind';
	 }
	 else if(partner_sk ==2)
	 {
	 last_watermark = prepare_lastwatermark_attribute(uber_watermark_name,partner_sk); 
	 partner_reg_ts = 'uber_registration_ts';
	 registration_ind = 'uber_registration_ind';
	 }
	 else
	 {
	 last_watermark = prepare_lastwatermark_attribute(doordash_watermark_name,partner_sk); 
	 partner_reg_ts = 'doordash_registration_ts';
	 registration_ind = 'doordash_registration_ind'
	 }

var t_fact_partner_txn_engagement_dtl_stmt = snowflake.createStatement({
    sqlText: `CREATE OR REPLACE TEMP TABLE ${db_refined}.dw_r_stage.t_fact_partner_txn_engagement_dtl AS
SELECT
    engmt.transaction_id txn_id,
    engmt.transaction_dt txn_dt,
    cust.household_id,
    cust.loyalty_active_id,
    cust.registration_channel_desc,
    engmt.j4u_engagement_ind,
    engmt.grocery_reward_engagement_ind,
    engmt.fuel_reward_own_engagement_ind,
    engmt.fuel_reward_partner_engagement_ind,
    engmt.business_partner_d1_sk,
    DATE(cust.${partner_reg_ts}) as ${partner_reg_ts}
FROM
    ${db_analytics}.dw_retail_exp.F_Transaction_Partner_Engagement engmt
	INNER JOIN ${db_analytics}.dw_retail_exp.D1_Customer_Business_Partner_Registration cust 
    on engmt.CUSTOMER_BUSINESS_PARTNER_REGISTRATION_D1_SK = cust.CUSTOMER_BUSINESS_PARTNER_REGISTRATION_D1_SK
	and cust.${registration_ind} = 1
    and DATEADD(DAY,84,cust.${partner_reg_ts})>=TO_TIMESTAMP('${last_watermark}')
    and DATEADD(DAY,84,cust.${partner_reg_ts})<TO_TIMESTAMP('${current_watermark}')
    and transaction_dt between cust.${partner_reg_ts} 
    and DATEADD(DAY,84,cust.${partner_reg_ts});`});
    
var t_total_partner_registration_stmt = snowflake.createStatement({
    sqlText: `CREATE OR REPLACE TEMP TABLE ${db_refined}.dw_r_stage.Total_registration
                as
                select 
                LOYALTY_ACTIVE_ID,
                    HOUSEHOLD_ID,
                    REGISTRATION_CHANNEL_DESC as REGISTRATION_CHANNEL_DSC,
                    ${partner_sk} business_partner_d1_sk,
                   'NOT_SHOPPED' ENGAGEMENT_TYPE,
                    CURRENT_TIMESTAMP() AS DW_CREATE_TS
                FROM   ${db_views}."DW_VIEWS".D1_Customer_Business_Partner_Registration cust 
                where cust.${registration_ind} = 1
                    and DATEADD(DAY,84,cust.${partner_reg_ts})>=TO_TIMESTAMP('${last_watermark}')
                    and DATEADD(DAY,84,cust.${partner_reg_ts})<TO_TIMESTAMP('${current_watermark}')
	;`});

var t_fact_partner_txn_engagement_dtl_rslt = t_fact_partner_txn_engagement_dtl_stmt.execute();
var t_total_partner_registration_stmt_rslt = t_total_partner_registration_stmt.execute();

var hh_partner_registration_redemption_pre_stmt = snowflake.createStatement({
    sqlText: `CREATE OR REPLACE TEMP TABLE ${db_refined}.dw_r_stage.t_hh_partner_registration_redemption_pre AS
SELECT
    case
        when engagement_week between ${partner_reg_ts}
        and ${partner_reg_ts} + 7 then 'REG-WEEK'
        when engagement_week between ${partner_reg_ts}
        and ${partner_reg_ts} + 14 then 'WEEK-1'
        when engagement_week between ${partner_reg_ts}
        and ${partner_reg_ts} + 21 then 'WEEK-2'
        when engagement_week between ${partner_reg_ts}
        and ${partner_reg_ts} + 28 then 'WEEK-3'
        when engagement_week between ${partner_reg_ts}
        and ${partner_reg_ts} + 35 then 'WEEK-4'
        when engagement_week between ${partner_reg_ts}
        and ${partner_reg_ts} + 42 then 'WEEK-5'
        when engagement_week between ${partner_reg_ts}
        and ${partner_reg_ts} + 49 then 'WEEK-6'
        when engagement_week between ${partner_reg_ts}
        and ${partner_reg_ts} + 56 then 'WEEK-7'
        when engagement_week between ${partner_reg_ts}
        and ${partner_reg_ts} + 63 then 'WEEK-8'
        when engagement_week between ${partner_reg_ts}
        and ${partner_reg_ts} + 70 then 'WEEK-9'
        when engagement_week between ${partner_reg_ts}
        and ${partner_reg_ts} + 77 then 'WEEK-10'
        when engagement_week between ${partner_reg_ts}
        and ${partner_reg_ts} + 84 then 'WEEK-11'
    END as engagement_week,
    LOYALTY_ACTIVE_ID,
    HOUSEHOLD_ID,
    REGISTRATION_CHANNEL_DSC,
    business_partner_d1_sk,
    ENGAGEMENT_TYPE,
    CURRENT_TIMESTAMP() AS DW_CREATE_TS
FROM
(
SELECT
    TXN_DT AS engagement_week,
    LOYALTY_ACTIVE_ID AS LOYALTY_ACTIVE_ID,
    HOUSEHOLD_ID AS HOUSEHOLD_ID,
    REGISTRATION_CHANNEL_DESC AS REGISTRATION_CHANNEL_DSC,
    business_partner_d1_sk,
    ${partner_reg_ts},
    'J4U' AS ENGAGEMENT_TYPE
FROM
    ${db_refined}.dw_r_stage.t_fact_partner_txn_engagement_dtl
WHERE
    J4U_ENGAGEMENT_IND = 'TRUE'
UNION
ALL
SELECT
    TXN_DT AS engagement_week,
    LOYALTY_ACTIVE_ID AS LOYALTY_ACTIVE_ID,
    HOUSEHOLD_ID AS HOUSEHOLD_ID,
    REGISTRATION_CHANNEL_DESC AS REGISTRATION_CHANNEL_DSC,
    business_partner_d1_sk,
    ${partner_reg_ts},
    'GR' AS ENGAGEMENT_TYPE
FROM
    ${db_refined}.dw_r_stage.t_fact_partner_txn_engagement_dtl
WHERE
    GROCERY_REWARD_ENGAGEMENT_IND = 'TRUE'
UNION
ALL
SELECT
    TXN_DT AS engagement_week,
    LOYALTY_ACTIVE_ID AS LOYALTY_ACTIVE_ID,
    HOUSEHOLD_ID AS HOUSEHOLD_ID,
    REGISTRATION_CHANNEL_DESC AS REGISTRATION_CHANNEL_DSC,
    business_partner_d1_sk,
    ${partner_reg_ts},
    'FR_OWN' AS ENGAGEMENT_TYPE
FROM
    ${db_refined}.dw_r_stage.t_fact_partner_txn_engagement_dtl
WHERE
    FUEL_REWARD_OWN_ENGAGEMENT_IND = 'TRUE'
UNION
ALL
SELECT
    TXN_DT AS engagement_week,
    LOYALTY_ACTIVE_ID AS LOYALTY_ACTIVE_ID,
    HOUSEHOLD_ID AS HOUSEHOLD_ID,
    REGISTRATION_CHANNEL_DESC AS REGISTRATION_CHANNEL_DSC,
    business_partner_d1_sk,
    ${partner_reg_ts},
    'FR_PARTNER' AS ENGAGEMENT_TYPE
FROM
    ${db_refined}.dw_r_stage.t_fact_partner_txn_engagement_dtl
WHERE
    FUEL_REWARD_PARTNER_ENGAGEMENT_IND = 'TRUE'
UNION
ALL
SELECT
    TXN_DT AS engagement_week,
    LOYALTY_ACTIVE_ID AS LOYALTY_ACTIVE_ID,
    HOUSEHOLD_ID AS HOUSEHOLD_ID,
    REGISTRATION_CHANNEL_DESC AS REGISTRATION_CHANNEL_DSC,
    business_partner_d1_sk,
    ${partner_reg_ts},
    'NOT_ENGAGED' AS ENGAGEMENT_TYPE
FROM
    ${db_refined}.dw_r_stage.t_fact_partner_txn_engagement_dtl
where
    (
        J4U_ENGAGEMENT_IND = 'FALSE'
        AND GROCERY_REWARD_ENGAGEMENT_IND = 'FALSE'
        AND FUEL_REWARD_OWN_ENGAGEMENT_IND = 'FALSE'
        AND FUEL_REWARD_PARTNER_ENGAGEMENT_IND = 'FALSE'
    )
);`});

var hh_partner_registration_redemption_pre_rslt = hh_partner_registration_redemption_pre_stmt.execute();
  
var hh_partner_registration_redemption_stmt = snowflake.createStatement({
    sqlText: `create OR REPLACE temp table ${db_refined}.dw_r_stage.t_hh_partner_registration_redemption as
select distinct * from ${db_refined}.dw_r_stage.t_hh_partner_registration_redemption_pre
where ENGAGEMENT_TYPE <> 'NOT_ENGAGED' ;`})

var hh_partner_registration_redemption_rslt = hh_partner_registration_redemption_stmt.execute();

var insert1_hh_partner_registration_redemption_stmt = snowflake.createStatement({
    sqlText: `INSERT into ${db_refined}.dw_r_stage.t_hh_partner_registration_redemption
select distinct *
from ${db_refined}.dw_r_stage.t_hh_partner_registration_redemption_pre a
where ENGAGEMENT_TYPE = 'NOT_ENGAGED' 
and (household_id, engagement_week) NOT IN 
(SELECT household_id, engagement_week from ${db_refined}.dw_r_stage.t_hh_partner_registration_redemption);`})

var insert1_hh_partner_registration_redemption_rslt = insert1_hh_partner_registration_redemption_stmt.execute();


var insert2_hh_partner_registration_redemption_stmt = snowflake.createStatement({
    sqlText: `
insert into ${db_analytics}.dw_retail_exp.hh_partner_registration_redemption
select * from ${db_refined}.dw_r_stage.t_hh_partner_registration_redemption;`})

var insert2_hh_partner_registration_redemption_rslt = insert2_hh_partner_registration_redemption_stmt.execute();
 var insert1_hh_not_shopped_customer_stmt = snowflake.createStatement({
    sqlText: `
insert into ${db_analytics}.dw_retail_exp.hh_partner_registration_redemption                                                                  
  select 
'REG-WEEK' engagement_week,
LOYALTY_ACTIVE_ID,
HOUSEHOLD_ID,
REGISTRATION_CHANNEL_DSC,
business_partner_d1_sk,
'NOT_SHOPPED'ENGAGEMENT_TYPE,
CURRENT_TIMESTAMP() AS DW_CREATE_TS
FROM ${db_refined}.dw_r_stage.TOTAL_REGISTRATION
where HOUSEHOLD_ID not in (select HOUSEHOLD_ID from ${db_refined}.dw_r_stage.
  t_hh_partner_registration_redemption where engagement_week='REG-WEEK')
UNION ALL
select 
'WEEK-1' engagement_week,
LOYALTY_ACTIVE_ID,
HOUSEHOLD_ID,
REGISTRATION_CHANNEL_DSC,
business_partner_d1_sk,
'NOT_SHOPPED'ENGAGEMENT_TYPE,
CURRENT_TIMESTAMP() AS DW_CREATE_TS
FROM ${db_refined}.dw_r_stage.TOTAL_REGISTRATION
where HOUSEHOLD_ID not in (select HOUSEHOLD_ID from ${db_refined}.dw_r_stage.
  t_hh_partner_registration_redemption where engagement_week='WEEK-1')
UNION ALL
select 
'WEEK-2' engagement_week,
LOYALTY_ACTIVE_ID,
HOUSEHOLD_ID,
REGISTRATION_CHANNEL_DSC,
business_partner_d1_sk,
'NOT_SHOPPED'ENGAGEMENT_TYPE,
CURRENT_TIMESTAMP() AS DW_CREATE_TS
FROM ${db_refined}.dw_r_stage.TOTAL_REGISTRATION
where HOUSEHOLD_ID not in (select HOUSEHOLD_ID from ${db_refined}.dw_r_stage.t_hh_partner_registration_redemption where engagement_week='WEEK-2')
UNION ALL
select 
'WEEK-3' engagement_week,
LOYALTY_ACTIVE_ID,
HOUSEHOLD_ID,
REGISTRATION_CHANNEL_DSC,
business_partner_d1_sk,
'NOT_SHOPPED'ENGAGEMENT_TYPE,
CURRENT_TIMESTAMP() AS DW_CREATE_TS
FROM ${db_refined}.dw_r_stage.TOTAL_REGISTRATION
where HOUSEHOLD_ID not in (select HOUSEHOLD_ID from ${db_refined}.dw_r_stage.t_hh_partner_registration_redemption where engagement_week='WEEK-3')
UNION ALL
select 
'WEEK-4' engagement_week,
LOYALTY_ACTIVE_ID,
HOUSEHOLD_ID,
REGISTRATION_CHANNEL_DSC,
business_partner_d1_sk,
'NOT_SHOPPED'ENGAGEMENT_TYPE,
CURRENT_TIMESTAMP() AS DW_CREATE_TS
FROM ${db_refined}.dw_r_stage.TOTAL_REGISTRATION
where HOUSEHOLD_ID not in (select HOUSEHOLD_ID from ${db_refined}.dw_r_stage.t_hh_partner_registration_redemption where engagement_week='WEEK-4')
UNION ALL
select 
'WEEK-5' engagement_week,
LOYALTY_ACTIVE_ID,
HOUSEHOLD_ID,
REGISTRATION_CHANNEL_DSC,
business_partner_d1_sk,
'NOT_SHOPPED'ENGAGEMENT_TYPE,
CURRENT_TIMESTAMP() AS DW_CREATE_TS
FROM ${db_refined}.dw_r_stage.TOTAL_REGISTRATION
where HOUSEHOLD_ID not in (select HOUSEHOLD_ID from ${db_refined}.dw_r_stage.t_hh_partner_registration_redemption where engagement_week='WEEK-5')
UNION ALL
select 
'WEEK-6' engagement_week,
LOYALTY_ACTIVE_ID,
HOUSEHOLD_ID,
REGISTRATION_CHANNEL_DSC,
business_partner_d1_sk,
'NOT_SHOPPED'ENGAGEMENT_TYPE,
CURRENT_TIMESTAMP() AS DW_CREATE_TS
FROM ${db_refined}.dw_r_stage.TOTAL_REGISTRATION
where HOUSEHOLD_ID not in (select HOUSEHOLD_ID from ${db_refined}.dw_r_stage.t_hh_partner_registration_redemption where engagement_week='WEEK-6')
UNION ALL
select 
'WEEK-7' engagement_week,
LOYALTY_ACTIVE_ID,
HOUSEHOLD_ID,
REGISTRATION_CHANNEL_DSC,
business_partner_d1_sk,
'NOT_SHOPPED'ENGAGEMENT_TYPE,
CURRENT_TIMESTAMP() AS DW_CREATE_TS
FROM ${db_refined}.dw_r_stage.TOTAL_REGISTRATION
where HOUSEHOLD_ID not in (select HOUSEHOLD_ID from ${db_refined}.dw_r_stage.t_hh_partner_registration_redemption where engagement_week='WEEK-7')
UNION ALL
select 
'WEEK-8' engagement_week,
LOYALTY_ACTIVE_ID,
HOUSEHOLD_ID,
REGISTRATION_CHANNEL_DSC,
business_partner_d1_sk,
'NOT_SHOPPED'ENGAGEMENT_TYPE,
CURRENT_TIMESTAMP() AS DW_CREATE_TS
FROM ${db_refined}.dw_r_stage.TOTAL_REGISTRATION
where HOUSEHOLD_ID not in (select HOUSEHOLD_ID from ${db_refined}.dw_r_stage.t_hh_partner_registration_redemption where engagement_week='WEEK-8')
UNION ALL
select 
'WEEK-9' engagement_week,
LOYALTY_ACTIVE_ID,
HOUSEHOLD_ID,
REGISTRATION_CHANNEL_DSC,
business_partner_d1_sk,
'NOT_SHOPPED'ENGAGEMENT_TYPE,
CURRENT_TIMESTAMP() AS DW_CREATE_TS
FROM ${db_refined}.dw_r_stage.TOTAL_REGISTRATION
where HOUSEHOLD_ID not in (select HOUSEHOLD_ID from ${db_refined}.dw_r_stage.t_hh_partner_registration_redemption where engagement_week='WEEK-9')
UNION ALL
select 
'WEEK-10' engagement_week,
LOYALTY_ACTIVE_ID,
HOUSEHOLD_ID,
REGISTRATION_CHANNEL_DSC,
business_partner_d1_sk,
'NOT_SHOPPED'ENGAGEMENT_TYPE,
CURRENT_TIMESTAMP() AS DW_CREATE_TS
FROM ${db_refined}.dw_r_stage.TOTAL_REGISTRATION
where HOUSEHOLD_ID not in (select HOUSEHOLD_ID from ${db_refined}.dw_r_stage.t_hh_partner_registration_redemption where engagement_week='WEEK-10')
UNION ALL
select 
'WEEK-11' engagement_week,
LOYALTY_ACTIVE_ID,
HOUSEHOLD_ID,
REGISTRATION_CHANNEL_DSC,
business_partner_d1_sk,
'NOT_SHOPPED'ENGAGEMENT_TYPE,
CURRENT_TIMESTAMP() AS DW_CREATE_TS
FROM ${db_refined}.dw_r_stage.TOTAL_REGISTRATION
where HOUSEHOLD_ID not in (select HOUSEHOLD_ID from ${db_refined}.dw_r_stage.t_hh_partner_registration_redemption where engagement_week='WEEK-11');`})

var insert1_hh_not_shopped_customer_stmt_rslt = insert1_hh_not_shopped_customer_stmt.execute();
insert2_hh_partner_registration_redemption_rslt.next();
var return_value1 = insert2_hh_partner_registration_redemption_rslt.getColumnValue(1);

insert1_hh_not_shopped_customer_stmt_rslt.next();
var return_value2 = insert1_hh_not_shopped_customer_stmt_rslt.getColumnValue(1);

var return_value = return_value1 + return_value2


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
  }
return "hh_partner_registration_redemption Table loaded"

$$;
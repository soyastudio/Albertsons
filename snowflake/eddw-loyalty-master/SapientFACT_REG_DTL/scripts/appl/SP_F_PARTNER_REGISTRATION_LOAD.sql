--liquibase formatted sql
--changeset SYSTEM:SP_F_PARTNER_REGISTRATION_LOAD runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_CONFIRMED_PRD;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE EDM_CONFIRMED_PRD.DW_APPL.SP_F_PARTNER_REGISTRATION_LOAD()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

//--------------------------------------------------------------------------------------------------------------------#
// Desc         : Stored proc to Load KPI target table F_PARTNER_REGISTRATION
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

var feed_extract_job_ini4 = snowflake.execute({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Job_Initiate(
                'OCRP',
                'SP_F_PARTNER_REGISTRATION_LOAD',
                'FACT_PARTNER_REG_DTL',
                null,
                'I',
                null,
                null,
                null,
                '${db_analytics}.DW_RETAIL_EXP.F_PARTNER_REGISTRATION',
				null)`
                
     });
	 
//return_value will give job run auto id 
feed_extract_job_ini4.next();
job_run_id4 = feed_extract_job_ini4.getColumnValue(1);

var get_wm_ts4 = snowflake.createStatement({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Get_Watermark_TS(${job_run_id4})`
                
     });
	 
//return_value will give job run auto id 
var ret_wm_ts4 = get_wm_ts4.execute();
ret_wm_ts4.next();
var last_watermark_from_tble4 = ret_wm_ts4.getColumnValue(1);

if (last_watermark_from_tble4 === null) {
  last_watermark4 = '2021-04-01 00:00:00.000';
} else {
  last_watermark4 = last_watermark_from_tble4;
}

results_array[0]=last_watermark4;

//---------insert for aci registration
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
when banner_nm ='STAR MARKET' then 'starmarket'
when banner_nm = 'PAK N SAV' then 'PAK N SAV BY SAFEWAY'
when banner_nm ='SHAW\\'S' then 'shaws'
when banner_nm ='PAVILIONS' then 'pavilions'
when banner_nm ='CARRS' then 'carrsqc'
when banner_nm ='SAFEWAY' then 'safeway'
else banner_nm end adobe_banner_nm
from "${db_views}"."DW_VIEWS"."D1_BANNER";`});

var adb_ban1 = adb_ban.execute();

var ins_reg_dtl=snowflake.createStatement({
			sqlText: `INSERT INTO ${db_analytics}.DW_RETAIL_EXP.F_PARTNER_REGISTRATION
				(day_id,
				transaction_dt,
				BUSINESS_PARTNER_D1_SK,
				banner_d1_sk,
				partner_Site_Visits_Cnt,
				registration_CNT,
				dw_create_ts,
				DW_UPDATE_TS)

with cte1 as
(
SELECT
abl.banner_d1_sk as banner_id,
'4' as partner_id,
transaction_date,
cal.fiscal_day_id as day_id,
Count(DISTINCT visit) AS visits,
Sum(CASE WHEN event_list LIKE '%,20333,%' THEN 1 ELSE 0 END) AS ACI_registrations
FROM (
SELECT
Cast(date_time AS DATE) as transaction_date ,
post_evar4 AS banner ,
Last_value(post_evar47)
OVER(partition BY Concat(post_visid_high, post_visid_low,visit_num, visit_start_time_gmt) ORDER BY post_evar47 DESC nulls first) AS hhid ,
Concat(post_visid_high,post_visid_low,visit_num,visit_start_time_gmt) AS visit ,
Concat(',', post_event_list, ',') AS event_list
FROM ${db_views}.dw_views.click_hit_data
WHERE
banner != 'albertsonscompanies'
and 
dw_createts > TO_TIMESTAMP('${last_watermark4}')
and 
dw_createts <= TO_TIMESTAMP('${current_watermark}')
) chd1_aci
inner join
${db_refined}.DW_R_STAGE.adobe_banner_lkp abl
ON chd1_aci.BANNER=abl.adobe_banner_nm
OR (chd1_aci.BANNER<>abl.adobe_banner_nm AND upper(chd1_aci.BANNER) = upper(abl.banner_nm))
inner join 
"${db_views}"."DW_VIEWS"."D0_FISCAL_DAY" cal
ON chd1_aci.transaction_date=cal.calendar_dt
WHERE visit IN
(
SELECT concat(post_visid_high, post_visid_low,visit_num, visit_start_time_gmt)
FROM ${db_views}.dw_views.click_hit_data
WHERE
hit_source NOT IN (5,7,8,9)
AND exclude_hit = 0
) and chd1_aci.banner is not null
GROUP BY 1,2,3,4
)
select
cte1.day_id,
cte1.transaction_date,
cte1.partner_id as BUSINESS_PARTNER_D1_SK,
cte1.banner_id as banner_d1_sk,
cte1.visits,
cte1.ACI_registrations as registration,
current_timestamp() as dw_create_ts,
current_timestamp() as dw_last_update_ts
from cte1
where cte1.banner_id is not null
;`});

var res_ins_reg_dtl = ins_reg_dtl.execute();
res_ins_reg_dtl.next();
no_rec_reg_dt1 = res_ins_reg_dtl.getColumnValue(1);

//--------------end load for aci registration



	
//-----------------------------------------------watermark for partners
var instacart_watermark_name = 'SP_F_PARTNER_REGISTRATION_LOAD';
var uber_watermark_name = 'SP_F_PARTNER_REGISTRATION_UBER_LOAD';
var doordash_watermark_name = 'SP_F_PARTNER_REGISTRATION_DD_LOAD';
var no_of_partner = 3;


function prepare_lastwatermark_attribute(watermark_name,partner_sk)
{
	var feed_extract_job_ini = snowflake.execute({
		sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Job_Initiate(
					'OCRP',
					'${watermark_name}',
					'FACT_PARTNER_REG_DTL',
					null, 
					'I',
					null,
					null,
					null,
					'${db_analytics}.DW_RETAIL_EXP.F_PARTNER_REGISTRATION',
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
		last_watermark = '2021-12-01 00:00:00.000';
	} else {
	  last_watermark = last_watermark_from_table;
	}
	return last_watermark
}

for (let partner_sk = 1;partner_sk <=no_of_partner;partner_sk++)
{
	 if (partner_sk == 1)
	 {
	 last_watermark = last_watermark4;
	 job_run_id=job_run_id4;
	 partner_campaign_id = 'allb_ist_web_j4uc_ih';
	 }
	 else if(partner_sk ==2)
	 {
	 last_watermark = prepare_lastwatermark_attribute(uber_watermark_name,partner_sk); 
	 partner_campaign_id = 'allb_reg_uber_j4uc_ih';
	 }
	 else if(partner_sk ==3)
	 {
	 last_watermark = prepare_lastwatermark_attribute(doordash_watermark_name,partner_sk); 
	 partner_campaign_id = 'allb_reg_dd_j4uc_ih';
	 }

// *******************START OF LOAD FOR Partner registration******************************


var ins_reg_dtl_partner=snowflake.createStatement({
			sqlText: `INSERT INTO ${db_analytics}.DW_RETAIL_EXP.F_PARTNER_REGISTRATION
				(day_id,
				transaction_dt,
				BUSINESS_PARTNER_D1_SK,
				banner_d1_sk,
				partner_Site_Visits_Cnt,
				registration_CNT,
				dw_create_ts,
				DW_UPDATE_TS)

with cte2 as
(
SELECT
abl.banner_d1_sk as banner_id,
${partner_sk} as partner_id,
transaction_date,
cal.fiscal_day_id as day_id,
Count(DISTINCT visit) AS visits,
Sum(
CASE
WHEN event_list LIKE '%,20333,%' THEN 1
ELSE 0
END
) AS partner_registrations
FROM (
SELECT
Cast(date_time AS DATE) as transaction_date ,
post_evar4 AS banner ,
post_evar2,
Last_value(post_evar47)
OVER(partition BY Concat(post_visid_high, post_visid_low,visit_num, visit_start_time_gmt) ORDER BY post_evar47 DESC nulls first) AS hhid ,
Concat(post_visid_high,post_visid_low,visit_num,visit_start_time_gmt) AS visit ,
Concat(',', post_event_list, ',') AS event_list
FROM ${db_views}.dw_views.click_hit_data
WHERE
banner != 'albertsonscompanies'
--and 
--post_evar2 in ('allb_ist_web_j4uc_ih','allb_reg_dd_j4uc_ih','allb_reg_uber_j4uc_ih')
and
dw_createts > TO_TIMESTAMP('${last_watermark}')
and 
dw_createts <= TO_TIMESTAMP('${current_watermark}')
) chd1_partner
inner join
${db_refined}.DW_R_STAGE.adobe_banner_lkp abl
ON chd1_partner.BANNER=abl.adobe_banner_nm
OR (chd1_partner.BANNER<>abl.adobe_banner_nm AND upper(chd1_partner.BANNER) = upper(abl.banner_nm))
inner join 
"${db_views}"."DW_VIEWS"."D0_FISCAL_DAY" cal
ON chd1_partner.transaction_date=cal.calendar_dt
WHERE visit IN
(
SELECT concat(post_visid_high, post_visid_low,visit_num, visit_start_time_gmt)
FROM ${db_views}.dw_views.click_hit_data
WHERE CONTAINS(post_prop19, 'account/short-registration')
AND hit_source NOT IN (5,7,8,9)
AND exclude_hit = 0
--AND post_evar2 in ('allb_ist_web_j4uc_ih','allb_reg_dd_j4uc_ih','allb_reg_uber_j4uc_ih')
AND post_evar2='${partner_campaign_id}'
) and chd1_partner.banner is not null
GROUP BY 1,2,3,4
)
select
cte2.day_id,
cte2.transaction_date,
cte2.partner_id as BUSINESS_PARTNER_D1_SK,
cte2.banner_id as banner_d1_sk,
cte2.visits,
cte2.partner_registrations as registration,
current_timestamp() as dw_create_ts,
current_timestamp() as dw_last_update_ts
from cte2 
where cte2.banner_id is not null
;`});

var res_ins_reg_dtl_partner = ins_reg_dtl_partner.execute();
res_ins_reg_dtl_partner.next();
no_rec_reg_dt1_partner = res_ins_reg_dtl_partner.getColumnValue(1);


// *******************END OF LOAD FOR FACT_PARTNER_REG_DTL TABLE******************************

var log_num_rec = snowflake.createStatement({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Job_Success(
                '${job_run_id}',
                '${no_rec_reg_dt1_partner}',
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
//---------------------------------------------------------------


return "F_PARTNER_REGISTRATION LOADED"

$$;
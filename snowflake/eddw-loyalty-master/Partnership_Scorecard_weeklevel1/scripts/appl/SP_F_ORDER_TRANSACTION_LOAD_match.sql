--liquibase formatted sql
--changeset SYSTEM:SP_F_ORDER_TRANSACTION_LOAD_match runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME_C>>;
use schema DW_APPL;

ALTER TASK SP_F_ORDER_TRANSACTION_LOAD_TASK SUSPEND;

ALTER TASK SP_F_ORDER_TRANSACTION_LOAD_TASK 
SET SCHEDULE = 'USING CRON * 15 * * * Asia/Kolkata'
 ;

 ALTER TASK SP_F_ORDER_TRANSACTION_LOAD_TASK RESUME;

CREATE OR REPLACE PROCEDURE "SP_F_ORDER_TRANSACTION_LOAD"()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS $$
//--------------------------------------------------------------------------------------------------------------------#
// Desc         : Stored proc to Load KPI target table F_Order_Transaction
//--------------------------------------------------------------------------------------------------------------------#

var db_confirmed = '<<EDM_DB_NAME_C>>';
var db_analytics = '<<EDM_DB_NAME>>';
var db_refined = '<<EDM_DB_NAME_R>>';
var db_views = '<<EDM_VIEW_NAME>>'

var results_array = [];
var current_watermark = new Date();
current_watermark = current_watermark.toISOString();


var current_wm_id1=snowflake.createStatement({sqlText: `SELECT TO_CHAR(MAX(txn_tm)) FROM ${db_views}.dw_bizops_views.GW_REG99_TXNS_2020 txnc 
                              WHERE txn_dte >= CURRENT_DATE - 365;`})
var current_wm_id2=current_wm_id1.execute();
current_wm_id2.next();
var current_wm_ts = current_wm_id2.getColumnValue(1);


 
var feed_extract_job_ini2 = snowflake.execute({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Job_Initiate(
                'OCRP',
                'SP_F_ORDER_TRANSACTION',
                'FACT_ORDER_TRANSACTION',
                null,
                'I',
                null,
                null,
                null,
                '${db_analytics}.DW_RETAIL_EXP.F_Order_Transaction',
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
var last_watermark_from_tble2 = ret_wm_ts2.getColumnValue(1);



if (last_watermark_from_tble2 === null) {
  last_watermark2 = '2021-04-01 00:00:00.000';
} else {
  last_watermark2 = last_watermark_from_tble2;
}


results_array[0]=last_watermark_from_tble2;
results_array[1]=last_watermark2;

// ************** Load for F_ORDER_TRANSACTION table BEGIN *****************

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
when banner_nm ='SHAW\\\\S' then 'shaws'
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
    inner join ${db_views}.DW_VIEWS.D1_DIVISION dd on dd.division_id = store.division_id
    inner join ${db_refined}.DW_R_STAGE.adobe_banner_lkp abl ON store.banner_nm = abl.ADOBE_BANNER_NM
    OR (store.banner_nm <> abl.ADOBE_BANNER_NM and upper(store.banner_nm) = upper(abl.BANNER_nm))
where dd.corporation_id = '001'
);`});

var stmt_lsfu1 = stmt_lsfu.execute();



var stmt4 = snowflake.createStatement({
			sqlText: `   INSERT INTO "${db_analytics}"."DW_RETAIL_EXP".F_ORDER_TRANSACTION           
                       (TRANSACTION_DT,
						RETAIL_CUSTOMER_D1_SK,
						Retail_store_D1_sk,
						banner_d1_sk,
						TRANSACTION_AMT,
						division_d1_sk,
						Fiscal_Day_Id ,
						Transaction_Tax_Amt,
						item_qty,
						dug_order_ind,
						delivery_order_ind,
						transaction_id ,						
						dw_create_ts,
						dw_last_update_ts)

WITH src_rec AS (
select DISTINCT current_card_nbr,RETAIL_CUSTOMER_D1_SK from
(SELECT DISTINCT
clp.Loyalty_Program_Card_Nbr as current_card_nbr,
d1_rc.RETAIL_CUSTOMER_D1_SK,
row_number() over (PARTITION BY current_card_nbr ORDER BY clp.dw_create_ts desc
                  ) as rn
FROM
${db_views}.DW_VIEWS.D1_RETAIL_CUSTOMER d1_rc
LEFT JOIN ${db_views}.DW_VIEWS.CUSTOMER_LOYALTY_PROGRAM clp on d1_rc.Retail_Customer_UUID = clp.Retail_Customer_UUID
--Left join ${db_views}.DW_VIEWS.CUSTOMER_PHONE_FAX_CONTACT cpfc on d1_rc.Retail_Customer_UUID = cpfc.Retail_Customer_UUID
left join ${db_views}.DW_VIEWS.CUSTOMER_ACCOUNT_STATUS cas ON d1_rc.Retail_Customer_UUID = cas.Retail_Customer_UUID
and cas.STATUS_TYPE_CD = 'ONLINE_ENROLLMENT'
and cas.dw_current_Version_ind = TRUE
AND cas.dw_logical_delete_ind = FALSE
WHERE d1_rc.DW_LOGICAL_DELETE_IND = FALSE)WHERE RN=1
),
final_rec AS (
    SELECT gwt.txn_dte as transaction_dt,
        s.RETAIL_CUSTOMER_D1_SK,        
        lsfu.division_D1_Sk,
        lsfu.banner_d1_sk,
        lsfu.Retail_store_D1_sk,
        cal.fiscal_day_id,
		gwt.NET_AMT as TRANSACTION_AMT,
		'0.00' as total_tax_amt,
		SUM(gwt.total_item_qty) as item_qty,
 		case when thc.register_nbr = 99 /* and dug.txn_id is not null and trim(dug.delivery_type_dsc) like '%DUG%'*/ then 
		TRUE 
		else 
		FALSE 
		end AS dug_order_ind,
		case when thc.register_nbr = 99 /*and dug.txn_id is not null and trim(dug.delivery_type_dsc) like '%DUG%'*/ then 
		FALSE 
		else 
		TRUE 
		end AS delivery_order_Ind,
		gwt.txn_id as transaction_id,
	    thc.register_nbr as register_nbr
		
		FROM ${db_views}.DW_BIZOPS_VIEWS.GW_REG99_TXNS_2020 gwt
		LEFT JOIN src_rec s ON s.current_card_nbr = gwt.card_nbr::varchar 
		LEFT JOIN  ${db_views}.dw_edw_views.txn_hdr thc ON gwt.TXN_ID = thc.TXN_ID     	
		LEFT JOIN ${db_views}.DW_EDW_VIEWS.TXN_FACTS tf 
		ON tf.txn_id = gwt.txn_id 
		left join  (select distinct txn_id,txn_dt,delivery_type_dsc from ${db_views}.dw_edw_views.gw_online_register_txn 
        where delivery_type_dsc like '%DUG%' AND TXN_ID IS NOT NULL) dug on tf.txn_id = dug.txn_id
       	INNER JOIN ${db_refined}.DW_R_STAGE.lsfu lsfu ON gwt.store_id = try_to_numeric(lsfu.store_id)
        INNER JOIN ${db_views}.DW_VIEWS.D0_FISCAL_DAY cal ON gwt.txn_dte=cal.calendar_dt        
     	
	        WHERE GWT.TXN_TM <= '${current_wm_ts}' AND
            GWT.TXN_TM >= '${last_watermark2}'
           
	        
    GROUP BY 1,2,3,4,5,6,7,8,10,11,12,13)
			
 select  
                                                TRANSACTION_DT,
						RETAIL_CUSTOMER_D1_SK,
						Retail_store_D1_sk,
						banner_d1_sk,
						TRANSACTION_AMT,
						division_d1_sk,
						Fiscal_Day_Id ,
						total_tax_amt,
						item_qty,
						dug_order_ind,
						delivery_order_ind,
						transaction_id ,						
						dw_create_ts,
						dw_last_update_ts
                        from           
				      
	(SELECT
	nvl(transaction_dt,current_date) as transaction_dt ,
	nvl(RETAIL_CUSTOMER_D1_SK,'-1') as RETAIL_CUSTOMER_D1_SK,
	nvl(Retail_store_D1_sk,'0') as Retail_store_D1_sk,
        nvl(banner_d1_sk,'0') as banner_d1_sk,
        nvl(transaction_amt,'0') as transaction_amt,
        nvl(division_d1_sk,'0') as division_d1_sk,
        nvl(fiscal_day_id,'0') as fiscal_day_id,
	nvl(total_tax_amt,'0') as total_tax_amt,
	nvl(item_qty,'0') as item_qty,
	nvl(dug_order_ind,'0') as dug_order_ind,
	nvl(delivery_order_Ind,'0') as delivery_order_Ind,
	nvl(transaction_id,'0') as transaction_id,
	current_timestamp() as dw_create_ts,
	current_timestamp() as dw_last_update_ts,
    row_number() over (partition by transaction_id order by transaction_dt desc) as rn
FROM final_rec )
where rn=1			      
 ;`});
   
   var res4 = stmt4.execute();
	res4.next();
    return_value1 = res4.getColumnValue(1);
 	
	var return_value = return_value1 
// *******************END OF LOAD FOR FACT_ORDER_TRANSACTION TABLE******************************


var stmt2_fpocm = snowflake.createStatement({
     sqlText: `call ${db_confirmed}.DW_APPL.SP_Feed_Extract_Job_Success(
                '${job_run_id2}',
                '${return_value1}',
                null,
                null)`
                
     });
 
var res2_fpocm = stmt2_fpocm.execute();
res2_fpocm.next();
return_value2 = res2_fpocm.getColumnValue(1);

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


return "F_Order_Transaction TABLE LOADED"

$$;

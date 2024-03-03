--liquibase formatted sql
--changeset SYSTEM:ocrp_loyalty_partnership runOnChange:true splitStatements:false OBJECT_TYPE:SP

use database EDM_CONFIRMED_PRD;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE OCRP_LOYALTY_PARTNERSHIP()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS 
$$
//--------------------------------------------------------------------------------------------------------------------#
// Desc         : Stored proc to Load instacart Data into EDM Table and load data from main table to kafka out table
//--------------------------------------------------------------------------------------------------------------------#
// Modification : Auth   : Vidushi Jaiswal
//              : Date   : 11/10/2021
//              : Change : Development of Uber data Extract to Loyalty
//--------------------------------------------------------------------------------------------------------------------#

var current_watermark = new Date();
current_watermark = current_watermark.toISOString();
var two_day_old = new Date();
two_day_old.setDate(new Date().getDate()-2);
two_day_old = two_day_old.toISOString();


var cur_db = snowflake.execute( {sqlText: `Select current_database()`} ); 
cur_db.next(); 
var env = cur_db.getColumnValue(1);
env = env.split('_');
env = env[env.length - 1];
var cnf_db = `EDM_CONFIRMED_${env}`;
var view_db = `EDM_VIEWS_${env}`;
var out_db = `EDM_CONFIRMED_OUT_${env}`;

var results_array = [];

var feed_extract_job_ini = snowflake.execute({
     sqlText: `call ${cnf_db}.DW_APPL.SP_Feed_Extract_Job_Initiate(
                'ocrp',
                'ocrp_loyalty_partnership',
                'ocrp_loyalty_partnership',
                null,
                'I',
                null,
                '${cnf_db}.dw_c_loyalty.ocrp_loyalty',
                null,
                '${out_db}.dw_loyalty.KAFKAOUTQUEUE',
				null)`
                
     });
	 
//return_value will give job run auto id 
feed_extract_job_ini.next();
job_run_id = feed_extract_job_ini.getColumnValue(1);


var get_wm_ts = snowflake.createStatement({
     sqlText: `call ${cnf_db}.DW_APPL.SP_Feed_Extract_Get_Watermark_TS('${job_run_id}')`
                
     });
//return_value4 will give job run auto id 
var ret_wm_ts = get_wm_ts.execute();
ret_wm_ts.next();
var last_watermark_from_tble = ret_wm_ts.getColumnValue(1);
	 
if (last_watermark_from_tble === null) {
last_watermark = two_day_old;
} else {
last_watermark = last_watermark_from_tble;
}	 

results_array[0]=last_watermark_from_tble;
results_array[1]=two_day_old;
results_array[2]=last_watermark;

// ************** Load for ocrp_loyalty table for Instamart BEGIN *****************	
	
	var stmt1 = snowflake.createStatement({
			sqlText: `insert into ${cnf_db}.dw_c_loyalty.OCRP_LOYALTY
					  with customer as(SELECT 
					                   LOYALTY_PROGRAM_CARD_NBR,
					                   PRIMARY_PHONE_NBR,
									   HOUSEHOLD_ID
									   from ${view_db}.DW_VIEWS.SMV_RETAIL_CUSTOMER_LOYALTY_PROGRAM_HOUSEHOLD									   
									   where HOUSEHOLD_ID not in ( 700000481406,200019083971,903041424453)
									   QUALIFY (ROW_NUMBER() OVER (PARTITION BY PRIMARY_PHONE_NBR ORDER  BY LOYALTY_PROGRAM_CARD_NBR asc)=1)									   
									  )
					select
						pos.STORE_ID as "storeNumber",
						'' as "terminalNumber",
						pos.Order_ID as "transactionNumber",
						pos.STORE_TXN_TS as "transactionTimestamp",
						sub_qry1.hhid as "memberId",
						ldp.DLVRY_PARTNER_NM as "transactionSource",
						ldp.partner_id as "transactionSourceId",
						coalesce (case when length(poi.UPC_ID) <= 5 then poi.UPC_ID::varchar else lpad(poi.UPC_ID,13,0) end,'') as itemCode,
						'' as entryId,
						coalesce(poi.unit_prc_amt,0.0) as unitPrice,
						coalesce(poi.ONLINE_REVENUE_AMT,0.0) as extendedPrice,
						'' as discountAllowed,
						case when (poi.upc_id = '0' AND poi.ALCOHOLIC_IND ='0') then 'true' 
						else 'false' end as pointsApplyItem,
						coalesce(cast((case when (poi.upc_id != 0 and ei.tradingStamp is null and poi.alcoholic_ind = 1) then 0
							when (poi.upc_id != 0 and ei.tradingStamp is null and poi.alcoholic_ind = 0) then 1
							else ei.tradingstamp end) as varchar),'0') as tradingStamp,
						poi.alcoholic_ind::varchar as alcoholFlag,
						case when ei.tradingStamp is null then 'false' else 'true' end as trading_match_ind,
						sub_qry1.loyalty_phone_nbr,
						sub_qry1.PRIMARY_PHONE_NBR,
						lsfu.banner_nm as "banner_nm",
						lsfu.banner_Id as "banner_Id",
						sub_qry1.LOYALTY_PROGRAM_CARD_NBR,
						poi.itm_qty,
						CASE WHEN sub_qry1.hhid >0 THEN 'Y' ELSE 'N' END as "match_flag_ind",
						CASE WHEN sub_qry1.hhid >0 THEN 'Y' ELSE 'N' END as "processed_ind",
						TO_TIMESTAMP('${current_watermark}') as "insert_ts",
						TO_TIMESTAMP('${current_watermark}') as "last_updated_ts",
						sub_qry1.user_id
					from ${view_db}.DW_EDW_VIEWS.partner_order_store pos
					inner join ${view_db}.DW_EDW_VIEWS.LU_STORE_FINANCE_OM lsfu
					on pos.store_id = lsfu.store_id
					inner join 
					(
					select 
					customer.LOYALTY_PROGRAM_CARD_NBR as LOYALTY_PROGRAM_CARD_NBR,
					customer.household_id as hhid ,
					customer.PRIMARY_PHONE_NBR as PRIMARY_PHONE_NBR,
					po.order_id as order_id,
					po.user_id as user_id,
					po.loyalty_phone_nbr as loyalty_phone_nbr
					from 
					${view_db}.DW_EDW_VIEWS.partner_order po
					join 
					customer 
					on po.loyalty_phone_nbr = customer.PRIMARY_PHONE_NBR
					) sub_qry1
					on
					sub_qry1.order_id=pos.order_id
					left join
					${view_db}.DW_EDW_VIEWS.PARTNER_ORDER_itm poi
					on poi.order_id = pos.order_id
					left join 
					(select distinct 
					upcid,
					brandnm,
					tradingstamp,
					rogcd 
					from ${view_db}.DW_VIEWS.ECATALOG_ITEM where upper(dw_logical_delete_ind) = 'FALSE') ei
					on trim(upper(poi.upc_id)) = trim(upper(ei.upcid)) and trim(upper(ei.rogcd)) = trim(upper(lsfu.rog_cd))
					left join ${view_db}.DW_EDW_VIEWS.LU_DELIVERY_PARTNER ldp on
					pos.partner_id = ldp.partner_id
					where pos.dw_last_updt_ts > TO_TIMESTAMP('${last_watermark}')
					AND pos.dw_last_updt_ts <= TO_TIMESTAMP('${current_watermark}')
					QUALIFY (ROW_NUMBER() OVER (PARTITION BY pos.order_id,pos.store_id,poi.upc_id,sub_qry1.hhid ORDER BY pos.store_txn_ts desc)=1)
					union
					select
						pos.STORE_ID as "storeNumber",
						'' as "terminalNumber",
						pos.Order_ID as "transactionNumber",
						pos.STORE_TXN_TS as "transactionTimestamp",
						null as "memberId",
						ldp.DLVRY_PARTNER_NM as "transactionSource",
						ldp.partner_id as "transactionSourceId",
						coalesce (case when length(poi.UPC_ID) <= 5 then poi.UPC_ID::varchar else lpad(poi.UPC_ID,13,0) end,'') as itemCode,
						'' as entryId,
						coalesce(poi.unit_prc_amt,0.0) as unitPrice,
						coalesce(poi.ONLINE_REVENUE_AMT,0.0) as extendedPrice,
						'' as discountAllowed,
						case when (poi.upc_id = '0' AND poi.ALCOHOLIC_IND ='0') then 'true' 
						else 'false' end as pointsApplyItem,
						coalesce(cast((case when (poi.upc_id != 0 and ei.tradingStamp is null and poi.alcoholic_ind = 1) then 0
							when (poi.upc_id != 0 and ei.tradingStamp is null and poi.alcoholic_ind = 0) then 1
							else ei.tradingstamp end) as varchar),'0') as tradingStamp,
						poi.alcoholic_ind::varchar as alcoholFlag,
						case when ei.tradingStamp is null then 'false' else 'true' end as trading_match_ind,						
						po.loyalty_phone_nbr,
						'' as PRIMARY_PHONE_NBR,
						lsfu.banner_nm as "banner_nm",
						lsfu.banner_Id as "banner_Id",
						null as LOYALTY_PROGRAM_CARD_NBR,
						poi.itm_qty,
						'N' as "match_flag_ind",
						'N' as "processed_ind",
						TO_TIMESTAMP('${current_watermark}') as "insert_ts",
						TO_TIMESTAMP('${current_watermark}') as "last_updated_ts",
						po.user_id
					from ${view_db}.DW_EDW_VIEWS.PARTNER_ORDER_store pos
					inner join ${view_db}.DW_EDW_VIEWS.LU_STORE_FINANCE_OM lsfu
					on pos.store_id = lsfu.store_id
					left join
					${view_db}.DW_EDW_VIEWS.PARTNER_ORDER_itm poi
					on poi.order_id = pos.order_id
					left join 
					(select distinct upcid,brandnm,tradingstamp,rogcd from ${view_db}.DW_VIEWS.ECATALOG_ITEM where upper(dw_logical_delete_ind) = 'FALSE') ei
					on trim(upper(poi.upc_id)) = trim(upper(ei.upcid)) and trim(upper(ei.rogcd)) = trim(upper(lsfu.rog_cd))
					left join ${view_db}.DW_EDW_VIEWS.LU_DELIVERY_PARTNER ldp on
                    pos.partner_id = ldp.partner_id
					inner join ${view_db}.DW_EDW_VIEWS.partner_order po
					on pos.order_id = po.order_id
					and po.loyalty_phone_nbr not in ('0','0.','0.0')
					where pos.order_id not in 
					(
						select po1.order_id as order_id 
						from ${view_db}.DW_EDW_VIEWS.partner_order po1 
						inner join customer 
						on po1.loyalty_phone_nbr = customer.PRIMARY_PHONE_NBR 
						where po1.loyalty_phone_nbr is not null
					)
					AND pos.dw_last_updt_ts > TO_TIMESTAMP('${last_watermark}')
					AND pos.dw_last_updt_ts <= TO_TIMESTAMP('${current_watermark}')
					QUALIFY (ROW_NUMBER() OVER (PARTITION BY pos.order_id,pos.store_id,poi.upc_id ORDER BY pos.store_txn_ts desc)=1)`});

//return_value1 will return number of rows inserted		
	var res1 = stmt1.execute();
	res1.next();
var return_value1 = res1.getColumnValue(1);

// ************** Load for ocrp_loyalty table for UBER BEGIN *****************	

var stmt2 = snowflake.createStatement({
			sqlText: `INSERT INTO ${cnf_db}.dw_c_loyalty.OCRP_LOYALTY
						WITH customer AS
							(SELECT LOYALTY_PROGRAM_CARD_NBR,
									PRIMARY_PHONE_NBR,
									HOUSEHOLD_ID
							 FROM ${view_db}.DW_VIEWS.SMV_RETAIL_CUSTOMER_LOYALTY_PROGRAM_HOUSEHOLD			
							where HOUSEHOLD_ID not in ( 700000481406,200019083971,903041424453)							 
							 QUALIFY (ROW_NUMBER() OVER (PARTITION BY PRIMARY_PHONE_NBR ORDER  BY LOYALTY_PROGRAM_CARD_NBR asc)=1)							
							  ),
						Order_Det As 
							(
							SELECT 
								customer.LOYALTY_PROGRAM_CARD_NBR,
								customer.household_id as hhid ,
								customer.PRIMARY_PHONE_NBR,
								pgod.order_id,
								pgod.partner_grocery_order_customer_integration_id as user_id,
								pgod.loyalty_phone_nbr,
								pgod.upc_id,
								pgod.unit_price_amt,
								pgod.revenue_amt,
								(CASE WHEN pgod.alcoholic_ind = 'false' THEN 0 ELSE 1 END) alcoholic_ind,
								pgod.item_qty
							FROM ${view_db}.dw_VIEWS.Partner_Grocery_Order_Detail pgod
							INNER JOIN customer 
							ON pgod.loyalty_phone_nbr = customer.PRIMARY_PHONE_NBR
							)
						SELECT
							pgoh.STORE_ID as "storeNumber",
							'' as "terminalNumber",
							pgoh.Order_ID as "transactionNumber",
							pgoh.store_transaction_ts as "transactionTimestamp",
							od.hhid as "memberId",
							pgoh.partner_nm as "transactionSource",
							pgoh.partner_id as "transactionSourceId",
							COALESCE (CASE WHEN LENGTH(od.upc_id) <= 5 THEN od.upc_id::VARCHAR ELSE LPAD(od.upc_id,13,0) END,'') as itemCode,
							'' as entryId,
							COALESCE(od.unit_price_amt,0.0) as unitPrice,
							COALESCE(od.revenue_amt,0.0) as extendedPrice,
							'' as discountAllowed,
							(CASE WHEN (od.upc_id = '0' AND od.ALCOHOLIC_IND ='0') THEN 'true' 
								  ELSE 'false' END) as pointsApplyItem,
							COALESCE(CAST((CASE WHEN (od.upc_id != 0 AND ei.tradingStamp IS NULL AND od.alcoholic_ind = 1) THEN 0
												WHEN (od.upc_id != 0 AND ei.tradingStamp IS NULL AND od.alcoholic_ind = 0) THEN 1
												ELSE ei.tradingstamp END) as VARCHAR),'0') as tradingStamp,
							od.alcoholic_ind as alcoholFlag,
							(CASE WHEN ei.tradingStamp IS NULL THEN 'false' ELSE 'true' END) as trading_match_ind,
							od.loyalty_phone_nbr,
							od.PRIMARY_PHONE_NBR,
							lsfu.banner_nm ,
							lsfu.banner_Id,
							od.LOYALTY_PROGRAM_CARD_NBR,
							od.item_qty,
							CASE WHEN od.hhid>0 THEN 'Y' ELSE 'N' END as "match_flag_ind",
							CASE WHEN od.hhid>0 THEN 'Y' ELSE 'N' END as "processed_ind",
							TO_TIMESTAMP('${current_watermark}') as "insert_ts",
							TO_TIMESTAMP('${current_watermark}') as "last_updated_ts",
							od.user_id
						FROM ${view_db}.dw_VIEWS.Partner_Grocery_Order_Header pgoh
						INNER JOIN ${view_db}.DW_EDW_VIEWS.LU_STORE_FINANCE_OM lsfu
						ON pgoh.store_id = lsfu.store_id 
						INNER JOIN Order_Det od 
						ON od.order_id = pgoh.order_id
						LEFT JOIN 
						(SELECT DISTINCT 
							upcid,
							brandnm,
							tradingstamp,
							rogcd 
						FROM ${view_db}.DW_VIEWS.ECATALOG_ITEM 
						WHERE UPPER(dw_logical_delete_ind) = 'FALSE'
						) ei
						ON TRIM(UPPER(od.upc_id)) = TRIM(UPPER(ei.upcid)) AND TRIM(UPPER(ei.rogcd)) = TRIM(UPPER(lsfu.rog_cd))
						LEFT JOIN ${view_db}.DW_EDW_VIEWS.LU_DELIVERY_PARTNER ldp 
						ON pgoh.partner_id = ldp.partner_id
						WHERE pgoh.dw_create_ts > TO_TIMESTAMP('${last_watermark}')
						  AND pgoh.dw_create_ts <= TO_TIMESTAMP('${current_watermark}')
						--QUALIFY (ROW_NUMBER() OVER (PARTITION BY pgoh.order_id,pgoh.store_id,od.upc_id,od.hhid ORDER BY pgoh.store_transaction_ts DESC)=1)
						UNION
						SELECT
							pgoh.STORE_ID as "storeNumber",
							'' as "terminalNumber",
							pgoh.Order_ID as "transactionNumber",
							pgoh.store_transaction_ts as "transactionTimestamp",
							null as "memberId",
							pgoh.partner_nm as "transactionSource",
							pgoh.partner_id as "transactionSourceId",
							COALESCE (CASE WHEN LENGTH(pgod.upc_id) <= 5 THEN pgod.upc_id::VARCHAR ELSE LPAD(pgod.upc_id,13,0) END,'') as itemCode,
							'' as entryId,
							COALESCE(pgod.unit_price_amt,0.0) as unitPrice,
							COALESCE(pgod.revenue_amt,0.0) as extendedPrice,
							'' as discountAllowed,
							(CASE WHEN (pgod.upc_id = '0' AND pgod.ALCOHOLIC_IND = 'false') THEN 'true' 
								  ELSE 'false' END) as pointsApplyItem,
							COALESCE(CAST((CASE WHEN (pgod.upc_id != 0 AND ei.tradingStamp IS NULL AND pgod.alcoholic_ind = 'true') THEN 0
												WHEN (pgod.upc_id != 0 AND ei.tradingStamp IS NULL AND pgod.alcoholic_ind = 'false') THEN 1
												ELSE ei.tradingstamp END) as VARCHAR),'0') as tradingStamp,
							(CASE WHEN pgod.alcoholic_ind = 'false' THEN 0 ELSE 1 END) as alcoholFlag,
							(CASE WHEN ei.tradingStamp IS NULL THEN 'false' ELSE 'true' END) as trading_match_ind,							
							pgod.loyalty_phone_nbr,
							'' as PRIMARY_PHONE_NBR,
							lsfu.banner_nm ,
							lsfu.banner_Id,
							null as LOYALTY_PROGRAM_CARD_NBR,
							pgod.item_qty,
							'N' as "match_flag_ind",
							'N' as "processed_ind",
							TO_TIMESTAMP('${current_watermark}') as "insert_ts",
							TO_TIMESTAMP('${current_watermark}') as "last_updated_ts",
							pgod.partner_grocery_order_customer_integration_id as user_id
						FROM ${view_db}.dw_VIEWS.Partner_Grocery_Order_Header pgoh
						INNER JOIN ${view_db}.DW_EDW_VIEWS.LU_STORE_FINANCE_OM lsfu
						ON pgoh.store_id = lsfu.store_id
						LEFT JOIN ${view_db}.DW_EDW_VIEWS.LU_DELIVERY_PARTNER ldp ON
						pgoh.partner_id = ldp.partner_id
						INNER JOIN ${view_db}.dw_VIEWS.Partner_Grocery_Order_Detail pgod
						ON pgod.order_id = pgoh.order_id
						AND pgod.loyalty_phone_nbr NOT IN ('0','0.','0.0')
						LEFT JOIN 
						(SELECT DISTINCT upcid,brandnm,tradingstamp,rogcd 
						 FROM ${view_db}.DW_VIEWS.ECATALOG_ITEM 
						 WHERE UPPER(dw_logical_delete_ind) = 'FALSE'
						) ei
						ON TRIM(UPPER(pgod.upc_id)) = TRIM(UPPER(ei.upcid)) AND TRIM(UPPER(ei.rogcd)) = TRIM(UPPER(lsfu.rog_cd))
						WHERE pgoh.order_id NOT IN 
						(SELECT pgo.order_id 
						 FROM ${view_db}.dw_VIEWS.Partner_Grocery_Order_Detail pgo 
						 INNER JOIN customer 
						 ON pgo.loyalty_phone_nbr = customer.PRIMARY_PHONE_NBR 
						 WHERE pgo.loyalty_phone_nbr IS NOT NULL
						)
						AND pgoh.dw_create_ts > TO_TIMESTAMP('${last_watermark}')
						AND pgoh.dw_create_ts <= TO_TIMESTAMP('${current_watermark}')
						--QUALIFY (ROW_NUMBER() OVER (PARTITION BY pgoh.order_id,pgoh.store_id,pgod.upc_id ORDER BY pgoh.store_transaction_ts DESC)=1)
						`});

//return_value2 will return number of rows inserted		
	var res2 = stmt2.execute();
	res2.next();
var return_value2 = res2.getColumnValue(1);

// ************** Load for ocrp_loyalty table for FOODSTORM BEGIN *****************	

var stmt6 = snowflake.createStatement({
			sqlText: `insert into ${cnf_db}.dw_c_loyalty.OCRP_LOYALTY
					  with customer as(SELECT 
										RETAIL_CUSTOMER_UUID,
					                   LOYALTY_PROGRAM_CARD_NBR,
					                   PRIMARY_PHONE_NBR,
									   HOUSEHOLD_ID
									   from ${view_db}.DW_VIEWS.SMV_RETAIL_CUSTOMER_LOYALTY_PROGRAM_HOUSEHOLD ---${view_db}.DW_VIEWS.SMV_RETAIL_CUSTOMER_LOYALTY_PROGRAM_HOUSEHOLD does not have column SOURCE_SYS_DEL_IND									   
									   where HOUSEHOLD_ID not in ( 700000481406,200019083971,903041424453)
									   QUALIFY (ROW_NUMBER() OVER (PARTITION BY PRIMARY_PHONE_NBR ORDER  BY LOYALTY_PROGRAM_CARD_NBR asc)=1) 
									  )
						select
						fo.STORE_ID as "storeNumber",
						'' as "terminalNumber",
						fo.Order_ID as "transactionNumber",
						fo.Delivery_Dt as "transactionTimestamp",
						sub_qry1.hhid as "memberId",
						fo.PARTNER_NM as "transactionSource",
						fo.partner_id as "transactionSourceId",
						coalesce (case when length(foi.UPC_ID) <= 5 then foi.UPC_ID::varchar else lpad(foi.UPC_ID,13,0) end,'') as itemCode,
						'' as entryId,
						coalesce(foi.item_price_amt,0.0) as unitPrice,
						coalesce(fo.TOTAL_AMT,0.0) as extendedPrice, ---coalesce(poi.ONLINE_REVENUE_AMT,0.0) as extendedPrice,
						'' as discountAllowed,
						case when foi.upc_id = '0' then 'true' else 'false' end as pointsApplyItem,
						coalesce(cast((case	when (foi.upc_id != 0 and ei.tradingStamp is null) then 1
						else ei.tradingstamp end) as varchar),'0') as tradingStamp,
						'0' as alcoholFlag, ---poi.alcoholic_ind::varchar as alcoholFlag
						case when ei.tradingStamp is null then 'false' else 'true' end as trading_match_ind,						
						sub_qry1.loyalty_phone_nbr,
						sub_qry1.PRIMARY_PHONE_NBR,
						lsfu.banner_nm as "banner_nm",
						lsfu.banner_Id as "banner_Id",
						sub_qry1.LOYALTY_PROGRAM_CARD_NBR,
						foi.item_qty,
						CASE WHEN sub_qry1.hhid >0 THEN 'Y' ELSE 'N' END as "match_flag_ind",
						CASE WHEN sub_qry1.hhid >0 THEN 'Y' ELSE 'N' END as "processed_ind",
						TO_TIMESTAMP('${current_watermark}') as "insert_ts",
						TO_TIMESTAMP('${current_watermark}') as "last_updated_ts",
						sub_qry1.user_id						
					from ${cnf_db}.DW_C_LOYALTY.FOODSTORM_ORDER fo
					inner join ${view_db}.DW_EDW_VIEWS.LU_STORE_FINANCE_OM lsfu  ---${view_db}.DW_VIEWS.D1_RETAIL_STORE has banner_id missing
					on fo.store_id = lsfu.store_id
					
					inner join 
					(
					select 
					customer.RETAIL_CUSTOMER_UUID as user_id,
					customer.LOYALTY_PROGRAM_CARD_NBR as LOYALTY_PROGRAM_CARD_NBR,
					customer.household_id as hhid ,
					customer.PRIMARY_PHONE_NBR as PRIMARY_PHONE_NBR,
					fo.order_id as order_id,
					--fo.Source_Customer_Id as user_id, ---po.user_id as user_id
					fo.Customer_Loyalty_Phone_Nbr as loyalty_phone_nbr
					from ${cnf_db}.DW_C_LOYALTY.FOODSTORM_ORDER fo
					join customer on fo.Customer_Loyalty_Phone_Nbr = customer.PRIMARY_PHONE_NBR
					) sub_qry1 on
					sub_qry1.order_id=fo.order_id
					
					left join
					${cnf_db}.DW_C_LOYALTY.FOODSTORM_ORDER_ITEM foi
					on fo.order_id = foi.order_id
					
					left join 
					(select distinct 
					upcid,
					brandnm,
					tradingstamp,
					rogcd 
					from ${view_db}.DW_VIEWS.ECATALOG_ITEM where upper(dw_logical_delete_ind) = 'FALSE') ei
					on trim(upper(foi.upc_id)) = trim(upper(ei.upcid)) and trim(upper(ei.rogcd)) = trim(upper(lsfu.rog_cd))
					left join ${view_db}.DW_EDW_VIEWS.LU_DELIVERY_PARTNER ldp on
					fo.partner_id = ldp.partner_id
					where fo.dw_create_ts > TO_TIMESTAMP('${last_watermark}')
					AND fo.dw_create_ts <= TO_TIMESTAMP('${current_watermark}')
					QUALIFY (ROW_NUMBER() OVER (PARTITION BY fo.order_id,fo.store_id,foi.upc_id,sub_qry1.hhid ORDER BY fo.Delivery_Dt desc)=1)
					
					
					union
					
					
					select
					fo.STORE_ID as "storeNumber",
					'' as "terminalNumber",
					fo.Order_ID as "transactionNumber",
					fo.Delivery_Dt as "transactionTimestamp",
					null as "memberId",
					fo.PARTNER_NM as "transactionSource",
					fo.partner_id as "transactionSourceId",
					coalesce (case when length(foi.UPC_ID) <= 5 then foi.UPC_ID::varchar else lpad(foi.UPC_ID,13,0) end,'') as itemCode,
					'' as entryId,
					coalesce(foi.item_price_amt,0.0) as unitPrice,
					coalesce(fo.TOTAL_AMT,0.0) as extendedPrice, ---coalesce(poi.ONLINE_REVENUE_AMT,0.0) as extendedPrice,
					'' as discountAllowed,
					case when foi.upc_id = '0' then 'true' else 'false' end as pointsApplyItem,
					coalesce(cast((case	when (foi.upc_id != 0 and ei.tradingStamp is null) then 1
					else ei.tradingstamp end) as varchar),'0') as tradingStamp,
					'0' as alcoholFlag, ---poi.alcoholic_ind::varchar as alcoholFlag
					case when ei.tradingStamp is null then 'false' else 'true' end as trading_match_ind,					
					fo.Customer_Loyalty_Phone_Nbr as loyalty_phone_nbr,
					'' as PRIMARY_PHONE_NBR,
					lsfu.banner_nm as "banner_nm",
					lsfu.banner_Id as "banner_Id",
					null as LOYALTY_PROGRAM_CARD_NBR,
					foi.item_qty,
					'N' as "match_flag_ind",
					'N' as "processed_ind",
					TO_TIMESTAMP('${current_watermark}') as "insert_ts",
					TO_TIMESTAMP('${current_watermark}') as "last_updated_ts",
					null as user_id
						
					from ${cnf_db}.DW_C_LOYALTY.FOODSTORM_ORDER fo
					inner join ${view_db}.DW_EDW_VIEWS.LU_STORE_FINANCE_OM lsfu---${view_db}.DW_VIEWS.D1_RETAIL_STORE has banner_id missing
					on fo.store_id = lsfu.store_id
					
					left join ${cnf_db}.DW_C_LOYALTY.FOODSTORM_ORDER_ITEM foi
					on foi.order_id = fo.order_id
					
					left join 
					(select distinct upcid,brandnm,tradingstamp,rogcd from ${view_db}.DW_VIEWS.ECATALOG_ITEM where 
					upper(dw_logical_delete_ind) = 'FALSE') ei
					on trim(upper(foi.upc_id)) = trim(upper(ei.upcid)) and trim(upper(ei.rogcd)) = trim(upper(lsfu.rog_cd))
					left join ${view_db}.DW_EDW_VIEWS.LU_DELIVERY_PARTNER ldp on
                    fo.partner_id = ldp.partner_id
					
					---inner join ${view_db}.DW_EDW_VIEWS.partner_order po
					---on pos.order_id = po.order_id
					---and po.loyalty_phone_nbr not in ('0','0.','0.0')
					
					where fo.order_id not in 
					(
						select fo.order_id as order_id 
						from ${cnf_db}.DW_C_LOYALTY.FOODSTORM_ORDER fo 
						inner join customer 
						on fo.Customer_Loyalty_Phone_Nbr = customer.PRIMARY_PHONE_NBR 
						where fo.Customer_Loyalty_Phone_Nbr is not null
					)
					AND fo.dw_create_ts > TO_TIMESTAMP('${last_watermark}')
					AND fo.dw_create_ts <= TO_TIMESTAMP('${current_watermark}')
					QUALIFY (ROW_NUMBER() OVER (PARTITION BY fo.order_id,fo.store_id,foi.upc_id ORDER BY fo.Delivery_Dt desc)=1)`});

//return_value1 will return number of rows inserted		
	var res1 = stmt6.execute();
	res1.next();
var return_value1 = res1.getColumnValue(1);



// ************** Load for kafka outqueue table BEGIN *****************


var stmt3 = snowflake.createStatement({
			sqlText: `insert into ${out_db}.dw_loyalty.KAFKAOUTQUEUE
			(MSG_SEQ,TOPIC,KEY,PAYLOAD,STATUS,CREATETIME,DW_SOURCE_CREATE_NM)
			SELECT ${out_db}.DW_APPL.KAFKAOUTQUEUE_SEQ.nextval as MSG_SEQ,*
            FROM
           (SELECT
            'EDDW_C02_PARTNERSHIP_REWARDS_PROD' as topic,
             concat(ol.TRANSACTION_NUMBER,'-',ol.STORE_NUMBER) as KEY,
             to_varchar(object_construct(
             'storeNumber',coalesce(to_char(ol.STORE_NUMBER),' '),
             'terminalNumber',coalesce(to_char(TERMINAL_NUMBER),' '),
             'transactionNumber',coalesce(to_char(ol.TRANSACTION_NUMBER),' '),
             'transactionTimestamp',TO_CHAR(convert_timezone('America/Denver','UTC',TRANSACTION_TIMESTAMP),'YYYY-MM-DD HH:MI:SS.FF3'),
             'memberId',coalesce(to_char(MEMBER_ID),' '),
             'transactionSource',TRANSACTION_SOURCE,
             'transactionSourceId',TRANSACTION_SOURCE_ID,
             'banner_nm',BANNER_NM,
             'banner_Id',BANNER_ID,
             'items',x.item)) as payload,
             0,
INSERT_TS,
'EDM_SHARE'
from ${cnf_db}.DW_C_LOYALTY.OCRP_LOYALTY ol
join 
(select TRANSACTION_NUMBER,STORE_NUMBER,ARRAY_AGG( distinct OBJECT_CONSTRUCT(
'itemCode',ol.ITEM_CODE,
'entryId',ol.ENTRY_ID,
'unitPrice',ol.UNIT_PRICE,
'extendedPrice',ol.EXTENDED_PRICE,
'discountAllowed',ol.DISCOUNT_ALLOWED,
'pointsApplyItem',ol.POINTS_APPLY_ITEM,
'tradingStamp',ol.TRADING_STAMP,
'alcoholFlag',ol.ALCOHOL_FLAG   
) ) item
from ${cnf_db}.DW_C_LOYALTY.OCRP_LOYALTY ol
where 
insert_ts > TO_TIMESTAMP('${last_watermark}')
AND insert_ts <= TO_TIMESTAMP('${current_watermark}')
AND match_flag_ind = 'Y'
group by 1,2
)
x
on ol.transaction_number=x.transaction_number
and ol.store_number = x.store_number
where
insert_ts > TO_TIMESTAMP('${last_watermark}')
AND insert_ts <= TO_TIMESTAMP('${current_watermark}')
AND match_flag_ind = 'Y'
group by 1,2,3,5
)y`});				
		
	var res3 = stmt3.execute();
	res3.next();
return_value3 = res3.getColumnValue(1);


// *******************END OF LOAD FOR KAFKAOUTQUEUE TABLE******************************


var stmt4 = snowflake.createStatement({
     sqlText: `call ${cnf_db}.DW_APPL.SP_Feed_Extract_Job_Success(
                '${job_run_id}',
                '${return_value1}',
                null,
                null)`
                
     });
 //return_value2 will give job run auto id
var res4 = stmt4.execute();
res4.next();
return_value4 = res4.getColumnValue(1);


var stmt5 = snowflake.createStatement({
     sqlText: `call ${cnf_db}.DW_APPL.SP_Feed_Extract_Set_Watermark_TS(
                ${job_run_id},
				'${last_watermark}',
				'${current_watermark}'
				)`
                
     });
 //return_value3 will give job run auto id 
var res5 = stmt5.execute();
res5.next();
return_value5 = res5.getColumnValue(1); 

return "stored proc is completed"; 
$$;

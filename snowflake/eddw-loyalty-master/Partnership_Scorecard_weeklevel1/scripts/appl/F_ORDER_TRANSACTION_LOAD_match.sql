--liquibase formatted sql
--changeset SYSTEM:F_ORDER_TRANSACTION_LOAD_match runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME_C>>;
use schema DW_APPL;



CREATE OR REPLACE PROCEDURE "SP_F_ORDER_TRANSACTION_LOAD"()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS $$

var db_confirmed = '<<EDM_DB_NAME_C>>';
var db_analytics = '<<EDM_DB_NAME>>';
var db_refined = '<<EDM_DB_NAME_R>>';
var db_views = '<<EDM_VIEW_NAME>>'

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

var stmt4_TEMP = snowflake.createStatement({
			sqlText: `CREATE OR REPLACE TEMP TABLE ${db_analytics}.DW_STAGE.F_ORDER_TRANSACTION_WRK_IMAHA02 AS
	WITH src_rec AS (
		select
			DISTINCT current_card_nbr,
			RETAIL_CUSTOMER_D1_SK
		from
			(
				SELECT
					DISTINCT clp.Loyalty_Program_Card_Nbr as current_card_nbr,
					d1_rc.RETAIL_CUSTOMER_D1_SK,
					row_number() over (
						PARTITION BY current_card_nbr
						ORDER BY
							clp.dw_create_ts desc
					) as rn
				FROM
					${db_views}.DW_VIEWS.D1_RETAIL_CUSTOMER d1_rc
					LEFT JOIN ${db_views}.DW_VIEWS.CUSTOMER_LOYALTY_PROGRAM clp on d1_rc.Retail_Customer_UUID = clp.Retail_Customer_UUID
					left join ${db_views}.DW_VIEWS.CUSTOMER_ACCOUNT_STATUS cas ON d1_rc.Retail_Customer_UUID = cas.Retail_Customer_UUID
					and cas.STATUS_TYPE_CD = 'ONLINE_ENROLLMENT'
					and cas.dw_current_Version_ind = TRUE
					AND cas.dw_logical_delete_ind = FALSE
				WHERE
					d1_rc.DW_LOGICAL_DELETE_IND = FALSE
			)
		WHERE
			RN = 1
	),
	final_rec AS (
		SELECT
			gwt.txn_dte as transaction_dt,
			s.RETAIL_CUSTOMER_D1_SK,
			lsfu.division_D1_Sk,
			lsfu.banner_d1_sk,
			lsfu.Retail_store_D1_sk,
			cal.fiscal_day_id,
			gwt.NET_AMT as TRANSACTION_AMT,
			'0.00' as total_tax_amt,
			SUM(gwt.total_item_qty) as item_qty,
			case when dug.delivery_type_dsc like '%DUG%' then TRUE else FALSE end AS dug_order_ind,
			case when dug.delivery_type_dsc like '%DUG%' then FALSE else TRUE end AS delivery_order_Ind,
			gwt.txn_id as transaction_id,
			thc.register_nbr as register_nbr
		FROM
			${db_confirmed}.DW_APPL.GW_REG99_TXNS_2020_STREAM_LOYALTY_2 gwt
			LEFT JOIN src_rec s ON s.current_card_nbr = gwt.card_nbr :: varchar
			LEFT JOIN ${db_views}.dw_edw_views.txn_hdr thc ON gwt.TXN_ID = thc.TXN_ID
			LEFT JOIN ${db_views}.DW_EDW_VIEWS.TXN_FACTS tf ON tf.txn_id = gwt.txn_id
			left join (
				select
					distinct txn_id,
					txn_dt,
					delivery_type_dsc
				from
					${db_views}.dw_edw_views.gw_online_register_txn
				where
					delivery_type_dsc like '%DUG%'
					AND TXN_ID IS NOT NULL
			) dug on tf.txn_id = dug.txn_id
			left JOIN ${db_refined}.DW_R_STAGE.lsfu lsfu ON gwt.store_id = try_to_numeric(lsfu.store_id)
			left JOIN ${db_views}.DW_VIEWS.D0_FISCAL_DAY cal ON gwt.txn_dte = cal.calendar_dt
		
		GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13
	)
select DISTINCT
	src.TRANSACTION_DT,
	src.RETAIL_CUSTOMER_D1_SK,
	src.Retail_store_D1_sk,
	src.banner_d1_sk,
	src.TRANSACTION_AMT,
	src.division_d1_sk,
	src.Fiscal_Day_Id,
	src.total_tax_amt as TRANSACTION_TAX_AMT,
	src.item_qty,
	src.dug_order_ind,
	src.delivery_order_ind,
	src.transaction_id,
	src.dw_create_ts,
	src.dw_last_update_ts,
	CASE WHEN TGT.transaction_id IS NULL THEN 'I' ELSE 'NA' END AS ACTION_CODE
from
	(
		SELECT
			nvl(transaction_dt, current_date) as transaction_dt,
			nvl(RETAIL_CUSTOMER_D1_SK, '-1') as RETAIL_CUSTOMER_D1_SK,
			nvl(Retail_store_D1_sk, '0') as Retail_store_D1_sk,
			nvl(banner_d1_sk, '0') as banner_d1_sk,
			nvl(transaction_amt, '0') as transaction_amt,
			nvl(division_d1_sk, '0') as division_d1_sk,
			nvl(fiscal_day_id, '0') as fiscal_day_id,
			nvl(total_tax_amt, '0') as total_tax_amt,
			nvl(item_qty, '0') as item_qty,
			nvl(dug_order_ind, '0') as dug_order_ind,
			nvl(delivery_order_Ind, '0') as delivery_order_Ind,
			nvl(transaction_id, '0') as transaction_id,
			current_timestamp() as dw_create_ts,
			current_timestamp() as dw_last_update_ts
		FROM
			final_rec
		QUALIFY row_number() over ( partition by transaction_id order by transaction_dt desc ) =1
	) src
	LEFT JOIN (
		SELECT
			transaction_id
		FROM
			${db_analytics}.DW_RETAIL_EXP.F_ORDER_TRANSACTION
	) AS tgt ON src.transaction_id = tgt.transaction_id
	WHERE
		tgt.transaction_id Is NULL`});
   var res4_TEMP = stmt4_TEMP.execute();
	      
      var stmt4 = snowflake.createStatement({sqlText: `INSERT INTO ${db_analytics}.DW_RETAIL_EXP.F_ORDER_TRANSACTION (
		TRANSACTION_DT,
		RETAIL_CUSTOMER_D1_SK,
		Retail_store_D1_sk,
		banner_d1_sk,
		TRANSACTION_AMT,
		division_d1_sk,
		Fiscal_Day_Id,
		Transaction_Tax_Amt,
		item_qty,
		dug_order_ind,
		delivery_order_ind,
		transaction_id,
		dw_create_ts,
		dw_last_update_ts
	)
	SELECT TRANSACTION_DT,
		RETAIL_CUSTOMER_D1_SK,
		Retail_store_D1_sk,
		banner_d1_sk,
		TRANSACTION_AMT,
		division_d1_sk,
		Fiscal_Day_Id,
		Transaction_Tax_Amt,
		item_qty,
		dug_order_ind,
		delivery_order_ind,
		transaction_id,
		dw_create_ts,
		dw_last_update_ts
	FROM ${db_analytics}.DW_STAGE.F_ORDER_TRANSACTION_WRK_IMAHA02
	WHERE ACTION_CODE ='I' `});
   var res4 = stmt4.execute();
	res4.next();
	return_value1 = res4.getColumnValue(1);
	var return_value = return_value1
// *******************END OF LOAD FOR FACT_ORDER_TRANSACTION TABLE******************************
return "F_Order_Transaction TABLE LOADED"
$$;

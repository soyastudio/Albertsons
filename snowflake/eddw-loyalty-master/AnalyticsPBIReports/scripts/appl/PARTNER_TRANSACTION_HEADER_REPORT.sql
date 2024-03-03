CREATE OR REPLACE PROCEDURE PARTNER_TRANSACTION_HEADER_REPORT()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS 
$$
	try {
	var result = "";
	var err_code = "";
	var Resultset = snowflake.execute( {sqlText:` USE database EDM_CONFIRMED_PRD;`} );
	var Resultset = snowflake.execute( {sqlText:` USE SCHEMA DW_APPL;`} );
	
	var Resultset = snowflake.execute( {sqlText:` drop table if exists DW_C_STAGE.f_partner_transaction_header_TMP;`} );
	var Resultset = snowflake.execute( {sqlText:` drop table if exists  DW_C_STAGE.f_partner_transaction_header_TMP1;`} );
	var Resultset = snowflake.execute( {sqlText:` drop table  if exists DW_C_STAGE.f_partner_transaction_header_TMP2;`} );
	var Resultset = snowflake.execute( {sqlText:` drop table  if exists DW_C_STAGE.f_partner_transaction_header_TMP3;`} );


	var Resultset = snowflake.execute( {sqlText:` Create TRANSIENT table if not exists  DW_C_STAGE.f_partner_transaction_header_TMP as 
	select partner_order_id,store_order_id,partner_order_integration_id,partner_nm,partner_dt,store_dt,store_id,partner_net_amt,transaction_total_amt, transaction_net_amt, transaction_tax_amt,partner_tax_amt,partner_id,instore_net_amt,instore_taxable_amt, instore_tax_paid,amount_Comparison,Tax_Comparison,TOTAL_TAX_PLAN_A_AMT , TOTAL_TAX_PLAN_B_AMT , TOTAL_TAX_PLAN_C_AMT , TOTAL_TAX_PLAN_D_AMT,TOTAL_TAX_PLAN_E_AMT,TOTAL_TAX_PLAN_F_AMT,TOTAL_TAX_PLAN_G_AMT,TOTAL_TAX_PLAN_H_AMT,TOTAL_TAXABLE_PLAN_A_AMT , TOTAL_TAXABLE_PLAN_B_AMT , TOTAL_TAXABLE_PLAN_C_AMT , TOTAL_TAXABLE_PLAN_D_AMT , TOTAL_TAXABLE_PLAN_E_AMT , TOTAL_TAXABLE_PLAN_F_AMT ,TOTAL_TAXABLE_PLAN_G_AMT , TOTAL_TAXABLE_PLAN_H_AMT,qr_status_dsc,ALCOHOL_ORDER_IND,Snap_Order_Ind,Dug_Order_Ind,Deli_Order_Ind,FFC_Order_Ind,Own_Brad_Item_Order_Ind
	From 
	(SELECT distinct partner_order_id_d as partner_order_id,partner_order_integration_id, txn_id as store_order_id, partner_nm as partner_nm, partner_transaction_dt as partner_dt, left(ac_og_tm,10) as store_dt, store_id, net_amt as partner_net_amt, transaction_total_amt, transaction_net_amt, transaction_tax_amt, item_tax_amt as partner_tax_amt,partner_id, instore_net_amt,instore_taxable_amt, instore_tax_paid,amount_Comparison,Tax_Comparison,TOTAL_TAX_PLAN_A_AMT , TOTAL_TAX_PLAN_B_AMT , TOTAL_TAX_PLAN_C_AMT , TOTAL_TAX_PLAN_D_AMT,TOTAL_TAX_PLAN_E_AMT,TOTAL_TAX_PLAN_F_AMT,TOTAL_TAX_PLAN_G_AMT,TOTAL_TAX_PLAN_H_AMT,TOTAL_TAXABLE_PLAN_A_AMT , TOTAL_TAXABLE_PLAN_B_AMT , TOTAL_TAXABLE_PLAN_C_AMT , TOTAL_TAXABLE_PLAN_D_AMT , TOTAL_TAXABLE_PLAN_E_AMT , TOTAL_TAXABLE_PLAN_F_AMT ,TOTAL_TAXABLE_PLAN_G_AMT , TOTAL_TAXABLE_PLAN_H_AMT,qr_status_dsc,ALCOHOL_ORDER_IND,Snap_Order_Ind,Dug_Order_Ind,Deli_Order_Ind,FFC_Order_Ind,Own_Brad_Item_Order_Ind
	FROM (SELECT *
	FROM EDM_VIEWS_PRD.DW_VIEWS.PARTNER_GROCERY_ORDER_DETAIL
	--WHERE store_transaction_ts::DATE between '2023-04-30' and '2023-05-20') partner_detail
	--WHERE store_transaction_ts > dateadd(day,-7,current_timestamp ()) ) partner_detail 
	WHERE dw_create_ts > dateadd(day,-7,CURRENT_TIMESTAMP())) partner_detail
	INNER JOIN
	(SELECT *, case when tender_amt is null then 'No match' when partner_net_amt = tender_amt then 'TRUE' else 'FALSE' end as amount_Comparison,
	CASE WHEN (Tax_AMT_Paid = instore_tax_paid) then 'TRUE' else 'FALSE' end as Tax_Comparison

	FROM (
	SELECT distinct partner_tt.order_id as partner_order_id_d, CASE WHEN qrcode.status_dsc ='APPROVED' THEN '3PM QR ACH USED'
    WHEN qrcode.status_dsc ='RETRIEVED' THEN '3PM QR USED'
    WHEN qrcode.status_dsc ='EXPIRED' THEN '3PM QR NOT USED'
    end as qr_status_dsc,g.ALCOHOL_ORDER_IND as ALCOHOL_ORDER_IND
         ,g.SNAP_ORDER_IND as Snap_Order_Ind
         ,g.DUG_ORDER_IND as Dug_Order_Ind
         ,g.DELI_ORDER_IND as Deli_Order_Ind
         ,g.FFC_ORDER_IND as FFC_Order_Ind
         ,g.OWN_BRAND_ITEM_ORDER_IND as Own_Brad_Item_Order_Ind,partner_hdr.alcoholic_ind as alchol_ind,partner_hdr.snap_ind as snapind,partner_tt.PARTNER_GROCERY_ORDER_CUSTOMER_INTEGRATION_ID as partner_order_integration_id, REPLACE(LTRIM(REPLACE(partner_tt.approval_cd, '0', ' ')), ' ', '0') as partner_approval_cd, 
	partner_hdr.transaction_dt as partner_transaction_dt, partner_hdr.store_transaction_ts, partner_hdr.net_amt as partner_net_amt, partner_hdr.store_id as partner_store_id, partner_hdr.partner_nm, REPLACE(masked_credit_card_nbr, 'X', 9) as new_credit_card,partner_hdr.partner_id
	FROM EDM_VIEWS_PRD.DW_VIEWS.PARTNER_GROCERY_ORDER_TENDER partner_tt
	LEFT JOIN EDM_VIEWS_PRD.DW_VIEWS.PARTNER_GROCERY_ORDER_HEADER partner_hdr
	ON partner_tt.order_id = partner_hdr.order_id 
	LEFT JOIN "EDM_VIEWS_PRD"."DW_PAYMENTS_VIEWS"."ORDER_QUICK_REFERENCE_3PL" qrcode
	ON qrcode.order_id = partner_tt.order_id
     left join  EDM_VIEWS_PRD.DW_VIEWS.F_PARTNER_ORDER_TRANSACTION g
    ON partner_tt.order_id = g.order_id
	and qrcode.order_id = g.order_id
	AND partner_tt.PARTNER_GROCERY_ORDER_CUSTOMER_INTEGRATION_ID = partner_hdr.PARTNER_GROCERY_ORDER_CUSTOMER_INTEGRATION_ID
	) ddt 
	LEFT JOIN 
	(SELECT tt.tender_nbr, tt.txn_id as tt_id, tt.tender_amt, hdr.txn_tm as ac_og_tm, dateadd(hour, st.hours_from_host_tm,hdr.txn_tm) as to_host_tm, 
	 dateadd(hour, 6, dateadd(hour, -st.hours_from_host_tm,hdr.txn_tm)) as ac_tm, 
	 date(dateadd(hour, 6, dateadd(hour, -st.hours_from_host_tm,hdr.txn_tm))) as ac_date, 
	  date(dateadd(hour, 7, dateadd(hour, -st.hours_from_host_tm,hdr.txn_tm))) as ac_date_daylightadd, 
	 REPLACE(LTRIM(REPLACE(TENDER_APPR_CD, '0', ' ')), ' ', '0') as ac_approval_cd , hdr.*, st.store_time_zone_cd, st.hours_from_host_tm, store_state_id, tt.tender_amt as transaction_total_amt, hdr.net_amt as transaction_net_amt, hdr.tax_amt as transaction_tax_amt,hdr.net_amt as instore_net_amt,hdr.tax_amt as Tax_AMT_Paid,
	TOTAL_TAXABLE_PLAN_A_AMT + TOTAL_TAXABLE_PLAN_B_AMT + TOTAL_TAXABLE_PLAN_C_AMT + TOTAL_TAXABLE_PLAN_D_AMT + TOTAL_TAXABLE_PLAN_E_AMT + TOTAL_TAXABLE_PLAN_F_AMT + TOTAL_TAXABLE_PLAN_G_AMT + TOTAL_TAXABLE_PLAN_H_AMT  as instore_taxable_amt,
	TOTAL_TAX_PLAN_A_AMT + TOTAL_TAX_PLAN_B_AMT + TOTAL_TAX_PLAN_C_AMT + TOTAL_TAX_PLAN_D_AMT + TOTAL_TAX_PLAN_E_AMT + TOTAL_TAX_PLAN_F_AMT + TOTAL_TAX_PLAN_G_AMT + TOTAL_TAX_PLAN_H_AMT as instore_tax_paid
	FROM "EDM_VIEWS_PRD"."DW_EDW_VIEWS"."TXN_TENDER" tt
	LEFT JOIN "EDM_VIEWS_PRD"."DW_EDW_VIEWS"."TXN_HDR" hdr
	ON tt.txn_id = hdr.txn_id and tt.txn_dte=hdr.txn_dte
	LEFT JOIN "EDM_VIEWS_PRD"."DW_EDW_VIEWS"."LU_STORE" st
	ON st.store_id = hdr.store_id
	WHERE tt.txn_dte > dateadd(day,-7,current_timestamp())
	AND hdr.txn_dte > dateadd(day,-7,current_timestamp())
	) ac_combined
	ON partner_approval_cd = ac_approval_cd
	AND try_to_number(partner_store_id) = try_to_number(ac_combined.store_id)
	AND partner_transaction_dt between ac_date and ac_date_daylightadd
	WHERE partner_transaction_dt > dateadd(day,-7,current_timestamp())
	AND partner_nm ='Uber'
	--AND store_id in (3368,3345,3376,1241,3082,3514,3234,230,3632,3407)
	)matched
	ON matched.partner_order_id_d = partner_detail.order_id 
	AND matched.partner_ORDER_INTEGRATION_ID = partner_detail.PARTNER_GROCERY_ORDER_CUSTOMER_INTEGRATION_ID
	AND matched.store_transaction_ts = partner_detail.store_transaction_ts
	) ;`} );

	var Resultset = snowflake.execute( {sqlText:` Create TRANSIENT table if not exists  DW_C_STAGE.f_partner_transaction_header_TMP1 as 
	select partner_order_id,store_order_id,partner_order_integration_id,partner_nm,partner_dt,store_dt,store_id,partner_net_amt,transaction_total_amt, transaction_net_amt, transaction_tax_amt,partner_tax_amt,partner_id, instore_net_amt,instore_taxable_amt,instore_tax_paid,Amount_Comparison,Tax_Comparison,TOTAL_TAX_PLAN_A_AMT , TOTAL_TAX_PLAN_B_AMT , TOTAL_TAX_PLAN_C_AMT , TOTAL_TAX_PLAN_D_AMT,TOTAL_TAX_PLAN_E_AMT,TOTAL_TAX_PLAN_F_AMT,TOTAL_TAX_PLAN_G_AMT,TOTAL_TAX_PLAN_H_AMT,TOTAL_TAXABLE_PLAN_A_AMT , TOTAL_TAXABLE_PLAN_B_AMT , TOTAL_TAXABLE_PLAN_C_AMT , TOTAL_TAXABLE_PLAN_D_AMT , TOTAL_TAXABLE_PLAN_E_AMT , TOTAL_TAXABLE_PLAN_F_AMT ,TOTAL_TAXABLE_PLAN_G_AMT , TOTAL_TAXABLE_PLAN_H_AMT,qr_status_dsc,ALCOHOL_ORDER_IND,Snap_Order_Ind,Dug_Order_Ind,Deli_Order_Ind,FFC_Order_Ind,Own_Brad_Item_Order_Ind
	From 
	(
	SELECT distinct partner_order_id_d as partner_order_id,partner_order_integration_id, txn_id as store_order_id, partner_nm as partner_nm, partner_transaction_dt as partner_dt, left(ac_og_tm,10) as store_dt, store_id, net_amt as partner_net_amt, transaction_total_amt, transaction_net_amt, transaction_tax_amt,item_tax_amt as partner_tax_amt, partner_id,instore_net_amt,instore_taxable_amt,instore_tax_paid,Amount_Comparison,Tax_Comparison,TOTAL_TAX_PLAN_A_AMT , TOTAL_TAX_PLAN_B_AMT , TOTAL_TAX_PLAN_C_AMT , TOTAL_TAX_PLAN_D_AMT,TOTAL_TAX_PLAN_E_AMT,TOTAL_TAX_PLAN_F_AMT,TOTAL_TAX_PLAN_G_AMT,TOTAL_TAX_PLAN_H_AMT,TOTAL_TAXABLE_PLAN_A_AMT , TOTAL_TAXABLE_PLAN_B_AMT , TOTAL_TAXABLE_PLAN_C_AMT , TOTAL_TAXABLE_PLAN_D_AMT , TOTAL_TAXABLE_PLAN_E_AMT , TOTAL_TAXABLE_PLAN_F_AMT ,TOTAL_TAXABLE_PLAN_G_AMT , TOTAL_TAXABLE_PLAN_H_AMT,qr_status_dsc,ALCOHOL_ORDER_IND,Snap_Order_Ind,Dug_Order_Ind,Deli_Order_Ind,FFC_Order_Ind,Own_Brad_Item_Order_Ind
	FROM (SELECT *
	FROM EDM_VIEWS_PRD.DW_VIEWS.PARTNER_GROCERY_ORDER_DETAIL
	--WHERE store_transaction_ts between current_date - 12 and current_date - 6) partner_detail

	where dw_create_ts > dateadd(day,-7,CURRENT_TIMESTAMP())) partner_detail
	INNER JOIN 
	  (
	-- partner_table and aci_transaction table join
	SELECT *, case when tender_amt is null then 'No match' when partner_net_amt = tender_amt then 'TRUE' else 'FALSE' end as Amount_Comparison, -- partner_net_amt, tender_amt, gross_amt, net_amt, mkdn_amt
	CASE WHEN (Tax_AMT_Paid = instore_tax_paid) then 'TRUE' else 'FALSE' end as Tax_Comparison
	FROM (
	SELECT distinct partner_tt.order_id as partner_order_id_d,CASE WHEN qrcode.status_dsc ='APPROVED' THEN '3PM QR ACH USED'
    WHEN qrcode.status_dsc ='RETRIEVED' THEN '3PM QR USED'
    WHEN qrcode.status_dsc ='EXPIRED' THEN '3PM QR NOT USED'
    end as qr_status_dsc,g.ALCOHOL_ORDER_IND as ALCOHOL_ORDER_IND
         ,g.SNAP_ORDER_IND as Snap_Order_Ind
         ,g.DUG_ORDER_IND as Dug_Order_Ind
         ,g.DELI_ORDER_IND as Deli_Order_Ind
         ,g.FFC_ORDER_IND as FFC_Order_Ind
         ,g.OWN_BRAND_ITEM_ORDER_IND as Own_Brad_Item_Order_Ind, partner_tt.PARTNER_GROCERY_ORDER_CUSTOMER_INTEGRATION_ID as partner_order_integration_id, REPLACE(LTRIM(REPLACE(partner_tt.approval_cd, '0', ' ')), ' ', '0') as partner_approval_cd, 
	partner_hdr.transaction_dt as partner_transaction_dt, partner_hdr.store_transaction_ts, partner_hdr.net_amt as partner_net_amt, partner_hdr.store_id as partner_store_id, partner_hdr.partner_nm, REPLACE(masked_credit_card_nbr, 'X', 9) as new_credit_card,partner_hdr.partner_id
	FROM EDM_VIEWS_PRD.DW_VIEWS.PARTNER_GROCERY_ORDER_TENDER partner_tt
	LEFT JOIN EDM_VIEWS_PRD.DW_VIEWS.PARTNER_GROCERY_ORDER_HEADER partner_hdr
	ON partner_tt.order_id = partner_hdr.order_id 
	LEFT JOIN "EDM_VIEWS_PRD"."DW_PAYMENTS_VIEWS"."ORDER_QUICK_REFERENCE_3PL" qrcode
	ON qrcode.order_id = partner_tt.order_id
     left join  EDM_VIEWS_PRD.DW_VIEWS.F_PARTNER_ORDER_TRANSACTION g
    ON partner_tt.order_id = g.order_id
	AND qrcode.order_id = g.order_id
	AND partner_tt.PARTNER_GROCERY_ORDER_CUSTOMER_INTEGRATION_ID = partner_hdr.PARTNER_GROCERY_ORDER_CUSTOMER_INTEGRATION_ID
	WHERE partner_hdr.partner_nm = 'Doordash' ) ddt 
	LEFT JOIN 
	(SELECT tt.tender_nbr, tt.txn_id as tt_id, tt.tender_amt, hdr.txn_tm as ac_og_tm, dateadd(hour, st.hours_from_host_tm,hdr.txn_tm) as to_host_tm, 
	 dateadd(hour, 6, dateadd(hour, -st.hours_from_host_tm,hdr.txn_tm)) as ac_tm, 
	 date(dateadd(hour, 6, dateadd(hour, -st.hours_from_host_tm,hdr.txn_tm))) as ac_date, 
	  date(dateadd(hour, 7, dateadd(hour, -st.hours_from_host_tm,hdr.txn_tm))) as ac_date_daylightadd, 
	 REPLACE(LTRIM(REPLACE(TENDER_APPR_CD, '0', ' ')), ' ', '0') as ac_approval_cd , hdr.*, st.store_time_zone_cd, st.hours_from_host_tm, 
	 store_state_id,tt.tender_amt as transaction_total_amt, hdr.net_amt as transaction_net_amt, hdr.tax_amt as transaction_tax_amt,
	 hdr.net_amt as instore_net_amt,hdr.tax_amt as Tax_AMT_Paid,
    TOTAL_TAXABLE_PLAN_A_AMT + TOTAL_TAXABLE_PLAN_B_AMT + TOTAL_TAXABLE_PLAN_C_AMT + TOTAL_TAXABLE_PLAN_D_AMT + TOTAL_TAXABLE_PLAN_E_AMT + TOTAL_TAXABLE_PLAN_F_AMT + TOTAL_TAXABLE_PLAN_G_AMT + TOTAL_TAXABLE_PLAN_H_AMT  as instore_taxable_amt,
	TOTAL_TAX_PLAN_A_AMT + TOTAL_TAX_PLAN_B_AMT + TOTAL_TAX_PLAN_C_AMT + TOTAL_TAX_PLAN_D_AMT + TOTAL_TAX_PLAN_E_AMT + TOTAL_TAX_PLAN_F_AMT + TOTAL_TAX_PLAN_G_AMT + TOTAL_TAX_PLAN_H_AMT as instore_tax_paid
	FROM "EDM_VIEWS_PRD"."DW_EDW_VIEWS"."TXN_TENDER" tt
	LEFT JOIN "EDM_VIEWS_PRD"."DW_EDW_VIEWS"."TXN_HDR" hdr
	ON tt.txn_id = hdr.txn_id and tt.txn_dte=hdr.txn_dte
	LEFT JOIN "EDM_VIEWS_PRD"."DW_EDW_VIEWS"."LU_STORE" st
	ON st.store_id = hdr.store_id
	WHERE tt.txn_dte > dateadd(day,-7,CURRENT_TIMESTAMP())
	AND hdr.txn_dte > dateadd(day,-7,CURRENT_TIMESTAMP())
	) ac_combined
	ON partner_approval_cd = ac_approval_cd
	AND try_to_number(partner_store_id) = try_to_number(ac_combined.store_id)
	AND partner_transaction_dt between ac_date and ac_date_daylightadd
	AND try_to_number(new_credit_card) = try_to_number(ac_combined.tender_nbr)
	WHERE partner_transaction_dt > dateadd(day,-7,CURRENT_TIMESTAMP())
	--and matchstatus <> 'No match'
	  )matched
	ON matched.partner_order_id_d = partner_detail.order_id 
	AND matched.partner_ORDER_INTEGRATION_ID = partner_detail.PARTNER_GROCERY_ORDER_CUSTOMER_INTEGRATION_ID
	AND left(matched.store_transaction_ts,7) = left(partner_detail.store_transaction_ts,7)
	); `} );

	var Resultset = snowflake.execute( {sqlText:` Create TRANSIENT table if not exists  DW_C_STAGE.f_partner_transaction_header_TMP3 as 
	select TXN_ID 
		,ORDER_ID 
		,RECEIPT_NBR_V1 
		,RECEIPT_NBR_V2 
		,CC_NBR_LASTFOUR 
		,TRANSACTION_TOTAL_AMT 
		,TRANSACTION_NET_AMT 
		,TRANSACTION_TAX_AMT 
		,POS_TM  
		,ACI_APPROVAL_CD 
		,ITEM_QTY  
		,ITEM_ID  
		,UPC_ID  
		,UPC_DSC  
		,ITEM_NET_AMT 
		,ACI_ALC_IND 
	From 
	(
	select distinct
		TXN_ID 
		,ORDER_ID 
		,RECEIPT_NBR_V1 
		,RECEIPT_NBR_V2 
		,CC_NBR_LASTFOUR 
		,TRANSACTION_TOTAL_AMT 
		,TRANSACTION_NET_AMT 
		,TRANSACTION_TAX_AMT 
		,POS_TM  
		,ACI_APPROVAL_CD 
		,ITEM_QTY  
		,ITEM_ID  
		,UPC_ID  
		,UPC_DSC  
		,ITEM_NET_AMT 
		,ACI_ALC_IND 
	 from 
	(

	--CTE key_tble with matched records
	with key_tble as (
	SELECT distinct partner_order_id_d, txn_id
	FROM (SELECT *
	FROM EDM_VIEWS_PRD.DW_VIEWS.PARTNER_GROCERY_ORDER_DETAIL
	--WHERE store_transaction_ts between current_date - 12 and current_date - 6) partner_detail

	where dw_create_ts > dateadd(day,-7,CURRENT_TIMESTAMP())) partner_detail
	INNER JOIN 
	  (
	-- partner_table and aci_transaction table join
	SELECT *, case when tender_amt is null then 'No match' when partner_net_amt = tender_amt then 'TRUE' else 'FALSE' end as matchstatus -- partner_net_amt, tender_amt, gross_amt, net_amt, mkdn_amt
	FROM (
	SELECT distinct partner_tt.order_id as partner_order_id_d, partner_tt.PARTNER_GROCERY_ORDER_CUSTOMER_INTEGRATION_ID as partner_order_integration_id, REPLACE(LTRIM(REPLACE(partner_tt.approval_cd, '0', ' ')), ' ', '0') as partner_approval_cd, 
	transaction_dt as partner_transaction_dt, store_transaction_ts, net_amt as partner_net_amt, store_id as partner_store_id, partner_nm, REPLACE(masked_credit_card_nbr, 'X', 9) as new_credit_card
	FROM EDM_VIEWS_PRD.DW_VIEWS.PARTNER_GROCERY_ORDER_TENDER partner_tt
	LEFT JOIN EDM_VIEWS_PRD.DW_VIEWS.PARTNER_GROCERY_ORDER_HEADER partner_hdr
	ON partner_tt.order_id = partner_hdr.order_id 
	AND partner_tt.PARTNER_GROCERY_ORDER_CUSTOMER_INTEGRATION_ID = partner_hdr.PARTNER_GROCERY_ORDER_CUSTOMER_INTEGRATION_ID
	WHERE partner_nm = 'Doordash' ) ddt 
	LEFT JOIN 
	(SELECT tt.tender_nbr, tt.txn_id as tt_id, tt.tender_amt, hdr.txn_tm as ac_og_tm, dateadd(hour, st.hours_from_host_tm,hdr.txn_tm) as to_host_tm, 
	 dateadd(hour, 6, dateadd(hour, -st.hours_from_host_tm,hdr.txn_tm)) as ac_tm, 
	 date(dateadd(hour, 6, dateadd(hour, -st.hours_from_host_tm,hdr.txn_tm))) as ac_date, 
	  date(dateadd(hour, 7, dateadd(hour, -st.hours_from_host_tm,hdr.txn_tm))) as ac_date_daylightadd, 
	 REPLACE(LTRIM(REPLACE(TENDER_APPR_CD, '0', ' ')), ' ', '0') as ac_approval_cd , hdr.*, st.store_time_zone_cd, st.hours_from_host_tm, store_state_id
	FROM "EDM_VIEWS_PRD"."DW_EDW_VIEWS"."TXN_TENDER" tt
	LEFT JOIN "EDM_VIEWS_PRD"."DW_EDW_VIEWS"."TXN_HDR" hdr
	ON tt.txn_id = hdr.txn_id and tt.txn_dte=hdr.txn_dte
	LEFT JOIN "EDM_VIEWS_PRD"."DW_EDW_VIEWS"."LU_STORE" st
	ON st.store_id = hdr.store_id
	WHERE tt.txn_dte > dateadd(day,-7,CURRENT_TIMESTAMP())
	AND hdr.txn_dte > dateadd(day,-7,CURRENT_TIMESTAMP())
	) ac_combined
	ON partner_approval_cd = ac_approval_cd
	AND try_to_number(partner_store_id) = try_to_number(ac_combined.store_id)
	AND partner_transaction_dt between ac_date and ac_date_daylightadd
	AND try_to_number(new_credit_card) = try_to_number(ac_combined.tender_nbr)
	WHERE partner_transaction_dt > dateadd(day,-7,CURRENT_TIMESTAMP())
	--and matchstatus <> 'No match'
	  )matched
	ON matched.partner_order_id_d = partner_detail.order_id 
	AND matched.partner_ORDER_INTEGRATION_ID = partner_detail.PARTNER_GROCERY_ORDER_CUSTOMER_INTEGRATION_ID
	AND left(matched.store_transaction_ts,7) = left(partner_detail.store_transaction_ts,7))

	--Item level
	SELECT tt.txn_id, tt.txn_dte,partner_order_id_d as order_id, xref.POS_TXN_ID as receipt_nbr_v1, CONCAT(LPAD(hdr.STORE_ID, 6, '0'), LPAD(hdr.REGISTER_NBR, 3, '0'), LPAD(hdr.REGISTER_TXN_SEQ_NBR, 4, '0'), RIGHT(DATE_PART(yy, hdr.TXN_TM),2), DATE_PART(mm,hdr.TXN_TM), DATE_PART(dd,hdr.TXN_TM),DATE_PART(hh, hdr.TXN_TM), DATE_PART(mi, hdr.TXN_TM)) as RECEIPT_NBR_v2, RIGHT(tt.tender_nbr,4) as cc_nbr_lastfour, tt.tender_amt as transaction_total_amt, hdr.net_amt as transaction_net_amt, hdr.tax_amt as transaction_tax_amt, hdr.txn_tm as pos_tm, REPLACE(LTRIM(REPLACE(TENDER_APPR_CD, '0', ' ')), ' ', '0') as aci_approval_cd , ITEM_QTY, upc.upc_id as item_id, upc.upc_id as upc_id, upc_dsc, facts.NET_AMT as item_net_amt, CASE WHEN upc.group_nm like 'alcohol%' and upc.category_nm not like 'NON-ALCOHOL%' and upc.category_nm not like 'ALCOHOLIC BEVERAGES SUPPLIES' then 'TRUE' else 'FALSE' end as aci_alc_ind
	FROM 
	key_tble kt
	left join
	(SELECT TXN_ID, TXN_DTE, UPC_ID, SUM(ITEM_QTY) as ITEM_QTY, SUM(NET_AMT) as NET_AMT
	FROM "EDM_VIEWS_PRD"."DW_EDW_VIEWS"."TXN_FACTS"
	GROUP BY TXN_ID,TXN_DTE, UPC_ID) facts
	on kt.txn_id = facts.txn_id
	LEFT JOIN "EDM_VIEWS_PRD"."DW_EDW_VIEWS"."TXN_TENDER" tt
	ON tt.txn_id = facts.txn_id and tt.txn_dte = facts.txn_dte
	LEFT JOIN "EDM_VIEWS_PRD"."DW_EDW_VIEWS"."TXN_HDR" hdr
	ON tt.txn_id = hdr.txn_id and tt.txn_dte=hdr.txn_dte
	LEFT JOIN "EDM_VIEWS_PRD"."DW_EDW_VIEWS"."LU_STORE" st
	ON st.store_id = hdr.store_id
	LEFT JOIN "EDM_VIEWS_PRD"."DW_EDW_VIEWS"."LU_UPC" upc
	ON upc.upc_id = facts.upc_id
	LEFT JOIN "EDM_VIEWS_PRD"."DW_EDW_VIEWS"."TXN_ID_XREF" xref
	ON tt.txn_id = xref.sk_txn_id
	WHERE tt.txn_dte > dateadd(day,-7,CURRENT_TIMESTAMP())
	AND hdr.txn_dte > dateadd(day,-7,CURRENT_TIMESTAMP())
	AND facts.txn_dte > dateadd(day,-7,CURRENT_TIMESTAMP())
	AND item_net_amt <> 0 ));`} );


	var Resultset = snowflake.execute( {sqlText:` Create TRANSIENT table if not exists  DW_C_STAGE.f_partner_transaction_header_TMP2 as 
	select partner_order_id	
		 , store_txn_id
		 , partner_date
		 , store_date
		 , store_id
		 , register_nbr
		 , partner_id
		 , partner_net_amt
		 , transaction_net_amt
		 , loyalty_markdown_AMt
		 , transaction_tax_amt
		 , partner_tax_amt
		 , transaction_total_amt
		 , instore_net_amt
		-- , Tax_AMT_Paid
	     , instore_tax_paid
		 , instore_taxable_amt
		 , TOTAL_TAX_PLAN_A_AMT
		 , TOTAL_TAX_PLAN_B_AMT
		 , TOTAL_TAX_PLAN_C_AMT
		 , TOTAL_TAX_PLAN_D_AMT
		 , TOTAL_TAX_PLAN_E_AMT
		 , TOTAL_TAX_PLAN_F_AMT
		 , TOTAL_TAX_PLAN_G_AMT
		 , TOTAL_TAX_PLAN_H_AMT
		 , TOTAL_TAXABLE_PLAN_A_AMT
		 , TOTAL_TAXABLE_PLAN_B_AMT
		 , TOTAL_TAXABLE_PLAN_C_AMT
		 , TOTAL_TAXABLE_PLAN_D_AMT
		 , TOTAL_TAXABLE_PLAN_E_AMT
		 , TOTAL_TAXABLE_PLAN_F_AMT
		 , TOTAL_TAXABLE_PLAN_G_AMT
		 , TOTAL_TAXABLE_PLAN_H_AMT
		 , Amount_Comparison
		 , Tax_Comparison 
		 ,qr_status_dsc,ALCOHOL_ORDER_IND,Snap_Order_Ind,Dug_Order_Ind,Deli_Order_Ind,FFC_Order_Ind,Own_Brad_Item_Order_Ind
	From 
	(select partner_order_id	
		 , store_txn_id
		 , partner_date
		 , store_date
		 , store_id
		 , register_nbr
		 , partner_id
		 , partner_net_amt
		 , transaction_net_amt
		 , loyalty_markdown_AMt
		 , transaction_tax_amt
		 , partner_tax_amt
		 , transaction_total_amt
		 , instore_net_amt
		-- , Tax_AMT_Paid
	     , instore_tax_paid
		 , instore_taxable_amt
		 , TOTAL_TAX_PLAN_A_AMT
		 , TOTAL_TAX_PLAN_B_AMT
		 , TOTAL_TAX_PLAN_C_AMT
		 , TOTAL_TAX_PLAN_D_AMT
		 , TOTAL_TAX_PLAN_E_AMT
		 , TOTAL_TAX_PLAN_F_AMT
		 , TOTAL_TAX_PLAN_G_AMT
		 , TOTAL_TAX_PLAN_H_AMT
		 , TOTAL_TAXABLE_PLAN_A_AMT
		 , TOTAL_TAXABLE_PLAN_B_AMT
		 , TOTAL_TAXABLE_PLAN_C_AMT
		 , TOTAL_TAXABLE_PLAN_D_AMT
		 , TOTAL_TAXABLE_PLAN_E_AMT
		 , TOTAL_TAXABLE_PLAN_F_AMT
		 , TOTAL_TAXABLE_PLAN_G_AMT
		 , TOTAL_TAXABLE_PLAN_H_AMT
		 , Amount_Comparison
		 , Tax_Comparison
		 ,qr_status_dsc,ALCOHOL_ORDER_IND,Snap_Order_Ind,Dug_Order_Ind,Deli_Order_Ind,FFC_Order_Ind,Own_Brad_Item_Order_Ind
		 
		 from (

WITH a AS (
	SELECT DISTINCT a.order_id AS partner_order_id
	, a.dlvry_id AS partner_delivery_id
	, CASE WHEN a.txn_id IS NULL
	THEN 0
	ELSE a.txn_id
	END AS store_txn_id
	, a.store_txn_ts::DATE AS partner_date
	, d.TXN_DTE::DATE AS store_date
	, a.store_id AS store_id
	, d.REGISTER_NBR AS register_nbr
	 ,b.partner_id AS partner_id
	, a.net_amt AS partner_net_amt
	, d.NET_AMT as transaction_net_amt
	, d.MKDN_AMT AS loyalty_markdown_AMt
	, d.TAX_AMT AS transaction_tax_amt
	, e.tender_amt as transaction_total_amt
	, d.NET_AMT as instore_net_amt
	--, d.tax_amt as Tax_AMT_Paid
	, d.TOTAL_TAX_PLAN_A_AMT AS TOTAL_TAX_PLAN_A_AMT
	, d.TOTAL_TAX_PLAN_B_AMT AS TOTAL_TAX_PLAN_B_AMT
	, d.TOTAL_TAX_PLAN_C_AMT AS TOTAL_TAX_PLAN_C_AMT
	, d.TOTAL_TAX_PLAN_D_AMT AS TOTAL_TAX_PLAN_D_AMT
	, d.TOTAL_TAX_PLAN_E_AMT AS TOTAL_TAX_PLAN_E_AMT
	, d.TOTAL_TAX_PLAN_F_AMT AS TOTAL_TAX_PLAN_F_AMT
	, d.TOTAL_TAX_PLAN_G_AMT AS TOTAL_TAX_PLAN_G_AMT
	, d.TOTAL_TAX_PLAN_H_AMT AS TOTAL_TAX_PLAN_H_AMT
	, d.TOTAL_TAXABLE_PLAN_A_AMT as TOTAL_TAXABLE_PLAN_A_AMT
	, d.TOTAL_TAXABLE_PLAN_B_AMT as TOTAL_TAXABLE_PLAN_B_AMT
	, d.TOTAL_TAXABLE_PLAN_C_AMT as TOTAL_TAXABLE_PLAN_C_AMT
	, d.TOTAL_TAXABLE_PLAN_D_AMT as TOTAL_TAXABLE_PLAN_D_AMT
	, d.TOTAL_TAXABLE_PLAN_E_AMT as TOTAL_TAXABLE_PLAN_E_AMT
	, d.TOTAL_TAXABLE_PLAN_F_AMT as TOTAL_TAXABLE_PLAN_F_AMT
	, d.TOTAL_TAXABLE_PLAN_G_AMT as TOTAL_TAXABLE_PLAN_G_AMT
	, d.TOTAL_TAXABLE_PLAN_H_AMT as TOTAL_TAXABLE_PLAN_H_AMT
	--, SUM(d.TOTAL_TAX_PLAN_A_AMT+d.TOTAL_TAX_PLAN_B_AMT+d.TOTAL_TAX_PLAN_C_AMT+d.TOTAL_TAX_PLAN_D_AMT) AS total_tax
    ,CASE WHEN qrcode.status_dsc ='APPROVED' THEN '3PM QR ACH USED'
          WHEN qrcode.status_dsc ='RETRIEVED' THEN '3PM QR USED'
          WHEN qrcode.status_dsc ='EXPIRED' THEN '3PM QR NOT USED'
          end as qr_status_dsc
	FROM EDM_VIEWS_PRD.Dw_EDW_VIEWS.PARTNER_ORDER_STORE_TENDER a
	JOIN EDM_VIEWS_PRD.Dw_EDW_VIEWS.partner_order b
	ON a.order_id = b.order_id
	AND a.dlvry_id = b.dlvry_id
	JOIN EDM_VIEWS_PRD.Dw_edw_VIEWS.PARTNER_ORDER_ITM c
	ON b.order_id = c.order_id
	AND b.dlvry_id = c.dlvry_id
    left join "EDM_VIEWS_PRD"."DW_PAYMENTS_VIEWS"."ORDER_QUICK_REFERENCE_3PL" qrcode
	ON a.order_id = try_to_number(qrcode.order_id)
	JOIN EDM_VIEWS_PRD.DW_EDW_VIEWS.TXN_HDR d
	ON a.txn_id = d.txn_id
	JOIN EDM_VIEWS_PRD.DW_EDW_VIEWS.TXN_TENDER e
	ON d.TXN_DTE = e.TXN_DTE
	AND d.TXN_ID = e.TXN_ID
	and REPLACE(LTRIM(REPLACE(a.approval_cd, '0', ' ')), ' ', '0') = REPLACE(LTRIM(REPLACE(e.TENDER_APPR_CD, '0', ' ')), ' ', '0')
	Join EDM_VIEWS_PRD.DW_EDW_VIEWS.LU_REGISTER f
	On d.REGISTER_NBR = f.REGISTER_NBR
	WHERE --a.store_id IN (3376) -- Filter out if requirement is for all stores
	--AND a.store_txn_ts::DATE BETWEEN '2022-06-01' AND '2022-06-01' 
	 a.dw_create_ts > dateadd(day,-7,current_timestamp ())-- Change by transaction date
	--GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14n 
	ORDER BY 3,4)
	SELECT partner_order_id	
         , partner_delivery_id	
		 , store_txn_id
		 , partner_date
		 ,store_date
		 , store_id
		 , register_nbr
		 , partner_id
		 , partner_net_amt
		 , transaction_net_amt
		 , loyalty_markdown_AMt
		 , transaction_tax_amt
		 ,transaction_total_amt
		 ,instore_net_amt
		-- ,Tax_AMT_Paid
		 , TOTAL_TAX_PLAN_A_AMT
		 , TOTAL_TAX_PLAN_B_AMT
		 , TOTAL_TAX_PLAN_C_AMT
		 , TOTAL_TAX_PLAN_D_AMT
		 , TOTAL_TAX_PLAN_E_AMT
		 , TOTAL_TAX_PLAN_F_AMT
		 , TOTAL_TAX_PLAN_G_AMT
		 , TOTAL_TAX_PLAN_H_AMT
		 , TOTAL_TAXABLE_PLAN_A_AMT
		 , TOTAL_TAXABLE_PLAN_B_AMT
		 , TOTAL_TAXABLE_PLAN_C_AMT
		 , TOTAL_TAXABLE_PLAN_D_AMT
		 , TOTAL_TAXABLE_PLAN_E_AMT
		 , TOTAL_TAXABLE_PLAN_F_AMT
		 , TOTAL_TAXABLE_PLAN_G_AMT
		 , TOTAL_TAXABLE_PLAN_H_AMT
         , qr_status_dsc
         ,g.ALCOHOL_ORDER_IND as ALCOHOL_ORDER_IND
         ,g.SNAP_ORDER_IND as Snap_Order_Ind
         ,g.DUG_ORDER_IND as Dug_Order_Ind
         ,g.DELI_ORDER_IND as Deli_Order_Ind
         ,g.FFC_ORDER_IND as FFC_Order_Ind
         ,g.OWN_BRAND_ITEM_ORDER_IND as Own_Brad_Item_Order_Ind
		 , SUM(TOTAL_TAX_PLAN_A_AMT + TOTAL_TAX_PLAN_B_AMT + TOTAL_TAX_PLAN_C_AMT + TOTAL_TAX_PLAN_D_AMT + TOTAL_TAX_PLAN_E_AMT + TOTAL_TAX_PLAN_F_AMT + TOTAL_TAX_PLAN_G_AMT + TOTAL_TAX_PLAN_H_AMT) AS instore_tax_paid
		 ,sum(TOTAL_TAXABLE_PLAN_A_AMT + TOTAL_TAXABLE_PLAN_B_AMT + TOTAL_TAXABLE_PLAN_C_AMT + TOTAL_TAXABLE_PLAN_D_AMT + TOTAL_TAXABLE_PLAN_E_AMT + TOTAL_TAXABLE_PLAN_F_AMT + TOTAL_TAXABLE_PLAN_G_AMT + TOTAL_TAXABLE_PLAN_H_AMT) as instore_taxable_amt
		 , SUM(c.tot_itm_tax_plan_a_amt + c.tot_itm_tax_plan_b_amt + c.tot_itm_tax_plan_c_amt + c.tot_itm_tax_plan_d_amt) AS partner_tax_amt
		 , CASE WHEN ((partner_net_amt - transaction_tax_amt ) = transaction_net_amt)
				THEN 'TRUE'
				ELSE 'FALSE'
			END AS Amount_Comparison
		 , CASE WHEN (transaction_tax_amt = instore_tax_paid)
				THEN 'TRUE'
				ELSE 'FALSE'
			END AS Tax_Comparison
	  FROM a
	  LEFT JOIN ( SELECT order_id
                 , dlvry_id
                 , COALESCE(SUM(itm_tax_plan_a_amt),0) AS tot_itm_tax_plan_a_amt
                 , COALESCE(SUM(itm_tax_plan_b_amt),0) AS tot_itm_tax_plan_b_amt       
                 , COALESCE(SUM(itm_tax_plan_c_amt),0) AS tot_itm_tax_plan_c_amt    
                 , COALESCE(SUM(itm_tax_plan_d_amt),0) AS tot_itm_tax_plan_d_amt
              FROM EDM_VIEWS_PRD.DW_EDW_VIEWS.PARTNER_ORDER_ITM_TAX
             GROUP BY 1,2
           ) c
    ON a.partner_order_id = c.order_id
   AND a.partner_delivery_id = c.dlvry_id
       left join  EDM_VIEWS_PRD.DW_VIEWS.F_PARTNER_ORDER_TRANSACTION g
    ON a.partner_order_id = try_to_number(g.order_id)
 	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37)) ;`} );

	var Resultset = snowflake.execute( {sqlText:` BEGIN TRANSACTION;`} );


	var Resultset = snowflake.execute( {sqlText:` insert into    EDM_ANALYTICS_PRD.DW_RETAIL_OPS.f_partner_transaction_header
	(select 
		
		c.business_partner_d1_sk as business_partner_d1_sk   
		,CASE WHEN b.retail_store_d1_sk is not null
				THEN b.retail_store_d1_sk
				ELSE '1'
			END AS retail_store_d1_sk
		,a.partner_order_id as partner_order_id          
		,a.partner_id   as     partner_id   
		,a.partner_order_integration_id as partner_order_integration_id
		,a.partner_nm as partner_nm                
		,a.partner_dt as partner_transaction_dt    
		,a.partner_net_amt as partner_total_net_amt 
		,a.store_order_id as transaction_id            
		,a.store_dt as transaction_dt                               
		,b.facility_integration_id   as facility_integration_id
		,a.store_id as retail_store_facility_nbr 
		,null as total_markdown_amt 
		,a.transaction_total_amt as transaction_total_amt
		,a.transaction_net_amt as total_net_amt             
		--,a.transaction_tax_amt as total_tax_amt             
		,a.total_tax_plan_a_amt as  total_tax_plan_a_amt      
		,a.total_tax_plan_b_amt AS total_tax_plan_b_amt      
		,a.total_tax_plan_c_amt AS total_tax_plan_c_amt      
		,a.total_tax_plan_d_amt AS total_tax_plan_d_amt  
		,a.amount_Comparison AS net_match_ind             
		,a.Tax_Comparison AS tax_match_ind  
 		,CURRENT_TIMESTAMP() AS dw_create_ts              
		,CURRENT_TIMESTAMP() AS dw_last_update_ts   
		,b.DIVISION_D1_SK   as DIVISION_D1_SK   
        ,b.BANNER_D1_SK as 		BANNER_D1_SK
		,a.total_tax_plan_e_amt AS total_tax_plan_e_amt 		
        ,a.total_tax_plan_f_amt AS total_tax_plan_f_amt 
		,a.total_tax_plan_g_amt AS total_tax_plan_g_amt 
		,a.total_tax_plan_h_amt AS total_tax_plan_h_amt 
		,a.TOTAL_TAXABLE_PLAN_A_AMT as TOTAL_TAXABLE_PLAN_A_AMT
		,a.TOTAL_TAXABLE_PLAN_B_AMT as TOTAL_TAXABLE_PLAN_B_AMT
		,a.TOTAL_TAXABLE_PLAN_C_AMT as TOTAL_TAXABLE_PLAN_C_AMT
		,a.TOTAL_TAXABLE_PLAN_D_AMT as TOTAL_TAXABLE_PLAN_D_AMT
		,a.TOTAL_TAXABLE_PLAN_E_AMT as TOTAL_TAXABLE_PLAN_E_AMT
		,a.TOTAL_TAXABLE_PLAN_F_AMT as TOTAL_TAXABLE_PLAN_F_AMT
		,a.TOTAL_TAXABLE_PLAN_G_AMT as TOTAL_TAXABLE_PLAN_G_AMT
		,a.TOTAL_TAXABLE_PLAN_H_AMT as TOTAL_TAXABLE_PLAN_H_AMT
		,a.partner_tax_amt as partner_tax_amt
		,a.instore_net_amt  as    instore_net_amt   
		--,a.Tax_AMT_Paid    as Tax_AMT_Paid       
		,a.instore_tax_paid  as instore_tax_paid   
		,a.instore_taxable_amt  as instore_taxable_amt
		,a.qr_status_dsc as qr_status_dsc
		,a.ALCOHOL_ORDER_IND as ALCOHOL_ORDER_IND
		,a.Snap_Order_Ind as Snap_Order_Ind
		,a.Dug_Order_Ind as Dug_Order_Ind
		,a.Deli_Order_Ind as Deli_Order_Ind
		,a.FFC_Order_Ind as FFC_Order_Ind
		,a.Own_Brad_Item_Order_Ind as Own_Brad_Item_Order_Ind				
	from  DW_C_STAGE.f_partner_transaction_header_TMP a 
	left outer join (select * from EDM_VIEWS_PRD.DW_VIEWS.D1_retail_store a
                     LEFT JOIN EDM_VIEWS_PRD.DW_VIEWS.D1_DIVISION_PARTNERS B
                     ON A.DIVISION_ID =B.DIVISION_ID
                     LEFT JOIN EDM_VIEWS_PRD.DW_VIEWS.D1_BANNER C
                     ON A.BANNER_NM =C.BANNER_NM
	AND a.retail_store_facility_nbr NOT IN ('N/A')
	) as b
	on a.store_id = try_to_number(b.retail_store_facility_nbr,10)
	left outer join (select * from EDM_VIEWS_PRD.DW_VIEWS.D1_BUSINESS_PARTNER where partner_id in ('2')) as c
	on a.partner_id = c.partner_id )`} );



	var Resultset = snowflake.execute( {sqlText:` insert into     EDM_ANALYTICS_PRD.DW_RETAIL_OPS.f_partner_transaction_header
	(select 
		
		c.business_partner_d1_sk as business_partner_d1_sk   
		,CASE WHEN b.retail_store_d1_sk is not null
				THEN b.retail_store_d1_sk
				ELSE '1'
			END AS retail_store_d1_sk
		,a.partner_order_id as partner_order_id          
		,a.partner_id   as     partner_id   
		,a.partner_order_integration_id as partner_order_integration_id
		,a.partner_nm as partner_nm                
		,a.partner_dt as partner_transaction_dt    
		,a.partner_net_amt as partner_total_net_amt 
		,a.store_order_id as transaction_id            
		,a.store_dt as transaction_dt                               
		,b.facility_integration_id   as facility_integration_id
		,a.store_id as retail_store_facility_nbr 
		,null as total_markdown_amt 
		,a.transaction_total_amt as transaction_total_amt
		,a.transaction_net_amt as total_net_amt             
		--,a.transaction_tax_amt as total_tax_amt             
		,a.total_tax_plan_a_amt as  total_tax_plan_a_amt      
		,a.total_tax_plan_b_amt AS total_tax_plan_b_amt      
		,a.total_tax_plan_c_amt AS total_tax_plan_c_amt      
		,a.total_tax_plan_d_amt AS total_tax_plan_d_amt  
		,a.amount_Comparison AS net_match_ind             
		,a.Tax_Comparison AS tax_match_ind  
 		,CURRENT_TIMESTAMP() AS dw_create_ts              
		,CURRENT_TIMESTAMP() AS dw_last_update_ts   
		,b.DIVISION_D1_SK   as DIVISION_D1_SK   
        ,b.BANNER_D1_SK as 		BANNER_D1_SK
		,a.total_tax_plan_e_amt AS total_tax_plan_e_amt 		
        ,a.total_tax_plan_f_amt AS total_tax_plan_f_amt 
		,a.total_tax_plan_g_amt AS total_tax_plan_g_amt 
		,a.total_tax_plan_h_amt AS total_tax_plan_h_amt 
		,a.TOTAL_TAXABLE_PLAN_A_AMT as TOTAL_TAXABLE_PLAN_A_AMT
		,a.TOTAL_TAXABLE_PLAN_B_AMT as TOTAL_TAXABLE_PLAN_B_AMT
		,a.TOTAL_TAXABLE_PLAN_C_AMT as TOTAL_TAXABLE_PLAN_C_AMT
		,a.TOTAL_TAXABLE_PLAN_D_AMT as TOTAL_TAXABLE_PLAN_D_AMT
		,a.TOTAL_TAXABLE_PLAN_E_AMT as TOTAL_TAXABLE_PLAN_E_AMT
		,a.TOTAL_TAXABLE_PLAN_F_AMT as TOTAL_TAXABLE_PLAN_F_AMT
		,a.TOTAL_TAXABLE_PLAN_G_AMT as TOTAL_TAXABLE_PLAN_G_AMT
		,a.TOTAL_TAXABLE_PLAN_H_AMT as TOTAL_TAXABLE_PLAN_H_AMT
		,a.partner_tax_amt as partner_tax_amt
		,a.instore_net_amt  as    instore_net_amt   
		--,a.Tax_AMT_Paid    as Tax_AMT_Paid       
		,a.instore_tax_paid  as instore_tax_paid   
		,a.instore_taxable_amt  as instore_taxable_amt
		,a.qr_status_dsc as qr_status_dsc
		,a.ALCOHOL_ORDER_IND as ALCOHOL_ORDER_IND
		,a.Snap_Order_Ind as Snap_Order_Ind
		,a.Dug_Order_Ind as Dug_Order_Ind
		,a.Deli_Order_Ind as Deli_Order_Ind
		,a.FFC_Order_Ind as FFC_Order_Ind
		,a.Own_Brad_Item_Order_Ind as Own_Brad_Item_Order_Ind	
        
	from  DW_C_STAGE.f_partner_transaction_header_TMP1 a 
	left outer join (select * from EDM_VIEWS_PRD.DW_VIEWS.D1_retail_store a
                     LEFT JOIN EDM_VIEWS_PRD.DW_VIEWS.D1_DIVISION_PARTNERS B
                     ON A.DIVISION_ID =B.DIVISION_ID
                     LEFT JOIN EDM_VIEWS_PRD.DW_VIEWS.D1_BANNER C
                     ON A.BANNER_NM =C.BANNER_NM
	AND a.retail_store_facility_nbr NOT IN ('N/A')
	) as b
	on a.store_id = try_to_number(b.retail_store_facility_nbr,10)
	left outer join (select * from EDM_VIEWS_PRD.DW_VIEWS.D1_BUSINESS_PARTNER where partner_id in ('3')) as c
	on a.partner_id = c.partner_id )`} );

	var Resultset = snowflake.execute( {sqlText:` insert into     EDM_ANALYTICS_PRD.DW_RETAIL_OPS.f_partner_transaction_header
	(select 
		
		c.business_partner_d1_sk as business_partner_d1_sk   
		,CASE WHEN b.retail_store_d1_sk is not null
				THEN b.retail_store_d1_sk
				ELSE '1'
			END AS retail_store_d1_sk
		--,b.retail_store_d1_sk   as retail_store_d1_sk 
		,a.partner_order_id as partner_order_id          
		,a.partner_id   as     partner_id  
			,NULL as partner_order_integration_id
		,'Instacart' as partner_nm                
		,a.partner_date as partner_transaction_dt    
		,a.partner_net_amt as partner_total_net_amt 
		,a.store_txn_id as transaction_id            
		,a.store_date as transaction_dt                                 
		,b.facility_integration_id   as facility_integration_id
		,a.store_id as retail_store_facility_nbr 
		,a.loyalty_markdown_AMt as total_markdown_amt 
		,a.transaction_total_amt as transaction_total_amt
		,a.transaction_net_amt as total_net_amt             
		--,a.Tax_AMT_Paid as total_tax_amt             
		,a.total_tax_plan_a_amt as  total_tax_plan_a_amt      
		,a.total_tax_plan_b_amt AS total_tax_plan_b_amt      
		,a.total_tax_plan_c_amt AS total_tax_plan_c_amt      
		,a.total_tax_plan_d_amt AS total_tax_plan_d_amt    
		,a.amount_Comparison AS net_match_ind             
		,a.Tax_Comparison AS tax_match_ind  		
		,CURRENT_TIMESTAMP() AS dw_create_ts              
		,CURRENT_TIMESTAMP() AS dw_last_update_ts 
		,b.DIVISION_D1_SK   as DIVISION_D1_SK   
        ,b.BANNER_D1_SK as 		BANNER_D1_SK	
		,a.total_tax_plan_e_amt AS total_tax_plan_e_amt 
		,a.total_tax_plan_f_amt AS total_tax_plan_f_amt 
		,a.total_tax_plan_g_amt AS total_tax_plan_g_amt 
		,a.total_tax_plan_h_amt AS total_tax_plan_h_amt 
		,a.TOTAL_TAXABLE_PLAN_A_AMT as TOTAL_TAXABLE_PLAN_A_AMT
		,a.TOTAL_TAXABLE_PLAN_B_AMT as TOTAL_TAXABLE_PLAN_B_AMT
		,a.TOTAL_TAXABLE_PLAN_C_AMT as TOTAL_TAXABLE_PLAN_C_AMT
		,a.TOTAL_TAXABLE_PLAN_D_AMT as TOTAL_TAXABLE_PLAN_D_AMT
		,a.TOTAL_TAXABLE_PLAN_E_AMT as TOTAL_TAXABLE_PLAN_E_AMT
		,a.TOTAL_TAXABLE_PLAN_F_AMT as TOTAL_TAXABLE_PLAN_F_AMT
		,a.TOTAL_TAXABLE_PLAN_G_AMT as TOTAL_TAXABLE_PLAN_G_AMT
		,a.TOTAL_TAXABLE_PLAN_H_AMT as TOTAL_TAXABLE_PLAN_H_AMT
		,a.partner_tax_amt as partner_tax_amt
	    ,a.instore_net_amt  as    instore_net_amt   
		--,a.Tax_AMT_Paid    as Tax_AMT_Paid       
		,a.instore_tax_paid  as instore_tax_paid   
		,a.instore_taxable_amt  as instore_taxable_amt
	    ,a.qr_status_dsc as qr_status_dsc
		,a.ALCOHOL_ORDER_IND as ALCOHOL_ORDER_IND
		,a.Snap_Order_Ind as Snap_Order_Ind
		,a.Dug_Order_Ind as Dug_Order_Ind
		,a.Deli_Order_Ind as Deli_Order_Ind
		,a.FFC_Order_Ind as FFC_Order_Ind
		,a.Own_Brad_Item_Order_Ind as Own_Brad_Item_Order_Ind
        
	from  DW_C_STAGE.f_partner_transaction_header_TMP2 a 
	left outer join (select * from EDM_VIEWS_PRD.DW_VIEWS.D1_retail_store a
                     LEFT JOIN EDM_VIEWS_PRD.DW_VIEWS.D1_DIVISION_PARTNERS B
                     ON A.DIVISION_ID =B.DIVISION_ID
                     LEFT JOIN EDM_VIEWS_PRD.DW_VIEWS.D1_BANNER C
                     ON A.BANNER_NM =C.BANNER_NM
	AND a.retail_store_facility_nbr NOT IN ('N/A')
	) as b
	on a.store_id = try_to_number(b.retail_store_facility_nbr,10)
	left outer join (select * from EDM_VIEWS_PRD.DW_VIEWS.D1_BUSINESS_PARTNER where partner_id in ('1')) as c
	on a.partner_id = c.partner_id )`} );


	var Resultset = snowflake.execute( {sqlText:` insert into   EDM_ANALYTICS_PRD.DW_RETAIL_OPS.f_partner_transaction_detail
	(select 
		
		 '3' as business_partner_d1_sk
		--,c.retail_store_d1_sk as retail_store_d1_sk
		,b.upc_d1_sk as upc_d1_sk
		,a.ORDER_ID as partner_order_id
		,a.TXN_ID as transaction_id
		,a.RECEIPT_NBR_V1 as receipt_1_nbr
		,a.RECEIPT_NBR_V2 as receipt_2_nbr
		,a.CC_NBR_LASTFOUR as last_four_tender_nbr
		,a.TRANSACTION_TOTAL_AMT as TRANSACTION_TOTAL_AMT
		,a.TRANSACTION_NET_AMT as TRANSACTION_NET_AMT
		,a.TRANSACTION_TAX_AMT as TRANSACTION_TAX_AMT
		,a.POS_TM as transaction_ts
		,a.ACI_APPROVAL_CD as tender_approval_cd
		,a.ITEM_QTY as item_qty
		,a.ITEM_ID as item_nbr
		,a.UPC_ID as upc_nbr
		,a.UPC_DSC as item_dsc
		,a.ITEM_NET_AMT as net_amt
		,a.ACI_ALC_IND as alchol_ind           
		,CURRENT_TIMESTAMP() AS dw_create_ts              
		,CURRENT_TIMESTAMP() AS dw_last_update_ts         
	from  DW_C_STAGE.f_partner_transaction_header_TMP3 a 
	left outer join (select * from EDM_VIEWS_PRD.DW_VIEWS.D1_UPC) as b
	on a.upc_id = b.upc_nbr )`} );

	var Resultset = snowflake.execute( {sqlText:` COMMIT;`} );

	var Resultset = snowflake.execute( {sqlText:` drop table  DW_C_STAGE.f_partner_transaction_header_TMP;`} );
	var Resultset = snowflake.execute( {sqlText:` drop table  DW_C_STAGE.f_partner_transaction_header_TMP1;`} );
	var Resultset = snowflake.execute( {sqlText:` drop table  DW_C_STAGE.f_partner_transaction_header_TMP2;`} );
	var Resultset = snowflake.execute( {sqlText:` drop table  DW_C_STAGE.f_partner_transaction_header_TMP3;`} );

	result += "Success";
	}
	catch (err)  {
	if (err_code == "")
	{
	err_code=err.code;
	err_state=err.state;
	err_msg=err.message;
	err_trace=err.stackTraceTxt;
	}
	if (result == "")
	{
	result = "FAILED:"+ err_msg
	}
		 var Resultset = snowflake.execute( {sqlText:` rollback;`} );
		  snowflake.execute( {sqlText:` Insert into  DW_C_STAGE.f_partner_transaction_header_ERROR_LOG VALUES
		  (:1,:2,:3,:4,current_timestamp::TIMESTAMP_NTZ,'UBER_DOORDASH_INSTACART_REPORT',CURRENT_USER(),CURRENT_ROLE())`,binds: [err_code,err_state,err_msg,err_trace]} );
	}
	  return result;
$$;
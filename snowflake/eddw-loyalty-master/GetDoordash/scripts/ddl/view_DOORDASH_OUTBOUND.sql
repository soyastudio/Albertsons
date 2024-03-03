--liquibase formatted sql
--changeset SYSTEM:DOORDASH_OUTBOUND runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view DOORDASH_OUTBOUND(
	TXN_ID COMMENT 'Unique identifier of an Transaction in DoorDash system',
	ORDER_ID COMMENT 'Unique identifier of an order in DoorDash system',
	RECEIPT_NBR_V1 COMMENT 'TBC w/ ACI)',
	RECEIPT_NBR_V2 COMMENT 'The numeric value printed on the physical receipt',
	CC_NBR_LASTFOUR COMMENT 'Last 4 digits of the credit card',
	TRANSACTION_TOTAL_AMT COMMENT 'The total amount of the transaction in ACI system',
	TRANSACTION_NET_AMT COMMENT 'The net amount of the transaction in ACI system',
	TRANSACTION_TAX_AMT COMMENT 'The tax amount of the transaction in the ACI system',
	POS_TM COMMENT 'Timestamp at the POS of this transaction',
	ACI_APPROVAL_CD COMMENT 'Approval code of the transaction',
	ITEM_QTY COMMENT 'Quantity of units purchases',
	ITEM_ID COMMENT 'The item identifier',
	UPC_ID COMMENT 'The UPC associated to the item',
	UPC_DSC COMMENT 'Description of the item',
	ITEM_NET_AMT COMMENT 'The unit price of the item',
	ACI_ALC_IND COMMENT 'Indicates if item is classified as alcoholic'
) COMMENT='VIEW FOR DOORDASH_OUTBOUND'
 as 
(

--CTE key_tble with matched records
with key_tble as (
SELECT distinct partner_order_id_d, txn_id
FROM (SELECT *
FROM EDM_VIEWS_PRD.DW_VIEWS.PARTNER_GROCERY_ORDER_DETAIL
WHERE store_transaction_ts between current_date - 12 and current_date - 6) partner_detail
INNER JOIN 
  (
-- partner_table and aci_transaction table join
SELECT *, case when tender_amt is null then 'No match' when partner_net_amt = tender_amt then 'Matched' else 'mismatch' end as matchstatus -- partner_net_amt, tender_amt, gross_amt, net_amt, mkdn_amt
FROM (
SELECT distinct partner_tt.order_id as partner_order_id_d, partner_tt.PARTNER_GROCERY_ORDER_CUSTOMER_INTEGRATION_ID as partner_order_integration_id, REPLACE(LTRIM(REPLACE(partner_tt.approval_cd, '0', ' ')), ' ', '0') as partner_approval_cd, 
transaction_dt as partner_transaction_dt, store_transaction_ts, net_amt as partner_net_amt, store_id as partner_store_id, partner_nm, REPLACE(masked_credit_card_nbr, 'X', 9) as new_credit_card
FROM EDM_VIEWS_PRD.DW_VIEWS.PARTNER_GROCERY_ORDER_TENDER partner_tt
LEFT JOIN EDM_VIEWS_PRD.DW_VIEWS.PARTNER_GROCERY_ORDER_HEADER partner_hdr
ON partner_tt.order_id = partner_hdr.order_id 
AND partner_tt.PARTNER_GROCERY_ORDER_CUSTOMER_INTEGRATION_ID = partner_hdr.PARTNER_GROCERY_ORDER_CUSTOMER_INTEGRATION_ID
AND partner_nm = 'Doordash') ddt 
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
WHERE tt.txn_dte between current_date-13 and current_date-4
AND hdr.txn_dte between current_date-13 and current_date-4
) ac_combined
ON partner_approval_cd = ac_approval_cd
AND try_to_number(partner_store_id) = try_to_number(ac_combined.store_id)
AND partner_transaction_dt between ac_date and ac_date_daylightadd
AND new_credit_card = ac_combined.tender_nbr
WHERE partner_transaction_dt between current_date-12 and current_date-6
and matchstatus <> 'No match'
  )matched
ON matched.partner_order_id_d = partner_detail.order_id 
AND matched.partner_ORDER_INTEGRATION_ID = partner_detail.PARTNER_GROCERY_ORDER_CUSTOMER_INTEGRATION_ID
AND matched.store_transaction_ts = partner_detail.store_transaction_ts)

--Item level
SELECT tt.txn_id, partner_order_id_d as order_id, xref.POS_TXN_ID as receipt_nbr_v1,
CONCAT(LPAD(hdr.STORE_ID, 6, '0'), LPAD(hdr.REGISTER_NBR, 3, '0'), LPAD(hdr.REGISTER_TXN_SEQ_NBR, 4, '0'), RIGHT(DATE_PART(yy, hdr.TXN_TM),2), DATE_PART(mm,hdr.TXN_TM), DATE_PART(dd,hdr.TXN_TM),DATE_PART(hh, hdr.TXN_TM), DATE_PART(mi, hdr.TXN_TM)) as RECEIPT_NBR_v2,
RIGHT(tt.tender_nbr,4) as cc_nbr_lastfour, 
tt.tender_amt as transaction_total_amt, hdr.net_amt as transaction_net_amt, hdr.tax_amt as transaction_tax_amt,
hdr.txn_tm as pos_tm, REPLACE(LTRIM(REPLACE(TENDER_APPR_CD, '0', ' ')), ' ', '0') as aci_approval_cd , ITEM_QTY, upc.upc_id as item_id, upc.upc_id as upc_id, upc_dsc,
facts.NET_AMT as item_net_amt, CASE WHEN upc.group_nm like 'alcohol%' and upc.category_nm not like 'NON-ALCOHOL%' and upc.category_nm not like 'ALCOHOLIC BEVERAGES SUPPLIES' then 'TRUE' else 'FALSE' end as aci_alc_ind
FROM 
(SELECT TXN_ID, TXN_DTE, UPC_ID, SUM(ITEM_QTY) as ITEM_QTY, SUM(NET_AMT) as NET_AMT
FROM "EDM_VIEWS_PRD"."DW_EDW_VIEWS"."TXN_FACTS"
GROUP BY TXN_ID,TXN_DTE, UPC_ID) facts
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
JOIN key_tble kt
ON kt.txn_id = tt.txn_id
);

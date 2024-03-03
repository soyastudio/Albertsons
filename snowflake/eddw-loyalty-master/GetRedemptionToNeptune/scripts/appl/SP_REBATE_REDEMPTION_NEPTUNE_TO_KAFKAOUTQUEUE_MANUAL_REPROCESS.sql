--liquibase formatted sql
--changeset SYSTEM:SP_REBATE_REDEMPTION_NEPTUNE_TO_KAFKAOUTQUEUE_MANUAL_REPROCESS runOnChange:true splitStatements:false OBJECT_TYPE:SP

Use database <<EDM_DB_NAME_OUT>>;
Use schema DW_APPL;

CREATE OR REPLACE PROCEDURE SP_REBATE_REDEMPTION_NEPTUNE_TO_KAFKAOUTQUEUE_MANUAL_REPROCESS(SRC_TBL VARCHAR, CNF_DB VARCHAR, VIEWS_DB VARCHAR, C_STAGE VARCHAR, STARTDAY VARCHAR, ENDDAY VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

		var src_tbl = SRC_TBL;
		var cnf_db = CNF_DB;
		var views_db = VIEWS_DB;
		var wrk_schema = C_STAGE;
		var red_tbl = 'TRANSACTION_HDR_REBATE_REDEMPTION_NEPTUNE';
		var tpc_nm = 'EDDW_C02_ECOMM_CouponService';
		var src_nm = 'Rebate_Redemption_Manual';
        var STARTDAY = STARTDAY;
		var ENDDAY = ENDDAY;
		var tgt_tbl = `${cnf_db}.${wrk_schema}.KAFKAOUTQUEUE_MANUAL_REPROCESS`;	
		var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.KAFKA_OUT_QUEUE_REBATE_REDEMPTION_MANUAL_REPROCESS_WRK`;
		//var src_wrk_tbl = `${cnf_db}.${wrk_schema}.${red_tbl}_src_WRK`;
		//var src_rerun_tbl = `${cnf_db}.${wrk_schema}.${red_tbl}_Rerun`;
        var src_clips_tbl = `${cnf_db}.${wrk_schema}.clip_details_Reprocess_tmp`; 	
		var src_retail_store_tbl = `${cnf_db}.${wrk_schema}.retail_store_MANUAL_REPROCESS_tmp`;
        var src_epe_transaction_header=`${cnf_db}.${wrk_schema}.EPE_TRANSACTION_HEADER_TMP_REBATE_MANUAL_REPROCESS`;
        var src_epe_transaction_item=`${cnf_db}.${wrk_schema}.EPE_TRANSACTION_ITEM_TMP_REBATE_MANUAL_REPROCESS`;
        var src_customer_alternate=`${cnf_db}.${wrk_schema}.CUSTOMER_ALTERNATE_TMP_REBATE_MANUAL_REPROCESS`;
        var src_epe_transaction_header_savings_dly_rebate=`${cnf_db}.${wrk_schema}.EPE_TRANSACTION_HEADER_SAVINGS_MANUAL_REPROCESS_REBATE`;  

 var sql_trunc_src_wrk_tbl= `truncate  table ${src_epe_transaction_header_savings_dly_rebate}`;

// persist  clips data in work table for the current transaction, includes data from previous failed run
 var sql_crt_src_wrk_tbl= `CREATE OR REPLACE TRANSIENT TABLE ${src_epe_transaction_header_savings_dly_rebate} AS
 SELECT 
EXTERNAL_OFFER_ID,
TRANSACTION_INTEGRATION_ID 
from ${views_db}.DW_VIEWS."EPE_TRANSACTION_HEADER_SAVINGS" 
WHERE DW_CURRENT_VERSION_IND = TRUE
AND DW_LOGICAL_DELETE_IND = FALSE 
AND DW_CREATE_TS>='2022-12-01'
--AND PROGRAM_CD = 'MF'
AND SAVINGS_CATEGORY_NM = '011-Non Discount Offers'
order by TRANSACTION_INTEGRATION_ID
`;

 var sql_alter_src_wrk_tbl= `alter table ${src_epe_transaction_header_savings_dly_rebate} cluster by (TRANSACTION_INTEGRATION_ID)`;
 try {
        //snowflake.execute({ sqlText: sql_trunc_src_wrk_tbl }); 
        snowflake.execute({ sqlText: sql_crt_src_wrk_tbl });
		//snowflake.execute({ sqlText: sql_alter_src_wrk_tbl});
       
    } catch (err)  {	    
        throw `Creation of Source Work table ${src_epe_transaction_header_savings_dly_rebate} Failed with error: ${err}`;   // Return a error message.
    }	



// persist  clips data in work table for the current transaction, includes data from previous failed run
 var sql_trunc_src_wrk_tbl= `truncate  table ${src_epe_transaction_header}`;

// persist  clips data in work table for the current transaction, includes data from previous failed run
 var sql_crt_src_wrk_tbl= `CREATE OR REPLACE TRANSIENT TABLE ${src_epe_transaction_header} AS
 SELECT T1.* FROM 
(
SELECT 
DISTINCT ORDER_ID as ORDER_ID,
cast(TRANSACTION_TS as date) as TRANSACTION_DT ,
TRANSACTION_TS as TXN_TM, 
HOUSEHOLD_ID,
STORE_NBR as STORE_ID,
try_to_numeric(STORE_NBR) as STORE_NBR, 
TRANSACTION_INTEGRATION_ID,
ROW_NUMBER() OVER(PARTITION BY ORDER_ID ORDER BY TRANSACTION_TS desc) rnk  
FROM  "${views_db}"."DW_VIEWS"."EPE_TRANSACTION_HEADER"   
WHERE   STATUS_CD = 'COMPLETED' AND DW_CURRENT_VERSION_IND = true AND DW_LOGICAL_DELETE_IND = false 
  and TRANSACTION_TS>=(CURRENT_DATE - ${STARTDAY}) and TRANSACTION_TS<(CURRENT_DATE - ${ENDDAY})
 )T1 WHERE RNK=1
order by TRANSACTION_INTEGRATION_ID
 `;
 
 var sql_alter_src_wrk_tbl= `alter table ${src_epe_transaction_header} cluster by (TRANSACTION_INTEGRATION_ID)`;
 
 try {
        //snowflake.execute({ sqlText: sql_trunc_src_wrk_tbl }); 
        snowflake.execute({ sqlText: sql_crt_src_wrk_tbl });
		//snowflake.execute({ sqlText: sql_alter_src_wrk_tbl});
       
    } catch (err)  {	    
        throw `Creation of Source Work table ${src_epe_transaction_header} Failed with error: ${err}`;   // Return a error message.
    }	



// persist  clips data in work table for the current transaction, includes data from previous failed run
 var sql_trunc_src_wrk_tbl= `truncate  table ${src_customer_alternate}`;

// persist  clips data in work table for the current transaction, includes data from previous failed run
 var sql_crt_src_wrk_tbl= `CREATE OR REPLACE TRANSIENT TABLE ${src_customer_alternate} AS
 select 
 RETAIL_CUSTOMER_UUID,
ALTERNATE_ID_TXT from (
 select 
 RETAIL_CUSTOMER_UUID,
ALTERNATE_ID_TXT ,
ROW_NUMBER() OVER (
PARTITION BY ALTERNATE_ID_TXT
ORDER BY(SOURCE_LAST_UPDATE_TS) DESC) qlf 
FROM  ${views_db}.DW_VIEWS.CUSTOMER_ALTERNATE
WHERE DW_CURRENT_VERSION_IND = TRUE AND DW_LOGICAL_DELETE_IND =FALSE
 
)t1 
where qlf=1
order by RETAIL_CUSTOMER_UUID
`;
 var sql_alter_src_wrk_tbl= `alter table ${src_customer_alternate} cluster by (ALTERNATE_ID_TXT)`;


 try {
        //snowflake.execute({ sqlText: sql_trunc_src_wrk_tbl }); 
        snowflake.execute({ sqlText: sql_crt_src_wrk_tbl });
		//snowflake.execute({ sqlText: sql_alter_src_wrk_tbl})
       
    } catch (err)  {	    
        throw `Creation of Source Work table ${src_customer_alternate} Failed with error: ${err}`;   // Return a error message.
    }	
	
// persist  clips data in work table for the current transaction, includes data from previous failed run
 var sql_trunc_src_wrk_tbl= `truncate  table ${src_epe_transaction_item}`;

// persist  clips data in work table for the current transaction, includes data from previous failed run
 var sql_crt_src_wrk_tbl= `CREATE OR REPLACE TRANSIENT TABLE ${src_epe_transaction_item} AS
 SELECT ARRAY_AGG(UPC) AS UPC,TRANSACTION_INTEGRATION_ID FROM(
SELECT 
object_construct('upc',TO_VARCHAR(ETI1.UPC_NBR),
'itemUnitCount',ETI1.ITEM_UNIT_QTY) AS UPC,
ETI1.TRANSACTION_INTEGRATION_ID
from ${views_db}.DW_VIEWS.EPE_TRANSACTION_ITEM ETI1 
JOIN ${src_epe_transaction_header_savings_dly_rebate} ETH
WHERE ETI1.TRANSACTION_INTEGRATION_ID = ETH.TRANSACTION_INTEGRATION_ID
)GROUP BY TRANSACTION_INTEGRATION_ID
order by TRANSACTION_INTEGRATION_ID`;
var sql_alter_src_wrk_tbl= `alter table ${src_epe_transaction_item} cluster by (TRANSACTION_INTEGRATION_ID)`;
 try {
        //snowflake.execute({ sqlText: sql_trunc_src_wrk_tbl }); 
        snowflake.execute({ sqlText: sql_crt_src_wrk_tbl });
		//snowflake.execute({ sqlText: sql_alter_src_wrk_tbl });
       
    } catch (err)  {	    
        throw `Creation of Source Work table ${src_clips_tbl} Failed with error: ${err}`;   // Return a error message.
    }


var sql_trunc_src_wrk_tbl = `delete from ${src_clips_tbl} where clip_dt >current_date-2`;
	var sql_crt_src_wrk_tbl = `INSERT INTO ${src_clips_tbl} 
SELECT CH.DW_CURRENT_VERSION_IND,CH.CLIP_SEQUENCE_ID, 
CH.HOUSEHOLD_ID, CH.CUSTOMER_GUID, CH.RETAIL_STORE_ID,CD.CLIP_ID,
CD.CLIP_TS,CD.OFFER_ID,to_date(CD.CLIP_TS) as clip_dt
FROM 
(
    SELECT 
	DW_CURRENT_VERSION_IND,
	CLIP_SEQUENCE_ID, 
	HOUSEHOLD_ID, 
	CUSTOMER_GUID, 
	RETAIL_STORE_ID
    from  ${views_db}.DW_VIEWS.CLIP_HEADER_LST  
    WHERE DW_CURRENT_VERSION_IND = TRUE -- and household_id=290074704837
    AND DW_LOGICAL_DELETE_IND = FALSE 
    QUALIFY ROW_NUMBER() OVER(PARTITION BY HOUSEHOLD_ID,CUSTOMER_GUID,CLUB_CARD_NBR, RETAIL_STORE_ID ORDER BY DW_CREATE_TS DESC)= 1
)CH
    INNER JOIN 
(
SELECT 
OFFER_ID,
CLIP_SEQUENCE_ID,
CLIP_ID,
CLIP_TS  
from ${views_db}.DW_VIEWS.CLIP_DETAILS_LST 
WHERE CLIP_TYPE_CD = 'C' AND  DW_CURRENT_VERSION_IND = TRUE  and clip_ts >coalesce((select max(clip_ts) as clip_ts from ${src_clips_tbl} where clip_ts is not null),'1900-01-01')
  AND DW_LOGICAL_DELETE_IND = FALSE 
)CD
ON CD.CLIP_SEQUENCE_ID = CH.CLIP_SEQUENCE_ID
QUALIFY ROW_NUMBER() OVER(PARTITION BY HOUSEHOLD_ID,OFFER_ID ORDER BY CLIP_ID ASC)= 1
order by to_date(CD.CLIP_TS),CH.HOUSEHOLD_ID,CD.OFFER_ID
`;
    try {
	     //snowflake.execute({ sqlText: sql_trunc_src_wrk_tbl});
        //snowflake.execute({ sqlText: sql_crt_src_wrk_tbl });
       
    } catch (err)  {	    
        throw `Creation of Source Work table ${src_clips_tbl} Failed with error: ${err}`;   // Return a error message.
    }
	
	
	
	var sql_trunc_src_wrk_tbl = `TRUNCATE TABLE ${src_retail_store_tbl}`;
// persist stream data in work table for the current transaction, includes data from previous failed run
	var sql_crt_src_wrk_tbl = `CREATE OR REPLACE TRANSIENT TABLE ${src_retail_store_tbl} AS
    SELECT 
	DISTINCT BANNER_NM,
	TRY_TO_NUMERIC(FACILITY_NBR) AS FACILITY_NBR 
	FROM  ${views_db}.DW_VIEWS.RETAIL_STORE
	WHERE 
	DW_CURRENT_VERSION_IND = TRUE 
	AND DW_LOGICAL_DELETE_IND = FALSE 
	and FACILITY_NBR is not null
	order by TRY_TO_NUMERIC(FACILITY_NBR)`;
	
	var sql_alter_src_wrk_tbl= `alter table ${src_retail_store_tbl} cluster by (FACILITY_NBR)`;
    
	try {
	    //snowflake.execute({ sqlText: sql_trunc_src_wrk_tbl });
        snowflake.execute({ sqlText: sql_crt_src_wrk_tbl });
		//snowflake.execute({ sqlText: sql_alter_src_wrk_tbl });
    } catch (err)  {	    
        throw `Creation of Source Work table ${src_retail_store_tbl} Failed with error: ${err}`;   // Return a error message.
    }
	
		


// query to load rerun queue table when encountered a failure
//var sql_ins_rerun_tbl = `CREATE OR REPLACE TABLE ${src_rerun_tbl} AS SELECT * FROM ${src_wrk_tbl}`;

var Trunc_tgt_wrk_tbl=`Truncate table ${tgt_wrk_tbl} `;
var create_tgt_wrk_table = `CREATE OR REPLACE TRANSIENT TABLE ${tgt_wrk_tbl} AS
with REDEMPTION_PAYLOAD as
(
SELECT
KEY
,PAYLOAD
FROM (
SELECT
KEY
,PAYLOAD
FROM ((
SELECT
KEY
,to_json(object_construct(
'identity',object_construct('identityValue',TO_VARCHAR(HOUSEHOLD_ID))
,'header',object_construct(
'txndatetm',TXN_TM
,'key',KEY
,'redemptionType','Rebate_Redemption'
)
,'basket',object_construct(
'type','STANDARD',
'contents',UPC_ID)
,'reference',TO_VARCHAR(ORDER_ID)||'_REBATE'
,'location', object_construct(
'incomingIdentifier',TO_VARCHAR(STORE_ID)
,'parentIncomingIdentifier',TO_VARCHAR(BANNER_NM)
)
,'operations',array_agg(distinct(object_construct(
'token',TO_VARCHAR(CLIP_ID)
,'operationType','redeem'
,'amount',MKDN_AMT
,'transactionDetails',object_construct('merchant_store_id',TO_VARCHAR(STORE_ID)
,'merchant_store_parent_id',TO_VARCHAR(BANNER_NM)
)
,'rebateemailaddress',REBATEEMAILADDRESS
)
)))
) AS PAYLOAD
from (
SELECT DISTINCT
ORDER_ID
,TXN_TM
,STORE_ID
,UPC_ID
,MKDN_AMT
,OMS_OFFER_ID
,HOUSEHOLD_ID
,case when LOWER(BANNER_NM)='star' then 'starmarket' else LOWER(BANNER_NM) end AS BANNER_NM 
,CLIP_ID
,CLIP_TS
,REBATEEMAILADDRESS
,src.KEY
FROM(
SELECT 
HDR.ORDER_ID
,HDR.TXN_TM
,HDR.STORE_ID
,UPC1.UPC   AS UPC_ID
,0 AS MKDN_AMT
,OMF.OMS_OFFER_ID
,clip.HOUSEHOLD_ID
,LOWER (replace(replace(replace( replace (RS.BANNER_NM, '\\'', ''),' ',''),'-',''),'*','')) AS BANNER_NM
,clip.CLIP_ID
,clip.CLIP_TS
,CDC.DIGITAL_ADDRESS_TXT as REBATEEMAILADDRESS
,clip.HOUSEHOLD_ID||'|'||HDR.STORE_ID||'|'||HDR.ORDER_ID||'|'||TO_VARCHAR(TO_TIMESTAMP_TZ(HDR.TXN_TM),'YYYYMMDDHH24MISS') AS KEY
FROM
${src_epe_transaction_header} HDR
JOIN  
${src_epe_transaction_header_savings_dly_rebate} ETHS  
ON HDR.TRANSACTION_INTEGRATION_ID = ETHS.TRANSACTION_INTEGRATION_ID
 JOIN  (SELECT EXTERNAL_OFFER_ID,PROGRAM_CD,Offer_Prototype_Cd,
 OMS_OFFER_ID from  ${views_db}.DW_VIEWS.OMS_OFFER   
WHERE DW_CURRENT_VERSION_IND=TRUE
AND DW_LOGICAL_DELETE_IND = FALSE
ANd PROGRAM_CD = 'MF'
)OMF
  ON OMF.EXTERNAL_OFFER_ID = ETHS.EXTERNAL_OFFER_ID
JOIN  ${src_clips_tbl} CLIP
ON CLIP.HOUSEHOLD_ID = HDR.HOUSEHOLD_ID 
AND CLIP.OFFER_ID = OMF.OMS_OFFER_ID 
JOIN ${src_customer_alternate}  CA
ON CLIP.CUSTOMER_GUID =CA.ALTERNATE_ID_TXT
join  ${views_db}.DW_VIEWS.CUSTOMER_DIGITAL_CONTACT CDC
ON CDC.RETAIL_CUSTOMER_UUID =CA.RETAIL_CUSTOMER_UUID
AND CDC.DW_CURRENT_VERSION_IND = TRUE
AND CDC.DW_LOGICAL_DELETE_IND = FALSE
LEFT OUTER JOIN ${src_retail_store_tbl} rs
ON RS.FACILITY_NBR = HDR.STORE_NBR
JOIN
${src_epe_transaction_item} UPC1
on UPC1.TRANSACTION_INTEGRATION_ID = HDR.TRANSACTION_INTEGRATION_ID  
)src
LEFT JOIN
(
SELECT split_part(k.key, '|', 3) as TGT_ORDER_ID FROM 
"${views_db}"."DW_VIEWS"."KAFKAOUTQUEUE" K
WHERE K.DW_SOURCE_CREATE_NM in ('Rebate_Redemption','Rebate_Redemption_Reprocess','Rebate_Redemption_Manual') 
and K.TOPIC='EDDW_C02_ECOMM_CouponService' 
and K.CREATETIME>'2022-05-17 00:00:00'
) TGT 
ON src.ORDER_ID = TGT.TGT_ORDER_ID
WHERE TGT.TGT_ORDER_ID IS NULL
)
GROUP BY UPC_ID,HOUSEHOLD_ID,STORE_ID,BANNER_NM,TXN_TM,ORDER_ID,KEY
))Final))
SELECT KEY,PAYLOAD FROM REDEMPTION_PAYLOAD	
`;
try {
//snowflake.execute ({sqlText: Trunc_tgt_wrk_tbl});
snowflake.execute ({sqlText: create_tgt_wrk_table});
	}
    catch (err) { 
	   // snowflake.execute ( {sqlText: sql_ins_rerun_tbl} );
        throw `Creation of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
				}   

 
// Transaction for Updates, Insert begins           
    var sql_begin = "BEGIN"	
						
// Processing Inserts
	var sql_inserts = `INSERT INTO ${tgt_tbl} 
						(
						 MSG_SEQ
						,TOPIC
						,KEY
						,PAYLOAD
						,STATUS
						,CREATETIME
						,DW_SOURCE_CREATE_NM
						)
						SELECT
						 KAFKAOUTQUEUE_SEQ.NEXTVAL As MSG_SEQ
						,'${tpc_nm}' As Topic
						,KEY
						,PAYLOAD
						,0
						,CURRENT_TIMESTAMP As CREATETIME
						,'${src_nm}' As DW_SOURCE_CREATE_NM
						from ${tgt_wrk_tbl}`;
						
var sql_commit = "COMMIT"
var sql_rollback = "ROLLBACK"
try {
        snowflake.execute({sqlText: sql_begin});
        snowflake.execute({sqlText: sql_inserts});
        snowflake.execute({sqlText: sql_commit});
	}
	
    catch (err)  {
        snowflake.execute({sqlText: sql_rollback });
       // snowflake.execute({sqlText: sql_ins_rerun_tbl});        
		throw `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
        }	
	
// ************** Load for Redemption table ENDs *****************

$$;

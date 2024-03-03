--liquibase formatted sql
--changeset SYSTEM:SP_Rebate_Redemption_Neptune_To_KafkaOutQueue runOnChange:true splitStatements:false OBJECT_TYPE:SP
USE DATABASE EDM_SANDBOX_PRD;
USE SCHEMA CORE_TECH;

CREATE OR REPLACE PROCEDURE SP_Rebate_Redemption_Neptune_To_KafkaOutQueue(SRC_TBL VARCHAR,CNF_DB VARCHAR,VIEWS_DB VARCHAR,C_STAGE VARCHAR) 
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
		var src_nm = 'Rebate_Redemption';
		
		var tgt_tbl = `${cnf_db}.DW_DCAT.KAFKAOUTQUEUE`;	
		var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.KAFKA_OUT_QUEUE_REBATE_REDEMPTION_WRK`;
		var src_wrk_tbl = `${cnf_db}.${wrk_schema}.${red_tbl}_src_WRK`;
		var src_rerun_tbl = `${cnf_db}.${wrk_schema}.${red_tbl}_Rerun`;
        var src_clips_tbl = `${cnf_db}.${wrk_schema}.clip_details_tmp`;		 	
		var src_retail_store_tbl = `${cnf_db}.${wrk_schema}.retail_store_tmp`;
        var src_epe_transaction_tbl = `${cnf_db}.${wrk_schema}.EPE_TRANSACTION_ITEM_REBATE_DLY_TMP`;


// persist  clips data in work table for the current transaction, includes data from previous failed run

   var sql_trunc_src_wrk_tbl= `truncate table ${src_epe_transaction_tbl}`;
 try {
        snowflake.execute({ sqlText: sql_trunc_src_wrk_tbl }); 
      //  snowflake.execute({ sqlText: sql_crt_src_wrk_tbl });
       
    } catch (err)  {	    
        throw `Creation of Source Work table ${src_clips_tbl} Failed with error: ${err}`;   // Return a error message.
    }
    
    
// persist  clips data in work table for the current transaction, includes data from previous failed run

   var sql_trunc_src_wrk_tbl= `insert into ${src_epe_transaction_tbl} SELECT ARRAY_AGG(UPC) AS UPC,TRANSACTION_INTEGRATION_ID FROM(
SELECT 
object_construct('upc',TO_VARCHAR(ETI1.UPC_NBR),
'itemUnitCount',ETI1.ITEM_UNIT_QTY) AS UPC,
ETI1.TRANSACTION_INTEGRATION_ID
from ${views_db}.DW_VIEWS.EPE_TRANSACTION_ITEM ETI1 
WHERE ETI1.DW_CREATE_TS>=(CURRENT_DATE-5)
AND ETI1.DW_CURRENT_VERSION_IND = TRUE
AND ETI1.DW_LOGICAL_DELETE_IND = FALSE
)GROUP BY TRANSACTION_INTEGRATION_ID`;
 try {
        snowflake.execute({ sqlText: sql_trunc_src_wrk_tbl }); 
      //  snowflake.execute({ sqlText: sql_crt_src_wrk_tbl });
       
    } catch (err)  {	    
        throw `Creation of Source Work table ${src_clips_tbl} Failed with error: ${err}`;   // Return a error message.
    }
    

// persist  clips data in work table for the current transaction, includes data from previous failed run

   var sql_trunc_src_wrk_tbl= `alter table ${src_clips_tbl} cluster by (clip_Ts)`;
 try {
        snowflake.execute({ sqlText: sql_trunc_src_wrk_tbl }); 
      //  snowflake.execute({ sqlText: sql_crt_src_wrk_tbl });
       
    } catch (err)  {	    
        throw `Creation of Source Work table ${src_clips_tbl} Failed with error: ${err}`;   // Return a error message.
    }

        var sql_trunc_src_wrk_tbl= `delete from ${src_clips_tbl} where clip_ts >current_date-2`;
	var sql_crt_src_wrk_tbl = `INSERT INTO ${src_clips_tbl} 
SELECT CH.DW_CURRENT_VERSION_IND,CH.CLIP_SEQUENCE_ID, 
CH.HOUSEHOLD_ID, CH.CUSTOMER_GUID, CH.RETAIL_STORE_ID,CD.CLIP_ID,
CD.CLIP_TS,CD.OFFER_ID
FROM 
(
    SELECT 
	DW_CURRENT_VERSION_IND,
	CLIP_SEQUENCE_ID, 
	HOUSEHOLD_ID, 
	CUSTOMER_GUID, 
	RETAIL_STORE_ID
    from  ${views_db}.DW_VIEWS.CLIP_HEADER 
    WHERE DW_CURRENT_VERSION_IND = TRUE 
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
from ${views_db}.DW_VIEWS.CLIP_DETAILS 
WHERE CLIP_TYPE_CD = 'C' AND  DW_CURRENT_VERSION_IND = TRUE 
  AND DW_LOGICAL_DELETE_IND = FALSE  and clip_ts >coalesce((select max(clip_ts) as clip_ts from ${src_clips_tbl} where clip_ts is not null),'1900-01-01')
)CD
ON CD.CLIP_SEQUENCE_ID = CH.CLIP_SEQUENCE_ID
QUALIFY ROW_NUMBER() OVER(PARTITION BY HOUSEHOLD_ID,OFFER_ID ORDER BY CLIP_ID ASC)= 1
`;
    try {
        snowflake.execute({ sqlText: sql_trunc_src_wrk_tbl }); 
        snowflake.execute({ sqlText: sql_crt_src_wrk_tbl });
       
    } catch (err)  {	    
        throw `Creation of Source Work table ${src_clips_tbl} Failed with error: ${err}`;   // Return a error message.
    }
	
   var sql_trunc_src_wrk_tbl= `alter table ${src_clips_tbl} cluster by (household_id,offer_id)`;
 try {
        snowflake.execute({ sqlText: sql_trunc_src_wrk_tbl }); 
      //  snowflake.execute({ sqlText: sql_crt_src_wrk_tbl });
       
    } catch (err)  {	    
        throw `Creation of Source Work table ${src_clips_tbl} Failed with error: ${err}`;   // Return a error message.
    }	
// persist stream data in work table for the current transaction, includes data from previous failed run
	var sql_trunc_src_wrk_tbl = `TRUNCATE TABLE ${src_retail_store_tbl} `;
	var sql_crt_src_wrk_tbl = `INSERT INTO ${src_retail_store_tbl} 
    SELECT 
	DISTINCT BANNER_NM,
	TRY_TO_NUMERIC(FACILITY_NBR) AS FACILITY_NBR 
	FROM  ${views_db}.DW_VIEWS.RETAIL_STORE
	WHERE 
	DW_CURRENT_VERSION_IND = TRUE 
	AND DW_LOGICAL_DELETE_IND = FALSE 
	and FACILITY_NBR is not null`;
    
	try {
	 snowflake.execute({ sqlText: sql_trunc_src_wrk_tbl });
        snowflake.execute({ sqlText: sql_crt_src_wrk_tbl });
    } catch (err)  {	    
        throw `Creation of Source Work table ${src_retail_store_tbl} Failed with error: ${err}`;   // Return a error message.
    }
	
		
// persist stream data in work table for the current transaction, includes data from previous failed run
	var sql_trunc_src_wrk_tbl = `TRUNCATE TABLE ${src_wrk_tbl} `;
	var sql_crt_src_wrk_tbl = `INSERT INTO ${src_wrk_tbl} 
    SELECT ORDER_ID,TRANSACTION_TS,HOUSEHOLD_ID,STORE_NBR,TRANSACTION_INTEGRATION_ID,STATUS_CD,DW_CURRENT_VERSION_IND,DW_LOGICAL_DELETE_IND,METADATA$ACTION,METADATA$ISUPDATE,METADATA$ROW_ID
       FROM ${src_tbl}
    UNION ALL
    SELECT ORDER_ID,TRANSACTION_TS,HOUSEHOLD_ID,STORE_NBR,TRANSACTION_INTEGRATION_ID,STATUS_CD,DW_CURRENT_VERSION_IND,DW_LOGICAL_DELETE_IND,METADATA$ACTION,METADATA$ISUPDATE,METADATA$ROW_ID
       FROM ${src_rerun_tbl}`;
    try {
	snowflake.execute({ sqlText: sql_trunc_src_wrk_tbl });
        snowflake.execute({ sqlText: sql_crt_src_wrk_tbl });
    } catch (err)  {	    
        throw `Creation of Source Work table ${src_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
    }

// query to load rerun queue table when encountered a failure

var sql_trunc_rerun_tbl = `Truncate TABLE ${src_rerun_tbl} `;
var sql_ins_rerun_tbl = `INSERT INTO ${src_rerun_tbl} SELECT * FROM ${src_wrk_tbl}`;

var trunc_tgt_wrk_table = `TRUNCATE TABLE ${tgt_wrk_tbl}`;
var create_tgt_wrk_table = `INSERT INTO ${tgt_wrk_tbl} 
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
(
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
FROM  ${src_wrk_tbl}
WHERE   STATUS_CD = 'COMPLETED' AND DW_CURRENT_VERSION_IND = true AND DW_LOGICAL_DELETE_IND = false 
 )T1 WHERE RNK=1
) HDR
JOIN  
(
SELECT 
EXTERNAL_OFFER_ID,
TRANSACTION_INTEGRATION_ID 
from ${views_db}.DW_VIEWS."EPE_TRANSACTION_HEADER_SAVINGS" 
WHERE DW_CURRENT_VERSION_IND = TRUE
AND DW_LOGICAL_DELETE_IND = FALSE 
AND DW_CREATE_TS>=(CURRENT_DATE-5)
--AND PROGRAM_CD = 'MF'
AND SAVINGS_CATEGORY_NM = '011-Non Discount Offers'      
) ETHS  
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
JOIN (
SELECT 
RETAIL_CUSTOMER_UUID,
ALTERNATE_ID_TXT  
FROM  ${views_db}.DW_VIEWS.CUSTOMER_ALTERNATE
WHERE DW_CURRENT_VERSION_IND = TRUE AND DW_LOGICAL_DELETE_IND =FALSE
QUALIFY ROW_NUMBER() OVER (
PARTITION BY ALTERNATE_ID_TXT
ORDER BY(SOURCE_LAST_UPDATE_TS) DESC)=1     
) CA
ON CLIP.CUSTOMER_GUID =CA.ALTERNATE_ID_TXT
join  ${views_db}.DW_VIEWS.CUSTOMER_DIGITAL_CONTACT CDC
ON CDC.RETAIL_CUSTOMER_UUID =CA.RETAIL_CUSTOMER_UUID
AND CDC.DW_CURRENT_VERSION_IND = TRUE
AND CDC.DW_LOGICAL_DELETE_IND = FALSE
LEFT OUTER JOIN ${src_retail_store_tbl} rs
ON RS.FACILITY_NBR = HDR.STORE_NBR
JOIN
${src_epe_transaction_tbl} UPC1
on UPC1.TRANSACTION_INTEGRATION_ID = HDR.TRANSACTION_INTEGRATION_ID  
)src
LEFT JOIN
(
SELECT 
KEY 
FROM 
${cnf_db}.DW_DCAT.KAFKAOUTQUEUE 
where DW_SOURCE_CREATE_NM in ('Rebate_Redemption' ,'Rebate_Redemption_Reprocess','Rebate_Redemption_Manual')
and TOPIC='EDDW_C02_ECOMM_CouponService' 
and CREATETIME>'2022-05-17 00:00:00'
) TGT
ON SRC.KEY = TGT.KEY
WHERE TGT.KEY IS NULL
)
GROUP BY UPC_ID,HOUSEHOLD_ID,STORE_ID,BANNER_NM,TXN_TM,ORDER_ID,KEY
))Final))
SELECT KEY,PAYLOAD FROM REDEMPTION_PAYLOAD	
`;
try {
snowflake.execute ({sqlText: trunc_tgt_wrk_table });
snowflake.execute ({sqlText: create_tgt_wrk_table});
	}
    catch (err) { 
            snowflake.execute ( {sqlText:sql_trunc_rerun_tbl } );
	    snowflake.execute ( {sqlText: sql_ins_rerun_tbl} );
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
	snowflake.execute ( {sqlText:sql_trunc_rerun_tbl } );
        snowflake.execute({sqlText: sql_ins_rerun_tbl});        
		throw `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
        }	
	
// ************** Load for Redemption table ENDs *****************
$$;

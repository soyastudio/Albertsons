--liquibase formatted sql
--changeset SYSTEM:SP_Digital_Redemption_Neptune_To_KafkaOutQueue_reprocess runOnChange:true splitStatements:false OBJECT_TYPE:SP
USE DATABASE <<EDM_DB_NAME>>;
USE SCHEMA DW_APPL;

CREATE OR REPLACE PROCEDURE SP_Digital_Redemption_Neptune_To_KafkaOutQueue_reprocess(SRC_TBL VARCHAR,CNF_DB VARCHAR,VIEWS_DB VARCHAR,C_STAGE VARCHAR,PRC VARCHAR) 
RETURNS STRING
LANGUAGE JAVASCRIPT
AS 
$$


		var src_tbl = SRC_TBL;
		var cnf_db = CNF_DB;
		var views_db = VIEWS_DB;
		var wrk_schema = C_STAGE;
		var red_tbl='TXN_FACTS_DIGITAL_REDEMPTION_NEPTUNE'; 
        var kafka_retry_tbl='COUPON_SRV_RESPONSE_REPROCESS';
		var tpc_nm='EDDW_C02_ECOMM_CouponService';
		var src_nm='Digital_Redemption_Reprocess';
		var reprocess = PRC;
		var tgt_tbl=`${cnf_db}.DW_DCAT.KAFKAOUTQUEUE`;	
		var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.KAFKA_OUT_QUEUE_DIGITAL_REDEMPTION_REPROCESS_WRK`;
		//var src_wrk_tbl = `${cnf_db}.${wrk_schema}.${red_tbl}_src_WRK`;
		//var src_rerun_tbl = `${cnf_db}.${wrk_schema}.${red_tbl}_Rerun`;	 	
		var src_clips_tbl = `${cnf_db}.${wrk_schema}.clip_details_tmp_digital_Reprocess`;	
		var src_clp_tbl = `${cnf_db}.${wrk_schema}.RETAIL_CUSTOMER_HH_LOYALTY_PROGRAM_REPROCESS`;
		


// Deletes Customer loyalty program  data in work table for the current transaction
	var sql_crt_src_wrk_tbl = `delete from ${src_clp_tbl} `;

try {
       
  snowflake.execute({ sqlText: sql_crt_src_wrk_tbl });
       
    } catch (err)  {	    
        return `Deletion of Source Work table ${src_clp_tbl} Failed with error: ${err}`;   
    }



// inserts  clips data in work table for the current transaction
	var sql_crt_src_wrk_tbl = `insert into ${src_clp_tbl} 
	select  distinct  HOUSEHOLD_ID,TRY_TO_NUMERIC(clp.LOYALTY_PROGRAM_CARD_NBR) as LOYALTY_PROGRAM_CARD_NBR 
		from  ${views_db}."DW_VIEWS"."RETAIL_CUSTOMER_HOUSEHOLD" rch inner join 
		${views_db}."DW_VIEWS"."CUSTOMER_LOYALTY_PROGRAM" clp 
		on
		rch.RETAIL_CUSTOMER_UUID = clp.RETAIL_CUSTOMER_UUID
		--and rch.STATUS_VALUE_TXT='100'
		and rch.DW_CURRENT_VERSION_IND=True
		and clp.DW_CURRENT_VERSION_IND=True`;

try {
       
  snowflake.execute({ sqlText: sql_crt_src_wrk_tbl });
       
    } catch (err)  {	    
        return `Insertion of Source Work table ${src_clp_tbl} Failed with error: ${err}`;   
    }


// persist  clips data in work table for the current transaction, includes data from previous failed run

   var sql_trunc_src_wrk_tbl= `alter table ${src_clips_tbl} cluster by (clip_Ts)`;
 try {
        snowflake.execute({ sqlText: sql_trunc_src_wrk_tbl }); 
      //  snowflake.execute({ sqlText: sql_crt_src_wrk_tbl });
       
    } catch (err)  {	    
        throw `Creation of Source Work table ${src_clips_tbl} Failed with error: ${err}`;   // Return a error message.
    }

        
// Deletes clips data in work table for the current transaction
	var sql_crt_src_wrk_tbl = `delete from ${src_clips_tbl} where clip_ts >current_date-2`;

try {
       
  snowflake.execute({ sqlText: sql_crt_src_wrk_tbl });
       
    } catch (err)  {	    
        return `Deletion of Source Work table ${src_clips_tbl} Failed with error: ${err}`;   
    }

  
  
// persist  clips data in work table for the current transaction, includes data from previous failed run
	var sql_crt_src_wrk_tbl = `Insert into ${src_clips_tbl}
SELECT CH.DW_CURRENT_VERSION_IND,CH.CLIP_SEQUENCE_ID, 
CH.HOUSEHOLD_ID, CH.CUSTOMER_GUID, CH.RETAIL_STORE_ID,CD.CLIP_ID,
CD.CLIP_TS,CD.OFFER_ID,CH.CLUB_CARD_NBR,CH.BANNER_NM
FROM 
(
    SELECT 
	DW_CURRENT_VERSION_IND,
	CLIP_SEQUENCE_ID, 
	HOUSEHOLD_ID, 
	CUSTOMER_GUID, 
	RETAIL_STORE_ID,
	CLUB_CARD_NBR,
  BANNER_NM
    from  ${views_db}.DW_VIEWS.CLIP_HEADER 
    WHERE DW_CURRENT_VERSION_IND = TRUE 
    AND DW_LOGICAL_DELETE_IND = FALSE 
 --  and household_id=990052745417
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
   
  snowflake.execute({ sqlText: sql_crt_src_wrk_tbl });
       
    } catch (err)  {	    
        return `Creation of Source Work table ${src_clips_tbl} Failed with error: ${err}`;  
    }

var sql_trunc_src_wrk_tbl= `alter table ${src_clips_tbl} cluster by (household_id,offer_id)`;
 try {
        snowflake.execute({ sqlText: sql_trunc_src_wrk_tbl }); 
      //  snowflake.execute({ sqlText: sql_crt_src_wrk_tbl });
       
    } catch (err)  {	    
        throw `Creation of Source Work table ${src_clips_tbl} Failed with error: ${err}`;   // Return a error message.
    }	


// persist  clips data in work table for the current transaction, includes data from previous failed run
/*	var sql_crt_src_wrk_tbl = `truncate table ${src_wrk_tbl}`;
try {
   
  snowflake.execute({ sqlText: sql_crt_src_wrk_tbl });
       
    } catch (err)  {	    
        return `Deletion of Source Work table ${src_wrk_tbl} Failed with error: ${err}`;  
    }
	*/



// persist  clips data in work table for the current transaction, includes data from previous failed run
	var sql_crt_src_wrk_tbl = `truncate table ${tgt_wrk_tbl}`;
try {
   
  snowflake.execute({ sqlText: sql_crt_src_wrk_tbl });
       
    } catch (err)  {	    
        return `Deletion of Source Work table ${tgt_wrk_tbl} Failed with error: ${err}`;  
    }
	

var create_tgt_wrk_table = `insert into ${tgt_wrk_tbl}  
								with  REDEMPTION_PAYLOAD as
(
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
							,'redemptionType','Digital_Redemption'
							  )
	,'basket',object_construct(
                              'type','STANDARD',
                              'contents',array_agg(object_construct('upc',TO_VARCHAR(UPC_ID),
																	'itemUnitCount',item_qty)))
	,'reference',TO_VARCHAR(TXN_ID)||'_DIGITAL'
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
											)
											)))
 ) AS PAYLOAD
from ( 
SELECT DISTINCT
	 TXN_ID
	,TXN_TM
	,STORE_ID
	,UPC_ID
	,MKDN_AMT
	,ITEM_QTY
	,OMS_OFFER_ID
	,HOUSEHOLD_ID
	,case when LOWER(BANNER_NM)='star' then 'starmarket' else LOWER(BANNER_NM) end AS BANNER_NM
	,CLIP_ID
	,CLIP_TS
	,src.KEY
	FROM(
	SELECT DISTINCT
	 HDR.TXN_ID
	,HDR.TXN_TM
	,HDR.STORE_ID
	,cast(TF1.UPC_ID as string) AS UPC_ID
	,ABS((TF1.MKDN_QTY*TF1.MKDN_AMT)*100) AS MKDN_AMT
	,TF1.MKDN_QTY as ITEM_QTY
	,OMF.OMS_OFFER_ID
	,coalesce(CLIPS_HDR_DTS.HOUSEHOLD_ID,CLIPS_HDR_DTS_NONCLP.HOUSEHOLD_ID) as HOUSEHOLD_ID
	,LOWER (replace(replace(replace( replace (str.BANNER_NM, '\\'', ''),' ',''),'-',''),'*','')) AS BANNER_NM
	,coalesce(CLIPS_HDR_DTS.CLIP_ID,CLIPS_HDR_DTS_NONCLP.CLIP_ID) as CLIP_ID
	,coalesce(CLIPS_HDR_DTS.CLIP_TS,CLIPS_HDR_DTS_NONCLP.CLIP_TS) as CLIP_TS
	,coalesce(CLIPS_HDR_DTS.HOUSEHOLD_ID,CLIPS_HDR_DTS_NONCLP.HOUSEHOLD_ID)||'|'||HDR.STORE_ID||'|'||HDR.TXN_ID||'|'||TO_VARCHAR(to_timestamp_tz(HDR.TXN_TM),'YYYYMMDDHH24MISS') AS KEY
	FROM (select distinct TXN_ID,TXN_DTE from  ${views_db}.DW_EDW_VIEWS.TXN_FACTS  Where  REV_DTL_SUBTYPE_ID=6 and TXN_DTE >= current_date-${reprocess} and TXN_DTE<current_date) TFS 
		JOIN (select * from ${views_db}.DW_EDW_VIEWS.TXN_HDR where TXN_DTE >= (CURRENT_DATE - ${reprocess}) and TXN_DTE<current_date) HDR
		ON TFS.TXN_ID = HDR.TXN_ID
		AND TFS.TXN_DTE = HDR.TXN_DTE
		JOIN (select * from ${views_db}.DW_EDW_VIEWS.TXN_FACTS where MKDN_QTY >0 and TXN_DTE >= (CURRENT_DATE - ${reprocess}) and TXN_DTE<current_date) TF1
		ON TF1.TXN_ID = HDR.TXN_ID
		AND TF1.TXN_DTE = HDR.TXN_DTE
		AND TF1.REV_DTL_SUBTYPE_ID=6		
		JOIN ${views_db}.DW_VIEWS.OMS_OFFER OMF
		ON OMF.OMS_OFFER_ID = TF1.OMS_OFFER_ID
		AND OMF.PROGRAM_CD = 'MF'
		AND OMF.DW_CURRENT_VERSION_IND=TRUE
		AND OMF.DW_LOGICAL_DELETE_IND = FALSE
		
	left outer		join ${src_clp_tbl} CLP
		on CLP.LOYALTY_PROGRAM_CARD_NBR=hdr.CARD_NBR
		
	left outer	JOIN  ${src_clips_tbl} CLIPS_HDR_DTS
		ON CLP.HOUSEHOLD_ID = CLIPS_HDR_DTS.HOUSEHOLD_ID
		--HDR.CARD_NBR = CLIPS_HDR_DTS.CLUB_CARD_NBR
		AND OMF.OMS_OFFER_ID = CLIPS_HDR_DTS.OFFER_ID
		AND CLP.LOYALTY_PROGRAM_CARD_NBR is not null

left outer JOIN  ${src_clips_tbl} CLIPS_HDR_DTS_NONCLP
		ON --CLP.HOUSEHOLD_ID = CLIPS_HDR_DTS_NONCLP.HOUSEHOLD_ID
		HDR.CARD_NBR = TRY_TO_NUMERIC(CLIPS_HDR_DTS_NONCLP.CLUB_CARD_NBR)
		AND OMF.OMS_OFFER_ID = CLIPS_HDR_DTS_NONCLP.OFFER_ID
         AND CLP.LOYALTY_PROGRAM_CARD_NBR is  null
		  
		
	left outer join  ${views_db}."DW_VIEWS"."RETAIL_STORE" str
      on
      TRY_TO_NUMBER(tf1.store_id) = TRY_TO_NUMBER(str.FACILITY_NBR)
    and  str.DW_CURRENT_VERSION_IND = TRUE
      AND str.dw_logical_delete_ind = FALSE
	where (CLIPS_HDR_DTS.HOUSEHOLD_ID is not null or CLIPS_HDR_DTS_NONCLP.HOUSEHOLD_ID is not null) 
	)src
	
	  
	LEFT JOIN 
	(SELECT KEY FROM "${views_db}"."DW_VIEWS".KAFKAOUTQUEUE where 
	 lower(DW_SOURCE_CREATE_NM) in ('digital_redemption','digital_redemption_reprocess','digital_redemption_manual') 
and TOPIC='EDDW_C02_ECOMM_CouponService' 
and CREATETIME>'2022-02-18 00:00:00'
	)tgt
	ON src.KEY = tgt.KEY	
	WHERE tgt.KEY IS NULL
)
GROUP BY HOUSEHOLD_ID,STORE_ID,BANNER_NM,TXN_TM,TXN_ID,KEY
)))
SELECT KEY,PAYLOAD FROM REDEMPTION_PAYLOAD
`;

try {

snowflake.execute ({sqlText: create_tgt_wrk_table});
	}
    catch (err) { 
	    
        return `Creation of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   
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
        return `Loading of table ${tgt_tbl} Failed with error: ${err}` ;    
        }	
	
// ************** Load for Redemption table ENDs *****************

$$;

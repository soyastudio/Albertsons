--liquibase formatted sql
--changeset SYSTEM:SP_ONETAG_TO_BIM_LOAD_IMPRESSIONS runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_CONFIRMED_<<ENV>>;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE SP_ONETAG_TO_BIM_LOAD_IMPRESSIONS (SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_USER_ACT VARCHAR, C_PROD VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

	// * ***********************************************************************
	// *
	// * Name:			SP_ONETAG_TO_BIM_LOAD_CLICK_STREAM_IMPRESSIONS
	// *
	// * Description:	Child Stored Proc to load data to CLICK_STREAM_IMPRESSIONS (BIM) table
	// *
	// * History
	// *
	// * Version	Date(DD/MM/YYYY)	Author			DM_VERSION		Revision History
	// * --------	----------------	------------	------------	-------------------
	// * 1.0		12-08-2023			Madhu			1.0.1			Initial Version
	// *
	// * ***********************************************************************

    var src_wrk_tbl = SRC_WRK_TBL;
    var WRK_SCHEMA  = C_STAGE;
    var lkp_tbl		= `${CNF_DB}.${C_PROD}.DIGITAL_PRODUCT_MASTER`;
    var tgt_tbl		= `${CNF_DB}.${C_USER_ACT}.CUSTOMER_SESSION_IMPRESSION`;
	var tgt_wrk_tbl	= `${CNF_DB}.${WRK_SCHEMA}.CUSTOMER_SESSION_IMPRESSION_WRK`;

	// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.
	// ************** Load for CUSTOMER_SESSION_IMPRESSION table BEGIN *****************
	var sql_empty_wrk_tbl = `TRUNCATE TABLE ${tgt_wrk_tbl}`;

	try {
		snowflake.execute ({sqlText: sql_empty_wrk_tbl });
	} catch (err) {
		throw `Truncation of source work table ${tgt_wrk_tbl} Failed with error: ${err}`;
	}

	var sql_command =`INSERT INTO ${tgt_wrk_tbl}
	(
		EVENT_ID
		,EVENT_TS
		,BASE_PRODUCT_NBR
		,IMPRESSION_TYPE_CD
		,PRODUCT_FINDING_METHOD_DSC
		,ROW_LOCATION_CD
		,SLOT_LOCATION_CD
		,MODEL_ID
		,LIST_PRICE_AMT				
		,CAROUSEL_NM
		,PRODUCT_UNIT_CNT
		,BASE_PRODUCT_NBR_VALID_IND
		,Dw_Create_Ts
		,Dw_Logical_Delete_Ind
		,Dw_Checksum_Value_Txt
	)
SELECT DISTINCT
	     src.EVENT_ID
		,src.EVENT_TS
        ,src.BASE_PRODUCT_NBR
        ,src.IMPRESSION_TYPE_CD
        ,src.PRODUCT_FINDING_METHOD_DSC
        ,src.ROW_LOCATION_CD
        ,src.SLOT_LOCATION_CD
        ,src.MODEL_ID
        ,src.LIST_PRICE_AMT				
        ,src.CAROUSEL_NM
        ,src.PRODUCT_UNIT_CNT
		,case when src.BASE_PRODUCT_NBR=src.flkp_BASE_PRODUCT_NBR then 'TRUE' else 'FALSE' END As BASE_PRODUCT_NBR_VALID_IND
		,src.Dw_Create_Ts
		,src.Dw_Logical_Delete_Ind
		,src.DW_Checksum_Value_Txt
	FROM (
			SELECT src.*,flkp_BASE_PRODUCT_NBR,
					
					MD5(CONCAT(src.EVENT_ID,src.EVENT_TS,src.BASE_PRODUCT_NBR)) as DW_Checksum_Value_Txt
			FROM (
SELECT
                         EVENT_ID
						,EVENT_TS
						,BASE_PRODUCT_NBR
						,IMPRESSION_TYPE_CD
						,PRODUCT_FINDING_METHOD_DSC
						,ROW_LOCATION_CD
						,SLOT_LOCATION_CD
						,MODEL_ID
						,LIST_PRICE_AMT				
						,CAROUSEL_NM
						,PRODUCT_UNIT_CNT	
						,Current_Timestamp As DW_Create_Ts
						,FALSE AS DW_Logical_Delete_Ind
                        
					FROM (
							SELECT
								 EVENT_ID
								,EVENT_TS
								,BASE_PRODUCT_NBR
								,IMPRESSION_TYPE_CD
								,PRODUCT_FINDING_METHOD_DSC
								,ROW_LOCATION_CD
								,SLOT_LOCATION_CD
								,MODEL_ID
								,LIST_PRICE_AMT				
								,CAROUSEL_NM
								,PRODUCT_UNIT_CNT								
                                ,ROW_NUMBER() OVER ( PARTITION BY EVENT_ID, BASE_PRODUCT_NBR ORDER BY EVENT_TS DESC ) AS rn
							FROM (
								SELECT
								   EVENT_ID
								   ,eventtime as EVENT_TS
								   ,CAROUSEL_PID as BASE_PRODUCT_NBR
								   ,'product-impressions' as IMPRESSION_TYPE_CD
                                   ,CAROUSEL_PFM AS PRODUCT_FINDING_METHOD_DSC
				,COALESCE(REGEXP_SUBSTR(CAROUSEL_PFM, 'R[0-9]{2}'), REGEXP_SUBSTR(CAROUSEL_PFM, 'r[0-9]{2}'))  as ROW_LOCATION_CD
    ,COALESCE(REGEXP_SUBSTR(CAROUSEL_PFM, 'S[0-9]{2}'), REGEXP_SUBSTR(CAROUSEL_PFM, 's[0-9]{2}')) as SLOT_LOCATION_CD
                                   ,NULLIF(CAROUSEL_DETAIL,'') as MODEL_ID
                                   ,CAROUSEL_LP as LIST_PRICE_AMT
                                   ,CAROUSEL_SECTION as CAROUSEL_NM
                                   ,CAROUSEL_UNITS as PRODUCT_UNIT_CNT                      
                                   
								FROM ${src_wrk_tbl}
                                --FROM edm_confirmed_qa.dw_c_user_activity.ONE_TAG_CAROUSEL
							   WHERE EVENT_ID IS NOT NULL
							     AND EVENT_TS IS NOT NULL
		                         AND BASE_PRODUCT_NBR IS NOT NULL		                                  
														)
						)
					WHERE rn = 1
				) src
			LEFT JOIN
			(
				SELECT DISTINCT
					BASE_PRODUCT_NBR as flkp_BASE_PRODUCT_NBR
				FROM ${lkp_tbl}
                --FROM EDM_CONFIRMED_QA.DW_C_PRODUCT.DIGITAL_PRODUCT_MASTER
				WHERE DW_Logical_Delete_Ind = FALSE
			) flkp on flkp. flkp_BASE_PRODUCT_NBR = src. BASE_PRODUCT_NBR
		) src
		LEFT JOIN
		(
			SELECT
				 EVENT_ID
				,EVENT_TS
				,BASE_PRODUCT_NBR
				,IMPRESSION_TYPE_CD
				,PRODUCT_FINDING_METHOD_DSC
				,ROW_LOCATION_CD
				,SLOT_LOCATION_CD
				,MODEL_ID
				,LIST_PRICE_AMT				
				,CAROUSEL_NM
				,PRODUCT_UNIT_CNT
				,DW_Logical_Delete_Ind
				,DW_Checksum_Value_Txt
			FROM ${tgt_tbl}
           -- FROM EDM_CONFIRMED_QA.DW_C_USER_ACTIVITY.IMPRESSIONS
			WHERE DW_CURRENT_VERSION_IND = TRUE
		) AS tgt
		ON
				src.EVENT_ID = tgt.EVENT_ID
			AND src.EVENT_TS = tgt.EVENT_TS
			AND src.BASE_PRODUCT_NBR = tgt.BASE_PRODUCT_NBR
		WHERE
			tgt.EVENT_ID IS NULL
		AND tgt.EVENT_TS IS NULL
		AND tgt.BASE_PRODUCT_NBR IS NULL
		OR (
			tgt.DW_Checksum_Value_Txt <> src.DW_Checksum_Value_Txt
			OR
            tgt.DW_Logical_Delete_Ind <> src.DW_Logical_Delete_Ind
		)`;

	try {
		snowflake.execute ({sqlText: sql_command});
	} catch (err) {
		throw "Creation of CUSTOMER_SESSION_IMPRESSION_WRK work table "+ tgt_wrk_tbl +" Failed with error: " + err;
	}

	// transaction begins
	var sql_begin = "BEGIN"


	// Processing Inserts
	var sql_inserts = `INSERT INTO ${tgt_tbl}
	(
	  EVENT_ID
	 ,EVENT_TS
	 ,BASE_PRODUCT_NBR
	 ,IMPRESSION_TYPE_CD
	 ,PRODUCT_FINDING_METHOD_DSC
	 ,ROW_LOCATION_CD
	 ,SLOT_LOCATION_CD
	 ,MODEL_ID
	 ,LIST_PRICE_AMT				
	 ,CAROUSEL_NM
	 ,PRODUCT_UNIT_CNT
	 ,BASE_PRODUCT_NBR_VALID_IND
     ,Dw_Create_Ts
	 ,DW_LAST_UPDATE_TS
     ,Dw_Logical_Delete_Ind
     ,DW_SOURCE_CREATE_NM
	 ,DW_SOURCE_UPDATE_NM
     ,DW_CURRENT_VERSION_IND
	 ,DW_Checksum_Value_Txt
     )
     SELECT DISTINCT
		EVENT_ID
		,EVENT_TS
		,BASE_PRODUCT_NBR
		,IMPRESSION_TYPE_CD
		,PRODUCT_FINDING_METHOD_DSC
		,ROW_LOCATION_CD
		,SLOT_LOCATION_CD
		,MODEL_ID
		,LIST_PRICE_AMT				
		,CAROUSEL_NM
		,PRODUCT_UNIT_CNT
		,BASE_PRODUCT_NBR_VALID_IND
		,CURRENT_TIMESTAMP
		,Current_Timestamp
		,Dw_Logical_Delete_Ind
		,'OneTag' as DW_SOURCE_CREATE_NM
		,'OneTag' as DW_SOURCE_UPDATE_NM
		,TRUE as DW_CURRENT_VERSION_IND
		,DW_Checksum_Value_Txt
		FROM ${tgt_wrk_tbl}
   `;

	var sql_commit = "COMMIT"
	var sql_rollback = "ROLLBACK"

	try {
		snowflake.execute({sqlText: sql_begin});
		snowflake.execute({sqlText: sql_inserts});
		snowflake.execute({sqlText: sql_commit});
	} catch (err) {
		snowflake.execute({sqlText: sql_rollback});
		throw "Loading of "+ tgt_tbl + " Failed with error: " + err;
	}

	// ************** Load for CUSTOMER_SESSION_IMPRESSION ENDs *****************
$$;

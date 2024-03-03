--liquibase formatted sql
--changeset SYSTEM:SP_ONETAG_TO_BIM_LOAD_CLICK_STREAM_VISITOR runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_CONFIRMED_<<ENV>>;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE SP_ONETAG_TO_BIM_LOAD_CLICK_STREAM_VISITOR (SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_USER_ACT VARCHAR, C_CUST VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT

AS
$$

	// * ***********************************************************************
	// *
	// * Name:			SP_ONETAG_TO_BIM_LOAD_CLICK_STREAM_VISITOR
	// *
	// * Description:	Child Stored Proc to load data to CUSTOMER_SESSION_VISITOR (BIM) table
	// *
	// * History
	// *
	// * Version	Date(DD/MM/YYYY)	Author			DM_VERSION		Revision History
	// * --------	----------------	------------	------------	-------------------
	// * 1.0		12-04-2023			Madhu			1.0.1			Initial Version
	// *
	// * ***********************************************************************

    var src_wrk_tbl = SRC_WRK_TBL;
    var lkp_tbl		= `${CNF_DB}.${C_CUST}.RETAIL_CUSTOMER`;
    var tgt_tbl		= `${CNF_DB}.${C_USER_ACT}.CUSTOMER_SESSION_VISITOR`;
	var tgt_wrk_tbl	= `${CNF_DB}.DW_C_STAGE.CUSTOMER_SESSION_VISITOR_WRK`;
	var lkp_map_tbl = `${CNF_DB}.DW_C_STAGE.VISITOR_MAPPING`;


	// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.
	// ************** Load for CUSTOMER_SESSION_VISITOR table BEGIN *****************
	var sql_empty_wrk_tbl = `TRUNCATE TABLE ${tgt_wrk_tbl}`;

	try {
		snowflake.execute ({sqlText: sql_empty_wrk_tbl });
	} catch (err) {
		throw `Truncation of source work table ${tgt_wrk_tbl} Failed with error: ${err}`;
	}

	var sql_mapping = `CREATE OR REPLACE TRANSIENT TABLE ${lkp_map_tbl} AS(
						SELECT 
						CASE WHEN tgt.VISITOR_ID  is NULL THEN ONETAG_OTHER_SEQ.NEXTVAL
							ELSE tgt.VISITOR_INTEGRATION_ID
							END AS VISITOR_INTEGRATION_ID
						,SRC.VISITOR_ID AS src_VISITOR_ID
                        ,tgt.VISITOR_ID AS TGT_VISITOR_ID
						FROM (
							SELECT DISTINCT USER_ABSVISITORID AS  VISITOR_ID
                            FROM ${src_wrk_tbl} 
							where USER_ABSVISITORID is not null
							) AS SRC
						 	LEFT JOIN (
									SELECT DISTINCT VISITOR_ID, VISITOR_INTEGRATION_ID 
						 			FROM ${tgt_tbl}
                                    --  FROM EDM_CONFIRMED_DEV.DW_C_USER_ACTIVITY.CLICK_STREAM_VISITOR
									) AS tgt 
                                       ON SRC.VISITOR_ID=TGT.VISITOR_ID )`;
		try {
		snowflake.execute ({sqlText: sql_mapping });
	} catch (err) {
		throw `SQL mapping table ${tgt_wrk_tbl} Failed with error: ${err}`;
	}

	var sql_command =`INSERT INTO ${tgt_wrk_tbl}
(   VISITOR_INTEGRATION_ID
		,DW_FIRST_EFFECTIVE_TS
		,DW_LAST_EFFECTIVE_TS
        ,VISITOR_ID
		,HOUSEHOLD_ID
		,CLUB_CARD_NBR
		,RETAIL_CUSTOMER_UUID
		,USER_TYPE_CD
        ,FRESHPASS_SUBSCRIPTION_STATUS_DSC
        ,FRESHPASS_SUBSCRIPTION_DT
        ,TOTAL_ORDER_CNT
        ,EMAIL_OPTIN_IND
        ,SMS_OPTIN_IND
        ,CAMERA_PREFERENCE_CD
        ,NOTIFICATION_PREFERENCE_CD
        ,LOCATION_SHARING_PREFERENCE_CD
        ,COOKIE_PREFERENCE_TXT
        ,RETAIL_CUSTOMER_UUID_VALID_IND
		,Dw_Logical_Delete_Ind
		,Dw_Create_Ts
        ,DML_Type
		,Dw_Checksum_Value_Txt
	)
SELECT DISTINCT
         map.VISITOR_INTEGRATION_ID
		,src.DW_FIRST_EFFECTIVE_TS
		,src.DW_LAST_EFFECTIVE_TS
        ,src.VISITOR_ID
		,src.HOUSEHOLD_ID
		,src.CLUB_CARD_NBR
		,src.RETAIL_CUSTOMER_UUID
		,src.USER_TYPE_CD
        ,src.FRESHPASS_SUBSCRIPTION_STATUS_DSC
        ,src.FRESHPASS_SUBSCRIPTION_DT
        ,src.TOTAL_ORDER_CNT
        ,src.EMAIL_OPTIN_IND
        ,src.SMS_OPTIN_IND
        ,src.CAMERA_PREFERENCE_CD
        ,src.NOTIFICATION_PREFERENCE_CD
        ,src.LOCATION_SHARING_PREFERENCE_CD
        ,src.COOKIE_PREFERENCE_TXT
		,src.RETAIL_CUSTOMER_UUID_VALID_IND
		,src.DW_Logical_Delete_Ind
		,src.DW_Create_Ts
		,CASE WHEN tgt.VISITOR_ID is NULL THEN 'I' ELSE 'U' END as DML_Type
		,src.DW_Checksum_Value_Txt
        
	FROM (
			SELECT src.*,
			CASE
				WHEN src.RETAIL_CUSTOMER_UUID=flkp.RETAIL_CUSTOMER_UUID THEN 'TRUE' 
				ELSE 'FALSE' 
			END As RETAIL_CUSTOMER_UUID_VALID_IND
		
			FROM (
					SELECT
                    VISITOR_ID,
                    DW_FIRST_EFFECTIVE_TS,
				    COALESCE(lead(DW_FIRST_EFFECTIVE_TS) over (partition by VISITOR_ID order by DW_FIRST_EFFECTIVE_TS asc),
                    TO_TIMESTAMP('9999-12-31'))as DW_LAST_EFFECTIVE_TS,
                    HOUSEHOLD_ID,
                    CLUB_CARD_NBR,
                    RETAIL_CUSTOMER_UUID,
                    USER_TYPE_CD,
                    FRESHPASS_SUBSCRIPTION_STATUS_DSC,
                    FRESHPASS_SUBSCRIPTION_DT,
                    TOTAL_ORDER_CNT,
                    EMAIL_OPTIN_IND,
                    SMS_OPTIN_IND,
                    CAMERA_PREFERENCE_CD,
                    NOTIFICATION_PREFERENCE_CD,
                    LOCATION_SHARING_PREFERENCE_CD,
                    COOKIE_PREFERENCE_TXT,
                    FALSE AS DW_Logical_Delete_Ind,
                    Current_Timestamp As DW_Create_Ts,
		    MD5(CONCAT(NVL(VISITOR_ID,'Unknown'),NVL(HOUSEHOLD_ID,-1),NVL(CLUB_CARD_NBR,-1),NVL(RETAIL_CUSTOMER_UUID,'Unknown'),NVL(USER_TYPE_CD,'Unknown'),
	   	    NVL(FRESHPASS_SUBSCRIPTION_STATUS_DSC,'Unknown'),NVL(FRESHPASS_SUBSCRIPTION_DT,'Unknown'),NVL(TOTAL_ORDER_CNT,0)
	            ,NVL(EMAIL_OPTIN_IND,FALSE),NVL(SMS_OPTIN_IND,FALSE),NVL(CAMERA_PREFERENCE_CD,FALSE),NVL(NOTIFICATION_PREFERENCE_CD,FALSE)
	            ,NVL(LOCATION_SHARING_PREFERENCE_CD,FALSE),NVL(COOKIE_PREFERENCE_TXT,FALSE))) as DW_Checksum_Value_Txt
                        
					FROM (
							 select
                            VISITOR_ID,
                            min(eventtime) as DW_FIRST_EFFECTIVE_TS,
                            max(eventtime) as DW_LAST_EFFECTIVE_TS,
                            HOUSEHOLD_ID,
                            CLUB_CARD_NBR,
                            RETAIL_CUSTOMER_UUID,
                            USER_TYPE_CD,
                            FRESHPASS_SUBSCRIPTION_STATUS_DSC,
                            FRESHPASS_SUBSCRIPTION_DT,
                            TOTAL_ORDER_CNT,
                            EMAIL_OPTIN_IND,
                            SMS_OPTIN_IND,
                            CAMERA_PREFERENCE_CD,
                            NOTIFICATION_PREFERENCE_CD,
                            LOCATION_SHARING_PREFERENCE_CD,
                            COOKIE_PREFERENCE_TXT
                        FROM
                            (
                                select
                                    *,(
                                        ROW_NUMBER() OVER (
                                            ORDER BY
                                                eventtime asc ,VISITOR_ID,
                                            HOUSEHOLD_ID,
                                            CLUB_CARD_NBR,
                                            RETAIL_CUSTOMER_UUID,
                                            USER_TYPE_CD,
                                            FRESHPASS_SUBSCRIPTION_STATUS_DSC,
                                            FRESHPASS_SUBSCRIPTION_DT,
                                            TOTAL_ORDER_CNT,
                                            EMAIL_OPTIN_IND,
                                            SMS_OPTIN_IND,
                                            CAMERA_PREFERENCE_CD,
                                            NOTIFICATION_PREFERENCE_CD,
                                            LOCATION_SHARING_PREFERENCE_CD,
                                            COOKIE_PREFERENCE_TXT
                                        ) - ROW_NUMBER() OVER (
                                            PARTITION BY VISITOR_ID,
                                            HOUSEHOLD_ID,
                                            CLUB_CARD_NBR,
                                            RETAIL_CUSTOMER_UUID,
                                            USER_TYPE_CD,
                                            FRESHPASS_SUBSCRIPTION_STATUS_DSC,
                                            FRESHPASS_SUBSCRIPTION_DT,
                                            TOTAL_ORDER_CNT,
                                            EMAIL_OPTIN_IND,
                                            SMS_OPTIN_IND,
                                            CAMERA_PREFERENCE_CD,
                                            NOTIFICATION_PREFERENCE_CD,
                                            LOCATION_SHARING_PREFERENCE_CD,
                                            COOKIE_PREFERENCE_TXT
                                            ORDER BY
                                                eventtime asc ,VISITOR_ID,
                                            HOUSEHOLD_ID,
                                            CLUB_CARD_NBR,
                                            RETAIL_CUSTOMER_UUID,
                                            USER_TYPE_CD,
                                            FRESHPASS_SUBSCRIPTION_STATUS_DSC,
                                            FRESHPASS_SUBSCRIPTION_DT,
                                            TOTAL_ORDER_CNT,
                                            EMAIL_OPTIN_IND,
                                            SMS_OPTIN_IND,
                                            CAMERA_PREFERENCE_CD,
                                            NOTIFICATION_PREFERENCE_CD,
                                            LOCATION_SHARING_PREFERENCE_CD,
                                            COOKIE_PREFERENCE_TXT
                                        )
                                    ) as windowgroup
                                FROM(
                                        SELECT
                                            distinct USER_ABSVISITORID as VISITOR_ID,
                                            eventtime as eventtime,
                                            IFNULL(TRY_TO_NUMBER(REGEXP_REPLACE(USER_HHID, 'ID not found|Guest','')), -1) as HOUSEHOLD_ID,
                                            IFNULL(TRY_TO_NUMBER(REGEXP_REPLACE(USER_CCN, 'ID not found|Guest','')), -1) as CLUB_CARD_NBR,
                                           case when REGEXP_REPLACE(USER_UUID, 'ID not found|Guest','')='' THEN NULL ELSE USER_UUID
END as RETAIL_CUSTOMER_UUID,
                                            lower(USER_UTYPE) as USER_TYPE_CD,
                                            USER_SUBSTS as FRESHPASS_SUBSCRIPTION_STATUS_DSC,
                                            USER_SUBDATE as FRESHPASS_SUBSCRIPTION_DT,
                                            NVL(TRY_TO_NUMBER(USER_ORDCNT),0) as TOTAL_ORDER_CNT,
                                            COALESCE(TRY_TO_BOOLEAN(user_emailoptin), FALSE) as EMAIL_OPTIN_IND,
                                            COALESCE(TRY_TO_BOOLEAN(user_smsoptin),FALSE) as SMS_OPTIN_IND,
                                            USER_CAMERAALLOWED as CAMERA_PREFERENCE_CD,
                                            USER_NOTIFICATIONALLOWED as NOTIFICATION_PREFERENCE_CD,
                                            USER_LOCATIONSHARING as LOCATION_SHARING_PREFERENCE_CD,
                                            USER_COOKIEPREF as COOKIE_PREFERENCE_TXT
                                        FROM
                                        ${src_wrk_tbl}  
					--edm_refined_dev.dw_r_user_activity.ONE_TAG_OTHER
                                        where
                                            USER_ABSVISITORID is not null
											and eventtime is not null
                                            --and USER_ABSVISITORID = 'd5494295-b5d7-4b11-9261-100094dabc14'
                                    )
                            )
                        group by
                            VISITOR_ID,
                            HOUSEHOLD_ID,
                            CLUB_CARD_NBR,
                            RETAIL_CUSTOMER_UUID,
                            USER_TYPE_CD,
                            FRESHPASS_SUBSCRIPTION_STATUS_DSC,
                            FRESHPASS_SUBSCRIPTION_DT,
                            TOTAL_ORDER_CNT,
                            EMAIL_OPTIN_IND,
                            SMS_OPTIN_IND,
                            CAMERA_PREFERENCE_CD,
                            NOTIFICATION_PREFERENCE_CD,
                            LOCATION_SHARING_PREFERENCE_CD,
                            COOKIE_PREFERENCE_TXT,
                            windowgroup
			)
		) src
		LEFT JOIN
		(
			SELECT DISTINCT RETAIL_CUSTOMER_UUID 
			FROM ${lkp_tbl}
                	--FROM EDM_CONFIRMED_DEV.DW_C_CUSTOMER.RETAIL_CUSTOMER
		) flkp ON flkp.RETAIL_CUSTOMER_UUID = src.RETAIL_CUSTOMER_UUID
	) src
	LEFT JOIN
	(
		SELECT DISTINCT
                	VISITOR_INTEGRATION_ID
			,VISITOR_ID
				,HOUSEHOLD_ID
				,CLUB_CARD_NBR
				,RETAIL_CUSTOMER_UUID
				,USER_TYPE_CD
                ,FRESHPASS_SUBSCRIPTION_STATUS_DSC
                ,FRESHPASS_SUBSCRIPTION_DT
                ,TOTAL_ORDER_CNT
                ,EMAIL_OPTIN_IND
                ,SMS_OPTIN_IND
                ,CAMERA_PREFERENCE_CD
                ,NOTIFICATION_PREFERENCE_CD
                ,LOCATION_SHARING_PREFERENCE_CD
		,COOKIE_PREFERENCE_TXT
		,RETAIL_CUSTOMER_UUID_VALID_IND
              	,DW_Logical_Delete_Ind
		,DW_First_Effective_Ts
		,DW_Checksum_Value_Txt
	FROM ${tgt_tbl}
            --FROM EDM_CONFIRMED_DEV.DW_C_USER_ACTIVITY.CLICK_STREAM_VISITOR
		--WHERE DW_Current_Version_Ind = TRUE
		) AS tgt ON src.VISITOR_ID = tgt.VISITOR_ID
    		AND src.DW_checksum_Value_txt = tgt.DW_Checksum_Value_Txt
	    LEFT JOIN 
		(
			SELECT src_visitor_id,
			visitor_integration_id 
			FROM ${lkp_map_tbl}
			--FROM edm_confirmed_dev.dw_c_user_activity.visitor_mapping
		) AS map
        ON src.visitor_id=map.src_visitor_id 
        WHERE
			tgt.VISITOR_ID IS NULL
		OR (
			tgt.DW_Checksum_Value_Txt <> src.DW_Checksum_Value_Txt
			OR
            tgt.DW_Logical_Delete_Ind <> src.DW_Logical_Delete_Ind
		)`;

	try {
		snowflake.execute ({sqlText: sql_command});
	} catch (err) {
		throw "Creation of CUSTOMER_SESSION_VISITOR work table "+ tgt_wrk_tbl +" Failed with error: " + err;
	}

	//SCD Type2 transaction begins
	var sql_begin = "BEGIN"

	// Processing Updates of Type 2 SCD
	
	var sql_updates_effectivedates = `UPDATE ${tgt_tbl} as tgt
					  SET DW_LAST_EFFECTIVE_TS = coalesce(q.leadeffectivets, To_timestamp('9999-12-31'))
					  FROM
    					  (
        					 select src.*,
            LEAD(src.DW_FIRST_EFFECTIVE_TS) OVER (PARTITION BY src.VISITOR_INTEGRATION_ID ORDER by src.DW_FIRST_EFFECTIVE_TS asc) as leadeffectivets,
            max(src.DW_FIRST_EFFECTIVE_TS) OVER (PARTITION BY src.VISITOR_INTEGRATION_ID) as maxfirsteffectivets,
            ROW_NUMBER() OVER (PARTITION BY src.VISITOR_INTEGRATION_ID ORDER by src.DW_FIRST_EFFECTIVE_TS DESC) as rownum from                         (
                    select distinct t.*
            					FROM ${tgt_tbl} t JOIN ${tgt_wrk_tbl} s
						ON t.VISITOR_ID = s.VISITOR_ID
						--AND t.DW_FIRST_EFFECTIVE_TS = s.DW_FIRST_EFFECTIVE_TS
    					  ) src
                          )q
					where
    					tgt.DW_Checksum_Value_Txt = q.DW_Checksum_Value_Txt
    					and tgt.DW_FIRST_EFFECTIVE_TS = q.DW_FIRST_EFFECTIVE_TS
    					and tgt.VISITOR_INTEGRATION_ID = q.VISITOR_INTEGRATION_ID
   					and tgt.DW_FIRST_EFFECTIVE_TS <> maxfirsteffectivets`;
	
	var sql_updates_current_version_ind_t = `UPDATE ${tgt_tbl} as tgt
						SET DW_CURRENT_VERSION_IND = TRUE  
						FROM (
				 			select distinct t.visitor_integration_id 
        						FROM ${tgt_tbl} t JOIN ${tgt_wrk_tbl} s
							ON t.VISITOR_ID = s.VISITOR_ID
							AND t.DW_FIRST_EFFECTIVE_TS = s.DW_FIRST_EFFECTIVE_TS
    						) src
						where tgt.VISITOR_INTEGRATION_ID = src.VISITOR_INTEGRATION_ID
    						and tgt.DW_LAST_EFFECTIVE_TS = TO_TIMESTAMP('9999-12-31')`;

	var sql_updates_current_version_ind_f = `UPDATE ${tgt_tbl} as tgt
				   SET DW_CURRENT_VERSION_IND = FALSE
				  FROM (
					select distinct t.visitor_integration_id  
					FROM ${tgt_tbl} t JOIN ${tgt_wrk_tbl} s
					ON t.VISITOR_ID = s.VISITOR_ID
					AND t.DW_FIRST_EFFECTIVE_TS = s.DW_FIRST_EFFECTIVE_TS
		     		   ) src
				   where tgt.VISITOR_INTEGRATION_ID = src.VISITOR_INTEGRATION_ID
    				    and tgt.DW_LAST_EFFECTIVE_TS <> TO_TIMESTAMP('9999-12-31')`;

	// Processing Inserts
	var sql_inserts = `INSERT INTO ${tgt_tbl}
	(
      VISITOR_INTEGRATION_ID
     ,Dw_First_Effective_Ts
     ,Dw_Last_Effective_Ts
     ,VISITOR_ID
     ,HOUSEHOLD_ID
     ,CLUB_CARD_NBR
     ,RETAIL_CUSTOMER_UUID
     ,USER_TYPE_CD
     ,FRESHPASS_SUBSCRIPTION_STATUS_DSC
     ,FRESHPASS_SUBSCRIPTION_DT
     ,TOTAL_ORDER_CNT
     ,EMAIL_OPTIN_IND
     ,SMS_OPTIN_IND
     ,CAMERA_PREFERENCE_CD
     ,NOTIFICATION_PREFERENCE_CD
     ,LOCATION_SHARING_PREFERENCE_CD
	 ,COOKIE_PREFERENCE_TXT
     ,RETAIL_CUSTOMER_UUID_VALID_IND
     ,Dw_Create_Ts
     ,DW_LAST_UPDATE_TS
     ,Dw_Logical_Delete_Ind
     ,Dw_Source_Create_Nm
     ,DW_SOURCE_UPDATE_NM
     ,Dw_Current_Version_Ind
	 ,DW_Checksum_Value_Txt
     )
     SELECT
         distinct
         VISITOR_INTEGRATION_ID
         ,Dw_First_Effective_Ts
	 ,Dw_Last_Effective_Ts
         ,VISITOR_ID
         ,HOUSEHOLD_ID
         ,CLUB_CARD_NBR
         ,RETAIL_CUSTOMER_UUID
		 ,USER_TYPE_CD
         ,FRESHPASS_SUBSCRIPTION_STATUS_DSC
         ,FRESHPASS_SUBSCRIPTION_DT
         ,TOTAL_ORDER_CNT
         ,EMAIL_OPTIN_IND
         ,SMS_OPTIN_IND
         ,CAMERA_PREFERENCE_CD
         ,NOTIFICATION_PREFERENCE_CD
         ,LOCATION_SHARING_PREFERENCE_CD
		 ,COOKIE_PREFERENCE_TXT
		 ,RETAIL_CUSTOMER_UUID_VALID_IND
         ,CURRENT_TIMESTAMP
         ,CURRENT_TIMESTAMP
         ,Dw_Logical_Delete_Ind
         ,'OneTag' as Dw_Source_Create_Nm
         ,'OneTag' as DW_SOURCE_UPDATE_NM
		 ,CASE WHEN DW_LAST_EFFECTIVE_TS is NULL 
		 		THEN TRUE
				ELSE FALSE END as DW_CURRENT_VERSION_IND
		,DW_Checksum_Value_Txt
     FROM ${tgt_wrk_tbl} WHERE VISITOR_ID IS NOT NULL
   `;

	var sql_commit = "COMMIT"
	var sql_rollback = "ROLLBACK"

	try {
		snowflake.execute({sqlText: sql_begin});
		snowflake.execute({sqlText: sql_mapping});
		snowflake.execute({sqlText: sql_inserts});
		snowflake.execute({sqlText: sql_updates_effectivedates});
		snowflake.execute({sqlText: sql_updates_current_version_ind_t});
		snowflake.execute({sqlText: sql_updates_current_version_ind_f});
		snowflake.execute({sqlText: sql_commit});
	} catch (err) {
		snowflake.execute({sqlText: sql_rollback});
		throw "Loading of "+ tgt_tbl + " Failed with error: " + err;
	}

	// ************** Load for CUSTOMER_SESSION_VISITOR ENDs *****************
$$;

--liquibase formatted sql
--changeset SYSTEM:SP_ONETAG_TO_BIM_LOAD_USER runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_CONFIRMED_<<parm_ENV>>;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE SP_ONETAG_TO_BIM_LOAD_USER (SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, CNF_SCHEMA VARCHAR, C_LOC VARCHAR, WRK_SCHEMA VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

	// * ***********************************************************************
	// *
	// * Name:			SP_ONETAG_TO_BIM_LOAD_USER
	// *
	// * Description:	Child Stored Proc to load data to USER (BIM) table
	// *
	// * History
	// *
	// * Version	Date(DD/MM/YYYY)	Author			DM_VERSION		Revision History
	// * --------	----------------	------------	------------	-------------------
	// * 1.0		12-04-2023			Madhu			1.0.1			Initial Version
	// *
	// * ***********************************************************************

    var src_wrk_tbl = SRC_WRK_TBL;
    var lkp_tbl		= `${CNF_DB}.${C_LOC}.RETAIL_CUSTOMER`;
    var tgt_tbl		= `${CNF_DB}.${CNF_SCHEMA}.CLICK_STREAM_VISITOR`;
	var tgt_wrk_tbl	= `${CNF_DB}.${WRK_SCHEMA}.CLICK_STREAM_VISITOR_WRK`;

	// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.
	// ************** Load for Retail_Store_3PL_Partner table BEGIN *****************
	var sql_empty_wrk_tbl = `TRUNCATE TABLE ${tgt_wrk_tbl}`;

	try {
		snowflake.execute ({sqlText: sql_empty_wrk_tbl });
	} catch (err) {
		throw `Truncation of source work table ${tgt_wrk_tbl} Failed with error: ${err}`;
	}

	var sql_command =`INSERT INTO ${tgt_wrk_tbl}
	(
		 VISITOR_ID
		,DW_FIRST_EFFECTIVE_TS
		,DW_LAST_EFFECTIVE_TS
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
		,Dw_Logical_Delete_Ind
		,Dw_Create_Ts		
		,DML_Type
		,Dw_Checksum_Value_Txt
	)
SELECT DISTINCT
		 src.VISITOR_ID
		,src.DW_FIRST_EFFECTIVE_TS
		,src.DW_LAST_EFFECTIVE_TS
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
			SELECT src.*,case when src.RETAIL_CUSTOMER_UUID=flkp.RETAIL_CUSTOMER_UUID then 'TRUE' else 'FALSE' END As RETAIL_CUSTOMER_UUID_VALID_IND,
					
					MD5( CONCAT( NVL(src.VISITOR_ID,'Unknown'), NVL(DW_FIRST_EFFECTIVE_TS,Current_Timestamp), NVL(DW_LAST_EFFECTIVE_TS,'9999-12-31 00:00:00.000') ) ) as DW_Checksum_Value_Txt
			FROM (
					SELECT
                         VISITOR_ID
						,DW_FIRST_EFFECTIVE_TS
						,DW_LAST_EFFECTIVE_TS
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
						,FALSE AS DW_Logical_Delete_Ind
						,Current_Timestamp As DW_Create_Ts
                        
					FROM (
							SELECT
								 VISITOR_ID
								,DW_FIRST_EFFECTIVE_TS
								,lead(DW_FIRST_EFFECTIVE_TS) over (partition by VISITOR_ID order by DW_FIRST_EFFECTIVE_TS asc) as DW_LAST_EFFECTIVE_TS
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
							FROM (
								SELECT DISTINCT
									--case when USER_ABSVISITORID is null then 'Unknown' else USER_ABSVISITORID end as VISITOR_ID
                                    USER_ABSVISITORID as VISITOR_ID
								   ,eventtime as DW_FIRST_EFFECTIVE_TS
								   --,lead(eventtime) over (partition by USER_ABSVISITORID order by eventtime asc) as DW_LAST_EFFECTIVE_TS
                                   ,IFNULL(TRY_TO_NUMBER(USER_HHID),0) as HOUSEHOLD_ID
								   ,IFNULL(TRY_TO_NUMBER(USER_CCN),0) as CLUB_CARD_NBR
                                   ,USER_UUID as RETAIL_CUSTOMER_UUID
									,USER_UTYPE as USER_TYPE_CD
                                   ,USER_SUBSTS as FRESHPASS_SUBSCRIPTION_STATUS_DSC
                                   ,USER_SUBDATE as FRESHPASS_SUBSCRIPTION_DT
                                   ,USER_ORDCNT as TOTAL_ORDER_CNT
                                   ,USER_EMAILOPTIN as EMAIL_OPTIN_IND
                                   ,USER_SMSOPTIN as SMS_OPTIN_IND
                                   ,USER_CAMERAALLOWED as CAMERA_PREFERENCE_CD
                                   ,USER_NOTIFICATIONALLOWED as NOTIFICATION_PREFERENCE_CD
                                   ,USER_LOCATIONSHARING as LOCATION_SHARING_PREFERENCE_CD
                                   ,USER_COOKIEPREF as COOKIE_PREFERENCE_TXT
							  --FROM ${src_wrk_tbl}
                                FROM edm_confirmed_dev.dw_c_user_activity.ONE_TAG_OTHER
                            
									)
						)
				) src
			LEFT JOIN
			(
				SELECT DISTINCT
					RETAIL_CUSTOMER_UUID 
				--FROM ${lkp_tbl}
                FROM EDM_CONFIRMED_DEV.DW_C_CUSTOMER.RETAIL_CUSTOMER
			) flkp on flkp.RETAIL_CUSTOMER_UUID = src.RETAIL_CUSTOMER_UUID
		) src
		LEFT JOIN
		(
			SELECT DISTINCT
				VISITOR_ID
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
			--FROM ${tgt_tbl}
            FROM EDM_CONFIRMED_DEV.DW_C_USER_ACTIVITY.CLICK_STREAM_VISITOR
			WHERE DW_Current_Version_Ind = TRUE
		) AS tgt
		ON
		src.VISITOR_ID = tgt.VISITOR_ID
		
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
		throw "Creation of Retail_Store_3PL_Partner work table "+ tgt_wrk_tbl +" Failed with error: " + err;
	}

	//SCD Type2 transaction begins
	var sql_begin = "BEGIN"

	// Processing Updates of Type 2 SCD
var sql_updates = `UPDATE ${tgt_tbl} as tgt
SET
		DW_Last_Effective_ts = src.Dw_First_Effective_Ts,
		DW_Current_Version_Ind = FALSE,
		DW_Last_Update_Ts = Current_Timestamp,
		DW_Source_Update_Nm = 'OneTag'
	FROM (
			SELECT
				VISITOR_ID
				,Dw_First_Effective_Ts
			FROM ${tgt_wrk_tbl}
			WHERE DML_Type = 'U'
			AND VISITOR_ID is not NULL
		) src
	WHERE
			tgt.VISITOR_ID	= src.VISITOR_ID
		AND tgt.DW_Last_Effective_ts	= '9999-12-31 00:00:00.000'
		AND tgt.DW_Current_Version_Ind = TRUE
		AND tgt.DW_Logical_Delete_Ind = FALSE`;

	// Processing Inserts
	var sql_inserts = `INSERT INTO ${tgt_tbl}
	(
      VISITOR_ID
	 ,Dw_First_Effective_Ts
     ,Dw_Last_Effective_Ts
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
     ,Dw_Logical_Delete_Ind
     ,Dw_Source_Create_Nm
     ,Dw_Current_Version_Ind
	 ,DW_Checksum_Value_Txt
     )
     SELECT
         distinct
         VISITOR_ID
		 ,Dw_First_Effective_Ts
		 ,CASE WHEN DW_LAST_EFFECTIVE_TS IS NULL
		 	THEN '9999-12-31 00:00:00.000'
			ELSE DW_LAST_EFFECTIVE_TS END AS Dw_Last_Effective_Ts
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
         ,Dw_Logical_Delete_Ind
         ,'OneTag' as Dw_Source_Create_Nm
		 ,CASE WHEN DW_LAST_EFFECTIVE_TS is NULL 
		 		THEN TRUE
				ELSE FALSE END as DW_CURRENT_VERSION_IND
		,DW_Checksum_Value_Txt
      FROM ${tgt_wrk_tbl}
--    FROM user_wrk
   `;

	var sql_commit = "COMMIT"
	var sql_rollback = "ROLLBACK"

	try {
		snowflake.execute({sqlText: sql_begin});
		snowflake.execute({sqlText: sql_updates});
		snowflake.execute({sqlText: sql_inserts});
		snowflake.execute({sqlText: sql_commit});
	} catch (err) {
		snowflake.execute({sqlText: sql_rollback});
		throw "Loading of "+ tgt_tbl + " Failed with error: " + err;
	}

	// ************** Load for One tag user ENDs *****************
$$;
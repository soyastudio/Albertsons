--liquibase formatted sql
--changeset SYSTEM:SP_ONETAG_TO_BIM_LOAD_CLICK_STREAM_EVENT_MASTER runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_CONFIRMED_<<ENV>>;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE SP_ONETAG_TO_BIM_LOAD_CLICK_STREAM_EVENT_MASTER (SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_USER_ACT VARCHAR, C_STAGE VARCHAR, C_CUST VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT

AS
$$

	// * ***********************************************************************
	// *
	// * Name:			SP_ONETAG_TO_BIM_LOAD_CLICK_STREAM_VISITOR
	// *
	// * Description:	Child Stored Proc to load data to CLICK_STREAM_VISITOR (BIM) table
	// *
	// * History
	// *
	// * Version	Date(DD/MM/YYYY)	Author			DM_VERSION		Revision History
	// * --------	----------------	------------	------------	-------------------
	// * 1.0		12-16-2023			Chandra			1.0.1			Initial Version
	// *
	// * ***********************************************************************


	var src_wrk_tbl = SRC_WRK_TBL;
    var cnf_db = CNF_DB;
    var cnf_schema = C_USER_ACT;
    var wrk_schema = C_STAGE;
	var c_cust = C_CUST;
    var lkp_tbl = cnf_db + "." + c_cust + ".RETAIL_CUSTOMER";
    var tgt_tbl = cnf_db + "." + cnf_schema + ".CUSTOMER_SESSION_EVENT_MASTER";
	var tgt_wrk_tbl	= cnf_db + "." + wrk_schema + ".CUSTOMER_SESSION_EVENT_MASTER_WRK";
	var lkp_retail_store_digital_tbl = cnf_db + ".DW_C_LOCATION.RETAIL_STORE_DIGITAL";   
	var lkp_facility_address_tbl = cnf_db + ".DW_C_LOCATION.FACILITY_ADDRESS";   
	var lkp_operating_system_tbl = cnf_db + "." + cnf_schema + ".CUSTOMER_SESSION_OPERATING_SYSTEM";   
	var lkp_retail_store_tbl = cnf_db + ".DW_C_LOCATION.RETAIL_STORE"; 
	var lkp_retail_order_group_tbl = cnf_db + ".DW_C_LOCATION.RETAIL_ORDER_GROUP"; 
	var lkp_division_tbl = cnf_db + ".DW_C_LOCATION.DIVISION"; 	
	var lkp_visitor_tbl =  cnf_db + "." + cnf_schema + ".CUSTOMER_SESSION_VISITOR";
	var lkp_page_tbl =  cnf_db + "." + cnf_schema + ".CUSTOMER_SESSION_PAGE";
	
	               
// identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.

    var tgt_wrk_tbl = cnf_db + "." + wrk_schema + ".CUSTOMER_SESSION_EVENT_MASTER_WRK";
    var tgt_tbl 	= cnf_db + "." + cnf_schema + ".CUSTOMER_SESSION_EVENT_MASTER";
               
// ** Load for ONE TAG Operating_System table BEGIN ***

// identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.

	var sql_truncate_tgt_wrk_tbl = `TRUNCATE TABLE `+ tgt_wrk_tbl ;

	try {
		snowflake.execute (
		{sqlText: sql_truncate_tgt_wrk_tbl  }
		);
	}
	catch (err)  {
		return "TRUNCATE of work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
	}

	var sql_crt_tgt_wrk_tbl = `INSERT INTO ${tgt_wrk_tbl} 
	(
		EVENT_ID,
		EVENT_TS,
		SESSION_ID,
		PAGE_INTEGRATION_ID,
		EVENT_NM,
		VISITOR_INTEGRATION_ID,
		VISITOR_ID,
		HOUSEHOLD_ID,
		CLUB_CARD_NBR,
		RETAIL_CUSTOMER_UUID,
		ADOBE_VISITOR_ID,
		EVENT_TYPE_CD,
		EVENT_DT,
		SESSION_SEQUENCE_NBR,
		BANNER_NM,
		Division_Id,
		DIVISION_NM,
		OPERATING_SYSTEM_INTEGRATION_ID,
		FACILITY_NBR,
		FACILITY_INTEGRATION_ID,
		POSTAL_ZONE_CD,
		APP_VERSION_CD,
		PAGE_URL_TXT,
		DEVICE_NM,
		DW_CREATE_TS,
		DW_LAST_UPDATE_TS,
		DW_LOGICAL_DELETE_IND,
		DW_SOURCE_CREATE_NM,
		DW_SOURCE_UPDATE_NM,
		DW_CURRENT_VERSION_IND
	) 
	SELECT DISTINCT
		src.EVENT_ID,
		src.EVENT_TS,
		src.SESSION_ID,
		COALESCE(src.PAGE_INTEGRATION_ID,-1) as PAGE_INTEGRATION_ID,
		src.EVENT_NM,
		COALESCE(src.VISITOR_INTEGRATION_ID,-1) as VISITOR_INTEGRATION_ID,
		src.VISITOR_ID,
		src.HOUSEHOLD_ID,
		src.CLUB_CARD_NBR,
		src.RETAIL_CUSTOMER_UUID,
		src.ADOBE_VISITOR_ID,
		src.EVENT_TYPE_CD,
		src.EVENT_DT,
		ROW_NUMBER() OVER (PARTITION BY src.SESSION_ID ORDER BY src.EVENT_TS) as SESSION_SEQUENCE_NBR,
		src.BANNER_NM,
		src.Division_Id,
		src.DIVISION_NM,
		COALESCE(src.OPERATING_SYSTEM_INTEGRATION_ID,-1) as OPERATING_SYSTEM_INTEGRATION_ID,
		src.FACILITY_NBR,
		src.FACILITY_INTEGRATION_ID,
		src.POSTAL_ZONE_CD,
		src.APP_VERSION_CD,
		src.PAGE_URL_TXT,
		src.DEVICE_NM,
		CURRENT_TIMESTAMP() as DW_CREATE_TS,
		CURRENT_TIMESTAMP() as DW_LAST_UPDATE_TS,
		FALSE as DW_LOGICAL_DELETE_IND,
		'OneTag' as DW_SOURCE_CREATE_NM,
		'OneTag' as DW_SOURCE_UPDATE_NM,
		TRUE as DW_CURRENT_VERSION_IND
		FROM
			(
			SELECT distinct
					OT.EVENT_ID as EVENT_ID,
					OT.EVENTTIME as EVENT_TS,
                   			REGEXP_REPLACE(OT.USER_SESSIONID, 'ID not found|Guest','') as SESSION_ID,
					PT.PAGE_INTEGRATION_ID as PAGE_INTEGRATION_ID,
					OT.EVENT_SUBEVENT as EVENT_NM,
					CSV.VISITOR_INTEGRATION_ID as VISITOR_INTEGRATION_ID,
					REGEXP_REPLACE(OT.USER_ABSVISITORID, 'ID not found|Guest','') as VISITOR_ID,
					IFNULL(TRY_TO_NUMBER(REGEXP_REPLACE(OT.USER_HHID, 'ID not found|Guest','')),-1) as HOUSEHOLD_ID,
					IFNULL(TRY_TO_NUMBER(REGEXP_REPLACE(OT.USER_CCN, 'ID not found|Guest','')),-1) as CLUB_CARD_NBR,
					REGEXP_REPLACE(OT.USER_UUID, 'ID not found|Guest','') as RETAIL_CUSTOMER_UUID,
					REGEXP_REPLACE(OT.USER_ADOBEVISITORID, 'ID not found|Guest','') as ADOBE_VISITOR_ID,
					OT.EVENT_NAME as EVENT_TYPE_CD,
					TO_DATE(OT.EVENTTIME) as EVENT_DT,
					REGEXP_REPLACE(OT.PAGE_BNR, 'ID not found|Guest','') as BANNER_NM,
					D.DIVISION_ID as DIVISION_ID,
					D.DIVISION_NM as DIVISION_NM,
					OS.OPERATING_SYSTEM_INTEGRATION_ID as OPERATING_SYSTEM_INTEGRATION_ID,
					RSD.RETAIL_STORE_ID as FACILITY_NBR,
					RSD.FACILITY_INTEGRATION_ID as FACILITY_INTEGRATION_ID,
					FA.POSTAL_ZONE_CD as POSTAL_ZONE_CD,
					OT.PAGE_APPVER as APP_VERSION_CD,
					OT.PAGE_RURL as PAGE_URL_TXT,
					OT.USER_DEVICE as DEVICE_NM

				FROM ${SRC_WRK_TBL} OT
				LEFT JOIN ${lkp_retail_store_digital_tbl} RSD
				ON RSD.RETAIL_STORE_ID = TRY_TO_NUMBER(OT.USER_SSID)
				AND RSD.DW_CURRENT_VERSION_IND = TRUE
				LEFT JOIN ${lkp_facility_address_tbl} FA
				ON RSD.FACILITY_INTEGRATION_ID = FA.FACILITY_INTEGRATION_ID
				AND FA.DW_CURRENT_VERSION_IND = TRUE
				LEFT JOIN ${lkp_operating_system_tbl} OS
				ON OT.USER_OS = OS.OPERATING_SYSTEM_CD
				LEFT JOIN ${lkp_page_tbl} PT
				ON OT.PAGE_PGNAME = PT.PAGE_NM
				LEFT JOIN ${lkp_retail_store_tbl} RS
				ON RS.FACILITY_NBR = OT.USER_SSID
				AND RS.DW_CURRENT_VERSION_IND = TRUE
				LEFT JOIN ${lkp_retail_order_group_tbl} ROG
				ON RS.ROG_ID = ROG.ROG_ID
				AND ROG.DW_CURRENT_VERSION_IND = TRUE
				LEFT JOIN ${lkp_division_tbl} D
				ON ROG.DIVISION_ID = D.DIVISION_ID
				AND D.CORPORATION_ID = '001'
				AND D.DW_CURRENT_VERSION_IND = TRUE
				LEFT JOIN ${lkp_visitor_tbl} CSV
				ON OT.USER_ABSVISITORID = CSV.VISITOR_ID
				AND OT.EVENTTIME BETWEEN CSV.DW_FIRST_EFFECTIVE_TS AND CSV.DW_LAST_EFFECTIVE_TS
				WHERE OT.EVENTTIME is not null
			)src  
			LEFT JOIN ${tgt_tbl} tgt on SRC.EVENT_Id=tgt.event_id and src.event_ts=tgt.event_ts
    where tgt.event_id is null 
     and tgt.event_ts is null
	`;
              
   try {
			snowflake.execute (
						  {sqlText: sql_crt_tgt_wrk_tbl  }
						  );
			}
  catch (err)  {
			return "Insert of work table "+ sql_crt_tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
			}
                                                          


// Processing Inserts
	  var sql_inserts = `INSERT INTO ${tgt_tbl} (
		EVENT_ID,
		EVENT_TS,
		SESSION_ID,
		PAGE_INTEGRATION_ID,
		EVENT_NM,
		VISITOR_INTEGRATION_ID,
		VISITOR_ID,
		HOUSEHOLD_ID,
		CLUB_CARD_NBR,
		RETAIL_CUSTOMER_UUID,
		ADOBE_VISITOR_ID,
		EVENT_TYPE_CD,
		EVENT_DT,
		SESSION_SEQUENCE_NBR,
		BANNER_NM,
		Division_Id,
		DIVISION_NM,
		OPERATING_SYSTEM_INTEGRATION_ID,
		FACILITY_NBR,
		FACILITY_INTEGRATION_ID,
		POSTAL_ZONE_CD,
		APP_VERSION_CD,
		PAGE_URL_TXT,
		DEVICE_NM,
		DW_CREATE_TS,
		DW_LAST_UPDATE_TS,
		DW_LOGICAL_DELETE_IND,
		DW_SOURCE_CREATE_NM,
		DW_SOURCE_UPDATE_NM,
		DW_CURRENT_VERSION_IND
		)
		SELECT
			EVENT_ID,
			EVENT_TS,
			SESSION_ID,
			PAGE_INTEGRATION_ID,
			EVENT_NM,
			VISITOR_INTEGRATION_ID,
			VISITOR_ID,
			HOUSEHOLD_ID,
			CLUB_CARD_NBR,
			RETAIL_CUSTOMER_UUID,
			ADOBE_VISITOR_ID,
			EVENT_TYPE_CD,
			EVENT_DT,
			SESSION_SEQUENCE_NBR,
			BANNER_NM,
			Division_Id,
			DIVISION_NM,
			OPERATING_SYSTEM_INTEGRATION_ID,
			FACILITY_NBR,
			FACILITY_INTEGRATION_ID,
			POSTAL_ZONE_CD,
			APP_VERSION_CD,
			PAGE_URL_TXT,
			DEVICE_NM,
			DW_CREATE_TS,
			DW_LAST_UPDATE_TS,
			DW_LOGICAL_DELETE_IND,
			DW_SOURCE_CREATE_NM,
			DW_SOURCE_UPDATE_NM,
			DW_CURRENT_VERSION_IND 
		FROM ${tgt_wrk_tbl}
			`;

    var sql_commit = "COMMIT"
    var sql_rollback = "ROLLBACK"
    var sql_begin = "BEGIN"	

try {
        snowflake.execute (
            {sqlText: sql_begin  }
            );
       
        snowflake.execute (
            {sqlText: sql_inserts  }
            );
		
		snowflake.execute (
            {sqlText: sql_commit  }
            );
        }
    catch (err)  {
        snowflake.execute (
            {sqlText: sql_rollback  }
            );
        return "Loading of ONE TAG BIM table "+ tgt_tbl + " Failed with error: " + err;   // Return a error message.
        }


// *** Loading of ONE TAG Operating_System table ENDs ****

$$;

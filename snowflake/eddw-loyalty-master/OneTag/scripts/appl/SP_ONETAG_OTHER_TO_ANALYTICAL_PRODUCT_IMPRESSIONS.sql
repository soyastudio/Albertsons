--liquibase formatted sql
--changeset SYSTEM:SP_ONETAG_OTHER_TO_BIM_LOAD runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_ANALYTIC_DB_NAME>>;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE "SP_ONETAG_PRODUCT_IMPRESSIONS_TO_ANALYTICAL_LOAD"("P_START_DATE_I" VARCHAR(16777216), "P_FLAG_I" VARCHAR(16777216))
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

    
    // Get Metadata from EDM_Environment_Variable Table 
    
        var cur_db = snowflake.execute( {sqlText: `Select current_database()`} ); 
    cur_db.next(); 
    var env = cur_db.getColumnValue(1); 
    env = env.split('_'); 	
    env = env[env.length - 1]; 
    var env_tbl_nm = `EDM_Environment_Variable_${env}`; 
    var env_schema_nm = 'DW_R_MASTERDATA'; 
    var env_db_nm = `EDM_REFINED_${env}`; 
    var event_start_date = P_START_DATE_I;
    var event_hist_load_ind = P_FLAG_I;
    

    try {
        var rs = snowflake.execute( {sqlText: `SELECT * FROM ${env_db_nm}.${env_schema_nm}.${env_tbl_nm}`} );
        var metaparams = {};
        while (rs.next()){
            metaparams[rs.getColumnValue(1)] = rs.getColumnValue(2);
        }
        var cnf_db = metaparams['CNF_DB']; //EDM_CONFIRMED_env
		var cnf_schema = metaparams['C_USER_ACT']; //DW_C_USER_ACTIVITY
		var tgt_schema = metaparams['A_EXP']; // DW_DIGITAL_EXP
		var tgt_db = metaparams['ANLYS_DB']; // EDM_ANALYTICS_DEV
       
    } catch (err) {
        throw `Error while fetching data from EDM_Environment_Variable_${env}`;
    }
    
    // Global Variable Declaration
    var src_event_master_tbl = `${cnf_db}.${cnf_schema}.CUSTOMER_SESSION_EVENT_MASTER`;
	var src_visitor_tbl = `${cnf_db}.${cnf_schema}.CUSTOMER_SESSION_VISITOR`;
	var src_impressions_tbl = `${cnf_db}.${cnf_schema}.CUSTOMER_SESSION_IMPRESSION`;
	var src_page_tbl = `${cnf_db}.${cnf_schema}.CUSTOMER_SESSION_PAGE`;
	
	var tgt_tbl = `${tgt_db}.${tgt_schema}.ONETAG_PRODUCT_IMPRESSION`;

   
	
    // get max of event timestamp in target table
	
    var sql_max_event_ts = "SELECT COALESCE(max(product_seen_ts), TO_TIMESTAMP('1900-01-01'))::string from " + tgt_tbl;
    	var stmt_max_event_ts = snowflake.createStatement({sqlText: sql_max_event_ts});
    	var queryText = stmt_max_event_ts.getSqlText();
    	var res_sql_max_event_ts = stmt_max_event_ts.execute();
    	res_sql_max_event_ts.next();
    	var max_event_ts = res_sql_max_event_ts.getColumnValue(1);
	
	
    	var final_max_event_ts = '';
    	switch(event_hist_load_ind) {
		case 'Y':
			final_max_event_ts = event_start_date;
			break;
		case 'N':
			final_max_event_ts = max_event_ts;
			break;
		default:
			final_max_event_ts = max_event_ts;
	}		

 

   	// query to load product impressions data into target table
	var sql_ins_tgt_tbl = `INSERT INTO ` + tgt_tbl + `(
								APPLICATION_DETAIL_TXT,
								BASE_PRODUCT_NBR,
								CAROUSEL_NM,
								CLUB_CARD_NBR,
								EVENT_ID,
								HOUSEHOLD_ID,
								MODEL_ID_TXT,
								PRODUCT_FINDING_METHOD_DSC,
								PAGE_NM,
								PRODUCT_SEEN_IND,
								PRODUCT_SEEN_TS,
								PRODUCTS_SEEN_IN_CAROUSEL_IND,
								RETAIL_CUSTOMER_UUID,
								ROW_LOCATION_CD,
								SLOT_LOCATION_CD,
								SESSION_ID,
								SESSION_SEQUENCE_NBR,
								COOKIE_PREFERENCE_CD,
								DW_LOGICAL_DELETE_IND,
                                DW_CURRENT_VERSION_IND,
								DW_CREATE_TS,
								DW_LAST_UPDATE_TS								
							)
							(
                            select distinct
								APPLICATION_DETAIL_TXT,
								BASE_PRODUCT_NBR,
								CAROUSEL_NM,
								CLUB_CARD_NBR,
								EVENT_ID,
								HOUSEHOLD_ID,
								MODEL_ID_TXT,
								PRODUCT_FINDING_METHOD_DSC,
								PAGE_NM,
								PRODUCT_SEEN_IND,
								PRODUCT_SEEN_TS,
								PRODUCTS_SEEN_IN_CAROUSEL_IND,
								RETAIL_CUSTOMER_UUID,
								ROW_LOCATION_CD,
								SLOT_LOCATION_CD,
								SESSION_ID,
								SESSION_SEQUENCE_NBR,
								COOKIE_PREFERENCE_CD,
								DW_LOGICAL_DELETE_IND,
								DW_CURRENT_VERSION_IND,
								DW_CREATE_TS,
								DW_LAST_UPDATE_TS
							FROM 
							(
							select distinct
								APPLICATION_DETAIL_TXT,
								src.BASE_PRODUCT_NBR AS BASE_PRODUCT_NBR,
								CAROUSEL_NM,
								CLUB_CARD_NBR,
								src.EVENT_ID as EVENT_ID,
								HOUSEHOLD_ID,
								MODEL_ID_TXT,
								PRODUCT_FINDING_METHOD_DSC,
								PAGE_NM,
								PRODUCT_SEEN_IND,
								PRODUCT_SEEN_TS,
								PRODUCTS_SEEN_IN_CAROUSEL_IND,
								RETAIL_CUSTOMER_UUID,
								ROW_LOCATION_CD,
								SLOT_LOCATION_CD,
								SESSION_ID,
								SESSION_SEQUENCE_NBR,
								COOKIE_PREFERENCE_CD,
								DW_LOGICAL_DELETE_IND,
								DW_CURRENT_VERSION_IND,
								DW_CREATE_TS,
								DW_LAST_UPDATE_TS,
								tgt.Event_ID as tgtEventID,
								tgt.BASE_PRODUCT_NBR as baseproductnbr
							FROM 
							(
								select distinct 
									CSEM.APP_VERSION_CD as APPLICATION_DETAIL_TXT,
									CSI.BASE_PRODUCT_NBR as BASE_PRODUCT_NBR,
									CSI.CAROUSEL_NM as CAROUSEL_NM,
									CASE 
										WHEN CSEM.CLUB_CARD_NBR = -1 THEN 'Club Card Nbr not found'
										ELSE TO_CHAR(CSEM.CLUB_CARD_NBR)
									END as CLUB_CARD_NBR,
								 	CSEM.EVENT_ID as EVENT_ID,
									CSEM.HOUSEHOLD_ID as HOUSEHOLD_ID,
									CSI.MODEL_ID as MODEL_ID_TXT,
									CSI.PRODUCT_FINDING_METHOD_DSC as PRODUCT_FINDING_METHOD_DSC,
									CSP.PAGE_NM as PAGE_NM,
									TRUE as PRODUCT_SEEN_IND,
									CSEM.EVENT_TS as PRODUCT_SEEN_TS,
									TRUE as PRODUCTS_SEEN_IN_CAROUSEL_IND,
									CASE 
										WHEN CSEM.RETAIL_CUSTOMER_UUID = '-1' OR CSEM.RETAIL_CUSTOMER_UUID IS NULL OR 
										     CSEM.RETAIL_CUSTOMER_UUID = ' ' OR CSEM.RETAIL_CUSTOMER_UUID='' 
                                                                                     THEN 'Retail Customer UUID not found'
										ELSE TO_CHAR(CSEM.RETAIL_CUSTOMER_UUID)
									END as RETAIL_CUSTOMER_UUID,
									CSI.ROW_LOCATION_CD as ROW_LOCATION_CD,
									CSI.SLOT_LOCATION_CD as SLOT_LOCATION_CD,
									CSEM.SESSION_ID as SESSION_ID,
									CSEM.SESSION_SEQUENCE_NBR as SESSION_SEQUENCE_NBR,
									CSV.COOKIE_PREFERENCE_TXT as COOKIE_PREFERENCE_CD,
									FALSE as DW_LOGICAL_DELETE_IND,
									TRUE AS DW_CURRENT_VERSION_IND,
									CURRENT_TIMESTAMP() as DW_CREATE_TS,
									CURRENT_TIMESTAMP() as DW_LAST_UPDATE_TS
								FROM ${src_event_master_tbl} CSEM
								LEFT JOIN ${src_impressions_tbl} CSI
								ON CSEM.EVENT_ID = CSI.EVENT_ID
								AND CSEM.EVENT_TS = CSI.EVENT_TS
								LEFT JOIN ${src_page_tbl} CSP
								ON CSEM.PAGE_INTEGRATION_ID = CSP.PAGE_INTEGRATION_ID
								LEFT JOIN ${src_visitor_tbl} CSV
								ON CSEM.VISITOR_INTEGRATION_ID = CSV.VISITOR_INTEGRATION_ID
								AND CSEM.EVENT_TS = CSV.DW_FIRST_EFFECTIVE_TS 
								WHERE UPPER(CSEM.EVENT_NM) = 'PRODUCT-IMPRESSIONS'
								AND CSEM.SESSION_ID IS NOT NULL AND CSEM.SESSION_SEQUENCE_NBR IS NOT NULL
								AND CSI.BASE_PRODUCT_NBR IS NOT NULL AND CSEM.EVENT_TS > '${final_max_event_ts}'
							) src
							LEFT JOIN 
								(
									SELECT 
										EVENT_ID,
										BASE_PRODUCT_NBR
									FROM ${tgt_tbl} 
									WHERE DW_CURRENT_VERSION_IND = TRUE
								) tgt on
								src.EVENT_ID = tgt.EVENT_ID AND
								src.BASE_PRODUCT_NBR = tgt.BASE_PRODUCT_NBR
						) WHERE tgtEventID IS NULL and EVENT_ID is not null
							)`;

                            
    try {
       snowflake.execute({ sqlText: sql_ins_tgt_tbl });
    } catch (err)  {
        throw `Insert into target table ${tgt_tbl} with error: ${err} with finalevents: {final_max_event_ts}`;   // Return a error message.
    } 
							
$$;

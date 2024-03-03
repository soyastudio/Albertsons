--liquidbase formatted sql
--changeset SYSTEM:ORDER_INFO runOnChange: true splitStatements:false OBJECT_TYPE:STOREDPROCEDURE

USE DATABASE <<TGT_EDM_DB_NAME>>;
USE SCHEMA <<SP_DEPLOY_SCHEMA>>;

CREATE OR REPLACE PROCEDURE <<TGT_EDM_DB_NAME>>.<<SP_DEPLOY_SCHEMA>>.SP_ORDER_INFO_Incremental()
RETURNS VARCHAR(16777216)
LANGUAGE javascript
EXECUTE AS CALLER

AS 

$$

var v_exception ='';
var v_start_date = ''
try
{
    var v_start_date_max = snowflake.createStatement({sqlText:"SELECT TO_CHAR(COALESCE(MAX(RUN_DATE),'2023-03-01'),'YYYY-MM-DD') FROM <<TGT_EDM_DB_NAME>>.<<TAB_DEPLOY_SCHEMA>>.CATALINA_OUTBOUND_LOAD_STATUS WHERE ID = (SELECT MAX(ID) FROM <<TGT_EDM_DB_NAME>>.<<TAB_DEPLOY_SCHEMA>>.CATALINA_OUTBOUND_LOAD_STATUS WHERE TABLE_NAME = 'ORDER_INFO' AND LOAD_STATUS = 'SUCCEEDED')" })
    var resultSet = v_start_date_max.execute(); 
    resultSet.next(); 
    // Retrieve the TIMESTAMP_LTZ and store it in an SfDate variable.
	var v_start_date = resultSet.getColumnValue(1);
}
catch (err)
{
	v_exception += err
}
var v_item_price_Sql = `WITH FULLFILLMENT AS (    
    SELECT DISTINCT ORDER_ID, FULLFILLMENT_TYPE_CD from <<SRC_EDM_DB_NAME>>.<<SRC_EDM_SCHEMA_NAME>>.GROCERY_SUB_ORDER
    WHERE DW_CURRENT_VERSION_IND = TRUE and FULLFILLMENT_TYPE_CD IS NOT NULL
)

select 
    RS.BANNER_NM as RETAILER_ID
    ,TH.FACILITY_NBR as RETAIL_STORE_ID 
    ,TH.Transaction_DT as TRANSACTION_DATE
    ,TH.TRANSACTION_TS as TRANSACTION_TIME
    ,TH.REGISTER_NBR as LANE_ID
    ,RT.REGISTER_DEPARTMENT_NM as REGISTER_NAME
    ,TH.TRANSACTION_ID as TRANSACTION_ID
    ,TH.CHECKER_NBR as OPERATOR_ID
    ,CASE 
       WHEN GOH.ORDER_ID IS NOT NULL THEN 'ONLINE'
       ELSE 'INSTORE'
     END as LANE_TYPE_CODE
    ,CASE WHEN GOH.ORDER_ID IS NULL THEN 'INSTORE'
		ELSE GOH.DEVICE_CD 
	 END as ORDER_INITIATION_LOCATION
    ,CASE
        WHEN GOH.ORDER_ID IS NOT NULL THEN GSO.FULLFILLMENT_TYPE_CD
        ELSE 'INSTORE'
     END as ORDER_DELIVERY_LOCATION
    ,TT.TENDER_SUBTYPE_ID AS PAYMENT_TYPE_CODE
    ,TS.TENDER_SUBTYPE_DSC as PAYMENT_TYPE_DESC
    ,TT.TENDER_AMT AS TENDER_AMT
    ,TH.total_gross_amt + TH.total_tax_amt + TH.total_markdown_amt + TH.total_manufacturer_coupon_amt - TH.total_miscellaneous_amt as ORDER_TOTAL_AMOUNT
    ,CURRENT_TIMESTAMP AS DW_CREATE_TS       

FROM <<SRC_EDM_DB_NAME>>.<<SRC_EDM_SCHEMA_NAME>>.TRANSACTION_HDR TH
LEFT JOIN <<SRC_EDM_DB_NAME>>.<<SRC_EDM_SCHEMA_NAME>>.D1_RETAIL_STORE RS
ON TH.FACILITY_NBR = RS.RETAIL_STORE_FACILITY_NBR
AND RS.DW_LOGICAL_DELETE_IND = FALSE
LEFT JOIN <<SRC_EDM_DB_NAME>>.<<SRC_EDM_SCHEMA_NAME>>.GROCERY_ORDER_HEADER GOH
ON TH.ORDER_ID = GOH.ORDER_ID
AND GOH.DW_CURRENT_VERSION_IND = TRUE
LEFT JOIN <<SRC_EDM_DB_NAME>>.<<SRC_EDM_EDW_SCHEMA_NAME>>.REGISTER_TYPE RT
ON TH.REGISTER_NBR = RT.REGISTER_NBR
LEFT JOIN FULLFILLMENT GSO 
ON GSO.ORDER_ID = TH.ORDER_ID
LEFT JOIN <<SRC_EDM_DB_NAME>>.<<SRC_EDM_SCHEMA_NAME>>.TRANSACTION_TENDER TT
ON TH.TRANSACTION_HDR_INTEGRATION_ID = TT.TRANSACTION_HDR_INTEGRATION_ID
LEFT JOIN <<SRC_EDM_DB_NAME>>.<<SRC_EDM_EDW_SCHEMA_NAME>>.TENDER_SUBTYPE TS
ON TT.TENDER_SUBTYPE_ID = TS.TENDER_SUBTYPE_ID
WHERE (TO_DATE(TH.DW_CREATE_TS) = TO_DATE('` + v_start_date +`') - 1 OR  TO_DATE(TH.DW_LAST_UPDATE_TS) = TO_DATE('` + v_start_date +`') - 1) `;


var v_delete_customer_ids_data = "DELETE FROM <<TGT_EDM_DB_NAME>>.<<TAB_DEPLOY_SCHEMA>>.ORDER_INFO_Incremental WHERE DW_CREATE_TS > current_date - 45"
var v_item_price_tab_fields = "RETAILER_ID ,RETAIL_STORE_ID ,TRANSACTION_DATE, TRANSACTION_TIME, LANE_ID, REGISTER_NAME, TRANSACTION_ID, OPERATOR_ID, LANE_TYPE_CODE, ORDER_INITIATION_LOCATION, ORDER_DELIVERY_LOCATION, PAYMENT_TYPE_CODE, PAYMENT_TYPE_DESC, TENDER_AMT, ORDER_TOTAL_AMOUNT, DW_CREATE_TS" ;
var v_load_tab_insert_query = "insert into <<TGT_EDM_DB_NAME>>.<<TAB_DEPLOY_SCHEMA>>.ORDER_INFO_Incremental "+ "("+v_item_price_tab_fields+" ) "+v_item_price_Sql+";";
var v_catalina_outbound_load_status_query_success = "insert into <<TGT_EDM_DB_NAME>>.<<TAB_DEPLOY_SCHEMA>>.CATALINA_OUTBOUND_LOAD_STATUS (TABLE_NAME, LOAD_STATUS, RUN_DATE, DW_CREATE_TS) VALUES ('ORDER_INFO','SUCCEEDED', current_date, current_timestamp);"
var v_catalina_outbound_load_status_query_fail = "insert into <<TGT_EDM_DB_NAME>>.<<TAB_DEPLOY_SCHEMA>>.CATALINA_OUTBOUND_LOAD_STATUS (TABLE_NAME, LOAD_STATUS, RUN_DATE, DW_CREATE_TS) VALUES ('ORDER_INFO','FAILED', current_date, current_timestamp);;"
try
	{
		var v_delete_old_data = snowflake.createStatement({sqlText: v_delete_customer_ids_data});
		var v_delete_old_date_exec = v_delete_old_data.execute();
		
		var v_load_create = snowflake.createStatement({sqlText: v_load_tab_insert_query});
		var load_results = v_load_create.execute();
			load_results.next();
		var load_op =load_results.getColumnValue(1);
		
		var v_load_status_success = snowflake.createStatement({sqlText: v_catalina_outbound_load_status_query_success});
		var load_status_s = v_load_status_success.execute();
	
	return 'Succeeded: '+load_op +v_item_price_Sql+v_start_date;
	}

catch (err)
	{
		var v_load_status_failed = snowflake.createStatement({sqlText: v_catalina_outbound_load_status_query_fail});
		var load_status_fail = v_load_status_failed.execute();
	return 'Failed:  '+ v_start_date+ err + v_item_price_Sql+v_start_date;
	}
$$;

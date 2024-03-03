--liquidbase formatted sql
--changeset SYSTEM:ITEMS_IN_ORDER runOnChange: true splitStatements:false OBJECT_TYPE:STOREDPROCEDURE

USE DATABASE <<TGT_EDM_DB_NAME>>;
USE SCHEMA <<SP_DEPLOY_SCHEMA>>;

CREATE OR REPLACE PROCEDURE <<TGT_EDM_DB_NAME>>.<<SP_DEPLOY_SCHEMA>>.SP_ITEMS_IN_ORDER_Incremental()
RETURNS VARCHAR(16777216)
LANGUAGE javascript
EXECUTE AS CALLER

AS 

$$

var v_exception ='';
var v_start_date = ''
try
{
    var v_start_date_max = snowflake.createStatement({sqlText:"SELECT TO_CHAR(COALESCE(MAX(RUN_DATE),'2023-03-01'),'YYYY-MM-DD') FROM <<TGT_EDM_DB_NAME>>.<<TAB_DEPLOY_SCHEMA>>.CATALINA_OUTBOUND_LOAD_STATUS WHERE ID = (SELECT MAX(ID) FROM <<TGT_EDM_DB_NAME>>.<<TAB_DEPLOY_SCHEMA>>.CATALINA_OUTBOUND_LOAD_STATUS WHERE TABLE_NAME = 'ITEMS_IN_ORDER' AND LOAD_STATUS = 'SUCCEEDED')" })
    var resultSet = v_start_date_max.execute(); 
    resultSet.next(); 
    // Retrieve the TIMESTAMP_LTZ and store it in an SfDate variable.
	var v_start_date = resultSet.getColumnValue(1);
}
catch (err)
{
	v_exception += err
}
var v_item_price_Sql = `select 
    RS.BANNER_NM as RETAILER_ID
    ,TH.FACILITY_NBR as RETAIL_STORE_ID 
    ,TH.Transaction_DT as TRANSACTION_DATE
    ,TH.TRANSACTION_TS as TRANSACTION_TIME
    ,TH.REGISTER_NBR as LANE_ID
    ,RT.REGISTER_DEPARTMENT_NM as REGISTER_NAME
    ,TH.TRANSACTION_ID as TRANSACTION_ID
    ,TI.UPC_NBR as TRADE_ITEM_ID
    ,CASE
       WHEN length(TI.UPC_NBR) < 6 then 'PLU'
       ELSE 'UPC'
     END as TRADE_ITEM_ID_TYPE
    ,TI.MEASURE_QTY as MEASURE_QTY
    ,TI.ITEM_QTY as ITEM_QTY
    ,CASE
       WHEN GD.ORDER_ID IS NOT NULL THEN UPPER(GD.UOM_CD)
       WHEN GD.ORDER_ID IS NULL AND TI.MEASURE_QTY = 0 THEN 'Each'
       WHEN GD.ORDER_ID IS NULL AND TI.MEASURE_QTY <> 0 AND D1U.SCAN_UNIT_IND = TRUE THEN 'Each'
       WHEN GD.ORDER_ID IS NULL AND TI.MEASURE_QTY <> 0 AND D1U.SCAN_UNIT_IND = FALSE THEN 'Lb'
	   ELSE 'N/A'
     END as UNIT_OF_MEASURE
    ,CASE
       WHEN GD.ORDER_ID IS NOT NULL THEN GD.ORDER_QTY
       WHEN GD.ORDER_ID IS NULL AND TI.MEASURE_QTY = 0 THEN TI.ITEM_QTY
       WHEN GD.ORDER_ID IS NULL AND TI.MEASURE_QTY <> 0 AND D1U.SCAN_UNIT_IND = TRUE THEN TI.ITEM_QTY
       WHEN GD.ORDER_ID IS NULL AND TI.MEASURE_QTY <> 0 AND D1U.SCAN_UNIT_IND = FALSE THEN TI.MEASURE_QTY
     END as UNIT_QUANTITY
    ,CASE
       WHEN GD.ORDER_ID IS NOT NULL THEN GD.UNIT_PRICE_AMT
       WHEN GD.ORDER_ID IS NULL AND TI.MEASURE_QTY = 0 THEN TI.GROSS_AMT/TI.ITEM_QTY
       WHEN GD.ORDER_ID IS NULL AND TI.MEASURE_QTY <> 0 AND D1U.SCAN_UNIT_IND = TRUE THEN TI.GROSS_AMT/TI.ITEM_QTY
       WHEN GD.ORDER_ID IS NULL AND TI.MEASURE_QTY <> 0 AND D1U.SCAN_UNIT_IND = FALSE THEN TI.GROSS_AMT/TI.MEASURE_QTY
     END as UNIT_PRICE
    ,D1U.SCAN_UNIT_IND
    ,TI.GROSS_AMT
    ,D1U.EQUIVALIZED_FACTOR_SOURCE_UPDATE_IND
    ,D1U.ITEM_DSC
	,CURRENT_TIMESTAMP AS DW_CREATE_TS
FROM <<SRC_EDM_DB_NAME>>.<<SRC_EDM_SCHEMA_NAME>>.TRANSACTION_HDR TH
LEFT JOIN <<SRC_EDM_DB_NAME>>.<<SRC_EDM_SCHEMA_NAME>>.TRANSACTION_ITEM TI
ON TH.TRANSACTION_HDR_INTEGRATION_ID = TI.TRANSACTION_HDR_INTEGRATION_ID
LEFT JOIN <<SRC_EDM_DB_NAME>>.<<SRC_EDM_SCHEMA_NAME>>.D1_RETAIL_STORE RS
ON TH.FACILITY_NBR = RS.RETAIL_STORE_FACILITY_NBR
AND RS.DW_LOGICAL_DELETE_IND = FALSE
LEFT JOIN <<SRC_EDM_DB_NAME>>.<<SRC_EDM_SCHEMA_NAME>>.GROCERY_ORDER_DETAIL GD
ON TH.ORDER_ID = GD.ORDER_ID
AND TI.UPC_NBR = GD.UPC_NBR
AND GD.DW_CURRENT_VERSION_IND = TRUE
LEFT JOIN <<SRC_EDM_DB_NAME>>.<<SRC_EDM_EDW_SCHEMA_NAME>>.REGISTER_TYPE RT
ON TH.REGISTER_NBR = RT.REGISTER_NBR
LEFT JOIN <<SRC_EDM_DB_NAME>>.<<SRC_EDM_SCHEMA_NAME>>.D1_UPC D1U
ON TI.UPC_NBR = D1U.UPC_NBR
WHERE (TO_DATE(TH.DW_CREATE_TS) = TO_DATE('` + v_start_date +`') - 1 OR  TO_DATE(TH.DW_LAST_UPDATE_TS) = TO_DATE('` + v_start_date +`') - 1) `;

var v_delete_items_in_order_data = "DELETE FROM <<TGT_EDM_DB_NAME>>.<<TAB_DEPLOY_SCHEMA>>.ITEMS_IN_ORDER_Incremental WHERE DW_CREATE_TS > current_date - 45"
var v_item_price_tab_fields = "RETAILER_ID ,RETAIL_STORE_ID ,TRANSACTION_DATE, TRANSACTION_TIME, LANE_ID, REGISTER_NAME, TRANSACTION_ID, TRADE_ITEM_ID, TRADE_ITEM_ID_TYPE, MEASURE_QTY, ITEM_QTY,  UNIT_OF_MEASURE, UNIT_QUANTITY, UNIT_PRICE, SCAN_UNIT_IND, GROSS_AMT, EQUIVALIZED_FACTOR_SOURCE_UPDATE_IND, ITEM_DSC, DW_CREATE_TS" ;
var v_load_tab_insert_query = "insert into <<TGT_EDM_DB_NAME>>.<<TAB_DEPLOY_SCHEMA>>.ITEMS_IN_ORDER_Incremental "+ "("+v_item_price_tab_fields+" ) "+v_item_price_Sql+";";
var v_catalina_outbound_load_status_fields = "TABLE_NAME, LOAD_STATUS, RUN_DATE, DW_CREATE_TS";
var v_catalina_outbound_load_status_query_success = "insert into <<TGT_EDM_DB_NAME>>.<<TAB_DEPLOY_SCHEMA>>.CATALINA_OUTBOUND_LOAD_STATUS (TABLE_NAME, LOAD_STATUS, RUN_DATE, DW_CREATE_TS) VALUES ('ITEMS_IN_ORDER','SUCCEEDED', current_date, current_timestamp);"
var v_catalina_outbound_load_status_query_fail = "insert into <<TGT_EDM_DB_NAME>>.<<TAB_DEPLOY_SCHEMA>>.CATALINA_OUTBOUND_LOAD_STATUS (TABLE_NAME, LOAD_STATUS, RUN_DATE, DW_CREATE_TS) VALUES ('ITEMS_IN_ORDER','FAILED', current_date, current_timestamp);;"
try
	{
		var v_delete_old_data = snowflake.createStatement({sqlText: v_delete_items_in_order_data});
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

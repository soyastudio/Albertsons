--liquidbase formatted sql
--changeset SYSTEM:ITEM_PRICE_ADJUSTMENTS runOnChange: true splitStatements:false OBJECT_TYPE:STOREDPROCEDURE

USE DATABASE <<TGT_EDM_DB_NAME>>;
USE SCHEMA <<SP_DEPLOY_SCHEMA>>;

CREATE OR REPLACE PROCEDURE <<TGT_EDM_DB_NAME>>.<<SP_DEPLOY_SCHEMA>>.SP_ITEM_PRICE_ADJUSTMENTS_Incremental()
RETURNS VARCHAR(16777216)
LANGUAGE javascript
EXECUTE AS CALLER

AS 

$$

var v_exception ='';
var v_start_date = ''
try
{
    var v_start_date_max = snowflake.createStatement({sqlText:"SELECT TO_CHAR(COALESCE(MAX(RUN_DATE),'2023-03-01'),'YYYY-MM-DD') FROM <<TGT_EDM_DB_NAME>>.<<TAB_DEPLOY_SCHEMA>>.CATALINA_OUTBOUND_LOAD_STATUS WHERE ID = (SELECT MAX(ID) FROM <<TGT_EDM_DB_NAME>>.<<TAB_DEPLOY_SCHEMA>>.CATALINA_OUTBOUND_LOAD_STATUS WHERE TABLE_NAME = 'ITEM_PRICE_ADJUSTMENTS' AND LOAD_STATUS = 'SUCCEEDED')" })
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
    ,TIM.OMS_OFFER_ID as ADJUSTMENT_ID
    ,OO.PROGRAM_CD as ADJUSTMENT_TYPE_CODE
    ,TI.ITEM_QTY AS APPLICABLE_UNIT_QUANTITY
    ,TIM.MARKDOWN_AMT AS ADJUSTMENT_UNIT_AMOUNT
	,CURRENT_TIMESTAMP AS DW_CREATE_TS        
FROM <<SRC_EDM_DB_NAME>>.<<SRC_EDM_SCHEMA_NAME>>.TRANSACTION_HDR TH
LEFT JOIN <<SRC_EDM_DB_NAME>>.<<SRC_EDM_SCHEMA_NAME>>.TRANSACTION_ITEM TI
ON TH.TRANSACTION_HDR_INTEGRATION_ID = TI.TRANSACTION_HDR_INTEGRATION_ID
LEFT JOIN <<SRC_EDM_DB_NAME>>.<<SRC_EDM_SCHEMA_NAME>>.RETAIL_STORE RS
ON TH.FACILITY_NBR = RS.FACILITY_NBR
AND RS.DW_CURRENT_VERSION_IND = TRUE
JOIN <<SRC_EDM_DB_NAME>>.<<SRC_EDM_SCHEMA_NAME>>.TRANSACTION_ITEM_MARKDOWN TIM
ON TI.TRANSACTION_HDR_INTEGRATION_ID = TIM.TRANSACTION_HDR_INTEGRATION_ID
AND TI.ITEM_SEQUENCE_NBR = TIM.ITEM_SEQUENCE_NBR
LEFT JOIN <<SRC_EDM_DB_NAME>>.<<SRC_EDM_SCHEMA_NAME>>.OMS_OFFER OO 
ON TIM.OMS_OFFER_ID = OO.OMS_OFFER_ID
AND OO.DW_CURRENT_VERSION_IND = TRUE
LEFT JOIN <<SRC_EDM_DB_NAME>>.<<SRC_EDM_EDW_SCHEMA_NAME>>.REGISTER_TYPE RT
ON TH.REGISTER_NBR = RT.REGISTER_NBR
 WHERE (TO_DATE(TH.DW_CREATE_TS) = TO_DATE('` + v_start_date +`') - 1 OR  TO_DATE(TH.DW_LAST_UPDATE_TS) = TO_DATE('` + v_start_date +`') - 1)  `;

var v_delete_item_price_adjustments_data = "DELETE FROM <<TGT_EDM_DB_NAME>>.<<TAB_DEPLOY_SCHEMA>>.ITEM_PRICE_ADJUSTMENTS_Incremental WHERE DW_CREATE_TS > current_date - 45"
var v_item_price_tab_fields = "RETAILER_ID ,RETAIL_STORE_ID ,TRANSACTION_DATE, TRANSACTION_TIME, LANE_ID, REGISTER_NAME, TRANSACTION_ID, TRADE_ITEM_ID, ADJUSTMENT_ID,ADJUSTMENT_TYPE_CODE,APPLICABLE_UNIT_QUANTITY,ADJUSTMENT_UNIT_AMOUNT,DW_CREATE_TS" ;
var v_load_tab_insert_query = "insert into <<TGT_EDM_DB_NAME>>.<<TAB_DEPLOY_SCHEMA>>.ITEM_PRICE_ADJUSTMENTS_Incremental "+ "("+v_item_price_tab_fields+" ) "+v_item_price_Sql+";";
var v_catalina_outbound_load_status_query_success = "insert into <<TGT_EDM_DB_NAME>>.<<TAB_DEPLOY_SCHEMA>>.CATALINA_OUTBOUND_LOAD_STATUS (TABLE_NAME, LOAD_STATUS, RUN_DATE, DW_CREATE_TS) VALUES ('ITEM_PRICE_ADJUSTMENTS','SUCCEEDED', current_date, current_timestamp);"
var v_catalina_outbound_load_status_query_fail = "insert into <<TGT_EDM_DB_NAME>>.<<TAB_DEPLOY_SCHEMA>>.CATALINA_OUTBOUND_LOAD_STATUS (TABLE_NAME, LOAD_STATUS, RUN_DATE, DW_CREATE_TS) VALUES ('ITEM_PRICE_ADJUSTMENTS','FAILED', current_date, current_timestamp);"
try
	{
		var v_delete_old_data = snowflake.createStatement({sqlText: v_delete_item_price_adjustments_data});
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

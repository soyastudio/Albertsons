--liquibase formatted sql
--changeset SYSTEM:SP_GroceryOrder_To_wysiwyg_Savings_Load runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_CONFIRMED_PRD;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE EDM_CONFIRMED_PRD.DW_APPL.SP_GROCERYORDER_TO_WYSIWYG_SAVINGS_LOAD()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

    // Global Variable Declaration
    
    var ref_db = "EDM_REFINED_PRD";
	var cnf_db = "EDM_CONFIRMED_PRD";
    var dw_prd_schema = "DW_C_ECOMMERCE";
    var wrk_schema = "DW_C_STAGE";
	var appl_schema = "DW_APPL";
    var src_tbl = ref_db + "." + appl_schema + ".esed_groceryorder_wysiwyg_r_stream";
    var src_rerun_tbl = cnf_db + "." + wrk_schema + ".esed_groceryorder_wysiwyg_Rerun";
    var src_wrk_tbl = cnf_db + "." + wrk_schema + ".esed_groceryorder_wysiwyg_wrk";
	var tgt_wrk_tbl = cnf_db + "." + wrk_schema + ".grocery_order_savings_created_wysiwyg_wrk";
	var tgt_tbl = cnf_db + "." + dw_prd_schema + ".grocery_order_savings_created_wysiwyg";


    // check if rerun queue table exists otherwise create it
	
	var sql_crt_rerun_tbl = `CREATE TABLE IF NOT EXISTS `+ src_rerun_tbl + ` DATA_RETENTION_TIME_IN_DAYS = 0
    AS SELECT * FROM `+ src_tbl +`  where 1=2;`;
	try {
      snowflake.execute (
          {sqlText: sql_crt_rerun_tbl  }
          );
  }
  catch (err)  {
    throw "Creation of rerun queue table "+ src_rerun_tbl +" Failed with error: " + err;   // Return a error message.
  }
    
    // persist stream data in work table for the current transaction, includes data from previous failed run

    var sql_crt_src_wrk_tbl = `create or replace table `+ src_wrk_tbl +` DATA_RETENTION_TIME_IN_DAYS = 1 as 
                                SELECT * FROM `+ src_tbl +` WHERE METADATA$ACTION = 'INSERT'
                                UNION ALL 
                                SELECT * FROM `+ src_rerun_tbl+``;
    try {
        snowflake.execute (
            {sqlText: sql_crt_src_wrk_tbl  }
            );
        }
    catch (err)  {
        throw "Creation of Source Work table "+ src_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }
	
    
    // Empty the rerun queue table
    var sql_empty_rerun_tbl = `TRUNCATE `+ src_rerun_tbl + ` `;
    try {
        snowflake.execute (
            {sqlText: sql_empty_rerun_tbl  }
            );
        }
    catch (err)  {
        throw "Truncation of rerun queue table "+ src_rerun_tbl +" Failed with error: " + err;   // Return a error message.
        }
    
    // query to load rerun queue table when encountered a failure

    var sql_ins_rerun_tbl = `INSERT OVERWRITE INTO  `+ src_rerun_tbl+` SELECT * FROM `+ src_wrk_tbl+``;
   
    var cr_src_tmp_wrk_tbl =`CREATE OR REPLACE TABLE ` + tgt_wrk_tbl + ` AS

    with Level_1_flatten
        as
        (
            select tbl.SRC_XML:"@"::string AS BODNm,
                    tbl.FILENAME AS FILENAME,
                    GetGroceryOrder.Value as Value,
                    GetGroceryOrder.SEQ::integer as SEQ,
                    GetGroceryOrder.index::integer as idx
             FROM  `+ src_wrk_tbl+` tbl
                ,LATERAL FLATTEN(tbl.SRC_XML:"$") GetGroceryOrder
                  Where tbl.src_xml like '%<Abs:StatusTypeCd>CREATE</Abs:StatusTypeCd>%'
          and date(created_ts) >= current_date - 8
        )
SELECT OrderId,GroceryOrderHeader_VersionNbr,BaseProductNbr,ItemDescription,GroceryOrderDetail_SavingsCategoryId,GroceryOrderDetail_SavingsCategoryNm,GroceryOrderDetail_SavingsAmt,CURRENT_TIMESTAMP as DW_CREATE_TS
from (
  SELECT XMLGET(GroceryOrderData.value,'Abs:OrderId'):"$"::string AS OrderId
  		,XMLGET(GroceryOrderData.value,'Abs:VersionNbr'):"$"::string AS GroceryOrderHeader_VersionNbr
                  ,GetGroceryOrder.SEQ::integer as SEQ
                ,GetGroceryOrder.idx::integer as idx
                ,GroceryOrderData.SEQ::integer as SEQ1
                ,GroceryOrderData.index::integer as idx1
                 FROM    Level_1_flatten GetGroceryOrder
                ,LATERAL FLATTEN(GetGroceryOrder.value:"$") GroceryOrderData
             WHERE    GetGroceryOrder.value like '<GroceryOrderData>%'
             AND    GroceryOrderData.value like '<Abs:GroceryOrderHeader>%' ) main      
  LEFT JOIN (
           SELECT
            XMLGET(GroceryOrderDetail.value,'Abs:BaseProductNbr'):"$"::string AS BaseProductNbr
            ,XMLGET(GroceryOrderDetail.value,'Abs:ItemDescription'):"$"::string AS ItemDescription
              ,GetGroceryOrder.SEQ::integer as SEQ
                ,GetGroceryOrder.idx::integer as idx
                ,GroceryOrderData.SEQ::integer as SEQ1
                ,GroceryOrderData.index::integer as idx1
                ,GrocerySubOrder.SEQ::integer as SEQ2
                ,GrocerySubOrder.index::integer as idx2
                ,GroceryOrderDetail.SEQ::integer as SEQ3
                ,GroceryOrderDetail.index::integer as idx3
                      FROM    Level_1_flatten GetGroceryOrder
                ,LATERAL FLATTEN(GetGroceryOrder.value:"$") GroceryOrderData
                ,LATERAL FLATTEN(GroceryOrderData.value:"$") GrocerySubOrder
                ,LATERAL FLATTEN(GrocerySubOrder.value:"$") GroceryOrderDetail
             WHERE    GetGroceryOrder.value like '<GroceryOrderData>%'
             AND    GroceryOrderData.value like '<Abs:GrocerySubOrder>%'
             AND    GrocerySubOrder.value like '<Abs:GroceryOrderDetail>%'
             AND    GroceryOrderDetail.value like '<Abs:ItemId>%'
             ) Item
              on Item.SEQ = Main.SEQ AND Item.idx = Main.idx
            LEFT JOIN (
            SELECT
            XMLGET(CustomerSavings.value,'Abs:SavingsCategoryId'):"$"::string AS GroceryOrderDetail_SavingsCategoryId
             ,XMLGET(CustomerSavings.value,'Abs:SavingsCategoryNm'):"$"::string AS GroceryOrderDetail_SavingsCategoryNm
             ,XMLGET(CustomerSavings.value,'Abs:SavingsAmt'):"$"::string AS GroceryOrderDetail_SavingsAmt
        ,GetGroceryOrder.SEQ::integer as SEQ
        ,GetGroceryOrder.idx::integer as idx
        ,GroceryOrderData.SEQ::integer as SEQ1
        ,GroceryOrderData.index::integer as idx1
        ,GrocerySubOrder.SEQ::integer as SEQ2
        ,GrocerySubOrder.index::integer as idx2
        ,GroceryOrderDetail.SEQ::integer as SEQ3
        ,GroceryOrderDetail.index::integer as idx3
    FROM    LEVEL_1_FLATTEN AS GetGroceryOrder
        ,LATERAL FLATTEN(TO_ARRAY(GetGroceryOrder.value:"$")) GroceryOrderData
        ,LATERAL FLATTEN(TO_ARRAY(GroceryOrderData.value:"$")) GrocerySubOrder
        ,LATERAL FLATTEN(TO_ARRAY(GrocerySubOrder.value:"$")) GroceryOrderDetail
        ,LATERAL FLATTEN(TO_ARRAY(GroceryOrderDetail.value:"$")) CustomerSavings
    WHERE    GetGroceryOrder.value like '<GroceryOrderData>%'
    AND    (GroceryOrderData.value like '<Abs:GrocerySubOrder>%' )
    AND    (GrocerySubOrder.value like '<Abs:GroceryOrderDetail>%' )
    AND    (GroceryOrderDetail.value like '<Abs:CustomerSavings>%' )
    AND    (CustomerSavings.value like '<Abs:SavingsCategoryType>%' ) ) sav
              on sav.SEQ = Item.SEQ AND sav.idx = Item.idx
                    AND sav.SEQ1 = Item.SEQ1 AND sav.idx1 = Item.idx1
                    AND sav.SEQ2 = Item.SEQ2 AND sav.idx2 = Item.idx2;`;
					
	try {
			snowflake.execute (
				{sqlText: cr_src_tmp_wrk_tbl  }
			)
		}
		catch (err)  {
			return "Creation of WYSIWYG savings src_tmp_wrk_tbl table "+ src_wrk_tbl +" Failed with error: " + err;   // Return a error message.
		}			
    
	var sql_begin = "BEGIN";

		// Processing deletes for store_id, upc_ids, points and point_group
		var sql_deletes = `delete from ` + tgt_tbl + `
				where (orderid,GroceryOrderHeader_VersionNbr)
				in (select distinct orderid,GroceryOrderHeader_VersionNbr
				from ` + tgt_wrk_tbl + `);`;
	
		// Processing Inserts 
		var sql_inserts = `INSERT INTO ` + tgt_tbl + `
							SELECT * FROM  ` + tgt_wrk_tbl + ` ;`;
							
							
	var sql_commit = "COMMIT"
		var sql_rollback = "ROLLBACK"
		try {
			snowflake.execute (
				{sqlText: sql_begin}
			);
			snowflake.execute (
				{sqlText: sql_deletes}
			);
			snowflake.execute (
				{sqlText: sql_inserts}
			);
			snowflake.execute (
				{sqlText: sql_commit}
			);    
		}
		catch (err)  { 
		    snowflake.execute ( 
						{sqlText: sql_ins_rerun_tbl }
						); 
            throw "Loading of table "+ tgt_flat_tbl +" Failed with error: " + err;   // Return a error message.
        }
				// **************        Load for Fact_Offer_request ENDs *****************
				
		return "Done"

	
$$;
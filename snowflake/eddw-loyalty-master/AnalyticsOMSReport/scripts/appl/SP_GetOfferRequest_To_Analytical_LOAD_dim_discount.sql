--liquibase formatted sql
--changeset SYSTEM:SP_GETOFFERREQUEST_TO_ANALYTICAL_LOAD_DIM_DISCOUNT runOnChange:true splitStatements:false OBJECT_TYPE:SP

use database EDM_ANALYTICS_PRD;
use schema dw_appl;

CREATE OR REPLACE PROCEDURE SP_GETOFFERREQUEST_TO_ANALYTICAL_LOAD_DIM_DISCOUNT
(SRC_WRK_TBL VARCHAR, ANL_DB VARCHAR, ANL_SCHEMA VARCHAR, WRK_SCHEMA VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$ 
    // **************        Load for dim_discount table BEGIN *****************
    var src_wrk_tbl = SRC_WRK_TBL;
    var anl_db = ANL_DB;
    var anl_schema = ANL_SCHEMA;
    var wrk_schema = WRK_SCHEMA; 
    
    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
    
    var tgt_wrk_tbl =  anl_db + "." + wrk_schema + ".dim_discount_WRK";
    var tgt_tbl = anl_db + "." + anl_schema + ".dim_discount";
    

    var seq = `create or replace sequence discount_seq`;

    try {
        snowflake.execute (
            {sqlText: seq  }
            );
            }
            catch (err)  {
        return "Creating of sequence failed: " + err;   // Return a error message.
        }

    var cr_src_wrk_tbl = `CREATE OR REPLACE TABLE `+ tgt_wrk_tbl +` AS
                            SELECT           Discount_Id
                                            ,src.Discount_Dsc                     
                                            ,src.DW_LOGICAL_DELETE_IND
                                            ,CASE WHEN tgt.Discount_Id is NULL then 'I' ELSE 'U' END as DML_Type
                                    FROM (
                                        SELECT   Discount_Dsc
                                                ,FALSE AS DW_Logical_delete_ind
                                        FROM (
                                                SELECT case when BENEFITVALUETYPE_CODE = 'NO_DISCOUNT' then 'No Discount' 
												        else BenefitValueType_Description end as Discount_Dsc 
                                                        ,row_number() over ( PARTITION BY Discount_Dsc ORDER BY to_timestamp_ntz(updatets) desc) as rn
                                                FROM   ` + src_wrk_tbl +`
                                                WHERE   Discount_Dsc is not null
                                                
                                            )
                                        WHERE rn = 1
                                    ) src
                                LEFT JOIN
                                        (
                                         SELECT 
                                                Discount_Id
                                               ,Discount_Dsc
                                               ,DW_Logical_delete_ind
                                         FROM   ` + tgt_tbl + `
                                         ) tgt  on  src.Discount_Dsc = tgt.Discount_Dsc
                                 where tgt.Discount_Dsc  is null 
                                  OR (
                                  src.DW_Logical_delete_ind <> tgt.DW_Logical_delete_ind
                                     )
                                  `;
                            
    
    try {
        snowflake.execute (
            {sqlText: cr_src_wrk_tbl  }
        )
    }
    catch (err)  {
        return "Creation of dim_discount tgt_wrk_tbl table "+ src_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }


/*

var sql_updates = // Processing Updates of Type 2 SCD
                  ` UPDATE ` + tgt_tbl + ` as tgt
                    SET  
                        ,DW_Logical_delete_ind = src.DW_Logical_delete_ind
                        ,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
                    FROM (  SELECT   Discount_Id
                            ,Discount_Dsc 
                            ,DW_Logical_delete_ind        
                            FROM    `+ tgt_wrk_tbl +` 
                            WHERE   DML_Type = 'U'
                          ) src
                    WHERE   src.Discount_Id = tgt.Discount_Id `;  

*/
         
var sql_begin = "BEGIN"

    // Processing Inserts
    var sql_inserts = `INSERT INTO ` + tgt_tbl + `
        ( 
         Discount_Id 
        ,Discount_Dsc
        ,DW_CREATE_TS 
        ,DW_LOGICAL_DELETE_IND
    )
    SELECT
         CASE WHEN DML_TYPE = 'I' THEN CASE WHEN max.max_value is NULL then s.nextval ELSE s.nextval + max.max_value END ELSE Discount_Id END as Discount_Id
        ,Discount_Dsc
        ,current_timestamp() AS DW_CREATE_TS 
        ,DW_LOGICAL_DELETE_IND
    FROM ` + tgt_wrk_tbl + `
    INNER JOIN (select distinct MAX(Discount_Id)  as max_value from ` + tgt_tbl + ` ) max inner join table(getnextval(discount_seq)) s
    WHERE DML_Type = 'I'
   `; 

    var sql_commit = "COMMIT"
    var sql_rollback = "ROLLBACK"
    try {
        snowflake.execute (
            {sqlText: sql_begin}
        );
        snowflake.execute (
            {sqlText: sql_inserts}
        );
        snowflake.execute (
            {sqlText: sql_commit}
        );    
    }
    catch (err) {
        snowflake.execute (
            {sqlText: sql_rollback}
        );
        return "Loading of dim_discount " + tgt_tbl + " Failed with error: " + err;   // Return a error message.
    }
            // **************        Load for dim_discount ENDs *****************
            
    return "Done"
$$;

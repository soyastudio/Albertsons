--liquibase formatted sql
--changeset SYSTEM:SP_GETOFFERREQUEST_TO_ANALYTICAL_LOAD_DIM_PRODUCT_GROUP_UPC runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_ANALYTICS_PRD;
use schema DW_RETAIL_EXP;

CREATE OR REPLACE PROCEDURE EDM_ANALYTICS_PRD.DW_RETAIL_EXP.SP_GETOFFERREQUEST_TO_ANALYTICAL_LOAD_DIM_PRODUCT_GROUP_UPC(SRC_WRK_TBL VARCHAR, PG_SRC_TBL VARCHAR, ANL_DB VARCHAR, ANL_SCHEMA VARCHAR, CNF_DB VARCHAR, CNF_SCHEMA VARCHAR, WRK_SCHEMA VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

    // **************        Load for DIM_Productgroup_UPC table BEGIN *****************
    var src_wrk_tbl = SRC_WRK_TBL;
    var pg_src_tbl = PG_SRC_TBL;    var anl_db = ANL_DB;
    var anl_schema = ANL_SCHEMA;
    var cnf_db = CNF_DB;
    var cnf_schema = CNF_SCHEMA;
    var wrk_schema = WRK_SCHEMA;
    
    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
    
    var tgt_wrk_tbl =  anl_db + "." + wrk_schema + ".DIM_Product_group_UPC_WRK";
    var tgt_tbl = anl_db + "." + anl_schema + ".DIM_Product_group_UPC";
    var upc_lkp_tbl = cnf_db + "." + cnf_schema + ".Corporate_item_upc_reference";
    var corp_item_lkp_tbl = cnf_db + "." + cnf_schema +  ".corporate_item";
    var src_wrk_tmp_tbl = anl_db + "." + wrk_schema + ".DIM_Product_group_UPC_SRC_WRK";

    var cr_src_wrk_tmp_tbl = `CREATE OR REPLACE TABLE `+ src_wrk_tmp_tbl +` as
    						                      select pg.Product_Group_Id 
                                                        ,Product_Group_Nm
                                                        ,UPC_Nbr
                                                   FROM
                                                   ( select Product_Group_Id
                                                      FROM
                                                        ( 
                                                        select Product_Group_Id
                                                        FROM
                                                            (select
                                                            Productgroup_productgroupid AS Product_Group_Id
                                                            ,row_number() over (partition by Product_Group_Id order by UPDATETS desc) as rn
                                                            FROM `+ src_wrk_tbl +`
                                                            WHERE Productgroup_productgroupnm not in
                                                            ('Any Product','OWN Brands Items - Corporate Managed UPC List' )
                                                            ) t
                                                            where rn = 1 
                                                            UNION ALL
                                                            SELECT Product_Group_Id
                                                            FROM
                                                            (select
                                                            INCLUDEDPRODUCTGROUPID AS Product_Group_Id
                                                            ,row_number() over (partition by Product_Group_Id order by UPDATETS desc) as rn
                                                            FROM `+ src_wrk_tbl +`
                                                            WHERE INCLUDEDPRODUCTGROUPNM not in
                                                            ('Any Product','OWN Brands Items - Corporate Managed UPC List' )
                                                            ) t
                                                             where rn = 1
                                                        )
                                                    ) src
                                                JOIN 
                                                (
                                                  select upc.Value::number as UPC_Nbr,
                                                Payload_Id as Product_Group_Id ,
                                                Payload_Name as Product_Group_Nm
                                                from `+ pg_src_tbl  +`
                                                ,LATERAL FLATTEN(payload_productGroupIds_upcIds, outer => TRUE) as upc 
                                                where UPC_Nbr is NOT NULL
                                                AND ( Payload_Name <> 'OWN Brands Items - Corporate Managed UPC List' AND
                                                    Payload_Name <> 'Any Product')
                                                ) pg
                                                ON pg.Product_Group_Id  = src.Product_Group_Id
                                                 `;

    try {
        snowflake.execute (
            {sqlText: cr_src_wrk_tmp_tbl  }
        );
    }
    catch (err)  { 
        return "Creation of DIM_Productgroup_UPC + cr_src_wrk_tbl+  table  Failed with error: " + err;   // Return a error message.
        }  



    var cr_tgt_wrk_tbl = `CREATE OR REPLACE TABLE `+ tgt_wrk_tbl +` AS
                                            SELECT src.Product_Group_Id
                                                ,src.Product_group_Nm
                                                ,src.UPC_Nbr 
                                                ,NULL AS Upc_Dsc
                                                ,NULL AS Retail_Item_Dsc
                                                ,NULL AS Internet_Item_Dsc
                                                ,NULL AS UPC_UOM
                                                ,NULL AS UOM_Size_Qty
                                                ,FALSE AS DW_LOGICAL_DELETE_IND
                                                ,CASE WHEN tgt.UPC_Nbr is NULL AND tgt.Product_group_Nm IS NULL then 'I' ELSE 'U' END as DML_Type
                                                FROM 
                                                (select distinct Product_Group_Id
                                                        , Product_Group_Nm
                                                        , Upc_Nbr 
                                                        FROM
                                                       `+ src_wrk_tmp_tbl +`
                                                ) src
                                                LEFT JOIN
                                                                (
                                                                 SELECT  Product_group_Id
                                                                        ,Product_group_Nm
                                                                        ,Upc_Nbr
                                                                        
                                                                 FROM   ` + tgt_tbl + `
                                                                 ) tgt  on  src.UPC_Nbr = tgt.UPC_Nbr
                                                                 AND src.Product_group_Id = tgt.Product_group_Id
                                                 `;


     try {
        snowflake.execute (
            {sqlText: cr_tgt_wrk_tbl }
        );
    }
    catch (err)  { 
        return "Creation of DIM_Productgroup_UPC + tgt_wrk_tbl +  table  Failed with error: " + err;   // Return a error message.
        }  



    var upd_src_wrk_tbl = `UPDATE `+ tgt_wrk_tbl +`  tgt
    					  set Upc_Dsc = src.Upc_Dsc
                         ,Retail_Item_Dsc = src.Retail_Item_Dsc
                         ,Internet_Item_Dsc = src.Internet_Item_Dsc
                         ,UPC_UOM = src.UPC_UOM
                         ,UOM_Size_Qty = src.UOM_Size_Qty
                         FROM 
                                                (select y.Product_Group_Id
                                                        ,y.Product_group_Nm
                                                        ,y.UPC_Nbr 
                                                        ,x.Upc_Dsc
                                                        ,x.Retail_Item_Dsc
                                                        ,x.Internet_Item_Dsc
                                                        ,x.UPC_UOM
                                                        ,x.UOM_Size_Qty
                                                        ,FALSE AS DW_Logical_delete_ind
                                                from
                                                (select 
                                                 b.upc_nbr as upc_nbr 
                                                ,Internet_Item_Dsc
                                                ,Retail_Item_Dsc
                                                ,Item_Dsc as Upc_Dsc
                                                ,Size_Qty as UOM_Size_Qty
                                                ,size_uom_cd as UPC_UOM
                                                ,PREFERED_CORPORATE_ITEM_SEQ_NBR
                                                , row_number() over (partition by b.upc_nbr order by PREFERED_CORPORATE_ITEM_SEQ_NBR) as rn
                                                from
                                                (select Product_Group_Id
                                                ,Product_group_Nm
                                                ,upc_Nbr
                                                from `+ tgt_wrk_tbl +`
                                                ) a
                                                JOIN  ` + upc_lkp_tbl+ ` b
                                                ON CAST(a.UPC_NBR AS NUMBER(14,0)) = b.UPC_NBR
												AND b.dw_current_version_ind = TRUE
												AND b.dw_logical_delete_ind = FALSE
                                                JOIN ` + corp_item_lkp_tbl + ` c
                                                ON b.CORPORATE_ITEM_INTEGRATION_ID = c.CORPORATE_ITEM_INTEGRATION_ID
												AND c.dw_current_version_ind = TRUE
												AND c.dw_logical_delete_ind = FALSE
                                                ) x
                                                JOIN `+ tgt_wrk_tbl +` y
                                                on x.upc_nbr = y.upc_nbr
                                                where rn = 1
                                                         ) src
                            WHERE tgt.Product_Group_Id = src.Product_Group_Id
                            AND tgt.UPC_Nbr = src.UPC_Nbr

                                                         `;
                                                         
    
    try {
        snowflake.execute (
            {sqlText: upd_src_wrk_tbl  }
        )
    }
    catch (err)  {
        return "Update of DIM_Productgroup_UPC tgt_wrk_tbl table "+ upd_src_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }


var sql_deletes = `INSERT INTO `+ tgt_wrk_tbl +`
                             SELECT tgt.Product_Group_Id
    ,tgt.Product_group_Nm
    ,tgt.UPC_Nbr
    ,tgt.Upc_Dsc
    ,tgt.Retail_Item_Dsc
    ,tgt.Internet_Item_Dsc
    ,tgt.UPC_UOM
    ,tgt.UOM_Size_Qty
    ,TRUE AS DW_Logical_delete_ind
    ,'U' AS DML_Type
    FROM `+ tgt_tbl +` tgt
    LEFT JOIN
    (
        SELECT DISTINCT
        Product_Group_Id
        ,Product_group_Nm
        ,UPC_Nbr
        FROM    `+ src_wrk_tmp_tbl +`
      
    ) src ON src.Product_Group_Id = tgt.Product_Group_Id
      
    AND src.UPC_NBR  = tgt.UPC_Nbr
    WHERE (tgt.Product_Group_Id ) in (select distinct Product_Group_Id 
        FROM   `+ src_wrk_tmp_tbl +`
    )
    AND  dw_logical_delete_ind = FALSE
    AND src.Product_Group_Id is NULL 
    AND src.UPC_Nbr is NULL
    `;

      
try {
        snowflake.execute (
            {sqlText: sql_deletes  }
            );
        }
    catch (err)  
    
    { return sql_deletes;
        return "Insert of Delete records for DIM_Product_group_UPC work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }  



var sql_updates = // Processing Updates of Type 2 SCD
                  `UPDATE ` + tgt_tbl + ` tgt
                    set RETAIL_ITEM_Dsc = src.RETAIL_ITEM_DSC
                    ,Product_group_Nm = src.Product_group_Nm
                    ,Upc_Dsc = src.Upc_DSC
                    ,INTERNET_ITEM_Dsc = src.INTERNET_ITEM_DSC
                    ,UOM_Size_Qty   = src.UOM_Size_Qty  
                    ,UPC_UOM = src.UPC_UOM
                    ,DW_LOGICAL_DELETE_IND = src.DW_LOGICAL_DELETE_IND
                    ,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
                    FROM 
                    (SELECT distinct
                     Product_group_Nm
                     ,Product_group_Id
                     ,UPC_Nbr
                     ,RETAIL_ITEM_DSC
                    ,UOM_Size_Qty  
                    ,UPC_UOM
                    ,INTERNET_ITEM_DSC
                    ,Upc_Dsc
                    ,DW_LOGICAL_DELETE_IND
                    FROM `+ tgt_wrk_tbl +`
                    WHERE DML_TYPE = 'U') src
                    WHERE tgt.Upc_Nbr = src.Upc_Nbr 
                    AND tgt.Product_group_Id = src.Product_group_Id`;  

                   
      var sql_begin = "BEGIN"

    // Processing Inserts
    var sql_inserts = `INSERT INTO ` + tgt_tbl + `
        (
         Product_group_Id
        ,Product_group_Nm
        ,UPC_Nbr
        ,Upc_Dsc
        ,Retail_Item_Dsc
        ,Internet_Item_Dsc
        ,UPC_UOM
        ,UOM_Size_Qty  
        ,DW_CREATE_TS 
        ,DW_LOGICAL_DELETE_IND
    )
    SELECT
        Product_group_Id
        ,Product_group_Nm
        ,UPC_Nbr 
        ,Upc_Dsc
        ,Retail_Item_Dsc
        ,Internet_Item_Dsc
        ,UPC_UOM
        ,UOM_Size_Qty  
        ,current_timestamp() AS DW_CREATE_TS 
        ,DW_LOGICAL_DELETE_IND
    FROM ` + tgt_wrk_tbl + `
    WHERE DML_TYPE = 'I'                                             
   `; 

    var sql_commit = "COMMIT"
    var sql_rollback = "ROLLBACK"
    try {
        snowflake.execute (
            {sqlText: sql_begin}
        );
        snowflake.execute (
            {sqlText: sql_updates}
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
        return "Loading of DIM_Productgroup_UPC " + tgt_tbl + " Failed with error: " + err;   // Return a error message.
    }
            // **************        Load for DIM_Product_group_UPC ENDs *****************
            
    return "Done"


$$;
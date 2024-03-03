--liquibase formatted sql
--changeset SYSTEM:SP_OMSPRODUCTGROUP_TO_BIM_LOAD_OMS_PRODUCT_GROUP_UPC runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_OMSPRODUCTGROUP_TO_BIM_LOAD_OMS_PRODUCT_GROUP_UPC(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_PROD VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

   
   var src_wrk_tbl = SRC_WRK_TBL;
   var cnf_db = CNF_DB;
   var cnf_schema = C_PROD;
   var wrk_schema = C_STAGE;
   var Refined_db = '<<EDM_DB_NAME_R>>';
   var Refined_schema ='DW_R_PRODUCT';
   
   var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.OMS_Product_Group_UPC_WRK`;
   var tgt_tbl = `${cnf_db}.${cnf_schema}.OMS_Product_Group_UPC`;
   var src_wrk_tmp_tbl = `${cnf_db}.${wrk_schema}.OMS_Product_Group_UPC_SRC_WRK`;
   var flat_tbl =  Refined_db +"."+ Refined_schema +".ProductGroup_Flat";

 // **************        Load for Oms_Product_Group_UPC table BEGIN *****************
    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
	
	    var cr_src_wrk_tbl = `CREATE OR REPLACE TABLE `+ src_wrk_tmp_tbl +` AS
							with cte AS(
							Select PAYLOAD_ID, MAX(lastUpdateTs) AS lastUpdateTs from ` + flat_tbl+` where payload_id in (Select payload_id from ` + SRC_WRK_TBL +`)
							GROUP BY PAYLOAD_ID
							)
                           ,flat_tmp as
							(
							SELECT
                            FTAB.PAYLOAD_ID as Product_Group_Id
                            ,F.value::string  as UPC_Cd
							,payload_productgroupinfo_upcVersion as Upc_Version_Txt
							,payload_productgroupinfo_upcDescription as Upc_Dsc
							,payload_productgroupinfo_listtype as List_Type_Dsc
							,payload_productgroupinfo_origin as Origin_Dsc
							,case when payload_productgrouptype = 'NON_BASE' then 'Accepted' else payload_productgroupinfo_status end as Status_Dsc
							,payload_productgroupinfo_groupId as Group_Id
							,payload_productgroupinfo_groupName as Group_Nm
							,payload_productgroupinfo_brandId as Brand_Id
							,payload_productgroupinfo_brandName	as Brand_Nm
							,payload_productgroupinfo_categoryId as Category_Id
							,payload_productgroupinfo_categoryDescription as Category_Dsc
							,payload_productgroupinfo_manufacturerCode as Manufacturer_Cd
							,payload_productgroupinfo_qty as Item_Qty
							,payload_productgroupinfo_uom as Item_Uom_Cd
							,payload_productgroupinfo_price as Item_Price_Amt
							,payload_productgroupinfo_score as Item_Score_Nbr
							,payload_productgroupinfo_createTs as Create_Ts
							,payload_productgroupinfo_updateTs as Update_Ts
							,payload_productgroupinfo_createdUser as Create_User_Id
							,payload_productgroupinfo_updatedUser as Update_User_Id
                            ,sourceaction
                            ,filename
                            ,FTAB.lastupdatets
                            ,row_number() over ( PARTITION BY Product_Group_Id, UPC_Cd
                        ORDER BY to_timestamp_ntz(FTAB.LASTUPDATETS) desc) as rn
                            FROM  ${flat_tbl} ftab						
							inner join cte ct on ct.payload_id = ftab.PAYLOAD_ID and ct.lastupdatets = ftab.lastupdatets														
							,LATERAL FLATTEN(input => PAYLOAD_PRODUCTGROUPIDS_UPCIDS, outer => TRUE) as F						
                            WHERE
                                Product_Group_Id is not NULL)
                            
                            SELECT distinct
                            Product_Group_Id
                            ,UPC_Cd
							,Upc_Version_Txt
							,Upc_Dsc
							,List_Type_Dsc
							,Origin_Dsc
							,Status_Dsc
							,Group_Id
							,Group_Nm
							,Brand_Id
							,Brand_Nm
							,Category_Id
							,Category_Dsc
							,Manufacturer_Cd
							,Item_Qty
							,Item_Uom_Cd
							,Item_Price_Amt
							,Item_Score_Nbr
							,Create_Ts
							,Update_Ts
							,Create_User_Id
							,Update_User_Id
                            ,sourceaction
                            ,filename
                            ,lastupdatets
                            FROM flat_tmp
							where rn = 1 and Product_Group_Id is not null and UPC_Cd is not null`;
    try {
        snowflake.execute (
            {sqlText: cr_src_wrk_tbl  }
        );
    }
    catch (err)  { 
        return "Creation of Oms_Product_Group_UPC "+ src_wrk_tmp_tbl +" Failed with error: " + err;   // Return a error message.
        }

	var create_tgt_wrk_table = `CREATE OR REPLACE TABLE ${tgt_wrk_tbl} as 
	SELECT DISTINCT
				src.Product_Group_Id
			   ,src.UPC_Cd
			   ,src.Upc_Version_Txt
			   ,src.Upc_Dsc
			   ,src.List_Type_Dsc
			   ,src.Origin_Dsc
			   ,src.Status_Dsc
			   ,src.Group_Id
			   ,src.Group_Nm
			   ,src.Brand_Id
			   ,src.Brand_Nm
			   ,src.Category_Id
			   ,src.Category_Dsc
			   ,src.Manufacturer_Cd
			   ,src.Item_Qty
			   ,src.Item_Uom_Cd
			   ,src.Item_Price_Amt
			   ,src.Item_Score_Nbr
			   ,src.Create_Ts
			   ,src.Update_Ts
			   ,src.Create_User_Id
			   ,src.Update_User_Id			 
			   ,src.filename
			   ,src.DW_Logical_delete_ind,
		CASE 
			WHEN 
				tgt.Product_Group_Id is NULL AND
			tgt.UPC_Cd is NULL
			THEN 'I' 
			ELSE 'U' 
		END as DML_Type,
		CASE 
			WHEN to_date(tgt.DW_First_Effective_TS) = CURRENT_DATE 
			THEN 1 
			Else 0 
		END as Sameday_chg_ind
	FROM (   
	
			SELECT
				Product_Group_Id
			   ,UPC_Cd
			   ,Upc_Version_Txt
			   ,Upc_Dsc
			   ,List_Type_Dsc
			   ,Origin_Dsc
			   ,Status_Dsc
			   ,Group_Id
			   ,Group_Nm
			   ,Brand_Id
			   ,Brand_Nm
			   ,Category_Id
			   ,Category_Dsc
			   ,Manufacturer_Cd
			   ,Item_Qty
			   ,Item_Uom_Cd
			   ,Item_Price_Amt
			   ,Item_Score_Nbr
			   ,Create_Ts
			   ,Update_Ts
			   ,Create_User_Id
			   ,Update_User_Id			 
			   ,filename
			   ,FALSE AS DW_Logical_delete_ind
				FROM ${src_wrk_tmp_tbl}
	) src  
	LEFT JOIN (
						SELECT
							 Product_Group_Id
							,UPC_Cd
							,Upc_Version_Txt
							,Upc_Dsc
							,List_Type_Dsc
							,Origin_Dsc
							,Status_Dsc
							,Group_Id
							,Group_Nm
							,Brand_Id
							,Brand_Nm
							,Category_Id
							,Category_Dsc
							,Manufacturer_Cd
							,Item_Qty
							,Item_Uom_Cd
							,Item_Price_Amt
							,Item_Score_Nbr
							,Create_Ts
							,Update_Ts
							,Create_User_Id
							,Update_User_Id
							,DW_Logical_delete_ind
							,dw_first_effective_ts
        FROM ${tgt_tbl}
		WHERE DW_CURRENT_VERSION_IND = TRUE AND dw_logical_delete_ind = FALSE
	) as tgt on
	src.Product_Group_Id = tgt.Product_Group_Id AND
	src.UPC_Cd = tgt.UPC_Cd
	WHERE
		tgt.Product_Group_Id is NULL AND
				tgt.UPC_Cd is NULL 
		OR (
		  NVL(tgt.Upc_Version_Txt,'-1') <> NVL(src.Upc_Version_Txt,'-1') OR
		  NVL(tgt.Upc_Dsc,'-1') <> NVL(src.Upc_Dsc,'-1') OR
		  NVL(tgt.List_Type_Dsc,'-1') <> NVL(src.List_Type_Dsc,'-1') OR
		  NVL(tgt.Origin_Dsc,'-1') <> NVL(src.Origin_Dsc,'-1') OR
		  NVL(tgt.Status_Dsc,'-1') <> NVL(src.Status_Dsc,'-1') OR
		  NVL(tgt.Group_Id,'-1') <> NVL(src.Group_Id,'-1') OR
		  NVL(tgt.Group_Nm,'-1') <> NVL(src.Group_Nm,'-1') OR
		  NVL(tgt.Brand_Id,'-1') <> NVL(src.Brand_Id,'-1') OR
		  NVL(tgt.Brand_Nm,'-1') <> NVL(src.Brand_Nm,'-1') OR
		  NVL(tgt.Category_Id,'-1') <> NVL(src.Category_Id,'-1') OR
		  NVL(tgt.Category_Dsc,'-1') <> NVL(src.Category_Dsc,'-1') OR
		  NVL(tgt.Manufacturer_Cd,'-1') <> NVL(src.Manufacturer_Cd,'-1') OR
		  NVL(tgt.Item_Qty,'-1') <> NVL(src.Item_Qty,'-1') OR
		  NVL(tgt.Item_Uom_Cd,'-1') <> NVL(src.Item_Uom_Cd,'-1') OR
		  NVL(tgt.Item_Price_Amt,'-1') <> NVL(src.Item_Price_Amt,'-1') OR
		  NVL(tgt.Item_Score_Nbr,'-1') <> NVL(src.Item_Score_Nbr,'-1') OR
		  NVL(tgt.Create_Ts,'9999-12-31 00:00:00.000') <> NVL(to_timestamp(src.Create_Ts),'9999-12-31 00:00:00.000') OR
		  NVL(tgt.Update_Ts,'9999-12-31 00:00:00.000') <> NVL(to_timestamp(src.Update_Ts),'9999-12-31 00:00:00.000') OR
		  NVL(tgt.Create_User_Id,'-1') <> NVL(src.Category_Id,'-1') OR
		  NVL(tgt.Update_User_Id,'-1') <> NVL(src.Update_User_Id,'-1') OR 
		  (tgt.dw_logical_delete_ind  <>  src.dw_logical_delete_ind ))
		 `;  	
	
	try {
		snowflake.execute ({ sqlText: create_tgt_wrk_table });
	}
    catch (err) {
        return `Creation of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
    }
    
		var sql_deletes = `INSERT INTO ${tgt_wrk_tbl}
         select  tgt.Product_Group_Id
				,tgt.UPC_Cd
				,tgt.Upc_Version_Txt
				,tgt.Upc_Dsc
				,tgt.List_Type_Dsc
				,tgt.Origin_Dsc
				,tgt.Status_Dsc
				,tgt.Group_Id
				,tgt.Group_Nm
				,tgt.Brand_Id
				,tgt.Brand_Nm
				,tgt.Category_Id
				,tgt.Category_Dsc
				,tgt.Manufacturer_Cd
				,tgt.Item_Qty
				,tgt.Item_Uom_Cd
				,tgt.Item_Price_Amt
				,tgt.Item_Score_Nbr
				,tgt.Create_Ts
				,tgt.Update_Ts
				,tgt.Create_User_Id
				,tgt.Update_User_Id
        ,src_wrk.FileName
        ,TRUE AS DW_Logical_delete_ind  
        ,'U' AS DML_Type  
        ,CASE WHEN to_date(DW_First_Effective_ts) = CURRENT_DATE  THEN 1 Else 0 END as Sameday_chg_ind
             FROM ${tgt_tbl} tgt
            LEFT JOIN
            (
            SELECT distinct Product_Group_Id
                           ,UPC_Cd
                          ,FileName
            FROM ${src_wrk_tmp_tbl}
            ) src 
              ON src.UPC_Cd = tgt.UPC_Cd
             AND src.Product_Group_Id = tgt.Product_Group_Id
			 LEFT JOIN
              (
               SELECT distinct Product_Group_Id
                ,FileName
                FROM ${src_wrk_tmp_tbl}
                 ) src_wrk
                on src_wrk.Product_Group_Id = tgt.Product_Group_Id
            WHERE    
             (tgt.Product_Group_Id ) in (select distinct PAYLOAD_ID
           FROM ${src_wrk_tbl}
          )
             AND dw_current_version_ind = TRUE
            AND dw_logical_delete_ind = FALSE
              and src.Product_Group_Id is NULL
           and src.UPC_Cd is NULL
            `;
try {
        snowflake.execute (
            {sqlText: sql_deletes  }
            );
        }
    catch (err)  {
        return "Insert of Delete records for Oms_Product_Group_UPC work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }


	
	// Transaction for Updates, Insert begins           
    var sql_begin = "BEGIN"
	
	// SCD Type2 - Processing Different day updates
	var sql_updates = `UPDATE ${tgt_tbl} as tgt
	SET 
		DW_Last_Effective_ts = timestampadd(minute, -1, current_timestamp),
		DW_CURRENT_VERSION_IND = FALSE,
		DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
		DW_SOURCE_UPDATE_NM = filename
	FROM ( 
		SELECT 
			Product_Group_Id,
			UPC_Cd,
			filename
		FROM ${tgt_wrk_tbl}
		WHERE 
			DML_Type = 'U' AND 
			Sameday_chg_ind = 0 AND
			Product_Group_Id is not NULL AND
			UPC_Cd is not NULL             
		) src
		WHERE 
			tgt.Product_Group_Id = src.Product_Group_Id AND
			tgt.UPC_Cd = src.UPC_Cd AND
			tgt.DW_CURRENT_VERSION_IND = TRUE AND
			tgt.DW_LOGICAL_DELETE_IND = FALSE`;
		
     	// SCD Type1 - Processing Sameday updates
	var sql_sameday = `
	UPDATE ${tgt_tbl} as tgt
	SET       
		DW_Logical_delete_ind = src.DW_Logical_delete_ind,
		DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
		DW_SOURCE_UPDATE_NM = src.filename,
		DW_CURRENT_VERSION_IND = case when DML_Type = 'U' AND Sameday_chg_ind = 1 and src.DW_LOGICAL_DELETE_IND <> TRUE then TRUE 
		                              else FALSE end
		,DW_Last_Effective_ts = case when DML_Type = 'U' AND Sameday_chg_ind = 1 and src.DW_LOGICAL_DELETE_IND =TRUE then 
								timestampadd(minute, -1, current_timestamp) else DW_Last_Effective_ts end
		,Upc_Version_Txt	= src.Upc_Version_Txt
		,Upc_Dsc            = src.Upc_Dsc
		,List_Type_Dsc      = src.List_Type_Dsc
		,Origin_Dsc         = src.Origin_Dsc
		,Status_Dsc         = src.Status_Dsc
		,Group_Id           = src.Group_Id
		,Group_Nm           = src.Group_Nm
		,Brand_Id           = src.Brand_Id
		,Brand_Nm           = src.Brand_Nm
		,Category_Id        = src.Category_Id
		,Category_Dsc       = src.Category_Dsc
		,Manufacturer_Cd    = src.Manufacturer_Cd
		,Item_Qty           = src.Item_Qty
		,Item_Uom_Cd        = src.Item_Uom_Cd
		,Item_Price_Amt     = src.Item_Price_Amt
		,Item_Score_Nbr     = src.Item_Score_Nbr
		,Create_Ts          = src.Create_Ts
		,Update_Ts          = src.Update_Ts
		,Create_User_Id     = src.Create_User_Id
		,Update_User_Id     = src.Update_User_Id
		FROM ( 
		
			SELECT
				Product_Group_Id,
				UPC_Cd,
				DW_Logical_delete_ind,
				filename
				,Upc_Version_Txt
				,Upc_Dsc
				,List_Type_Dsc
				,Origin_Dsc
				,Status_Dsc
				,Group_Id
				,Group_Nm
				,Brand_Id
				,Brand_Nm
				,Category_Id
				,Category_Dsc
				,Manufacturer_Cd
				,Item_Qty
				,Item_Uom_Cd
				,Item_Price_Amt
				,Item_Score_Nbr
				,Create_Ts
				,Update_Ts
				,Create_User_Id
				,Update_User_Id
				,DML_Type
				,Sameday_chg_ind				
			FROM ${tgt_wrk_tbl}
			WHERE 
				DML_Type = 'U' AND 
				Sameday_chg_ind = 1 AND
				Product_Group_Id is not NULL AND
				UPC_Cd is not NULL
		) src
		WHERE
			tgt.Product_Group_Id = src.Product_Group_Id AND
			tgt.UPC_Cd = src.UPC_Cd AND  
			tgt.DW_CURRENT_VERSION_IND = TRUE AND
			tgt.DW_LOGICAL_DELETE_IND = FALSE`;
                                
                            
	// Processing Inserts
	var sql_inserts = `INSERT INTO ${tgt_tbl} (
	Product_Group_Id,
	UPC_Cd,
	DW_First_Effective_ts, 
	DW_Last_Effective_ts, 
	DW_CREATE_TS,          
	DW_LOGICAL_DELETE_IND,  
	DW_SOURCE_CREATE_NM,   
	DW_CURRENT_VERSION_IND 
	,Upc_Version_Txt
	,Upc_Dsc
	,List_Type_Dsc
	,Origin_Dsc
	,Status_Dsc
	,Group_Id
	,Group_Nm
	,Brand_Id
	,Brand_Nm
	,Category_Id
	,Category_Dsc
	,Manufacturer_Cd
	,Item_Qty
	,Item_Uom_Cd
	,Item_Price_Amt
	,Item_Score_Nbr
	,Create_Ts
	,Update_Ts
	,Create_User_Id
	,Update_User_Id	
	)
	SELECT
		Product_Group_Id,
		UPC_Cd,
		CURRENT_TIMESTAMP as DW_First_Effective_ts,
		'9999-12-31 00:00:00.000',
		CURRENT_TIMESTAMP,
		DW_Logical_delete_ind,
		filename,
		TRUE as DW_CURRENT_VERSION_IND
		,Upc_Version_Txt
		,Upc_Dsc
		,List_Type_Dsc
		,Origin_Dsc
		,Status_Dsc
		,Group_Id
		,Group_Nm
		,Brand_Id
		,Brand_Nm
		,Category_Id
		,Category_Dsc
		,Manufacturer_Cd
		,Item_Qty
		,Item_Uom_Cd
		,Item_Price_Amt
		,Item_Score_Nbr
		,Create_Ts
		,Update_Ts
		,Create_User_Id
		,Update_User_Id
	FROM ${tgt_wrk_tbl}
	WHERE 
		Sameday_chg_ind = 0 AND
		Product_Group_Id is not NULL AND
		UPC_Cd is not NULL
	`;
    
	var sql_commit = "COMMIT";
    var sql_rollback = "ROLLBACK";
    
	try {
        snowflake.execute ({ sqlText: sql_begin });
        snowflake.execute ({ sqlText: sql_updates });
		snowflake.execute ({ sqlText: sql_sameday });
        snowflake.execute ({ sqlText: sql_inserts });
        snowflake.execute ({ sqlText: sql_commit });
	} catch (err) {
        snowflake.execute ({ sqlText: sql_rollback });
       return `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
	}
                // **************        Load for Oms_Product_Group_UPC table ENDs *****************

         
                // **************        Load for OMS_Product_Group_UPC table ENDs *****************
    
$$;

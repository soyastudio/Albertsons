--liquibase formatted sql
--changeset SYSTEM:SP_GetOfferOMS_TO_BIM_LOAD_OMS_Offer_Qualification_Product_Group runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE SP_GETOFFEROMS_TO_BIM_LOAD_OMS_OFFER_QUALIFICATION_PRODUCT_GROUP(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_PRODUCT VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$


	var src_wrk_tbl = SRC_WRK_TBL;
	var cnf_schema = C_PRODUCT;
	var tgt_wrk_tbl = `${CNF_DB}.${C_STAGE}.OMS_Offer_Qualification_Product_Group_WRK`;
	var tgt_tbl = `${CNF_DB}.${cnf_schema}.OMS_Offer_Qualification_Product_Group`;
	var src_wrk_tmp_tbl = `${CNF_DB}.${C_STAGE}.OMS_Offer_Qualification_Product_Group_src_wrk`;

                       
    // **************        Load for OMS_Offer_Qualification_Product_Group table BEGIN *****************
    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
	
        var sql_truncate_src_wrk_tmp_tbl = `Truncate table  ${src_wrk_tmp_tbl}`;


	    var cr_src_wrk_tbl = `INSERT INTO `+ src_wrk_tmp_tbl +` 
                            with flat_tmp as
                            ( select 
                             payload_id as  OMS_Offer_Id
							 ,PAYLOAD_QUALIFICATIONPRODUCTGROUPS
                            ,filename
                            ,row_number() over ( PARTITION BY OMS_Offer_Id
                                                  ORDER BY to_timestamp_ltz(LastUpdateTs) desc) as rn
                             from ` + src_wrk_tbl +` 
                             WHERE OMS_Offer_Id IS NOT NULL AND PAYLOAD_QUALIFICATIONPRODUCTGROUPS  IS NOT NULL
							 ) 
								SELECT DISTINCT
								OMS_Offer_Id,
								Product_Group_Id,
								EXCLUDED_OMS_PRODUCT_GROUP_ID,
								Quantity_Unit_Type_Dsc,
								Quantity_Unit_Dsc,
								Conjunction_Txt,
								Minimum_Purchase_Amt,
								Unique_Product_Ind,
								Inherited_From_Offer_Request_Ind,
								filename
                                FROM (
										SELECT  
										OMS_Offer_Id,
										PAYLOAD_PRODUCTGROUPS.value:id::string AS Product_Group_Id,
										PAYLOAD_PRODUCTGROUPS.value:excludedProductGroupId::string AS EXCLUDED_OMS_PRODUCT_GROUP_ID,
										PAYLOAD_PRODUCTGROUPS.value:quantityUnitType::string AS Quantity_Unit_Type_Dsc,
										PAYLOAD_PRODUCTGROUPS.value:quantityUnitDesc::string AS Quantity_Unit_Dsc,
										PAYLOAD_PRODUCTGROUPS.value:conjunction::string AS Conjunction_Txt,
										PAYLOAD_PRODUCTGROUPS.value:minPurchaseAmount::string AS Minimum_Purchase_Amt,
										PAYLOAD_PRODUCTGROUPS.value:uniqueproduct::boolean AS Unique_Product_Ind,
										PAYLOAD_PRODUCTGROUPS.value:inheritedFromOfferRequest::boolean AS Inherited_From_Offer_Request_Ind,
										filename, 
										rn
										FROM flat_tmp
										,LATERAL FLATTEN(input => PAYLOAD_QUALIFICATIONPRODUCTGROUPS, outer => TRUE ) as PAYLOAD_PRODUCTGROUPS
									)
								WHERE 
							        OMS_Offer_Id is not NULL AND
							        Product_Group_Id is not NULL
                                     and rn = 1
   `;
    try {
        snowflake.execute ({ sqlText: sql_truncate_src_wrk_tmp_tbl});
        snowflake.execute (
            {sqlText: cr_src_wrk_tbl  }
        );
    }
    catch (err)  { 
        return "Creation of OMS_Offer_Qualification_Product_Group src_wrk_tmp_tbl table "+ src_wrk_tmp_tbl +" Failed with error: " + err;   // Return a error message.
        }


    var sql_truncate_wrk_tbl = `Truncate table  ${tgt_wrk_tbl}`;


	var create_tgt_wrk_table = `INSERT INTO ${tgt_wrk_tbl}  
	SELECT DISTINCT
		src.OMS_Offer_Id,
		src.Product_Group_Id,
		src.EXCLUDED_OMS_PRODUCT_GROUP_ID,
		src.Quantity_Unit_Type_Dsc,
		src.Quantity_Unit_Dsc,
		src.Conjunction_Txt,
		src.Minimum_Purchase_Amt,
		src.Unique_Product_Ind,
		src.Inherited_From_Offer_Request_Ind,
		src.filename,
		src.DW_Logical_delete_ind,
		CASE 
			WHEN 
				tgt.OMS_Offer_Id is NULL AND
			tgt.Product_Group_Id is NULL
			THEN 'I' 
			ELSE 'U' 
		END as DML_Type,
		CASE 
			WHEN tgt.DW_First_Effective_dt = CURRENT_DATE 
			THEN 1 
			Else 0 
		END as Sameday_chg_ind
	FROM (   
	
			SELECT
				OMS_Offer_Id,
				Product_Group_Id,
				EXCLUDED_OMS_PRODUCT_GROUP_ID,
				Quantity_Unit_Type_Dsc,
				Quantity_Unit_Dsc,
				Conjunction_Txt,
				Minimum_Purchase_Amt,
				Unique_Product_Ind,
				Inherited_From_Offer_Request_Ind,
				filename,
				FALSE AS DW_Logical_delete_ind
				FROM ${src_wrk_tmp_tbl}
	) src  
	LEFT JOIN (
		SELECT
			OMS_Offer_Id,
			Product_Group_Id,
			EXCLUDED_OMS_PRODUCT_GROUP_ID,
			Quantity_Unit_Type_Dsc,
			Quantity_Unit_Dsc,
			Conjunction_Txt,
			Minimum_Purchase_Amt,
			Unique_Product_Ind,
			Inherited_From_Offer_Request_Ind,
			DW_Logical_delete_ind,
			DW_First_Effective_dt
        FROM ${tgt_tbl}
		WHERE DW_CURRENT_VERSION_IND = TRUE
	) as tgt on
	src.OMS_Offer_Id = tgt.OMS_Offer_Id AND
	src.Product_Group_Id = tgt.Product_Group_Id
	WHERE
		tgt.OMS_Offer_Id is NULL AND
		tgt.Product_Group_Id is NULL 
		 OR (
		 NVL(tgt.EXCLUDED_OMS_PRODUCT_GROUP_ID,'-1') <> NVL(src.EXCLUDED_OMS_PRODUCT_GROUP_ID,'-1') 
		 OR NVL(tgt.Quantity_Unit_Type_Dsc,'-1') <> NVL(src.Quantity_Unit_Type_Dsc,'-1')
		 OR NVL(tgt.Quantity_Unit_Dsc,'-1') <> NVL(src.Quantity_Unit_Dsc,'-1')
		 OR NVL(tgt.Conjunction_Txt,'-1') <> NVL(src.Conjunction_Txt,'-1')
		 OR NVL(tgt.Minimum_Purchase_Amt,'-1') <> NVL(src.Minimum_Purchase_Amt,'-1')
		 OR NVL(SRC.Unique_Product_Ind, -1)  <>  NVL(tgt.Unique_Product_Ind, -1)
		 OR NVL(SRC.Inherited_From_Offer_Request_Ind, -1 )  <>  NVL(tgt.Inherited_From_Offer_Request_Ind,-1)
         OR (SRC.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND)
		 )
		 `;  	
	
	try {
    snowflake.execute ({ sqlText: sql_truncate_wrk_tbl});
		snowflake.execute ({ sqlText: create_tgt_wrk_table });
	}
    catch (err) {
        return `Creation of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
    }
    
		var sql_deletes = `INSERT INTO ${tgt_wrk_tbl}
         select  tgt.OMS_Offer_Id
		,tgt.Product_Group_Id
		,tgt.EXCLUDED_OMS_PRODUCT_GROUP_ID
		,tgt.Quantity_Unit_Type_Dsc
		,tgt.Quantity_Unit_Dsc
		,tgt.Conjunction_Txt
		,tgt.Minimum_Purchase_Amt
		,tgt.Unique_Product_Ind
		,tgt.Inherited_From_Offer_Request_Ind
        ,src_wrk.FileName
        ,TRUE AS DW_Logical_delete_ind  
        ,'U' AS DML_Type  
        ,CASE WHEN DW_First_Effective_dt = CURRENT_DATE  THEN 1 Else 0 END as Sameday_chg_ind
             FROM ${tgt_tbl} tgt
            LEFT JOIN
            (
            SELECT distinct OMS_Offer_Id
                           ,Product_Group_Id
                          ,FileName
            FROM ${src_wrk_tmp_tbl}
            ) src 
              ON src.Product_Group_Id = tgt.Product_Group_Id
             AND src.OMS_Offer_Id = tgt.OMS_Offer_Id
			 			LEFT JOIN
              (
               SELECT distinct OMS_Offer_Id
                ,FileName
                FROM ${src_wrk_tmp_tbl}
                 ) src_wrk
                on src_wrk.OMS_Offer_Id = tgt.OMS_Offer_Id
            WHERE    
             (tgt.OMS_Offer_Id ) in (select distinct OMS_Offer_Id
           FROM ${src_wrk_tmp_tbl}
          )
             AND 
              dw_current_version_ind = TRUE
            AND dw_logical_delete_ind = FALSE
              and src.OMS_Offer_Id is NULL
              and src.Product_Group_Id is NULL
            `;
try {
        snowflake.execute (
            {sqlText: sql_deletes  }
            );
        }
    catch (err)  {
        return "Insert of Delete records for Conditional_Stored_Value work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }
    
	// Transaction for Updates, Insert begins           
    var sql_begin = "BEGIN"
	
	// SCD Type2 - Processing Different day updates
	var sql_updates = `UPDATE ${tgt_tbl} as tgt
	SET 
		
		DW_Last_Effective_dt = CURRENT_DATE - 1,
		DW_CURRENT_VERSION_IND = FALSE,
		DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
		DW_SOURCE_UPDATE_NM = filename
	FROM ( 
		SELECT 
			OMS_Offer_Id,
			Product_Group_Id,
			filename
		FROM ${tgt_wrk_tbl}
		WHERE 
			DML_Type = 'U' AND 
			Sameday_chg_ind = 0 AND
			OMS_Offer_Id is not NULL AND
			Product_Group_Id is not NULL
		) src
		WHERE 
			tgt.OMS_Offer_Id = src.OMS_Offer_Id AND
			tgt.Product_Group_Id = src.Product_Group_Id AND
			tgt.DW_CURRENT_VERSION_IND = TRUE AND
			tgt.DW_LOGICAL_DELETE_IND = FALSE`;
		
     	// SCD Type1 - Processing Sameday updates
	var sql_sameday = `
	UPDATE ${tgt_tbl} as tgt
	SET 
		EXCLUDED_OMS_PRODUCT_GROUP_ID = src.EXCLUDED_OMS_PRODUCT_GROUP_ID,
		Quantity_Unit_Type_Dsc = src.Quantity_Unit_Type_Dsc,
		Quantity_Unit_Dsc      = src.Quantity_Unit_Dsc,
		Conjunction_Txt        = src.Conjunction_Txt,
		Minimum_Purchase_Amt   = src.Minimum_Purchase_Amt,
		Unique_Product_Ind     = src.Unique_Product_Ind,
		Inherited_From_Offer_Request_Ind   = src.Inherited_From_Offer_Request_Ind,
		DW_Logical_delete_ind = src.DW_Logical_delete_ind,
		DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
		DW_SOURCE_UPDATE_NM = filename
		FROM ( 
			SELECT
				OMS_Offer_Id,
				Product_Group_Id,
				EXCLUDED_OMS_PRODUCT_GROUP_ID,
				Quantity_Unit_Type_Dsc,
				Quantity_Unit_Dsc,
				Conjunction_Txt,
				Minimum_Purchase_Amt,
				Unique_Product_Ind,
				Inherited_From_Offer_Request_Ind,
				DW_Logical_delete_ind,
				filename
			FROM ${tgt_wrk_tbl}
			WHERE 
				DML_Type = 'U' AND 
				Sameday_chg_ind = 1 AND
				OMS_Offer_Id is not NULL AND
				Product_Group_Id is not NULL
		) src
		WHERE
			tgt.OMS_Offer_Id = src.OMS_Offer_Id AND
			tgt.Product_Group_Id = src.Product_Group_Id AND  
			tgt.DW_CURRENT_VERSION_IND = TRUE AND
			tgt.DW_LOGICAL_DELETE_IND = FALSE`;
                                
                            
	// Processing Inserts
	var sql_inserts = `INSERT INTO ${tgt_tbl} (
	OMS_Offer_Id,
	Product_Group_Id,
	DW_First_Effective_dt, 
	DW_Last_Effective_dt, 
	EXCLUDED_OMS_PRODUCT_GROUP_ID,
	Quantity_Unit_Type_Dsc,
	Quantity_Unit_Dsc,
	Conjunction_Txt,
	Minimum_Purchase_Amt,
	Unique_Product_Ind,
	Inherited_From_Offer_Request_Ind,
	DW_CREATE_TS,          
	DW_LOGICAL_DELETE_IND,  
	DW_SOURCE_CREATE_NM,   
	DW_CURRENT_VERSION_IND  
	)
	SELECT
		OMS_Offer_Id,
		Product_Group_Id,
		CURRENT_DATE as DW_First_Effective_dt,
		'31-DEC-9999',
		EXCLUDED_OMS_PRODUCT_GROUP_ID,
		Quantity_Unit_Type_Dsc,
		Quantity_Unit_Dsc,
		Conjunction_Txt,
		Minimum_Purchase_Amt,
		Unique_Product_Ind,
		Inherited_From_Offer_Request_Ind,
		CURRENT_TIMESTAMP,
		DW_Logical_delete_ind,
		filename,
		TRUE as DW_CURRENT_VERSION_IND
	FROM ${tgt_wrk_tbl}
	WHERE 
		Sameday_chg_ind = 0 AND
		OMS_Offer_Id is not NULL AND
		Product_Group_Id is not NULL
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
                // **************        Load for OMS_Offer_Qualification_Product_Group table ENDs *****************
                
                $$;

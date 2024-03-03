 --liquibase formatted sql
--changeset SYSTEM:sp_GetOfferRequest_To_BIM_Offer_Request_Buy_Product_Group runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE sp_GetOfferRequest_To_BIM_Offer_Request_Buy_Product_Group
(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, CNF_SCHEMA VARCHAR, WRK_SCHEMA VARCHAR)
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS $$
    
  var cnf_db = CNF_DB ;
	var wrk_schema = WRK_SCHEMA ;
	var cnf_schema = CNF_SCHEMA;
	var src_wrk_tbl = SRC_WRK_TBL;
	
	// **************	Load for Offer_Request_Buy_Product_Group table BEGIN *****************
	// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
	var tgt_wrk_tbl = cnf_db + "." + wrk_schema + ".Offer_Request_Buy_Product_Group_wrk";
	var tgt_tbl = cnf_db +"."+ cnf_schema +".Offer_Request_Buy_Product_Group";
	var tgt_exp_tbl = cnf_db + "." + wrk_schema + ".Offer_Request_Buy_Product_Group_Exceptions";
	var sql_command = `CREATE OR REPLACE TABLE ` + tgt_wrk_tbl + ` as 
					    WITH src_wrk_tbl_recs as
						(	SELECT 	DISTINCT AnyProductInd AS Any_Product_Ind
											, ConjunctionDsc AS Conjunction_Dsc
											, ProductGroup_DisplayOrderNbr AS Display_Order_Nbr /* As Filed Name is not Confirmed, using Default Value. Need to change*/
											, ExcludedProductGroup_ProductGroupId AS Excluded_Product_Group_Id
											, ExcludedProductGroup_ProductGroupNm AS Excluded_Product_Group_Nm
											, GiftCardInd AS Gift_Card_Ind
											, InheritedInd AS Inherited_Ind
											, ItemQty AS Item_Qty
											, MaximumPurchaseAmt AS Maximum_Purchase_Amt
											, MinimumPurchaseAmt AS Minimum_Purchase_Amt
											, OfferRequestId AS Offer_Request_Id
											, ProductGroupDsc AS Product_Group_Dsc
											, ProductGroup_ProductGroupId AS Product_Group_Id
											, ProductGroup_ProductGroupNm AS Product_Group_Nm
											, ProductGroup_ProductGroupVersionId AS Product_Group_Version_Id
											, AttachedOfferType_StoreGroupVersionId AS Store_Group_Version_Id
											, UniqueItemInd AS Unique_Item_Ind
											, ProductGroup_UOMCd AS Unit_Of_Measure_Cd
											, UOMDsc AS Unit_Of_Measure_Dsc
											, ProductGroup_UOMNm AS Unit_Of_Measure_Nm
											, AttachedOfferTypeId AS User_Interface_Unique_Id
											, creationdt
											, actiontypecd 
											, FileName
											, Row_number() OVER ( partition BY OfferRequestId, AttachedOfferTypeId, ProductGroup_ProductGroupNm, ProductGroup_ProductGroupVersionId, AttachedOfferType_StoreGroupVersionId ORDER BY To_timestamp_ntz(creationdt) DESC) AS rn
									FROM ` + src_wrk_tbl +`
						)
						SELECT 	src.Any_Product_Ind
								, src.Conjunction_Dsc
								, src.Display_Order_Nbr
								, src.Excluded_Product_Group_Id
								, src.Excluded_Product_Group_Nm
								, src.Gift_Card_Ind
								, src.Inherited_Ind
								, src.Item_Qty
								, src.Maximum_Purchase_Amt
								, src.Minimum_Purchase_Amt
								, src.Offer_Request_Id
								, src.Product_Group_Dsc
								, src.Product_Group_Id
								, src.Product_Group_Nm
								, src.Product_Group_Version_Id
								, src.Store_Group_Version_Id
								, src.Unique_Item_Ind
								, src.Unit_Of_Measure_Cd
								, src.Unit_Of_Measure_Dsc
								, src.Unit_Of_Measure_Nm
								, src.User_Interface_Unique_Id
								, src.dw_logical_delete_ind 
								, src.FileName 
								, CASE WHEN (tgt.Offer_Request_Id IS NULL AND tgt.User_Interface_Unique_Id IS NULL AND tgt.Product_Group_Nm IS NULL AND tgt.Product_Group_Version_Id IS NULL AND tgt.Store_Group_Version_Id IS NULL) THEN 'I' ELSE 'U' END AS DML_Type
								, CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1  ELSE 0 END AS Sameday_chg_ind 
								FROM   	(SELECT		Any_Product_Ind
													, Conjunction_Dsc
													, Display_Order_Nbr
													, Excluded_Product_Group_Id
													, Excluded_Product_Group_Nm
													, Gift_Card_Ind
													, Inherited_Ind
													, Item_Qty
													, Maximum_Purchase_Amt
													, Minimum_Purchase_Amt
													, Offer_Request_Id
													, Product_Group_Dsc
													, Product_Group_Id
													, Product_Group_Nm
													, Product_Group_Version_Id
													, Store_Group_Version_Id
													, Unique_Item_Ind
													, Unit_Of_Measure_Cd
													, Unit_Of_Measure_Dsc
													, Unit_Of_Measure_Nm
													, User_Interface_Unique_Id
													, creationdt
													, FALSE AS DW_Logical_delete_ind 
													, FileName														
										 FROM src_wrk_tbl_recs
											WHERE  rn = 1
											AND	Offer_Request_Id  IS NOT NULL 
											AND User_Interface_Unique_Id  IS NOT NULL 
											AND Product_Group_Nm  IS NOT NULL 
											AND Product_Group_Version_Id  IS NOT NULL 
											AND Store_Group_Version_Id  IS NOT NULL 
											AND	UPPER(ActionTypeCd) <> 'DELETE'												
										) src
								LEFT JOIN 	(SELECT 	tgt.Any_Product_Ind
														, tgt.Conjunction_Dsc
														, tgt.Display_Order_Nbr
														, tgt.Excluded_Product_Group_Id
														, tgt.Excluded_Product_Group_Nm
														, tgt.Gift_Card_Ind
														, tgt.Inherited_Ind
														, tgt.Item_Qty
														, tgt.Maximum_Purchase_Amt
														, tgt.Minimum_Purchase_Amt
														, tgt.Offer_Request_Id
														, tgt.Product_Group_Dsc
														, tgt.Product_Group_Id
														, tgt.Product_Group_Nm
														, tgt.Product_Group_Version_Id
														, tgt.Store_Group_Version_Id
														, tgt.Unique_Item_Ind
														, tgt.Unit_Of_Measure_Cd
														, tgt.Unit_Of_Measure_Dsc
														, tgt.Unit_Of_Measure_Nm
														, tgt.User_Interface_Unique_Id
														, tgt.dw_logical_delete_ind 
														, tgt.dw_first_effective_dt 											
											FROM ` + tgt_tbl + ` tgt
											WHERE DW_CURRENT_VERSION_IND = TRUE	
								) tgt 
								ON 	tgt.Offer_Request_Id = src.Offer_Request_Id 
								AND tgt.User_Interface_Unique_Id = src.User_Interface_Unique_Id 
								AND tgt.Product_Group_Nm = src.Product_Group_Nm
								AND tgt.Product_Group_Version_Id = src.Product_Group_Version_Id
								AND tgt.Store_Group_Version_Id = src.Store_Group_Version_Id
						WHERE  (tgt.Offer_Request_Id IS NULL AND tgt.User_Interface_Unique_Id IS NULL AND tgt.Product_Group_Nm IS NULL AND tgt.Product_Group_Version_Id IS NULL AND tgt.Store_Group_Version_Id IS NULL)
						OR 		(
									 NVL(src.Any_Product_Ind,'-1') <> NVL(tgt.Any_Product_Ind,'-1')
									 OR NVL(src.Conjunction_Dsc,'-1') <> NVL(tgt.Conjunction_Dsc,'-1')
									 OR NVL(src.Display_Order_Nbr,'-1') <> NVL(tgt.Display_Order_Nbr,'-1')
									 OR NVL(src.Excluded_Product_Group_Id,'-1') <> NVL(tgt.Excluded_Product_Group_Id,'-1')
									 OR NVL(src.Excluded_Product_Group_Nm,'-1') <> NVL(tgt.Excluded_Product_Group_Nm,'-1')
									 OR NVL(src.Gift_Card_Ind,'-1') <> NVL(tgt.Gift_Card_Ind,'-1')
									 OR NVL(src.Inherited_Ind,'-1') <> NVL(tgt.Inherited_Ind,'-1')
									 OR NVL(src.Item_Qty,'-1') <> NVL(tgt.Item_Qty,'-1')
									 OR NVL(src.Maximum_Purchase_Amt,'-1') <> NVL(tgt.Maximum_Purchase_Amt,'-1')
									 OR NVL(src.Minimum_Purchase_Amt,'-1') <> NVL(tgt.Minimum_Purchase_Amt,'-1')
									 OR NVL(src.Product_Group_Dsc,'-1') <> NVL(tgt.Product_Group_Dsc,'-1')
									 OR NVL(src.Product_Group_Id,'-1') <> NVL(tgt.Product_Group_Id,'-1')
									 OR NVL(src.Unique_Item_Ind,'-1') <> NVL(tgt.Unique_Item_Ind,'-1')
									 OR NVL(src.Unit_Of_Measure_Cd,'-1') <> NVL(tgt.Unit_Of_Measure_Cd,'-1')
									 OR NVL(src.Unit_Of_Measure_Dsc,'-1') <> NVL(tgt.Unit_Of_Measure_Dsc,'-1')
									 OR NVL(src.Unit_Of_Measure_Nm,'-1') <> NVL(tgt.Unit_Of_Measure_Nm,'-1')
									 OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
								)
						UNION ALL
							SELECT 	tgt.Any_Product_Ind
									, tgt.Conjunction_Dsc
									, tgt.Display_Order_Nbr
									, tgt.Excluded_Product_Group_Id
									, tgt.Excluded_Product_Group_Nm
									, tgt.Gift_Card_Ind
									, tgt.Inherited_Ind
									, tgt.Item_Qty
									, tgt.Maximum_Purchase_Amt
									, tgt.Minimum_Purchase_Amt
									, tgt.Offer_Request_Id
									, tgt.Product_Group_Dsc
									, tgt.Product_Group_Id
									, tgt.Product_Group_Nm
									, tgt.Product_Group_Version_Id
									, tgt.Store_Group_Version_Id
									, tgt.Unique_Item_Ind
									, tgt.Unit_Of_Measure_Cd
									, tgt.Unit_Of_Measure_Dsc
									, tgt.Unit_Of_Measure_Nm
									, tgt.User_Interface_Unique_Id					
									, TRUE AS DW_Logical_delete_ind
									, tgt.DW_SOURCE_CREATE_NM
									, 'U' as DML_Type
									, CASE WHEN tgt.DW_First_Effective_dt = CURRENT_DATE THEN 1 Else 0 END as Sameday_chg_ind
						FROM	` + tgt_tbl + ` tgt
							WHERE	DW_CURRENT_VERSION_IND = TRUE
								AND		DW_LOGICAL_DELETE_IND = FALSE
								AND		(Offer_Request_Id, User_Interface_Unique_Id, Store_Group_Version_Id) in 
										(
											SELECT 	DISTINCT Offer_Request_Id
													,User_Interface_Unique_Id
													,Store_Group_Version_Id
											FROM	src_wrk_tbl_recs src
												WHERE 	rn = 1
													AND		upper(ActionTypeCd) = 'DELETE'
													AND		Offer_Request_Id is not null
													and		User_Interface_Unique_Id is not null
													and		Store_Group_Version_Id is not null
										)					  				
									`;
	try {
        snowflake.execute (
            {sqlText: sql_command  }
            );
        }
    catch (err)  {
        return "Creation of Offer_Request_Buy_Product_Group work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }
		
		 
													
	//SCD Type2 transaction begins
    var sql_begin = "BEGIN"
	var sql_updates = `// Processing Updates of Type 2 SCD
						UPDATE ` + tgt_tbl + ` as tgt
						SET		 DW_Last_Effective_dt = CURRENT_DATE-1
								,DW_CURRENT_VERSION_IND = FALSE
								,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
								,DW_SOURCE_UPDATE_NM = FileName
						FROM	(	SELECT 	 Offer_Request_Id
											, User_Interface_Unique_Id 
											, Product_Group_Nm 
											, Product_Group_Version_Id
											, Store_Group_Version_Id
											,FileName
									FROM 	`+ tgt_wrk_tbl +` 
										WHERE	DML_Type = 'U'
											AND	Sameday_chg_ind = 0
											AND	Offer_Request_Id  IS NOT NULL 
											AND User_Interface_Unique_Id  IS NOT NULL 
											AND Product_Group_Nm  IS NOT NULL 
											AND Product_Group_Version_Id  IS NOT NULL 
											AND Store_Group_Version_Id  IS NOT NULL 
								) src
						WHERE	tgt.Offer_Request_Id = src.Offer_Request_Id 
							AND tgt.User_Interface_Unique_Id = src.User_Interface_Unique_Id 
							AND tgt.Product_Group_Nm = src.Product_Group_Nm
							AND tgt.Product_Group_Version_Id = src.Product_Group_Version_Id
							AND tgt.Store_Group_Version_Id = src.Store_Group_Version_Id
							AND	tgt.DW_CURRENT_VERSION_IND = TRUE`;
    var sql_sameday = `// Processing Sameday updates
						UPDATE ` + tgt_tbl + ` as tgt
						SET		 Any_Product_Ind = src.Any_Product_Ind
								, Conjunction_Dsc = src.Conjunction_Dsc
								, Display_Order_Nbr = src.Display_Order_Nbr
								, Excluded_Product_Group_Id = src.Excluded_Product_Group_Id
								, Excluded_Product_Group_Nm = src.Excluded_Product_Group_Nm
								, Gift_Card_Ind = src.Gift_Card_Ind
								, Inherited_Ind = src.Inherited_Ind
								, Item_Qty = src.Item_Qty
								, Maximum_Purchase_Amt = src.Maximum_Purchase_Amt
								, Minimum_Purchase_Amt = src.Minimum_Purchase_Amt
								, Product_Group_Dsc = src.Product_Group_Dsc
								, Product_Group_Id = src.Product_Group_Id
								, Unique_Item_Ind = src.Unique_Item_Ind
								, Unit_Of_Measure_Cd = src.Unit_Of_Measure_Cd
								, Unit_Of_Measure_Dsc = src.Unit_Of_Measure_Dsc
								, Unit_Of_Measure_Nm = src.Unit_Of_Measure_Nm
								, DW_Logical_delete_ind = src.DW_Logical_delete_ind
								, DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
								, DW_SOURCE_UPDATE_NM = FileName
						FROM	(	SELECT 	 Any_Product_Ind
											, Conjunction_Dsc
											, Display_Order_Nbr
											, Excluded_Product_Group_Id
											, Excluded_Product_Group_Nm
											, Gift_Card_Ind
											, Inherited_Ind
											, Item_Qty
											, Maximum_Purchase_Amt
											, Minimum_Purchase_Amt
											, Offer_Request_Id
											, Product_Group_Dsc
											, Product_Group_Id
											, Product_Group_Nm
											, Product_Group_Version_Id
											, Store_Group_Version_Id
											, Unique_Item_Ind
											, Unit_Of_Measure_Cd
											, Unit_Of_Measure_Dsc
											, Unit_Of_Measure_Nm
											, User_Interface_Unique_Id
											, DW_Logical_delete_ind
											, FileName
									FROM 	`+ tgt_wrk_tbl +` 
									WHERE	DML_Type = 'U'
									AND	Sameday_chg_ind = 1
									AND	Offer_Request_Id  IS NOT NULL 
									AND User_Interface_Unique_Id  IS NOT NULL 
									AND Product_Group_Nm  IS NOT NULL 
									AND Product_Group_Version_Id  IS NOT NULL 
									AND Store_Group_Version_Id  IS NOT NULL 
								) src
						WHERE	tgt.Offer_Request_Id = src.Offer_Request_Id 
							AND tgt.User_Interface_Unique_Id = src.User_Interface_Unique_Id 
							AND tgt.Product_Group_Nm = src.Product_Group_Nm
							AND tgt.Product_Group_Version_Id = src.Product_Group_Version_Id
							AND tgt.Store_Group_Version_Id = src.Store_Group_Version_Id
							AND	tgt.DW_CURRENT_VERSION_IND = TRUE`;

						// Processing Inserts
	
	
	var sql_inserts = `INSERT INTO ` + tgt_tbl + `
						(	 Any_Product_Ind
							, Conjunction_Dsc
							, Display_Order_Nbr
							, Excluded_Product_Group_Id
							, Excluded_Product_Group_Nm
							, Gift_Card_Ind
							, Inherited_Ind
							, Item_Qty
							, Maximum_Purchase_Amt
							, Minimum_Purchase_Amt
							, Offer_Request_Id
							, Product_Group_Dsc
							, Product_Group_Id
							, Product_Group_Nm
							, Product_Group_Version_Id
							, Store_Group_Version_Id
							, Unique_Item_Ind
							, Unit_Of_Measure_Cd
							, Unit_Of_Measure_Dsc
							, Unit_Of_Measure_Nm
							, User_Interface_Unique_Id
							, DW_First_Effective_Dt     
							, DW_Last_Effective_Dt     
							, DW_CREATE_TS
							, DW_LOGICAL_DELETE_IND
							, DW_SOURCE_CREATE_NM
							, DW_CURRENT_VERSION_IND
						)
						SELECT 	 Any_Product_Ind
								, Conjunction_Dsc
								, Display_Order_Nbr
								, Excluded_Product_Group_Id
								, Excluded_Product_Group_Nm
								, Gift_Card_Ind
								, Inherited_Ind
								, Item_Qty
								, Maximum_Purchase_Amt
								, Minimum_Purchase_Amt
								, Offer_Request_Id
								, Product_Group_Dsc
								, Product_Group_Id
								, Product_Group_Nm
								, Product_Group_Version_Id
								, Store_Group_Version_Id
								, Unique_Item_Ind
								, Unit_Of_Measure_Cd
								, Unit_Of_Measure_Dsc
								, Unit_Of_Measure_Nm
								, User_Interface_Unique_Id
								, CURRENT_DATE
								, '31-DEC-9999'
								, CURRENT_TIMESTAMP
								, DW_Logical_delete_ind
								, FileName
								, TRUE
						FROM	`+ tgt_wrk_tbl +`	
						WHERE	Sameday_chg_ind = 0
						AND	Offer_Request_Id  IS NOT NULL 
						AND User_Interface_Unique_Id  IS NOT NULL 
						AND Product_Group_Nm  IS NOT NULL 
						AND Product_Group_Version_Id  IS NOT NULL 
						AND Store_Group_Version_Id  IS NOT NULL `;
    var sql_commit = "COMMIT"
    var sql_rollback = "ROLLBACK"
	try {
        snowflake.execute (
            {sqlText: sql_begin  }
            );
		snowflake.execute (
            {sqlText: sql_updates  }
            );
        snowflake.execute (
            {sqlText: sql_sameday  }
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
        return "Loading of Offer_Request_Buy_Product_Group table "+ tgt_tbl + " Failed with error: " + err;   // Return a error message.
        }
		
		var sql_exceptions = `INSERT INTO ` + tgt_exp_tbl	+`  
		                               select 	    Any_Product_Ind
													, Conjunction_Dsc
													, Display_Order_Nbr
													, Excluded_Product_Group_Id
													, Excluded_Product_Group_Nm
													, Gift_Card_Ind
													, Inherited_Ind
													, Item_Qty
													, Maximum_Purchase_Amt
													, Minimum_Purchase_Amt
													, Offer_Request_Id
													, Product_Group_Dsc
													, Product_Group_Id
													, Product_Group_Nm
													, Product_Group_Version_Id
													, Store_Group_Version_Id
													, Unique_Item_Ind
													, Unit_Of_Measure_Cd
													, Unit_Of_Measure_Dsc
													, Unit_Of_Measure_Nm
													, User_Interface_Unique_Id
													, FileName 
													, DW_Logical_delete_ind
												    , DML_Type
												    , Sameday_chg_ind
												    ,CASE 	WHEN Offer_Request_Id is NULL THEN 'Offer_Request_Id is NULL' 
															WHEN User_Interface_Unique_Id is NULL THEN 'User_Interface_Unique_Id is NULL'
															WHEN Product_Group_Nm is NULL THEN 'Product_Group_Nm is NULL'
															WHEN Product_Group_Version_Id is NULL THEN 'Product_Group_Version_Id is NULL'
															WHEN Store_Group_Version_Id is NULL THEN 'Store_Group_Version_Id is NULL'
															ELSE NULL END AS Exception_Reason
												    ,CURRENT_TIMESTAMP AS DW_CREATE_TS
												 FROM `+ tgt_wrk_tbl +` 
													 WHERE Offer_Request_Id  IS NULL 
														OR User_Interface_Unique_Id  IS NULL 
														OR Product_Group_Nm  IS NULL 
														OR Product_Group_Version_Id  IS NULL 
														OR Store_Group_Version_Id  IS NULL`;
												
     try {
        snowflake.execute (
            {sqlText: sql_exceptions  }
            );
        }
    catch (err)  {
        return "Insert into tgt Exception table "+ tgt_exp_tbl +" Failed with error: " + err;   // Return a error message.
        }
		
	// **************	Load for Offer_Request_Buy_Product_Group table ENDs *****************
	$$;
  

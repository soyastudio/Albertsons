--liquibase formatted sql
--changeset SYSTEM:sp_GetOfferRequest_To_BIM_LOAD_Offer_Request_Product_Group runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETOFFERREQUEST_TO_BIM_LOAD_OFFER_REQUEST_PRODUCT_GROUP(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, CNF_SCHEMA VARCHAR, WRK_SCHEMA VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
  
    
    var cnf_db = CNF_DB ;
	var wrk_schema = WRK_SCHEMA ;
	var cnf_schema = CNF_SCHEMA;
	var src_wrk_tbl = SRC_WRK_TBL;
	
	// **************	Load for Offer_Request_Product_Group table BEGIN *****************
	// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
	var tgt_wrk_tbl = cnf_db + "." + wrk_schema + ".Offer_Request_Product_Group_wrk";
	var tgt_tbl = cnf_db +"."+ cnf_schema +".Offer_Request_Product_Group";
	var tgt_exp_tbl = cnf_db + "." + wrk_schema + ".Offer_Request_Product_Group_Exceptions";
	var sql_command = `CREATE OR REPLACE TABLE ` + tgt_wrk_tbl + ` as 
					    WITH src_wrk_tbl_recs as
						(	SELECT 	DISTINCT  OfferRequestId AS Offer_Request_Id
											, AttachedOfferTypeId AS User_Interface_Unique_Id
											, ProductGroup_ProductGroupId AS Product_Group_Id
											,ProductGroup_ProductGroupVersionId AS Product_Group_Version_Id
											, ProductGroup_DisplayOrderNbr AS Display_Order_Nbr /* As Filed Name is not Confirmed, using Default Value. Need to change*/
											, ItemQty AS Item_Qty
											, ProductGroup_UOMCd AS Unit_Of_Measure_Cd
											, UOMDsc AS Unit_Of_Measure_Dsc
											, ProductGroup_UOMNm AS Unit_Of_Measure_Nm
											, GiftCardInd AS Gift_Card_Ind
											, AnyProductInd AS Any_Product_Ind
											, UniqueItemInd AS Unique_Item_Ind
											, ConjunctionDsc AS Conjunction_Dsc
											, MinimumPurchaseAmt AS Minimum_Purchase_Amt
											, MaximumPurchaseAmt AS Maximum_Purchase_Amt
											, InheritedInd AS Inherited_Ind
											, ExcludedProductGroup_ProductGroupId AS Excluded_Product_Group_Id
											, ExcludedProductGroup_ProductGroupNm AS Excluded_Product_Group_Nm
											, CORPORATEITEMCD AS Corporate_Item_Cd 
											, UPC_QUALIFIER AS Representative_UPC_Cd 
											, UPCNBR AS Representative_UPC_Nbr 
											, UPCTXT AS Representative_UPC_Txt 
											, REPRESENTATIVESTATUS_DESCRIPTION AS Representative_UPC_Dsc 
											, REPRESENTATIVESTATUS_STATUSTYPECD AS Representative_Status_Type_Cd 									
											, REPRESENTATIVESTATUS_DESCRIPTION AS Representative_Status_Type_Dsc
											, REPRESENTATIVESTATUS_EFFECTIVEDTTM AS Representative_Status_Type_Effective_Ts
											, EffectiveEndDt AS Representative_Status_Type_Effective_End_Dt 
											, STATUSREASON_CODE AS Status_Reason_Cd
											, STATUSREASON_DESCRIPTION AS Status_Reason_Dsc 
											, STATUSREASON_SHORTDESCRIPTION AS Status_Reason_Short_Dsc 
											, ITEMOFFERPRICEAMT AS Item_Offer_Price_Amt 
											, ITEMOFFERPRICE_UOMCD AS Item_Offer_UOM_Cd 
											, ITEMOFFERPRICE_UOMNM AS Item_Offer_UOM_Nm
											, EffectiveStartTs AS Item_Offer_Effective_Start_Dt
											, EffectiveEndTs AS Item_Offer_Effective_End_Dt
											, creationdt
											, actiontypecd 
											, FileName
											, Row_number() OVER ( partition BY OfferRequestId, AttachedOfferTypeId, ProductGroup_ProductGroupId ORDER BY To_timestamp_ntz(creationdt) DESC) AS rn
									FROM ` + src_wrk_tbl +`
						)
						SELECT  src.Offer_Request_Id
											, src.User_Interface_Unique_Id
											, src.Product_Group_Id
											,src.Product_Group_Version_Id
											, src.Display_Order_Nbr
											, src.Item_Qty
											, src.Unit_Of_Measure_Cd
											, src.Unit_Of_Measure_Dsc
											, src.Unit_Of_Measure_Nm
											, src.Gift_Card_Ind
											, src.Any_Product_Ind
											, src.Unique_Item_Ind
											, src.Conjunction_Dsc
											, src.Minimum_Purchase_Amt
											, src.Maximum_Purchase_Amt
											, src.Inherited_Ind
											, src.Excluded_Product_Group_Id
											, src.Excluded_Product_Group_Nm
											,src.Corporate_Item_Cd 
											,src.Representative_UPC_Cd 
											,src.Representative_UPC_Nbr 
											,src.Representative_UPC_Txt 
											,src.Representative_UPC_Dsc 
											,src.Representative_Status_Type_Cd 									
											,src.Representative_Status_Type_Dsc
											,src.Representative_Status_Type_Effective_Ts
											,src.Representative_Status_Type_Effective_End_Dt 
											,src.Status_Reason_Cd
											,src.Status_Reason_Dsc 
											,src.Status_Reason_Short_Dsc 
											,src.Item_Offer_Price_Amt 
											,src.Item_Offer_UOM_Cd 
											,src.Item_Offer_UOM_Nm
											,src.Item_Offer_Effective_Start_Dt
											,src.Item_Offer_Effective_End_Dt
								, src.dw_logical_delete_ind 
								, src.FileName 
								, CASE WHEN (tgt.Offer_Request_Id IS NULL AND tgt.User_Interface_Unique_Id IS NULL AND tgt.Product_Group_Id IS NULL ) THEN 'I' ELSE 'U' END AS DML_Type
								, CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1  ELSE 0 END AS Sameday_chg_ind 
								FROM   	(SELECT		Offer_Request_Id
												, User_Interface_Unique_Id
												, Product_Group_Id
												,Product_Group_Version_Id
												, Display_Order_Nbr
												, Item_Qty
												, Unit_Of_Measure_Cd
												, Unit_Of_Measure_Dsc
												, Unit_Of_Measure_Nm
												, Gift_Card_Ind
												, Any_Product_Ind
												, Unique_Item_Ind
												, Conjunction_Dsc
												, Minimum_Purchase_Amt
												, Maximum_Purchase_Amt
												, Inherited_Ind
												, Excluded_Product_Group_Id
												, Excluded_Product_Group_Nm
												,Corporate_Item_Cd 
												,Representative_UPC_Cd 
												,Representative_UPC_Nbr 
												,Representative_UPC_Txt 
												,Representative_UPC_Dsc 
												,Representative_Status_Type_Cd 									
												,Representative_Status_Type_Dsc
												,Representative_Status_Type_Effective_Ts
												,Representative_Status_Type_Effective_End_Dt 
												,Status_Reason_Cd
												,Status_Reason_Dsc 
												,Status_Reason_Short_Dsc 
												,Item_Offer_Price_Amt 
												,Item_Offer_UOM_Cd 
												,Item_Offer_UOM_Nm
												,Item_Offer_Effective_Start_Dt
												,Item_Offer_Effective_End_Dt
												, creationdt
												, FALSE AS DW_Logical_delete_ind 
												, FileName														
										 FROM src_wrk_tbl_recs
											WHERE  rn = 1
											--AND	Offer_Request_Id  IS NOT NULL 
											--AND User_Interface_Unique_Id  IS NOT NULL 
											--AND Product_Group_Id IS NOT NULL 
											AND	UPPER(ActionTypeCd) <> 'DELETE'												
										) src
								LEFT JOIN 	(SELECT 	tgt.Offer_Request_Id
													, tgt.User_Interface_Unique_Id
													, tgt.Product_Group_Id
													,tgt.Product_Group_Version_Id
													, tgt.Display_Order_Nbr
													, tgt.Item_Qty
													, tgt.Unit_Of_Measure_Cd
													, tgt.Unit_Of_Measure_Dsc
													, tgt.Unit_Of_Measure_Nm
													, tgt.Gift_Card_Ind
													, tgt.Any_Product_Ind
													, tgt.Unique_Item_Ind
													, tgt.Conjunction_Dsc
													, tgt.Minimum_Purchase_Amt
													, tgt.Maximum_Purchase_Amt
													, tgt.Inherited_Ind
													, tgt.Excluded_Product_Group_Id
													, tgt.Excluded_Product_Group_Nm
													,tgt.Corporate_Item_Cd 
													,tgt.Representative_UPC_Cd 
													,tgt.Representative_UPC_Nbr 
													,tgt.Representative_UPC_Txt 
													,tgt.Representative_UPC_Dsc 
													,tgt.Representative_Status_Type_Cd 									
													,tgt.Representative_Status_Type_Dsc
													,tgt.Representative_Status_Type_Effective_Ts
													,tgt.Representative_Status_Type_Effective_End_Dt 
													,tgt.Status_Reason_Cd
													,tgt.Status_Reason_Dsc 
													,tgt.Status_Reason_Short_Dsc 
													,tgt.Item_Offer_Price_Amt 
													,tgt.Item_Offer_UOM_Cd 
													,tgt.Item_Offer_UOM_Nm
													,tgt.Item_Offer_Effective_Start_Dt
													,tgt.Item_Offer_Effective_End_Dt
													, tgt.dw_logical_delete_ind 
													, tgt.dw_first_effective_dt 											
											FROM ` + tgt_tbl + ` tgt
											WHERE DW_CURRENT_VERSION_IND = TRUE	
								) tgt 
								ON 	tgt.Offer_Request_Id = src.Offer_Request_Id 
								AND tgt.User_Interface_Unique_Id = src.User_Interface_Unique_Id 
								AND tgt.Product_Group_Id = src.Product_Group_Id 
						WHERE  (tgt.Offer_Request_Id IS NULL AND tgt.User_Interface_Unique_Id IS NULL AND tgt.Product_Group_Id IS NULL )
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
									 OR NVL(src.Product_Group_Version_Id,'-1') <> NVL(tgt.Product_Group_Version_Id,'-1')
									 OR NVL(src.Unique_Item_Ind,'-1') <> NVL(tgt.Unique_Item_Ind,'-1')
									 OR NVL(src.Unit_Of_Measure_Cd,'-1') <> NVL(tgt.Unit_Of_Measure_Cd,'-1')
									 OR NVL(src.Unit_Of_Measure_Dsc,'-1') <> NVL(tgt.Unit_Of_Measure_Dsc,'-1')
									 OR NVL(src.Unit_Of_Measure_Nm,'-1') <> NVL(tgt.Unit_Of_Measure_Nm,'-1')
									 OR NVL(src.Corporate_Item_Cd,'-1') <> NVL(tgt.Corporate_Item_Cd,'-1')
									 OR NVL(src.Representative_UPC_Cd,'-1') <> NVL(tgt.Representative_UPC_Cd,'-1')
									 OR NVL(src.Representative_UPC_Nbr,'-1') <> NVL(tgt.Representative_UPC_Nbr,'-1')
									 OR NVL(src.Representative_UPC_Txt,'-1') <> NVL(tgt.Representative_UPC_Txt,'-1')
									 OR NVL(src.Representative_UPC_Dsc,'-1') <> NVL(tgt.Representative_UPC_Dsc,'-1')
									 OR NVL(src.Representative_Status_Type_Cd,'-1') <> NVL(tgt.Representative_Status_Type_Cd,'-1')
									 OR NVL(src.Representative_Status_Type_Dsc,'-1') <> NVL(tgt.Representative_Status_Type_Dsc,'-1')
									 OR NVL(src.Representative_Status_Type_Effective_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Representative_Status_Type_Effective_Ts,'9999-12-31 00:00:00.000')
									 OR NVL(src.Representative_Status_Type_Effective_End_Dt,'9999-12-31') <> NVL(tgt.Representative_Status_Type_Effective_End_Dt,'9999-12-31')
									 OR NVL(src.Status_Reason_Cd,'-1') <> NVL(tgt.Status_Reason_Cd,'-1')
									 OR NVL(src.Status_Reason_Dsc,'-1') <> NVL(tgt.Status_Reason_Dsc,'-1')
									 OR NVL(src.Status_Reason_Short_Dsc,'-1') <> NVL(tgt.Status_Reason_Short_Dsc,'-1')
									 OR NVL(src.Item_Offer_Price_Amt,'-1') <> NVL(tgt.Item_Offer_Price_Amt,'-1')
									 OR NVL(src.Item_Offer_UOM_Cd,'-1') <> NVL(tgt.Item_Offer_UOM_Cd,'-1')
									 OR NVL(src.Item_Offer_UOM_Nm,'-1') <> NVL(tgt.Item_Offer_UOM_Nm,'-1')									 
									 OR NVL(src.Item_Offer_Effective_Start_Dt,'9999-12-31') <> NVL(tgt.Item_Offer_Effective_Start_Dt,'9999-12-31')
									 OR NVL(src.Item_Offer_Effective_End_Dt,'9999-12-31') <> NVL(tgt.Item_Offer_Effective_End_Dt,'9999-12-31')
									 OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
								)
						UNION ALL
							SELECT 	tgt.Offer_Request_Id
									, tgt.User_Interface_Unique_Id
									, tgt.Product_Group_Id
									, tgt.Product_Group_Version_Id
									, tgt.Display_Order_Nbr
									, tgt.Item_Qty
									, tgt.Unit_Of_Measure_Cd
									, tgt.Unit_Of_Measure_Dsc
									, tgt.Unit_Of_Measure_Nm
									, tgt.Gift_Card_Ind
									, tgt.Any_Product_Ind
									, tgt.Unique_Item_Ind
									, tgt.Conjunction_Dsc
									, tgt.Minimum_Purchase_Amt
									, tgt.Maximum_Purchase_Amt
									, tgt.Inherited_Ind
									, tgt.Excluded_Product_Group_Id
									, tgt.Excluded_Product_Group_Nm	
									,tgt.Corporate_Item_Cd 
									,tgt.Representative_UPC_Cd 
									,tgt.Representative_UPC_Nbr 
									,tgt.Representative_UPC_Txt 
									,tgt.Representative_UPC_Dsc 
									,tgt.Representative_Status_Type_Cd 									
									,tgt.Representative_Status_Type_Dsc
									,tgt.Representative_Status_Type_Effective_Ts
									,tgt.Representative_Status_Type_Effective_End_Dt 
									,tgt.Status_Reason_Cd
									,tgt.Status_Reason_Dsc 
									,tgt.Status_Reason_Short_Dsc 
									,tgt.Item_Offer_Price_Amt 
									,tgt.Item_Offer_UOM_Cd 
									,tgt.Item_Offer_UOM_Nm
									,tgt.Item_Offer_Effective_Start_Dt
									,tgt.Item_Offer_Effective_End_Dt
									, TRUE AS DW_Logical_delete_ind
									--, tgt.DW_SOURCE_CREATE_NM
									,src.filename
									, 'U' as DML_Type
									
									, CASE WHEN tgt.DW_First_Effective_dt = CURRENT_DATE THEN 1 Else 0 END as Sameday_chg_ind
						FROM	` + tgt_tbl + ` tgt
						inner join src_wrk_tbl_recs src on 
					src.Offer_Request_Id = tgt.Offer_Request_Id
					
					
							WHERE	DW_CURRENT_VERSION_IND = TRUE
							AND     rn = 1
							AND		upper(ActionTypeCd) = 'DELETE'
							AND		DW_LOGICAL_DELETE_IND = FALSE
								AND		(tgt.Offer_Request_Id) in 
										(
											SELECT 	DISTINCT Offer_Request_Id
													
													
											FROM	src_wrk_tbl_recs src
												WHERE 	rn = 1
													AND		upper(ActionTypeCd) = 'DELETE'
													AND		Offer_Request_Id is not null
													
										)					  				
									`;
	try {
        snowflake.execute (
            {sqlText: sql_command  }
            );
        }
    catch (err)  {
        return "Creation of Offer_Request_Product_Group work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
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
											, Product_Group_Id
								 
											
							
											,FileName
									FROM 	`+ tgt_wrk_tbl +` 
										WHERE	DML_Type = 'U'
											AND	Sameday_chg_ind = 0
											AND	Offer_Request_Id  IS NOT NULL 
											AND User_Interface_Unique_Id  IS NOT NULL 
											AND Product_Group_Id IS NOT NULL
								) src
						WHERE	tgt.Offer_Request_Id = src.Offer_Request_Id 
							AND tgt.User_Interface_Unique_Id = src.User_Interface_Unique_Id
							AND tgt.Product_Group_Id = src.Product_Group_Id
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
								, Product_Group_Version_Id = src.Product_Group_Version_Id
								, Unique_Item_Ind = src.Unique_Item_Ind
								, Unit_Of_Measure_Cd = src.Unit_Of_Measure_Cd
								, Unit_Of_Measure_Dsc = src.Unit_Of_Measure_Dsc
								, Unit_Of_Measure_Nm = src.Unit_Of_Measure_Nm
								,Corporate_Item_Cd = src.Corporate_Item_Cd
								,Representative_UPC_Cd = src.Representative_UPC_Cd
								,Representative_UPC_Nbr = src.Representative_UPC_Nbr
								,Representative_UPC_Txt = src.Representative_UPC_Txt
								,Representative_UPC_Dsc = src.Representative_UPC_Dsc
								,Representative_Status_Type_Cd = src.Representative_Status_Type_Cd									
								,Representative_Status_Type_Dsc = src.Representative_Status_Type_Dsc
								,Representative_Status_Type_Effective_Ts = src.Representative_Status_Type_Effective_Ts
								,Representative_Status_Type_Effective_End_Dt = src.Representative_Status_Type_Effective_End_Dt
								,Status_Reason_Cd = src.Status_Reason_Cd
								,Status_Reason_Dsc = src.Status_Reason_Dsc
								,Status_Reason_Short_Dsc = src.Status_Reason_Short_Dsc
								,Item_Offer_Price_Amt = src.Item_Offer_Price_Amt
								,Item_Offer_UOM_Cd = src.Item_Offer_UOM_Cd
								,Item_Offer_UOM_Nm = src.Item_Offer_UOM_Nm
								,Item_Offer_Effective_Start_Dt = src.Item_Offer_Effective_Start_Dt
								,Item_Offer_Effective_End_Dt = src.Item_Offer_Effective_End_Dt
								, DW_Logical_delete_ind = src.DW_Logical_delete_ind
								, DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
								, DW_SOURCE_UPDATE_NM = FileName
						FROM	(	SELECT 	Offer_Request_Id
												, User_Interface_Unique_Id
												, Product_Group_Id
												,Product_Group_Version_Id
												, Display_Order_Nbr
												, Item_Qty
												, Unit_Of_Measure_Cd
												, Unit_Of_Measure_Dsc
												, Unit_Of_Measure_Nm
												, Gift_Card_Ind
												, Any_Product_Ind
												, Unique_Item_Ind
												, Conjunction_Dsc
												, Minimum_Purchase_Amt
												, Maximum_Purchase_Amt
												, Inherited_Ind
												, Excluded_Product_Group_Id
												, Excluded_Product_Group_Nm 
												,Corporate_Item_Cd 
												,Representative_UPC_Cd 
												,Representative_UPC_Nbr 
												,Representative_UPC_Txt 
												,Representative_UPC_Dsc 
												,Representative_Status_Type_Cd 									
												,Representative_Status_Type_Dsc
												,Representative_Status_Type_Effective_Ts
												,Representative_Status_Type_Effective_End_Dt 
												,Status_Reason_Cd
												,Status_Reason_Dsc 
												,Status_Reason_Short_Dsc 
												,Item_Offer_Price_Amt 
												,Item_Offer_UOM_Cd 
												,Item_Offer_UOM_Nm
												,Item_Offer_Effective_Start_Dt
												,Item_Offer_Effective_End_Dt			
											, DW_Logical_delete_ind
											, FileName
									FROM 	`+ tgt_wrk_tbl +` 
									WHERE	DML_Type = 'U'
									AND	Sameday_chg_ind = 1
									AND	Offer_Request_Id  IS NOT NULL 
									AND User_Interface_Unique_Id  IS NOT NULL
									AND Product_Group_Id  IS NOT NULL									
								) src
						WHERE	tgt.Offer_Request_Id = src.Offer_Request_Id 
							AND tgt.User_Interface_Unique_Id = src.User_Interface_Unique_Id 
							AND tgt.Product_Group_Id = src.Product_Group_Id 
							AND	tgt.DW_CURRENT_VERSION_IND = TRUE`;

						// Processing Inserts
	
	
	var sql_inserts = `INSERT INTO ` + tgt_tbl + `
						(	Offer_Request_Id
							, User_Interface_Unique_Id
							, Product_Group_Id
							,Product_Group_Version_Id
							, Display_Order_Nbr
							, Item_Qty
							, Unit_Of_Measure_Cd
							, Unit_Of_Measure_Dsc
							, Unit_Of_Measure_Nm
							, Gift_Card_Ind
							, Any_Product_Ind
							, Unique_Item_Ind
							, Conjunction_Dsc
							, Minimum_Purchase_Amt
							, Maximum_Purchase_Amt
							, Inherited_Ind
							, Excluded_Product_Group_Id
							, Excluded_Product_Group_Nm 
							,Corporate_Item_Cd 
							,Representative_UPC_Cd 
							,Representative_UPC_Nbr 
							,Representative_UPC_Txt 
							,Representative_UPC_Dsc 
							,Representative_Status_Type_Cd 									
							,Representative_Status_Type_Dsc
							,Representative_Status_Type_Effective_Ts
							,Representative_Status_Type_Effective_End_Dt 
							,Status_Reason_Cd
							,Status_Reason_Dsc 
							,Status_Reason_Short_Dsc 
							,Item_Offer_Price_Amt 
							,Item_Offer_UOM_Cd 
							,Item_Offer_UOM_Nm
							,Item_Offer_Effective_Start_Dt
							,Item_Offer_Effective_End_Dt                          
							, DW_First_Effective_Dt     
							, DW_Last_Effective_Dt     
							, DW_CREATE_TS
							, DW_LOGICAL_DELETE_IND
							, DW_SOURCE_CREATE_NM
							, DW_CURRENT_VERSION_IND
						)
						SELECT 	 Offer_Request_Id
							, User_Interface_Unique_Id
							, Product_Group_Id
							,Product_Group_Version_Id
							, Display_Order_Nbr
							, Item_Qty
							, Unit_Of_Measure_Cd
							, Unit_Of_Measure_Dsc
							, Unit_Of_Measure_Nm
							, Gift_Card_Ind
							, Any_Product_Ind
							, Unique_Item_Ind
							, Conjunction_Dsc
							, Minimum_Purchase_Amt
							, Maximum_Purchase_Amt
							, Inherited_Ind
							, Excluded_Product_Group_Id
							, Excluded_Product_Group_Nm 
							,Corporate_Item_Cd 
							,Representative_UPC_Cd 
							,Representative_UPC_Nbr 
							,Representative_UPC_Txt 
							,Representative_UPC_Dsc 
							,Representative_Status_Type_Cd 									
							,Representative_Status_Type_Dsc
							,Representative_Status_Type_Effective_Ts
							,Representative_Status_Type_Effective_End_Dt 
							,Status_Reason_Cd
							,Status_Reason_Dsc 
							,Status_Reason_Short_Dsc 
							,Item_Offer_Price_Amt 
							,Item_Offer_UOM_Cd 
							,Item_Offer_UOM_Nm
							,Item_Offer_Effective_Start_Dt
							,Item_Offer_Effective_End_Dt
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
						AND Product_Group_Id  IS NOT NULL `;
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
        return "Loading of Offer_Request_Product_Group table "+ tgt_tbl + " Failed with error: " + err;   // Return a error message.
        }
		
		var truncate_exceptions = `DELETE FROM ${tgt_exp_tbl};`;

		var sql_exceptions = `INSERT INTO ` + tgt_exp_tbl	+`  
		                               select 	    Offer_Request_Id
													, User_Interface_Unique_Id
													, Product_Group_Id
													,Product_Group_Version_Id
													, Display_Order_Nbr
													, Item_Qty
													, Unit_Of_Measure_Cd
													, Unit_Of_Measure_Dsc
													, Unit_Of_Measure_Nm
													, Gift_Card_Ind
													, Any_Product_Ind
													, Unique_Item_Ind
													, Conjunction_Dsc
													, Minimum_Purchase_Amt
													, Maximum_Purchase_Amt
													, Inherited_Ind
													, Excluded_Product_Group_Id
													, Excluded_Product_Group_Nm
													,Corporate_Item_Cd 
													,Representative_UPC_Cd 
													,Representative_UPC_Nbr 
													,Representative_UPC_Txt 
													,Representative_UPC_Dsc 
													,Representative_Status_Type_Cd 									
													,Representative_Status_Type_Dsc
													,Representative_Status_Type_Effective_Ts
													,Representative_Status_Type_Effective_End_Dt 
													,Status_Reason_Cd
													,Status_Reason_Dsc 
													,Status_Reason_Short_Dsc 
													,Item_Offer_Price_Amt 
													,Item_Offer_UOM_Cd 
													,Item_Offer_UOM_Nm
													,Item_Offer_Effective_Start_Dt
													,Item_Offer_Effective_End_Dt			
													, FileName 
													, DW_Logical_delete_ind
												    , DML_Type
												    , Sameday_chg_ind
												    ,CASE 	WHEN Offer_Request_Id is NULL THEN 'Offer_Request_Id is NULL' 
															WHEN User_Interface_Unique_Id is NULL THEN 'User_Interface_Unique_Id is NULL'
															WHEN Product_Group_Id is NULL THEN 'Product_Group_Id is NULL'
															ELSE NULL END AS Exception_Reason
												    ,CURRENT_TIMESTAMP AS DW_CREATE_TS
												 FROM `+ tgt_wrk_tbl +` 
													 WHERE Offer_Request_Id  IS NULL 
														OR User_Interface_Unique_Id  IS NULL 
														OR Product_Group_Id IS NULL`;
												
     try {
        snowflake.execute (
            {sqlText: sql_exceptions  }
            );
		snowflake.execute (
            {sqlText: sql_exceptions  }
            );
		snowflake.execute(
                     {sqlText: truncate_exceptions}
                     ); 
        snowflake.execute (
            {sqlText: sql_exceptions  }
            );
		snowflake.execute (
                     {sqlText: sql_commit  }
                     );	
        }
    catch (err)  {
        return "Insert into tgt Exception table "+ tgt_exp_tbl +" Failed with error: " + err;   // Return a error message.
        }
		
	// **************	Load for Offer_Request_Product_Group table ENDs *****************
	
	
$$;

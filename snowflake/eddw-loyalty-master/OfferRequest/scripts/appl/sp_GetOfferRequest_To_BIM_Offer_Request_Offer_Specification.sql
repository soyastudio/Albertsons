--liquibase formatted sql
--changeset SYSTEM:sp_GetOfferRequest_To_BIM_Offer_Request_Offer_Specification runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETOFFERREQUEST_TO_BIM_OFFER_REQUEST_OFFER_SPECIFICATION(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, CNF_SCHEMA VARCHAR, WRK_SCHEMA VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
 
     
    
	var cnf_db = CNF_DB;
	var wrk_schema = WRK_SCHEMA;
	var cnf_schema = CNF_SCHEMA;
	var src_wrk_tbl = SRC_WRK_TBL;
	
	// **************	Load for Offer_Request_Offer_Specification table BEGIN *****************
	// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
	var tgt_wrk_tbl = cnf_db + "." + wrk_schema + ".Offer_Request_Offer_Specification_wrk";
	var tgt_tbl = cnf_db +"."+ cnf_schema +".Offer_Request_Offer_Specification";
	var tgt_exp_tbl = cnf_db + "." + wrk_schema + ".Offer_Request_Offer_Specification_Exceptions";
	var sql_command = `CREATE OR REPLACE TABLE ` + tgt_wrk_tbl + ` as 
					WITH src_wrk_tbl_recs as
					(	SELECT DISTINCT  AttachedOfferType_DisplayOrderNbr as Display_Order_Nbr
											,FrequencyDsc as Instant_Win_Frequency_Dsc
											,PrizeItemQty as Instant_Win_Prize_Item_Qty
											,InstantWinProgramId as Instant_Win_Program_Id
											,InstantWinVersionId as Instant_Win_Version_Id
											,LoyaltyPgmTagInd as Loyalty_Program_Tag_Ind
											,OfferRequestId as Offer_Request_Id
											,CustomerFriendlyCategory_Code as POD_Customer_Friendly_Category_Cd
											,CustomerFriendlyCategory_Description as POD_Customer_Friendly_Category_Dsc
											,CustomerFriendlyCategory_ShortDescription as POD_Customer_Friendly_Category_Short_Dsc
											,PODDetailType_DisclaimerTxt as POD_Disclaimer_Txt
											,PODDetailType_DisplayEndDt as POD_Display_End_Dt
											,PODDetailType_DisplayStartDt as POD_Display_Start_Dt
											,HeadlineTxt as POD_Headline_Txt
											,PODdetailtype_headlinesubtxt as POD_Headline_Sub_Txt
											,OfferDsc as POD_Offer_Dsc
											,OFFERDETAIL_CODE as POD_Offer_Detail_Cd
											,OFFERDETAIL_DESCRIPTION as POD_Offer_Detail_Dsc
											,OFFERDETAIL_SHORTDESCRIPTION as POD_Offer_Detail_Short_Dsc
											,PriceInfoTxt as POD_Price_Info_Txt
											,ItemQty as POD_Item_Qty
											,poddetailtype_UOMCD as POD_Unit_Of_Measure_Cd
											,poddetailtype_UOMNM as POD_Unit_Of_Measure_Nm
											,UsageLimitTypeTxt as Usage_Limit_Type_Txt
											,ProtoType_Code as Proto_Type_Cd
											,ProtoType_Description as Proto_Type_Dsc
											,ProtoType_ShortDescription as Proto_Type_Short_Dsc
											,AttachedOfferType_StoreGroupVersionId as Store_Group_Version_Id
											,TagAmt as Store_Tag_Amt
											,TagDsc as Store_Tag_Dsc
											,TagNbr as Store_Tag_Nbr
											,AttachedOfferTypeId as User_Interface_Unique_Id
											, creationdt
											, actiontypecd 
											, FileName
											, Row_number() OVER ( partition BY OfferRequestId, User_Interface_Unique_Id --Store_Group_Version_Id
											ORDER BY To_timestamp_ntz(creationdt) DESC) AS rn
									FROM ` + src_wrk_tbl +`			
					)
					SELECT 	 src.Display_Order_Nbr
								,src.Instant_Win_Frequency_Dsc
								,src.Instant_Win_Prize_Item_Qty
								,src.Instant_Win_Program_Id
								,src.Instant_Win_Version_Id
								,src.Loyalty_Program_Tag_Ind
								,src.Offer_Request_Id
								,src.POD_Customer_Friendly_Category_Cd
								,src.POD_Customer_Friendly_Category_Dsc
								,src.POD_Customer_Friendly_Category_Short_Dsc
								,src.POD_Disclaimer_Txt
								,src.POD_Display_End_Dt
								,src.POD_Display_Start_Dt
								,src.POD_Headline_Txt
								,src.POD_Headline_Sub_Txt
								,src.POD_Offer_Dsc
								,src.POD_Offer_Detail_Cd
								,src.POD_Offer_Detail_Dsc
								,src.POD_Offer_Detail_Short_Dsc
								,src.POD_Price_Info_Txt
								,src.POD_Item_Qty
								,src.POD_Unit_Of_Measure_Cd
								,src.POD_Unit_Of_Measure_Nm
								,src.Usage_Limit_Type_Txt
								,src.Proto_Type_Cd
								,src.Proto_Type_Dsc
								,src.Proto_Type_Short_Dsc
								,src.Store_Group_Version_Id
								,src.Store_Tag_Amt
								,src.Store_Tag_Dsc
								,src.Store_Tag_Nbr
								,src.User_Interface_Unique_Id
								,src.dw_logical_delete_ind
							,src.FileName 
							,CASE WHEN (tgt.Offer_Request_Id IS NULL AND tgt.User_Interface_Unique_Id IS NULL) THEN 'I' ELSE 'U' END AS DML_Type
							,CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1  ELSE 0 END AS Sameday_chg_ind 
					FROM   (SELECT 	 Display_Order_Nbr
													,Instant_Win_Frequency_Dsc
													,Instant_Win_Prize_Item_Qty
													,Instant_Win_Program_Id
													,Instant_Win_Version_Id
													,Loyalty_Program_Tag_Ind
													,Offer_Request_Id
													,POD_Customer_Friendly_Category_Cd
													,POD_Customer_Friendly_Category_Dsc
													,POD_Customer_Friendly_Category_Short_Dsc
													,POD_Disclaimer_Txt
													,POD_Display_End_Dt
													,POD_Display_Start_Dt
													,POD_Headline_Txt
													,POD_Headline_Sub_Txt
													,POD_Offer_Dsc
													,POD_Offer_Detail_Cd
													,POD_Offer_Detail_Dsc
													,POD_Offer_Detail_Short_Dsc
													,POD_Price_Info_Txt
													,POD_Item_Qty
													,POD_Unit_Of_Measure_Cd
													,POD_Unit_Of_Measure_Nm
													,Usage_Limit_Type_Txt
													,Proto_Type_Cd
													,Proto_Type_Dsc
													,Proto_Type_Short_Dsc
													,Store_Group_Version_Id
													,Store_Tag_Amt
													,Store_Tag_Dsc
													,Store_Tag_Nbr
													,User_Interface_Unique_Id
													,creationdt 
									,FALSE AS DW_Logical_delete_ind
									,FileName 
							FROM	src_wrk_tbl_recs
							WHERE  rn = 1
							AND UPPER(ActionTypeCd) <> 'DELETE'
					) src 
					LEFT JOIN 	(SELECT tgt.Display_Order_Nbr
														,tgt.Instant_Win_Frequency_Dsc
														,tgt.Instant_Win_Prize_Item_Qty
														,tgt.Instant_Win_Program_Id
														,tgt.Instant_Win_Version_Id
														,tgt.Loyalty_Program_Tag_Ind
														,tgt.Offer_Request_Id
														,tgt.POD_Customer_Friendly_Category_Cd
														,tgt.POD_Customer_Friendly_Category_Dsc
														,tgt.POD_Customer_Friendly_Category_Short_Dsc
														,tgt.POD_Disclaimer_Txt
														,tgt.POD_Display_End_Dt
														,tgt.POD_Display_Start_Dt
														,tgt.POD_Headline_Txt
														,tgt.POD_Headline_Sub_Txt
														,tgt.POD_Offer_Dsc
														,tgt.POD_Offer_Detail_Cd
														,tgt.POD_Offer_Detail_Dsc
														,tgt.POD_Offer_Detail_Short_Dsc
														,tgt.POD_Price_Info_Txt
														,tgt.Proto_Type_Cd
														,tgt.POD_Item_Qty
														,tgt.POD_Unit_Of_Measure_Cd
														,tgt.POD_Unit_Of_Measure_Nm
														,tgt.Usage_Limit_Type_Txt
														,tgt.Proto_Type_Dsc
														,tgt.Proto_Type_Short_Dsc
														,tgt.Store_Group_Version_Id
														,tgt.Store_Tag_Amt
														,tgt.Store_Tag_Dsc
														,tgt.Store_Tag_Nbr
														,tgt.User_Interface_Unique_Id
														
										,tgt.dw_logical_delete_ind 
										,tgt.dw_first_effective_dt 
								FROM ` + tgt_tbl + ` tgt
								WHERE DW_CURRENT_VERSION_IND = TRUE
					) tgt 
						ON tgt.Offer_Request_Id = src.Offer_Request_Id 
						AND tgt.User_Interface_Unique_Id = src.User_Interface_Unique_Id	
						WHERE  	(tgt.Offer_Request_Id IS NULL AND tgt.User_Interface_Unique_Id IS NULL)
						OR 	
						(      NVL(src.Display_Order_Nbr,'-1') <> NVL(tgt.Display_Order_Nbr,'-1')
										OR NVL(src.Instant_Win_Frequency_Dsc,'-1') <> NVL(tgt.Instant_Win_Frequency_Dsc,'-1')
										OR NVL(src.Instant_Win_Prize_Item_Qty,'-1') <> NVL(tgt.Instant_Win_Prize_Item_Qty,'-1')
										OR NVL(src.Instant_Win_Program_Id,'-1') <> NVL(tgt.Instant_Win_Program_Id,'-1')
										OR NVL(src.Instant_Win_Version_Id,'-1') <> NVL(tgt.Instant_Win_Version_Id,'-1')
										OR NVL(src.Loyalty_Program_Tag_Ind,'-1') <> NVL(tgt.Loyalty_Program_Tag_Ind,'-1')
										OR NVL(src.POD_Customer_Friendly_Category_Cd,'-1') <> NVL(tgt.POD_Customer_Friendly_Category_Cd,'-1')
										OR NVL(src.POD_Customer_Friendly_Category_Dsc,'-1') <> NVL(tgt.POD_Customer_Friendly_Category_Dsc,'-1')
										OR NVL(src.POD_Customer_Friendly_Category_Short_Dsc,'-1') <> NVL(tgt.POD_Customer_Friendly_Category_Short_Dsc,'-1')
										OR NVL(src.POD_Disclaimer_Txt,'-1') <> NVL(tgt.POD_Disclaimer_Txt,'-1')
										OR NVL(src.POD_Display_End_Dt,'9999-12-31') <> NVL(tgt.POD_Display_End_Dt,'9999-12-31')
										OR NVL(src.POD_Display_Start_Dt,'9999-12-31') <> NVL(tgt.POD_Display_Start_Dt,'9999-12-31')
										OR NVL(src.POD_Headline_Txt,'-1') <> NVL(tgt.POD_Headline_Txt,'-1')
										OR NVL(src.POD_Headline_Sub_Txt,'-1') <> NVL(tgt.POD_Headline_Sub_Txt,'-1')
										OR NVL(src.POD_Offer_Dsc,'-1') <> NVL(tgt.POD_Offer_Dsc,'-1')
										OR NVL(src.POD_Offer_Detail_Cd,'-1') <> NVL(tgt.POD_Offer_Detail_Cd,'-1')
										OR NVL(src.POD_Offer_Detail_Dsc,'-1') <> NVL(tgt.POD_Offer_Detail_Dsc,'-1')
										OR NVL(src.POD_Offer_Detail_Short_Dsc,'-1') <> NVL(tgt.POD_Offer_Detail_Short_Dsc,'-1')
										OR NVL(src.POD_Price_Info_Txt,'-1') <> NVL(tgt.POD_Price_Info_Txt,'-1')
										OR NVL(src.POD_Item_Qty,'-1') <> NVL(tgt.POD_Item_Qty,'-1')
										OR NVL(src.POD_Unit_Of_Measure_Cd,'-1') <> NVL(tgt.POD_Unit_Of_Measure_Cd,'-1')
										OR NVL(src.POD_Unit_Of_Measure_Nm,'-1') <> NVL(tgt.POD_Unit_Of_Measure_Nm,'-1')
										OR NVL(src.Usage_Limit_Type_Txt,'-1') <> NVL(tgt.Usage_Limit_Type_Txt,'-1')
										OR NVL(src.Proto_Type_Cd,'-1') <> NVL(tgt.Proto_Type_Cd,'-1')
										OR NVL(src.Proto_Type_Dsc,'-1') <> NVL(tgt.Proto_Type_Dsc,'-1')
										OR NVL(src.Proto_Type_Short_Dsc,'-1') <> NVL(tgt.Proto_Type_Short_Dsc,'-1')
										OR NVL(src.Store_Group_Version_Id,'-1') <> NVL(tgt.Store_Group_Version_Id,'-1')
										OR NVL(src.Store_Tag_Amt,'-1') <> NVL(tgt.Store_Tag_Amt,'-1')
										OR NVL(src.Store_Tag_Dsc,'-1') <> NVL(tgt.Store_Tag_Dsc,'-1')
										OR NVL(src.Store_Tag_Nbr,'-1') <> NVL(tgt.Store_Tag_Nbr,'-1')
		  						    
						OR 		src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
						)
					UNION ALL
					SELECT 	tgt.Display_Order_Nbr
									,tgt.Instant_Win_Frequency_Dsc
									,tgt.Instant_Win_Prize_Item_Qty
									,tgt.Instant_Win_Program_Id
									,tgt.Instant_Win_Version_Id
									,tgt.Loyalty_Program_Tag_Ind
									,tgt.Offer_Request_Id
									,tgt.POD_Customer_Friendly_Category_Cd
									,tgt.POD_Customer_Friendly_Category_Dsc
									,tgt.POD_Customer_Friendly_Category_Short_Dsc
									,tgt.POD_Disclaimer_Txt
									,tgt.POD_Display_End_Dt
									,tgt.POD_Display_Start_Dt
									,tgt.POD_Headline_Txt
									,tgt.POD_Headline_Sub_Txt
									,tgt.POD_Offer_Dsc
									,tgt.POD_Offer_Detail_Cd
									,tgt.POD_Offer_Detail_Dsc
									,tgt.POD_Offer_Detail_Short_Dsc
									,tgt.POD_Price_Info_Txt
									,tgt.POD_Item_Qty
									,tgt.POD_Unit_Of_Measure_Cd
									,tgt.POD_Unit_Of_Measure_Nm
									,tgt.Usage_Limit_Type_Txt
									,tgt.Proto_Type_Cd
									,tgt.Proto_Type_Dsc
									,tgt.Proto_Type_Short_Dsc
									,tgt.Store_Group_Version_Id
									,tgt.Store_Tag_Amt
									,tgt.Store_Tag_Dsc
									,tgt.Store_Tag_Nbr
									,tgt.User_Interface_Unique_Id					
									
							,TRUE AS DW_Logical_delete_ind
							,src.Filename
							,'U' as DML_Type
                            ,CASE WHEN tgt.DW_First_Effective_dt = CURRENT_DATE THEN 1 Else 0 END as Sameday_chg_ind
					FROM ` + tgt_tbl + ` tgt
					 inner join src_wrk_tbl_recs src on src.Offer_Request_Id = tgt.Offer_Request_Id
					
					WHERE	DW_CURRENT_VERSION_IND = TRUE
					and rn = 1
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
        return "Creation of Offer_Request_Offer_Specification work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
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
                                            ,User_Interface_Unique_Id
											,FileName
									FROM 	`+ tgt_wrk_tbl +` 
									WHERE	DML_Type = 'U'
									AND		Sameday_chg_ind = 0
									AND 	Offer_Request_Id is not NULL
									AND 	User_Interface_Unique_Id is not NULL
								) src
						WHERE	tgt.Offer_Request_Id = src.Offer_Request_Id
						AND		tgt.User_Interface_Unique_Id = src.User_Interface_Unique_Id
					    AND		tgt.DW_CURRENT_VERSION_IND = TRUE`;
						
    var sql_sameday = `// Processing Sameday updates
						UPDATE ` + tgt_tbl + ` as tgt
						SET		 Display_Order_Nbr=src.Display_Order_Nbr
								,Instant_Win_Frequency_Dsc=src.Instant_Win_Frequency_Dsc
								,Instant_Win_Prize_Item_Qty=src.Instant_Win_Prize_Item_Qty
								,Instant_Win_Program_Id=src.Instant_Win_Program_Id
								,Instant_Win_Version_Id=src.Instant_Win_Version_Id
								,Loyalty_Program_Tag_Ind=src.Loyalty_Program_Tag_Ind
								,POD_Customer_Friendly_Category_Cd=src.POD_Customer_Friendly_Category_Cd
								,POD_Customer_Friendly_Category_Dsc=src.POD_Customer_Friendly_Category_Dsc
								,POD_Customer_Friendly_Category_Short_Dsc=src.POD_Customer_Friendly_Category_Short_Dsc
								,POD_Disclaimer_Txt=src.POD_Disclaimer_Txt
								,POD_Display_End_Dt=src.POD_Display_End_Dt
								,POD_Display_Start_Dt=src.POD_Display_Start_Dt
								,POD_Headline_Txt=src.POD_Headline_Txt
								,POD_Headline_Sub_Txt = src.POD_Headline_Sub_Txt
								,POD_Offer_Dsc=src.POD_Offer_Dsc
								,POD_Offer_Detail_Cd=src.POD_Offer_Detail_Cd
								,POD_Offer_Detail_Dsc=src.POD_Offer_Detail_Dsc
								,POD_Offer_Detail_Short_Dsc=src.POD_Offer_Detail_Short_Dsc
								,POD_Price_Info_Txt=src.POD_Price_Info_Txt
								,POD_Item_Qty=src.POD_Item_Qty
								,POD_Unit_Of_Measure_Cd=src.POD_Unit_Of_Measure_Cd
								,POD_Unit_Of_Measure_Nm=src.POD_Unit_Of_Measure_Nm
								,Usage_Limit_Type_Txt=src.Usage_Limit_Type_Txt
								,Proto_Type_Cd=src.Proto_Type_Cd
								,Proto_Type_Dsc=src.Proto_Type_Dsc
								,Proto_Type_Short_Dsc=src.Proto_Type_Short_Dsc
								,Store_Group_Version_Id = src.Store_Group_Version_Id
								,Store_Tag_Amt=src.Store_Tag_Amt
								,Store_Tag_Dsc=src.Store_Tag_Dsc
								,Store_Tag_Nbr=src.Store_Tag_Nbr
								,DW_Logical_delete_ind = src.DW_Logical_delete_ind
								,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
								,DW_SOURCE_UPDATE_NM = FileName
						FROM	(	SELECT 	 Display_Order_Nbr
											,Instant_Win_Frequency_Dsc
											,Instant_Win_Prize_Item_Qty
											,Instant_Win_Program_Id
											,Instant_Win_Version_Id
											,Loyalty_Program_Tag_Ind
											,Offer_Request_Id
											,POD_Customer_Friendly_Category_Cd
											,POD_Customer_Friendly_Category_Dsc
											,POD_Customer_Friendly_Category_Short_Dsc
											,POD_Disclaimer_Txt
											,POD_Display_End_Dt
											,POD_Display_Start_Dt
											,POD_Headline_Txt
											,POD_Headline_Sub_Txt
											,POD_Offer_Dsc
											,POD_Offer_Detail_Cd
											,POD_Offer_Detail_Dsc
											,POD_Offer_Detail_Short_Dsc
											,POD_Price_Info_Txt
											,POD_Item_Qty
											,POD_Unit_Of_Measure_Cd
											,POD_Unit_Of_Measure_Nm
											,Usage_Limit_Type_Txt
											,Proto_Type_Cd
											,Proto_Type_Dsc
											,Proto_Type_Short_Dsc
											,Store_Group_Version_Id
											,Store_Tag_Amt
											,Store_Tag_Dsc
											,Store_Tag_Nbr
											,User_Interface_Unique_Id
											,DW_Logical_delete_ind
											,FileName
									FROM 	`+ tgt_wrk_tbl +` 
									WHERE	DML_Type = 'U'
									AND		Sameday_chg_ind = 1
									AND		Offer_Request_Id is not NULL
									AND		User_Interface_Unique_Id is not NULL
								
								) src
						WHERE	tgt.Offer_Request_Id = src.Offer_Request_Id
						AND		tgt.User_Interface_Unique_Id = src.User_Interface_Unique_Id
					    AND		tgt.DW_CURRENT_VERSION_IND = TRUE`;

	// Processing Inserts
	
	var sql_inserts = `INSERT INTO ` + tgt_tbl + `
						(	 OFFER_REQUEST_ID ,  
							USER_INTERFACE_UNIQUE_ID, 
							DW_First_Effective_Dt   ,
							DW_Last_Effective_Dt  ,
							STORE_GROUP_VERSION_ID ,  
							DISPLAY_ORDER_NBR  ,
							PROTO_TYPE_CD , 
							PROTO_TYPE_DSC  ,
							PROTO_TYPE_SHORT_DSC,  
							STORE_TAG_NBR , 
							STORE_TAG_AMT , 
							LOYALTY_PROGRAM_TAG_IND , 
							STORE_TAG_DSC  ,
							POD_HEADLINE_TXT , 
							POD_Headline_Sub_Txt ,
							POD_OFFER_DSC , 
							POD_Offer_Detail_Cd, 
							POD_Offer_Detail_Dsc ,
							POD_Offer_Detail_Short_Dsc ,
							POD_PRICE_INFO_TXT  ,
							POD_Item_Qty, 
							POD_Unit_Of_Measure_Cd, 
							POD_Unit_Of_Measure_Nm ,
							Usage_Limit_Type_Txt ,
							POD_DISCLAIMER_TXT , 
							POD_DISPLAY_START_DT , 
							POD_DISPLAY_END_DT , 
							POD_CUSTOMER_FRIENDLY_CATEGORY_CD , 
							POD_CUSTOMER_FRIENDLY_CATEGORY_DSC , 
							POD_CUSTOMER_FRIENDLY_CATEGORY_SHORT_DSC , 
							INSTANT_WIN_PROGRAM_ID , 
							INSTANT_WIN_VERSION_ID , 
							INSTANT_WIN_PRIZE_ITEM_QTY , 
							INSTANT_WIN_FREQUENCY_DSC  ,
							DW_CREATE_TS           ,
						    DW_LOGICAL_DELETE_IND   ,
							DW_SOURCE_CREATE_NM    ,
						    DW_CURRENT_VERSION_IND
						)
						SELECT 	OFFER_REQUEST_ID ,  
							USER_INTERFACE_UNIQUE_ID, 
							CURRENT_DATE   ,
							'31-DEC-9999'  ,
							STORE_GROUP_VERSION_ID ,  
							DISPLAY_ORDER_NBR  ,
							PROTO_TYPE_CD , 
							PROTO_TYPE_DSC  ,
							PROTO_TYPE_SHORT_DSC,  
							STORE_TAG_NBR , 
							STORE_TAG_AMT , 
							LOYALTY_PROGRAM_TAG_IND , 
							STORE_TAG_DSC  ,
							POD_HEADLINE_TXT , 
							POD_Headline_Sub_Txt ,
							POD_OFFER_DSC , 
							POD_Offer_Detail_Cd, 
							POD_Offer_Detail_Dsc ,
							POD_Offer_Detail_Short_Dsc ,
							POD_PRICE_INFO_TXT  ,
							POD_Item_Qty, 
							POD_Unit_Of_Measure_Cd, 
							POD_Unit_Of_Measure_Nm ,
							Usage_Limit_Type_Txt ,
							POD_DISCLAIMER_TXT , 
							POD_DISPLAY_START_DT , 
							POD_DISPLAY_END_DT , 
							POD_CUSTOMER_FRIENDLY_CATEGORY_CD , 
							POD_CUSTOMER_FRIENDLY_CATEGORY_DSC , 
							POD_CUSTOMER_FRIENDLY_CATEGORY_SHORT_DSC , 
							INSTANT_WIN_PROGRAM_ID , 
							INSTANT_WIN_VERSION_ID , 
							INSTANT_WIN_PRIZE_ITEM_QTY , 
							INSTANT_WIN_FREQUENCY_DSC  ,
							CURRENT_TIMESTAMP,
						  DW_Logical_delete_ind,
						  FileName,
						  TRUE				
						  FROM	`+ tgt_wrk_tbl +`	
						WHERE	Sameday_chg_ind = 0
						AND		Offer_Request_Id is not null
						AND		USER_INTERFACE_UNIQUE_ID is not null`;
						
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
        return "Loading of Offer_Request_Offer_Specification table "+ tgt_tbl + " Failed with error: " + err;   // Return a error message.
        }
		var truncate_exceptions = `DELETE FROM ${tgt_exp_tbl};`;
		
		var sql_exceptions = `INSERT INTO ` + tgt_exp_tbl +`
		                                select Distinct
													OFFER_REQUEST_ID  ,
													USER_INTERFACE_UNIQUE_ID  ,
													STORE_GROUP_VERSION_ID  ,
													DISPLAY_ORDER_NBR ,
													PROTO_TYPE_CD ,
													PROTO_TYPE_DSC ,
													PROTO_TYPE_SHORT_DSC ,
													STORE_TAG_NBR ,
													STORE_TAG_AMT ,
													LOYALTY_PROGRAM_TAG_IND ,
													STORE_TAG_DSC ,
													POD_HEADLINE_TXT ,
													POD_Headline_Sub_Txt,
													POD_OFFER_DSC ,
													POD_Offer_Detail_Cd,
													POD_Offer_Detail_Dsc,
													POD_Offer_Detail_Short_Dsc,
													POD_PRICE_INFO_TXT ,
													POD_Item_Qty,
													POD_Unit_Of_Measure_Cd,
													POD_Unit_Of_Measure_Nm,
													Usage_Limit_Type_Txt,
													POD_DISCLAIMER_TXT ,
													POD_DISPLAY_START_DT ,
													POD_DISPLAY_END_DT ,
													POD_CUSTOMER_FRIENDLY_CATEGORY_CD ,
													POD_CUSTOMER_FRIENDLY_CATEGORY_DSC ,
													POD_CUSTOMER_FRIENDLY_CATEGORY_SHORT_DSC ,
													INSTANT_WIN_PROGRAM_ID ,
													INSTANT_WIN_VERSION_ID ,
													INSTANT_WIN_PRIZE_ITEM_QTY ,
													INSTANT_WIN_FREQUENCY_DSC ,	
													FileName ,
													DW_Logical_delete_ind,
												    DML_Type,
												    Sameday_chg_ind,
												    CASE 	WHEN Offer_Request_Id is NULL THEN 'Offer_Request_Id is NULL' 
															WHEN User_Interface_Unique_Id is NULL THEN 'User_Interface_Unique_Id is NULL'
														    ELSE NULL END AS Exception_Reason,
												    CURRENT_TIMESTAMP AS DW_CREATE_TS
												 FROM `+ tgt_wrk_tbl +` 
													 WHERE 		Offer_Request_Id is  null
														OR		User_Interface_Unique_Id is  null
													
													`;
	
    try {
	
		 snowflake.execute (
                     {sqlText: sql_begin }
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
		
	// **************	Load for Offer_Request_Offer_Specification table table ENDs *****************
	
	
	
$$;

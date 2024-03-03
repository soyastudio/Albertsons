--liquibase formatted sql
--changeset SYSTEM:sp_GetOfferRequest_To_BIM_Offer_Request_Discount_Tier runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETOFFERREQUEST_TO_BIM_OFFER_REQUEST_DISCOUNT_TIER(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, CNF_SCHEMA VARCHAR, WRK_SCHEMA VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
 
	 
    
    var cnf_db = CNF_DB ;
	var wrk_schema = WRK_SCHEMA ;
	var cnf_schema = CNF_SCHEMA;
	var src_wrk_tbl = SRC_WRK_TBL;
	
	// **************	Load for Offer_Request_Discount_Tier table BEGIN *****************
	// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
	var tgt_wrk_tbl = cnf_db + "." + wrk_schema + ".Offer_Request_Discount_Tier_wrk";
	var tgt_tbl = cnf_db +"."+ cnf_schema +".Offer_Request_Discount_Tier";
	var tgt_exp_tbl = cnf_db + "." + wrk_schema + ".Offer_Request_Discount_Tier_Exceptions";
	var sql_command = `CREATE OR REPLACE TABLE ` + tgt_wrk_tbl + ` as 
					    WITH src_wrk_tbl_recs as
						(	SELECT 	DISTINCT 		
											OfferRequestId as Offer_Request_Id
											,AttachedOfferTypeId as User_Interface_Unique_Id
											,DiscountVersion_DiscountVersionId as Discount_Version_Id
											,Discount_DiscountId as Discount_Id
											,TierLevelnbr as Tier_Level_Nbr
											,ProductGroup_ProductGroupId as Product_Group_Id 
											,DiscountAmt as Discount_Amt
											,LimitType_LimitQty as Limit_Qty
											,LimitType_LimitWt as Limit_Wt
											,LimitType_LimitVol as Limit_Vol
											,LimitType_UOMCd as Unit_Of_Measure_Cd
											,LimitType_UOMNm as Unit_Of_Measure_Nm
											,LimitType_LimitAmt as Limit_Amt
											,RewardQty as Reward_Qty
											,ReceiptTxt as Receipt_Txt
											,DiscountUptoQty as Discount_Up_to_Qty
											, creationdt
											, actiontypecd 
											, FileName
											, Row_number() OVER ( partition BY OfferRequestId, AttachedOfferTypeId, DiscountVersion_DiscountVersionId, Discount_DiscountId, TierLevelnbr, ProductGroup_ProductGroupId  ORDER BY To_timestamp_ntz(creationdt) DESC) AS rn
									FROM ` + src_wrk_tbl +`								
									
						)
						SELECT 	  src.Offer_Request_Id      ,
								src.User_Interface_Unique_Id  ,
								src.Discount_Version_Id   ,
								src.Discount_Id           ,
								src.Tier_Level_Nbr        ,
								src.Product_Group_Id      ,
								src.Discount_Amt          ,
								src.Limit_Qty             ,
								src.Limit_Wt              ,
								src.Limit_Vol             ,
								src.Unit_Of_Measure_Cd    ,
								src.Unit_Of_Measure_Nm    ,
								src.Limit_Amt             ,
								src.Reward_Qty            ,
								src.Receipt_Txt           ,
								src.Discount_Up_to_Qty  ,
								src.dw_logical_delete_ind ,
								src.FileName, 
								CASE WHEN (tgt.Offer_Request_Id IS NULL AND tgt.User_Interface_Unique_Id IS NULL AND tgt.Discount_Version_Id IS NULL AND tgt.Discount_Id IS NULL AND tgt.Tier_Level_Nbr IS NULL AND tgt.Product_Group_Id IS NULL ) THEN 'I' ELSE 'U' END AS DML_Type
								, CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1  ELSE 0 END AS Sameday_chg_ind 
								FROM   	(SELECT		 		Offer_Request_Id      ,
													User_Interface_Unique_Id  ,
													Discount_Version_Id   ,
													Discount_Id           ,
													Tier_Level_Nbr        ,
													Product_Group_Id      ,
													Discount_Amt          ,
													Limit_Qty             ,
													Limit_Wt              ,
													Limit_Vol             ,
													Unit_Of_Measure_Cd    ,
													Unit_Of_Measure_Nm    ,
													Limit_Amt             ,
													Reward_Qty            ,
													Receipt_Txt           ,
													Discount_Up_to_Qty  ,
													creationdt ,
													FALSE AS DW_Logical_delete_ind ,
													FileName 														
										 FROM src_wrk_tbl_recs
											WHERE  rn = 1											
											
											AND	UPPER(ActionTypeCd) <> 'DELETE'												
										) src
								LEFT JOIN 	(SELECT 			tgt.Offer_Request_Id      ,
														tgt.User_Interface_Unique_Id  ,
														tgt.Discount_Version_Id   ,
														tgt.Discount_Id           ,
														tgt.Tier_Level_Nbr        ,
														tgt.Product_Group_Id      ,
														tgt.Discount_Amt          ,
														tgt.Limit_Qty             ,
														tgt.Limit_Wt              ,
														tgt.Limit_Vol             ,
														tgt.Unit_Of_Measure_Cd    ,
														tgt.Unit_Of_Measure_Nm    ,
														tgt.Limit_Amt             ,
														tgt.Reward_Qty            ,
														tgt.Receipt_Txt           ,
														tgt.Discount_Up_to_Qty  ,
														tgt.dw_logical_delete_ind ,
														tgt.dw_first_effective_dt 											
											FROM ` + tgt_tbl + ` tgt
											WHERE DW_CURRENT_VERSION_IND = TRUE	
								) tgt 
								ON  tgt.Offer_Request_Id = src.Offer_Request_Id 
								AND tgt.User_Interface_Unique_Id = src.User_Interface_Unique_Id 
								AND tgt.Discount_Version_Id = src.Discount_Version_Id 
								AND tgt.Discount_Id = src.Discount_Id 
								AND tgt.Tier_Level_Nbr = src.Tier_Level_Nbr
								AND tgt.Product_Group_Id = src.Product_Group_Id   
							
						WHERE  (tgt.Offer_Request_Id IS NULL AND tgt.User_Interface_Unique_Id IS NULL AND tgt.Discount_Version_Id IS NULL AND tgt.Discount_Id IS NULL AND tgt.Tier_Level_Nbr IS NULL AND tgt.Product_Group_Id IS NULL )
						OR 		(
									  NVL(src.Discount_Amt,'-1') <> NVL(tgt.Discount_Amt,'-1')
									OR NVL(src.Limit_Qty,'-1') <> NVL(tgt.Limit_Qty,'-1')
									OR NVL(src.Limit_Wt,'-1') <> NVL(tgt.Limit_Wt,'-1')
									OR NVL(src.Limit_Vol,'-1') <> NVL(tgt.Limit_Vol,'-1')
									OR NVL(src.Unit_Of_Measure_Cd,'-1') <> NVL(tgt.Unit_Of_Measure_Cd,'-1')
									OR NVL(src.Unit_Of_Measure_Nm,'-1') <> NVL(tgt.Unit_Of_Measure_Nm,'-1')
									OR NVL(src.Limit_Amt,'-1') <> NVL(tgt.Limit_Amt,'-1')
									OR NVL(src.Reward_Qty,'-1') <> NVL(tgt.Reward_Qty,'-1')
									OR NVL(src.Receipt_Txt,'-1') <> NVL(tgt.Receipt_Txt,'-1')
									OR NVL(src.Discount_Up_to_Qty,'-1') <> NVL(tgt.Discount_Up_to_Qty,'-1')
									 OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
								)
						UNION ALL
							SELECT 		 tgt.Offer_Request_Id      ,
									tgt.User_Interface_Unique_Id  ,
									tgt.Discount_Version_Id   ,
									tgt.Discount_Id           ,
									tgt.Tier_Level_Nbr        ,
									tgt.Product_Group_Id      ,
									tgt.Discount_Amt          ,
									tgt.Limit_Qty             ,
									tgt.Limit_Wt              ,
									tgt.Limit_Vol             ,
									tgt.Unit_Of_Measure_Cd    ,
									tgt.Unit_Of_Measure_Nm    ,
									tgt.Limit_Amt             ,
									tgt.Reward_Qty            ,
									tgt.Receipt_Txt           ,
									tgt.Discount_Up_to_Qty  ,					
									TRUE AS DW_Logical_delete_ind ,
									--tgt.DW_SOURCE_CREATE_NM ,
									src.filename ,
									'U' as DML_Type , 
									CASE WHEN tgt.DW_First_Effective_dt = CURRENT_DATE THEN 1 Else 0 END as Sameday_chg_ind
						FROM	` + tgt_tbl + ` tgt
						inner join src_wrk_tbl_recs src on 
					src.Offer_Request_Id = tgt.Offer_Request_Id
					
							WHERE	DW_CURRENT_VERSION_IND = TRUE
							and rn = 1
							AND		upper(ActionTypeCd) = 'DELETE'
								AND		DW_LOGICAL_DELETE_IND = FALSE
								AND (tgt.Offer_Request_Id) IN 
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
        return "Creation of Offer_Request_Discount_Tier work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
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
											,Discount_Version_Id
											,Discount_Id
											,Tier_Level_Nbr
											,Product_Group_Id
											,FileName
									FROM 	`+ tgt_wrk_tbl +` 
										WHERE	DML_Type = 'U'
											AND	Sameday_chg_ind = 0
										AND		Offer_Request_Id is not null
										and		User_Interface_Unique_Id is not null
										and		Discount_Version_Id is not null
										and		Discount_Id is not null
										and		Tier_Level_Nbr is not null
										and		Product_Group_Id is not null
								) src
						WHERE	tgt.Offer_Request_Id = src.Offer_Request_Id 
							AND tgt.User_Interface_Unique_Id = src.User_Interface_Unique_Id 
							AND tgt.Discount_Version_Id = src.Discount_Version_Id
							AND tgt.Discount_Id = src.Discount_Id
							AND tgt.Tier_Level_Nbr = src.Tier_Level_Nbr
							AND tgt.Product_Group_Id = src.Product_Group_Id
							AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
							
    var sql_sameday = `// Processing Sameday updates
						UPDATE ` + tgt_tbl + ` as tgt
						SET		 Discount_Amt = src.Discount_Amt
								,Limit_Qty = src.Limit_Qty
								,Limit_Wt = src.Limit_Wt
								,Limit_Vol = src.Limit_Vol
								,Unit_Of_Measure_Cd = src.Unit_Of_Measure_Cd
								,Unit_Of_Measure_Nm = src.Unit_Of_Measure_Nm
								,Limit_Amt = src.Limit_Amt
								,Reward_Qty = src.Reward_Qty
								,Receipt_Txt = src.Receipt_Txt
								,Discount_Up_to_Qty = src.Discount_Up_to_Qty
								, DW_Logical_delete_ind = src.DW_Logical_delete_ind
								, DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
								, DW_SOURCE_UPDATE_NM = FileName
						FROM	(	SELECT 	  		 Offer_Request_Id      ,
											User_Interface_Unique_Id  ,
											Discount_Version_Id   ,
											Discount_Id           ,
											Tier_Level_Nbr        ,
											Product_Group_Id      ,
											Discount_Amt          ,
											Limit_Qty             ,
											Limit_Wt              ,
											Limit_Vol             ,
											Unit_Of_Measure_Cd    ,
											Unit_Of_Measure_Nm    ,
											Limit_Amt             ,
											Reward_Qty            ,
											Receipt_Txt           ,
											Discount_Up_to_Qty  ,
											DW_Logical_delete_ind ,
											FileName
									FROM 	`+ tgt_wrk_tbl +` 
									WHERE	DML_Type = 'U'
									AND		Sameday_chg_ind = 1
									AND		Offer_Request_Id is not null
									and		User_Interface_Unique_Id is not null
									and		Discount_Version_Id is not null
									and		Discount_Id is not null
									and		Tier_Level_Nbr is not null
									and		Product_Group_Id is not null
								) src
						WHERE	tgt.Offer_Request_Id = src.Offer_Request_Id 
							AND tgt.User_Interface_Unique_Id = src.User_Interface_Unique_Id 
							AND tgt.Discount_Version_Id = src.Discount_Version_Id
							AND tgt.Discount_Id = src.Discount_Id
							AND tgt.Tier_Level_Nbr = src.Tier_Level_Nbr
							AND tgt.Product_Group_Id = src.Product_Group_Id
							AND	tgt.DW_CURRENT_VERSION_IND = TRUE`;

						// Processing Inserts
	
	
	var sql_inserts = `INSERT INTO ` + tgt_tbl + `
						(	 Offer_Request_Id      ,
							User_Interface_Unique_Id  ,
							Discount_Version_Id   ,
							Discount_Id           ,
							Tier_Level_Nbr        ,
							Product_Group_Id      ,
							Discount_Amt          ,
							Limit_Qty             ,
							Limit_Wt              ,
							Limit_Vol             , 
							Unit_Of_Measure_Cd    ,
							Unit_Of_Measure_Nm    ,
							Limit_Amt             ,
							Reward_Qty            ,
							Receipt_Txt           ,
							Discount_Up_to_Qty  ,
							DW_First_Effective_Dt   ,  
							DW_Last_Effective_Dt     ,
							DW_CREATE_TS,
							DW_LOGICAL_DELETE_IND,
							DW_SOURCE_CREATE_NM,
							DW_CURRENT_VERSION_IND
						)
						SELECT 	 	 Offer_Request_Id      ,
								User_Interface_Unique_Id  ,
								Discount_Version_Id   ,
								Discount_Id           ,
								Tier_Level_Nbr        ,
								Product_Group_Id      ,
								Discount_Amt          ,
								Limit_Qty             ,
								Limit_Wt              ,
								Limit_Vol             ,
								Unit_Of_Measure_Cd    ,
								Unit_Of_Measure_Nm    ,
								Limit_Amt             ,
								Reward_Qty            ,
								Receipt_Txt           ,
								Discount_Up_to_Qty  ,
								CURRENT_DATE ,
								'31-DEC-9999' ,
								CURRENT_TIMESTAMP ,
								DW_Logical_delete_ind ,
								FileName ,
								TRUE
						FROM	`+ tgt_wrk_tbl +`	
						WHERE	Sameday_chg_ind = 0
						AND		Offer_Request_Id is not null
						and		User_Interface_Unique_Id is not null
						and		Discount_Version_Id is not null
						and		Discount_Id is not null
						and		Tier_Level_Nbr is not null 
						and		Product_Group_Id is not null `;
						
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
        return "Loading of Offer_Request_Discount_Tier table "+ tgt_tbl + " Failed with error: " + err;   // Return a error message.
        }
		var truncate_exceptions =`DELETE FROM ${tgt_exp_tbl}`;

		var sql_exceptions = `INSERT INTO ` + tgt_exp_tbl	+`  /*EDM_CONFIRMED_PRD.DW_C_STAGE.Offer_Request_Discount_Tier_Exceptions */
		                               select 	   OFFER_REQUEST_ID ,
													USER_INTERFACE_UNIQUE_ID ,
													DISCOUNT_VERSION_ID ,
													DISCOUNT_ID ,
													Product_Group_Id ,
													TIER_LEVEL_NBR ,
													DISCOUNT_AMT ,
													LIMIT_QTY ,
													LIMIT_WT ,
													LIMIT_VOL ,
													UNIT_OF_MEASURE_CD ,
													UNIT_OF_MEASURE_NM ,
													LIMIT_AMT ,
													REWARD_QTY ,
													RECEIPT_TXT ,
													DISCOUNT_UP_TO_QTY ,
													FileName ,
													DW_Logical_delete_ind,
												    	DML_Type,
												    	Sameday_chg_ind ,
												    	CASE 	WHEN Offer_Request_Id is NULL THEN 'Offer_Request_Id is NULL' 
															WHEN User_Interface_Unique_Id is NULL THEN 'User_Interface_Unique_Id is NULL'
															WHEN Discount_Version_Id is NULL THEN 'Discount_Version_Id is NULL'
															WHEN Discount_Id is NULL THEN 'Discount_Id is NULL'
															WHEN Tier_Level_Nbr is NULL THEN 'Tier_Level_Nbr is NULL'
															WHEN Product_Group_Id is NULL THEN 'Product_Group_Id is NULL'
															ELSE NULL END AS Exception_Reason
												    ,CURRENT_TIMESTAMP AS DW_CREATE_TS
												 FROM `+ tgt_wrk_tbl +` 
													 WHERE 		Offer_Request_Id is  null
														OR		User_Interface_Unique_Id is  null
														OR		Discount_Version_Id is  null
														OR		Discount_Id is  null
														OR		Tier_Level_Nbr is  null
														OR		Product_Group_Id is  null`;
												
     try 
{
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
	 snowflake.execute (
{sqlText: sql_rollback  }
);
        return "Insert into tgt Exception table "+ tgt_exp_tbl +" Failed with error: " + err;   // Return a error message.
        }
		
	// **************	Load for Offer_Request_Discount_Tier table ENDs *****************
	
	
$$;

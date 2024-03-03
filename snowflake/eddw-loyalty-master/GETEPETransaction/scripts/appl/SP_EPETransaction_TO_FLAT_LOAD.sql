--liquibase formatted sql
--changeset SYSTEM:SP_EPETransaction_TO_FLAT_LOAD runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME_R>>;
use schema <<EDM_DB_NAME_R>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME_R>>.DW_APPL.SP_EPETRANSACTION_TO_FLAT_LOAD()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
	
    // Global Variable Declaration
    var wrk_schema = "DW_R_STAGE";
    var ref_db = "<<EDM_DB_NAME_R>>";
    var ref_schema = "DW_R_RETAILSALE";
	var appl_schema = " DW_APPL"
    var src_tbl = ref_db + "." + appl_schema + ".ESED_EPETransaction_R_STREAM";
    var src_wrk_tmp_tbl = ref_db + "." + wrk_schema + ".ESED_EPETransaction_wrktmp";
    var src_wrk_tbl = ref_db + "." + wrk_schema + ".ESED_EPETransaction_wrk";
	var src_rerun_tbl = ref_db + "." + wrk_schema + ".ESED_EPETransaction_Rerun";
    var tgt_flat_tbl = ref_db + "." + ref_schema + ".EPETransaction_Flat";
	

	// persist stream data in work table for the current transaction, includes data from previous failed run
	var sql_crt_src_wrk_tbl = `create or replace transient table `+ src_wrk_tmp_tbl +`  as
                                select * from `+ src_tbl +`  WHERE METADATA$ACTION = 'INSERT'   
                                UNION ALL 
                                select * from `+ src_rerun_tbl;
    try {
        snowflake.execute (
            {sqlText: sql_crt_src_wrk_tbl  }
            );
        }
    catch (err)  {
        throw "Creation of Source Work table "+ src_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }
	
	// Empty the rerun queue table
	var sql_empty_rerun_tbl = `TRUNCATE TABLE `+ src_rerun_tbl +` `;
	try {
        snowflake.execute ({sqlText: sql_empty_rerun_tbl });
       }
     catch (err) { 
      throw "Truncation of rerun queue table "+ src_rerun_tbl +" Failed with error: " + err;   // Return a error message.
     }
	
	// query to load rerun queue table when encountered a failure
    var sql_ins_rerun_tbl = `CREATE or REPLACE transient table   `+ src_rerun_tbl+` as SELECT * FROM `+ src_wrk_tmp_tbl +``; 
    
var sql_ins_src_wrk_tbl =   `create or replace transient table `+ src_wrk_tbl +` as
with LVL_1_FLATTEN as
			(select 
			tbl.filename as filename
			,tbl.src_json as src_json
			,EPE.value as value
			,EPE.seq as seq
            
			from `+ src_wrk_tmp_tbl +` tbl
			,LATERAL FLATTEN(tbl.SRC_JSON) EPE           
            
			)
			select *
			from
			(select distinct filename,epe.seq,
			  epe.src_json:items::array as Items
             ,epe.src_json:memberId::string as Transaction_MemberId
             ,epe.src_json:redemption::array as EPE_Redemption
             ,epe.src_json:status::string as Transaction_Status
             ,epe.src_json:storeNumber::string  as StoreNumber
             ,epe.src_json:storeTimeZone::string as StoreTimeZone
             ,epe.src_json:terminalNumber::string as terminalNumber
             ,epe.src_json:totalCardSavings::string as totalCardSavings
             ,epe.src_json:transactionNumber::string as transactionNumber
             ,epe.src_json:transactionSource::string as transactionSource
			 ,epe.src_json:fulfillmentStoreNumber::string as fulfillmentStoreNumber
             ,epe.src_json:transactionTimestamp::string as transactionTimestamp
             ,epe.src_json:updatedDate::string as updatedDate
             ,epe.src_json:transactionTotal::string as transactionTotal
             ,epe.src_json:txnLevelSavings::array as txnLevelSavings
			 ,epe.src_json:cardSavings::array as CardSavings
             ,epe.src_json:usageLimit::array as usageLimit
			 ,epe.src_json:printing::array as Printing
             ,epe.src_json:messages::array as Messages
			 ,epe.src_json:epeErrors::string as epeErrors
			 ,epe.src_json:notifications::array as notifications
			 ,epe.src_json:scoreCard::array as scoreCard
			 ,epe.src_json:points::array as points
			 ,epe.src_json:_class::String as Txn_Class
             ,epe.src_json:createdDate::string as createdDate
			 ,epe.src_json:transactionUniqueID::string as transactionUniqueID
            // ,epe.src_json:usageLimit::string as usageLimit
			 from lvl_1_flatten epe) epe;` ; 

    try {
            snowflake.execute ({sqlText: sql_ins_src_wrk_tbl  } );
        }
    catch (err)  { 

            throw "Loading of table in src temp table Failed with error: " + err;   // Return a error message.
            }
            
  var insert_into_flat_dml =`INSERT INTO `+ tgt_flat_tbl +`
                     select distinct filename ,
 Transaction_MemberId  ,
 Transaction_Status    ,
 StoreNumber     ,
 StoreTimeZone   ,
 terminalNumber ,  
 cardSavings_savingsCategoryId ,
 cardSavings_savingsCategoryName ,
 cardSavings_savingsAmount ,
 totalCardSavings ,
 transactionNumber ,
 transactionTimestamp ,
 updatedDate ,
 transactionTotal ,
 Items_ClippedOffers ,
 Items_ClubCardSaving ,
 Items_Department ,
 Items_DiscountAllowed ,
 Items_EntryId ,
 Items_ExtendedPrice ,
 Items_ExternalSavings ,
 Items_ItemCode ,
 Items_NetPromotionAmount ,
 Items_PointsApplyItem ,
 Items_QuantityType ,
 Items_QuantityValue ,
 Items_Savings ,
 Items_UnitPrice ,
 Items_WIC ,
 Items_DepartmentGroupNumber ,
 Items_pricePer ,
 Items_pricePerbasePrice ,
 Items_price ,
 Items_sellByWeight ,
 Items_averageWeight ,
 Items_basePricePer ,
 Items_LinkPluNumber ,
 Items_ItemPluNumber ,
 Items_Strartdate ,
 Items_EndDate ,
 Items_Qty ,
 RedemptionAmount ,
 RedemptionCount ,
 RedemptionofferId ,
 TxnLevel_offerId     ,
 TxnLevel_calculateUsage ,
 TxnLevel_description ,
 TxnLevel_endDate  ,
 TxnLevel_externalOfferId  ,
 TxnLevel_Points  ,
 TxnLevel_programCode ,
 TxnLevel_source  ,
 TxnLevel_startDate ,
 TxnLevel_categoryName ,
 TxnLevel_discountMessage ,
 TxnLevel_discountAmount ,
 TxnLevel_discountType ,
 TxnLevel_discountLevel ,
 TxnLevel_linkPluNumber ,
 TxnLevel_netPromotionAmount ,
 TxnLevel_categoryId ,
 TxnLevel_minPurchaseQuantity ,
 TxnLevel_discountQty ,
 TxnLevel_usageCount ,
 offersAdded ,
 offersRemoved , 
 Printing_MessageNumber ,
 Printing_PrintLine ,
 Points_programCode ,
 Points_earns ,
 Points_burns ,
 Messages , 
 epeErrors ,
 notifications , 
 scoreCard , 
 Txn_Class ,
 createdDate ,
 transactionSource,
 Items_arPrice,
 Items_arSavings,
 current_timestamp() as DW_CREATE_TS,
 //usageLimit,
 Items_epeErrors,
txnlevelsavings_nondigital,
transactionUniqueID,
TXNLEVEL_clipIds,
TxnLevel_promoCode,
fulfillmentStoreNumber,
TxnLevel_ProgramType 
			from
			(select distinct filename,epe.seq
			  ,Transaction_MemberId
             ,Transaction_Status
             ,StoreNumber
             ,StoreTimeZone
             ,terminalNumber
             ,totalCardSavings
             ,transactionNumber
             ,transactionSource
			 ,fulfillmentStoreNumber
             ,transactionTimestamp
             ,updatedDate
             ,transactionTotal
             ,createdDate
             ,CardSavings
             ,Txn_Class
             ,Messages
			 ,epeErrors
			 ,notifications
			 ,scoreCard
			 ,Points
			 ,transactionUniqueID
            // ,usageLimit			
			from `+ src_wrk_tbl +` as epe
           )epe
            left join
    ( 
    select distinct Items.value:clippedOffers::array as Items_ClippedOffers
             ,Items.value:clubCardSaving::String as Items_ClubCardSaving
             ,Items.value:department::String as Items_Department
             ,Items.value:discountAllowed::String as Items_DiscountAllowed
             ,Items.value:entryId::String as Items_EntryId
             ,Items.value:extendedPrice::String as Items_ExtendedPrice
             ,Items.value:externalSavings::array as Items_ExternalSavings
             ,Items.value:itemCode::String as Items_ItemCode
             ,Items.value:netPromotionAmount::String as Items_NetPromotionAmount
             ,Items.value:pointsApplyItem::String as Items_PointsApplyItem
             ,Items.value:quantityType::String as Items_QuantityType
			 ,Items.value:departmentGroupNumber::String as Items_DepartmentGroupNumber
             ,Items.value:quantityValue::String as Items_QuantityValue
             ,Items.value:savings::array as Items_Savings
             ,Items.value:unitPrice::String as Items_UnitPrice
             ,Items.value:wic::String as Items_WIC
			 ,Items.value:linkPluNumber::String as Items_LinkPluNumber
			 ,Items.value:itemPluNumber::String as Items_ItemPluNumber
			 ,Items.value:startDate::String as Items_Strartdate
			 ,Items.Value:endDate::String as Items_EndDate
			 ,Items.Value:qty::String as Items_Qty
			 ,Items.Value:pricePer::String as Items_pricePer
			 ,Items.Value:pricePerbasePrice::String as Items_pricePerbasePrice
			 ,Items.Value:price::String as Items_price
			 ,Items.Value:sellByWeight::String as Items_sellByWeight
			 ,Items.Value:averageWeight::String as Items_averageWeight
			 ,Items.Value:basePricePer::String as Items_basePricePer
		     ,Items.Value:epeErrors::array as Items_epeErrors
             ,Items.Value:arPrice::String as Items_arPrice
             ,Items.Value:arSavings::String as Items_arSavings
			 ,e.seq
     from `+ src_wrk_tbl +` e
    ,LATERAL FLATTEN(input => ITEMS)  Items   
    ) Items on Items.seq = epe.seq 
    
    left join
    (Select Redemption.value:amount::String as RedemptionAmount
             ,Redemption.value:count::String as RedemptionCount
             ,Redemption.value:offerId::String as RedemptionofferId
             ,e.seq
   
     from `+ src_wrk_tbl +` e
    ,LATERAL FLATTEN(input => EPE_Redemption)  Redemption 
    )Redemption on Redemption.seq=epe.seq
    
    left join
    (
     Select
    TxnLevel.value:calculateUsage::string as TxnLevel_calculateUsage
	,TxnLevel.value:categoryName::string as TxnLevel_categoryName
	,TxnLevel.value:discountMessage::string as TxnLevel_discountMessage
	,TxnLevel.value:discountAmount::string as TxnLevel_discountAmount
	,TxnLevel.value:discountType::string as TxnLevel_discountType
	,TxnLevel.value:discountLevel::string as TxnLevel_discountLevel
	,TxnLevel.value:linkPluNumber::string as TxnLevel_linkPluNumber
	,TxnLevel.value:netPromotionAmount::string as TxnLevel_netPromotionAmount
	,TxnLevel.value:categoryId::string as TxnLevel_categoryId
	,TxnLevel.value:minPurchaseQuantity::string as TxnLevel_minPurchaseQuantity
	,TxnLevel.value:discountQty::string as TxnLevel_discountQty
	,TxnLevel.value:usageCount::string as TxnLevel_usageCount
    ,TxnLevel.value:description::string as TxnLevel_description
    ,TxnLevel.value:endDate::String as TxnLevel_endDate
    ,TxnLevel.value:externalOfferId::string as TxnLevel_externalOfferId
    ,TxnLevel.value:offerId::string as TxnLevel_offerId
    ,TxnLevel.value:points::array as TxnLevel_Points
    ,TxnLevel.value:programCode::string as TxnLevel_programCode
    ,TxnLevel.value:source::string as TxnLevel_source
    ,TxnLevel.value:startDate::String as TxnLevel_startDate
	,TxnLevel.value:nonDigital::String as txnlevelsavings_nondigital 
    ,TxnLevel.value:clipIds::String as TXNLEVEL_clipIds 
	,TxnLevel.value:promoCode::String as TxnLevel_promoCode
	,TxnLevel.value:programType::String as TxnLevel_ProgramType
    ,e.seq
      from `+ src_wrk_tbl +` e
    ,LATERAL FLATTEN(input => txnLevelSavings) TxnLevel 
     )TxnLevel on TxnLevel.seq=epe.seq
     Left join
     (
     Select   e.seq,usageLimit.value:offersAdded::array as offersAdded
             ,usageLimit.value:offersRemoved::array as offersRemoved
     From 
     `+ src_wrk_tbl +` e
    ,LATERAL FLATTEN(input => usageLimit) usageLimit 
    )usageLimit on usageLimit.seq=epe.seq
     Left join
     (
     Select   e.seq,Printing.value:messageNumber::String as Printing_MessageNumber
             ,Printing.value:printLine::String as Printing_PrintLine
     From 
     `+ src_wrk_tbl +` e
    ,LATERAL FLATTEN(input => Printing) Printing 
    )Printing on Printing.seq=epe.seq
       Left join
     (
     Select   e.seq,points.value:programCode::String as Points_programCode
             ,points.value:earns::array as Points_earns
			 ,points.value:burns::String as Points_burns
     From 
     `+ src_wrk_tbl +` e
    ,LATERAL FLATTEN(input => points) points 
    )points on points.seq=epe.seq
	Left join
     (
     Select   e.seq
	         ,cardSavings.value:savingsCategoryId::String as cardSavings_savingsCategoryId
             ,cardSavings.value:savingsCategoryName::String as cardSavings_savingsCategoryName
			 ,cardSavings.value:savingsAmount::String as cardSavings_savingsAmount
     From 
     `+ src_wrk_tbl +` e
    ,LATERAL FLATTEN(input => cardSavings) cardSavings 
    )cardSavings on cardSavings.seq=epe.seq
    ;`  ;
    
	try {
            snowflake.execute (
            {sqlText: insert_into_flat_dml  }
            );
        }
    catch (err)  { 
		    snowflake.execute ( 
						{sqlText: sql_ins_rerun_tbl }
						);
            throw "Loading of table "+ tgt_flat_tbl +" Failed with error: " + err;   // Return a error message.
        }

$$;

--liquibase formatted sql
--changeset SYSTEM:SP_GETPARTNERREWARDRECONCILIATION_TO_FLAT_LOAD runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME_R>>;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE "SP_GETPARTNERREWARDRECONCILIATION_TO_FLAT_LOAD"()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$

    var cur_db = snowflake.execute( {sqlText: `Select current_database()`} ); 
	cur_db.next(); 
	var env = cur_db.getColumnValue(1); 
	env = env.split('_'); 
	env = env[env.length - 1]; 
	var env_tbl_nm = `EDM_Environment_Variable_${env}`; 
	var env_schema_nm = 'DW_R_MASTERDATA'; 
	var env_db_nm = `EDM_REFINED_${env}`; 

	try { 
    var rs = snowflake.execute( {sqlText: `SELECT * FROM ${env_db_nm}.${env_schema_nm}.${env_tbl_nm}`} ); 
    var metaparams = {};
    while (rs.next()){
      metaparams[rs.getColumnValue(1)] = rs.getColumnValue(2);
    }
    var ref_db = metaparams['REF_DB']; 
    var ref_schema = metaparams['R_LOYAL']; 
    var app_schema = metaparams['APPL']; 
    var wrk_schema = metaparams['R_STAGE'];

	} catch (err) { 
    throw `Error while fetching data from EDM_Environment_Variable_${env}`; 
	}
    var variant_nm = 'ESED_PartnerRewardReconciliation';
	var bod_nm = 'GetPartnerRewardReconciliation';

    var src_tbl = `${ref_db}.${app_schema}.${variant_nm}_R_STREAM`;
    var src_wrk_tbl = `${ref_db}.${wrk_schema}.${variant_nm}_wrk`;
    var src_rerun_tbl = `${ref_db}.${wrk_schema}.${variant_nm}_Rerun`;
    var tgt_flat_tbl = `${ref_db}.${ref_schema}.${bod_nm}_FLAT`;
	
	
	// persist stream data in work table for the current transaction, includes data from previous failed run
	var sql_crt_src_wrk_tbl = `create or replace table ${src_wrk_tbl} as
                            select * from ${src_tbl} 
                            UNION ALL 
                            select * from ${src_rerun_tbl}`;
    try {
        snowflake.execute ({ sqlText: sql_crt_src_wrk_tbl });
    } catch (err)  {
        throw `Creation of Source Work table ${src_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
    }
	
	// Empty the rerun queue table
	var sql_empty_rerun_tbl = `TRUNCATE TABLE ${src_rerun_tbl}`;
	
    try {
        snowflake.execute ({sqlText: sql_empty_rerun_tbl });
    } catch (err) { 
        throw `Truncation of rerun queue table ${src_rerun_tbl} Failed with error: ${err}`;   // Return a error message.
    }
    
	// query to load rerun queue table when encountered a failure
	var sql_ins_rerun_tbl = `CREATE OR REPLACE TABLE ${src_rerun_tbl} DATA_RETENTION_TIME_IN_DAYS = 0 AS 
							            SELECT * FROM ${src_wrk_tbl}`;
							
	var insert_into_flat_dml =`INSERT INTO ${tgt_flat_tbl}
    WITH LEVEL_1_FLATTEN AS (	
    SELECT tbl.SRC_XML:"@"::string AS BODNm
    ,tbl.FILENAME AS FILENAME
    ,GetPartnerRewardReconciliation.value as Value
    ,GetPartnerRewardReconciliation.SEQ::integer as SEQ
    ,GetPartnerRewardReconciliation.index::integer as idx
    FROM ${src_wrk_tbl} tbl
    ,LATERAL FLATTEN(tbl.SRC_XML:"$") GetPartnerRewardReconciliation
    )
    SELECT FILENAME
          ,BODNm
          ,DocumentID
		,ExpectedMessageCnt
		,MessageSequenceNbr
		,ExternalTargetInd
		,InterchangeTime
		,InterchangeDate
		,InternalFileTransferInd
		,ReceiverId
		,SenderId
		,RoutingSystemNm
		,Note
		,TargetApplicationCd
		,SourceApplicationCd
		,Document_Description
		,CreationDt
		,DocumentNm
		,InboundOutboundInd
		,AlternateDocumentID
		,GatewayNm
		,PIIdataInd
		,PCIdataInd
		,PHIdataInd
		,BusinessSensitivityLevel_Description
		,BusinessSensitivityLevel_Code
		,BusinessSensitivityLevel_ShortDescription
		,DataClassificationLevel_Code
		,DataClassificationLevel_Description
		,DataClassificationLevel_ShortDescription
		,ActionTypeCd
		,RecordTypeCd
		,PartnerParticipantId
		,PartnerId
		,PostalZoneCd
		,PartnerSiteId
		,UpdateDtTm
		,CreateUserId
		,CreateTs
		,CreateDtTm
		,UpdateTs
		,UpdateUserId
		,TotalPurchQty
		,FuelPumpId
		,RegisterId
		,ReconMsgId
		,PurchDiscLimitQty
		,AccountingUnitId
		,AccountId
		,DiscountAmt_CurrencyCd
		,DiscountAmt_DecimalNbr
		,DiscountAmt_TransactionAmt
		,DiscountAmt_CurrencyExchangeRt
		,FuelGradeCd_ShortDescription
		,FuelGradeCd_Description
		,FuelGradeCd_Code
		,NonFuelPurchAmt_TransactionAmt
		,NonFuelPurchAmt_DecimalNbr
		,NonFuelPurchAmt_CurrencyCd
		,NonFuelPurchAmt_CurrencyExchangeRt
		,PromoPriceAmt_CurrencyExchangeRt
		,PromoPriceAmt_CurrencyCd
		,PromoPriceAmt_DecimalNbr
		,PromoPriceAmt_TransactionAmt
		,PurchDiscLimitAmt_CurrencyExchangeRt
		,PurchDiscLimitAmt_TransactionAmt
		,PurchDiscLimitAmt_CurrencyCd
		,PurchDiscLimitAmt_DecimalNbr
		,PurchUOMCd_Code
		,PurchUOMCd_ShortDescription
		,PurchUOMCd_Description
		,ReconErrorTypeCd_Code
		,ReconErrorTypeCd_ShortDescription
		,ReconErrorTypeCd_Description
		,RegularPriceAmt_TransactionAmt
		,RegularPriceAmt_DecimalNbr
		,RegularPriceAmt_CurrencyCd
		,RegularPriceAmt_CurrencyExchangeRt
		,SettlementAmt_CurrencyExchangeRt
		,SettlementAmt_CurrencyCd
		,SettlementAmt_DecimalNbr
		,SettlementAmt_TransactionAmt
		,TenderTypeCd_ShortDescription
		,TenderTypeCd_Code
		,TenderTypeCd_Description
		,TotalFuelPurchAmt_TransactionAmt
		,TotalFuelPurchAmt_DecimalNbr
		,TotalFuelPurchAmt_CurrencyCd
		,TotalFuelPurchAmt_CurrencyExchangeRt
		,TotalPurchaseAmt_CurrencyExchangeRt
		,TotalPurchaseAmt_TransactionAmt
		,TotalPurchaseAmt_CurrencyCd
		,TotalPurchaseAmt_DecimalNbr
		,TotalSavingValAmt_DecimalNbr
		,TotalSavingValAmt_CurrencyCd
		,TotalSavingValAmt_CurrencyExchangeRt
		,TotalSavingValAmt_TransactionAmt
		,TxnFeeAmt_TransactionAmt
		,TxnFeeAmt_DecimalNbr
		,TxnFeeAmt_CurrencyCd
		,TxnFeeAmt_CurrencyExchangeRt
		,TxnNetPymtAmt_CurrencyCd
		,TxnNetPymtAmt_DecimalNbr
		,TxnNetPymtAmt_CurrencyExchangeRt
		,TxnNetPymtAmt_TransactionAmt
		,EffectiveDtTm
		,StatusType_Description
		,StatusCd
		,ReferenceNbr
		,TransactionTs
		,TransactionId
		,AltTransactionId
		,AltTransactionTs
		,AltTransactionType_Description
		,AltTransactionType_Code
		,AltTransactionType_ShortDescription
		,TransactionTypeCd_ShortDescription
		,TransactionTypeCd_Description
		,TransactionTypeCd_Code
		,Document_ReleaseId
		,Document_VersionId
		,Document_SystemEnvironmentCd
		,StatusCd_Type
          ,CURRENT_TIMESTAMP AS DW_CreateTs
    FROM
    (
	SELECT 
		FILENAME
		,BODnm
		,GET(DocumentData.value, '@ReleaseId')::string AS Document_ReleaseId
		,GET(DocumentData.value, '@VersionId')::string AS Document_VersionId
		,GET(DocumentData.value, '@SystemEnvironmentCd')::string AS Document_SystemEnvironmentCd
		,XMLGET(DocumentData.value, 'Abs:DocumentID'):"$"::string AS DocumentID
		,XMLGET(DocumentData.value, 'Abs:ExpectedMessageCnt'):"$"::string AS ExpectedMessageCnt
		,XMLGET(DocumentData.value, 'Abs:MessageSequenceNbr'):"$"::string AS MessageSequenceNbr
		,XMLGET(DocumentData.value, 'Abs:ExternalTargetInd'):"$"::string AS ExternalTargetInd
		,XMLGET(DocumentData.value, 'Abs:InterchangeTime'):"$"::string AS InterchangeTime
		,XMLGET(DocumentData.value, 'Abs:InterchangeDate'):"$"::string AS InterchangeDate
		,XMLGET(DocumentData.value, 'Abs:InternalFileTransferInd'):"$"::string AS InternalFileTransferInd
		,XMLGET(DocumentData.value, 'Abs:ReceiverId'):"$"::string AS ReceiverId
		,XMLGET(DocumentData.value, 'Abs:SenderId'):"$"::string AS SenderId
		,XMLGET(DocumentData.value, 'Abs:RoutingSystemNm'):"$"::string AS RoutingSystemNm
		,XMLGET(DocumentData.value, 'Abs:Note'):"$"::string AS Note
		,XMLGET(DocumentData.value, 'Abs:TargetApplicationCd'):"$"::string AS TargetApplicationCd
		,XMLGET(DocumentData.value, 'Abs:SourceApplicationCd'):"$"::string AS SourceApplicationCd
		,XMLGET(DocumentData.value, 'Abs:Description'):"$"::string AS Document_Description
		,XMLGET(DocumentData.value, 'Abs:CreationDt'):"$"::string AS CreationDt
		,XMLGET(DocumentData.value, 'Abs:DocumentNm'):"$"::string AS DocumentNm
		,XMLGET(DocumentData.value, 'Abs:InboundOutboundInd'):"$"::string AS InboundOutboundInd
		,XMLGET(DocumentData.value, 'Abs:AlternateDocumentID'):"$"::string AS AlternateDocumentID
		,XMLGET(DocumentData.value, 'Abs:GatewayNm'):"$"::string AS GatewayNm
		,GetPartnerRewardReconciliation.SEQ::integer as SEQ
		,GetPartnerRewardReconciliation.idx::integer as idx
		,DocumentData.SEQ::integer as SEQ1
		,DocumentData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetPartnerRewardReconciliation
		,LATERAL FLATTEN(TO_ARRAY(GetPartnerRewardReconciliation.value:"$")) DocumentData
	WHERE	GetPartnerRewardReconciliation.value like '<DocumentData>%'
	AND	(DocumentData.value like '<Document>%' OR DocumentData.value like '<Document%ReleaseId%' OR DocumentData.value like '<Document%VersionId%' OR DocumentData.value like '<Document%SystemEnvironmentCd%' )
) Document
LEFT JOIN
(
	SELECT
		XMLGET(Document.value, 'Abs:PIIdataInd'):"$"::string AS PIIdataInd
		,XMLGET(Document.value, 'Abs:PCIdataInd'):"$"::string AS PCIdataInd
		,XMLGET(Document.value, 'Abs:PHIdataInd'):"$"::string AS PHIdataInd
		,GetPartnerRewardReconciliation.SEQ::integer as SEQ
		,GetPartnerRewardReconciliation.idx::integer as idx
		,DocumentData.SEQ::integer as SEQ1
		,DocumentData.index::integer as idx1
		,Document.SEQ::integer as SEQ2
		,Document.index::integer as idx2
	FROM	LEVEL_1_FLATTEN AS GetPartnerRewardReconciliation
		,LATERAL FLATTEN(TO_ARRAY(GetPartnerRewardReconciliation.value:"$")) DocumentData
		,LATERAL FLATTEN(TO_ARRAY(DocumentData.value:"$")) Document
	WHERE	GetPartnerRewardReconciliation.value like '<DocumentData>%'
	AND	(DocumentData.value like '<Document>%' OR DocumentData.value like '<Document%ReleaseId%' OR DocumentData.value like '<Document%VersionId%' OR DocumentData.value like '<Document%SystemEnvironmentCd%' )
	AND	(Document.value like '<Abs:DataClassification>%' )
) DataClassification on DataClassification.SEQ = Document.SEQ AND DataClassification.idx = Document.idx
	AND	DataClassification.SEQ1 = Document.SEQ1 AND DataClassification.idx1 = Document.idx1
LEFT JOIN
(
	SELECT
		XMLGET(DataClassification.value, 'Abs:Description'):"$"::string AS BusinessSensitivityLevel_Description
		,XMLGET(DataClassification.value, 'Abs:Code'):"$"::string AS BusinessSensitivityLevel_Code
		,XMLGET(DataClassification.value, 'Abs:ShortDescription'):"$"::string AS BusinessSensitivityLevel_ShortDescription
		,GetPartnerRewardReconciliation.SEQ::integer as SEQ
		,GetPartnerRewardReconciliation.idx::integer as idx
		,DocumentData.SEQ::integer as SEQ1
		,DocumentData.index::integer as idx1
		,Document.SEQ::integer as SEQ2
		,Document.index::integer as idx2
	FROM	LEVEL_1_FLATTEN AS GetPartnerRewardReconciliation
		,LATERAL FLATTEN(TO_ARRAY(GetPartnerRewardReconciliation.value:"$")) DocumentData
		,LATERAL FLATTEN(TO_ARRAY(DocumentData.value:"$")) Document
		,LATERAL FLATTEN(TO_ARRAY(Document.value:"$")) DataClassification
	WHERE	GetPartnerRewardReconciliation.value like '<DocumentData>%'
	AND	(DocumentData.value like '<Document>%' OR DocumentData.value like '<Document%ReleaseId%' OR DocumentData.value like '<Document%VersionId%' OR DocumentData.value like '<Document%SystemEnvironmentCd%' )
	AND	(Document.value like '<Abs:DataClassification>%' )
	AND	(DataClassification.value like '<Abs:BusinessSensitivityLevel>%' )
) BusinessSensitivityLevel on BusinessSensitivityLevel.SEQ = DataClassification.SEQ AND BusinessSensitivityLevel.idx = DataClassification.idx
	AND	BusinessSensitivityLevel.SEQ1 = DataClassification.SEQ1 AND BusinessSensitivityLevel.idx1 = DataClassification.idx1
	AND	BusinessSensitivityLevel.SEQ2 = DataClassification.SEQ2 AND BusinessSensitivityLevel.idx2 = DataClassification.idx2
LEFT JOIN
(
	SELECT
		XMLGET(DataClassification.value, 'Abs:Code'):"$"::string AS DataClassificationLevel_Code
		,XMLGET(DataClassification.value, 'Abs:Description'):"$"::string AS DataClassificationLevel_Description
		,XMLGET(DataClassification.value, 'Abs:ShortDescription'):"$"::string AS DataClassificationLevel_ShortDescription
		,GetPartnerRewardReconciliation.SEQ::integer as SEQ
		,GetPartnerRewardReconciliation.idx::integer as idx
		,DocumentData.SEQ::integer as SEQ1
		,DocumentData.index::integer as idx1
		,Document.SEQ::integer as SEQ2
		,Document.index::integer as idx2
	FROM	LEVEL_1_FLATTEN AS GetPartnerRewardReconciliation
		,LATERAL FLATTEN(TO_ARRAY(GetPartnerRewardReconciliation.value:"$")) DocumentData
		,LATERAL FLATTEN(TO_ARRAY(DocumentData.value:"$")) Document
		,LATERAL FLATTEN(TO_ARRAY(Document.value:"$")) DataClassification
	WHERE	GetPartnerRewardReconciliation.value like '<DocumentData>%'
	AND	(DocumentData.value like '<Document>%' OR DocumentData.value like '<Document%ReleaseId%' OR DocumentData.value like '<Document%VersionId%' OR DocumentData.value like '<Document%SystemEnvironmentCd%' )
	AND	(Document.value like '<Abs:DataClassification>%' )
	AND	(DataClassification.value like '<Abs:DataClassificationLevel>%' )
) DataClassificationLevel on DataClassificationLevel.SEQ = DataClassification.SEQ AND DataClassificationLevel.idx = DataClassification.idx
	AND	DataClassificationLevel.SEQ1 = DataClassification.SEQ1 AND DataClassificationLevel.idx1 = DataClassification.idx1
	AND	DataClassificationLevel.SEQ2 = DataClassification.SEQ2 AND DataClassificationLevel.idx2 = DataClassification.idx2
LEFT JOIN
(
	SELECT
		XMLGET(DocumentData.value, 'Abs:ActionTypeCd'):"$"::string AS ActionTypeCd
		,XMLGET(DocumentData.value, 'Abs:RecordTypeCd'):"$"::string AS RecordTypeCd
		,GetPartnerRewardReconciliation.SEQ::integer as SEQ
		,GetPartnerRewardReconciliation.idx::integer as idx
	FROM	LEVEL_1_FLATTEN AS GetPartnerRewardReconciliation
		,LATERAL FLATTEN(TO_ARRAY(GetPartnerRewardReconciliation.value:"$")) DocumentData
	WHERE	GetPartnerRewardReconciliation.value like '<DocumentData>%'
	AND	(DocumentData.value like '<DocumentAction>%' )
) DocumentAction on DocumentAction.SEQ = Document.SEQ AND DocumentAction.idx = Document.idx
LEFT JOIN
(
	SELECT
		XMLGET(GetPartnerRewardReconciliation.value, 'Abs:PartnerParticipantId'):"$"::string AS PartnerParticipantId
		,XMLGET(GetPartnerRewardReconciliation.value, 'Abs:PartnerId'):"$"::string AS PartnerId
		,XMLGET(GetPartnerRewardReconciliation.value, 'Abs:PostalZoneCd'):"$"::string AS PostalZoneCd
		,XMLGET(GetPartnerRewardReconciliation.value, 'Abs:PartnerSiteId'):"$"::string AS PartnerSiteId
		,GetPartnerRewardReconciliation.SEQ::integer as SEQ
		,GetPartnerRewardReconciliation.idx::integer as idx
	FROM	LEVEL_1_FLATTEN AS GetPartnerRewardReconciliation
	WHERE	GetPartnerRewardReconciliation.value like '<PartnerRewardReconciliationData>%'
) PartnerRewardReconciliationData on PartnerRewardReconciliationData.SEQ = Document.SEQ
LEFT JOIN
(
	SELECT
		XMLGET(PartnerRewardReconciliationData.value, 'Abs:UpdateDtTm'):"$"::string AS UpdateDtTm
		,XMLGET(PartnerRewardReconciliationData.value, 'Abs:CreateUserId'):"$"::string AS CreateUserId
		,XMLGET(PartnerRewardReconciliationData.value, 'Abs:CreateTs'):"$"::string AS CreateTs
		,XMLGET(PartnerRewardReconciliationData.value, 'Abs:CreateDtTm'):"$"::string AS CreateDtTm
		,XMLGET(PartnerRewardReconciliationData.value, 'Abs:UpdateTs'):"$"::string AS UpdateTs
		,XMLGET(PartnerRewardReconciliationData.value, 'Abs:UpdateUserId'):"$"::string AS UpdateUserId
		,GetPartnerRewardReconciliation.SEQ::integer as SEQ
		,GetPartnerRewardReconciliation.idx::integer as idx
	FROM	LEVEL_1_FLATTEN AS GetPartnerRewardReconciliation
		,LATERAL FLATTEN(TO_ARRAY(GetPartnerRewardReconciliation.value:"$")) PartnerRewardReconciliationData
	WHERE	GetPartnerRewardReconciliation.value like '<PartnerRewardReconciliationData>%'
	AND	(PartnerRewardReconciliationData.value like '<Abs:AuditData>%' )
) AuditData on AuditData.SEQ = PartnerRewardReconciliationData.SEQ AND AuditData.idx = PartnerRewardReconciliationData.idx
LEFT JOIN
(
	SELECT
		XMLGET(PartnerRewardReconciliationData.value, 'Abs:TotalPurchQty'):"$"::string AS TotalPurchQty
		,XMLGET(PartnerRewardReconciliationData.value, 'Abs:FuelPumpId'):"$"::string AS FuelPumpId
		,XMLGET(PartnerRewardReconciliationData.value, 'Abs:RegisterId'):"$"::string AS RegisterId
		,XMLGET(PartnerRewardReconciliationData.value, 'Abs:ReconMsgId'):"$"::string AS ReconMsgId
		,XMLGET(PartnerRewardReconciliationData.value, 'Abs:PurchDiscLimitQty'):"$"::string AS PurchDiscLimitQty
		,GetPartnerRewardReconciliation.SEQ::integer as SEQ
		,GetPartnerRewardReconciliation.idx::integer as idx
		,PartnerRewardReconciliationData.SEQ::integer as SEQ1
		,PartnerRewardReconciliationData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetPartnerRewardReconciliation
		,LATERAL FLATTEN(TO_ARRAY(GetPartnerRewardReconciliation.value:"$")) PartnerRewardReconciliationData
	WHERE	GetPartnerRewardReconciliation.value like '<PartnerRewardReconciliationData>%'
	AND	(PartnerRewardReconciliationData.value like '<Abs:RewardReconciliation>%' )
) RewardReconciliation on RewardReconciliation.SEQ = PartnerRewardReconciliationData.SEQ AND RewardReconciliation.idx = PartnerRewardReconciliationData.idx
LEFT JOIN
(
	SELECT
		XMLGET(RewardReconciliation.value, 'Abs:AccountingUnitId'):"$"::string AS AccountingUnitId
		,XMLGET(RewardReconciliation.value, 'Abs:AccountId'):"$"::string AS AccountId
		,GetPartnerRewardReconciliation.SEQ::integer as SEQ
		,GetPartnerRewardReconciliation.idx::integer as idx
		,PartnerRewardReconciliationData.SEQ::integer as SEQ1
		,PartnerRewardReconciliationData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetPartnerRewardReconciliation
		,LATERAL FLATTEN(TO_ARRAY(GetPartnerRewardReconciliation.value:"$")) PartnerRewardReconciliationData
		,LATERAL FLATTEN(TO_ARRAY(PartnerRewardReconciliationData.value:"$")) RewardReconciliation
	WHERE	GetPartnerRewardReconciliation.value like '<PartnerRewardReconciliationData>%'
	AND	(PartnerRewardReconciliationData.value like '<Abs:RewardReconciliation>%' )
	AND	(RewardReconciliation.value like '<Abs:AccountData>%' )
) AccountData on AccountData.SEQ = RewardReconciliation.SEQ AND AccountData.idx = RewardReconciliation.idx
	AND	AccountData.SEQ1 = RewardReconciliation.SEQ1 AND AccountData.idx1 = RewardReconciliation.idx1
LEFT JOIN
(
	SELECT
		XMLGET(RewardReconciliation.value, 'Abs:CurrencyCd'):"$"::string AS DiscountAmt_CurrencyCd
		,XMLGET(RewardReconciliation.value, 'Abs:DecimalNbr'):"$"::string AS DiscountAmt_DecimalNbr
		,XMLGET(RewardReconciliation.value, 'Abs:TransactionAmt'):"$"::string AS DiscountAmt_TransactionAmt
		,XMLGET(RewardReconciliation.value, 'Abs:CurrencyExchangeRt'):"$"::string AS DiscountAmt_CurrencyExchangeRt
		,GetPartnerRewardReconciliation.SEQ::integer as SEQ
		,GetPartnerRewardReconciliation.idx::integer as idx
		,PartnerRewardReconciliationData.SEQ::integer as SEQ1
		,PartnerRewardReconciliationData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetPartnerRewardReconciliation
		,LATERAL FLATTEN(TO_ARRAY(GetPartnerRewardReconciliation.value:"$")) PartnerRewardReconciliationData
		,LATERAL FLATTEN(TO_ARRAY(PartnerRewardReconciliationData.value:"$")) RewardReconciliation
	WHERE	GetPartnerRewardReconciliation.value like '<PartnerRewardReconciliationData>%'
	AND	(PartnerRewardReconciliationData.value like '<Abs:RewardReconciliation>%' )
	AND	(RewardReconciliation.value like '<Abs:DiscountAmt>%' )
) DiscountAmt on DiscountAmt.SEQ = RewardReconciliation.SEQ AND DiscountAmt.idx = RewardReconciliation.idx
	AND	DiscountAmt.SEQ1 = RewardReconciliation.SEQ1 AND DiscountAmt.idx1 = RewardReconciliation.idx1
LEFT JOIN
(
	SELECT
		XMLGET(RewardReconciliation.value, 'Abs:ShortDescription'):"$"::string AS FuelGradeCd_ShortDescription
		,XMLGET(RewardReconciliation.value, 'Abs:Description'):"$"::string AS FuelGradeCd_Description
		,XMLGET(RewardReconciliation.value, 'Abs:Code'):"$"::string AS FuelGradeCd_Code
		,GetPartnerRewardReconciliation.SEQ::integer as SEQ
		,GetPartnerRewardReconciliation.idx::integer as idx
		,PartnerRewardReconciliationData.SEQ::integer as SEQ1
		,PartnerRewardReconciliationData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetPartnerRewardReconciliation
		,LATERAL FLATTEN(TO_ARRAY(GetPartnerRewardReconciliation.value:"$")) PartnerRewardReconciliationData
		,LATERAL FLATTEN(TO_ARRAY(PartnerRewardReconciliationData.value:"$")) RewardReconciliation
	WHERE	GetPartnerRewardReconciliation.value like '<PartnerRewardReconciliationData>%'
	AND	(PartnerRewardReconciliationData.value like '<Abs:RewardReconciliation>%' )
	AND	(RewardReconciliation.value like '<Abs:FuelGradeCd>%' )
) FuelGradeCd on FuelGradeCd.SEQ = RewardReconciliation.SEQ AND FuelGradeCd.idx = RewardReconciliation.idx
	AND	FuelGradeCd.SEQ1 = RewardReconciliation.SEQ1 AND FuelGradeCd.idx1 = RewardReconciliation.idx1
LEFT JOIN
(
	SELECT
		XMLGET(RewardReconciliation.value, 'Abs:TransactionAmt'):"$"::string AS NonFuelPurchAmt_TransactionAmt
		,XMLGET(RewardReconciliation.value, 'Abs:DecimalNbr'):"$"::string AS NonFuelPurchAmt_DecimalNbr
		,XMLGET(RewardReconciliation.value, 'Abs:CurrencyCd'):"$"::string AS NonFuelPurchAmt_CurrencyCd
		,XMLGET(RewardReconciliation.value, 'Abs:CurrencyExchangeRt'):"$"::string AS NonFuelPurchAmt_CurrencyExchangeRt
		,GetPartnerRewardReconciliation.SEQ::integer as SEQ
		,GetPartnerRewardReconciliation.idx::integer as idx
		,PartnerRewardReconciliationData.SEQ::integer as SEQ1
		,PartnerRewardReconciliationData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetPartnerRewardReconciliation
		,LATERAL FLATTEN(TO_ARRAY(GetPartnerRewardReconciliation.value:"$")) PartnerRewardReconciliationData
		,LATERAL FLATTEN(TO_ARRAY(PartnerRewardReconciliationData.value:"$")) RewardReconciliation
	WHERE	GetPartnerRewardReconciliation.value like '<PartnerRewardReconciliationData>%'
	AND	(PartnerRewardReconciliationData.value like '<Abs:RewardReconciliation>%' )
	AND	(RewardReconciliation.value like '<Abs:NonFuelPurchAmt>%' )
) NonFuelPurchAmt on NonFuelPurchAmt.SEQ = RewardReconciliation.SEQ AND NonFuelPurchAmt.idx = RewardReconciliation.idx
	AND	NonFuelPurchAmt.SEQ1 = RewardReconciliation.SEQ1 AND NonFuelPurchAmt.idx1 = RewardReconciliation.idx1
LEFT JOIN
(
	SELECT
		XMLGET(RewardReconciliation.value, 'Abs:CurrencyExchangeRt'):"$"::string AS PromoPriceAmt_CurrencyExchangeRt
		,XMLGET(RewardReconciliation.value, 'Abs:CurrencyCd'):"$"::string AS PromoPriceAmt_CurrencyCd
		,XMLGET(RewardReconciliation.value, 'Abs:DecimalNbr'):"$"::string AS PromoPriceAmt_DecimalNbr
		,XMLGET(RewardReconciliation.value, 'Abs:TransactionAmt'):"$"::string AS PromoPriceAmt_TransactionAmt
		,GetPartnerRewardReconciliation.SEQ::integer as SEQ
		,GetPartnerRewardReconciliation.idx::integer as idx
		,PartnerRewardReconciliationData.SEQ::integer as SEQ1
		,PartnerRewardReconciliationData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetPartnerRewardReconciliation
		,LATERAL FLATTEN(TO_ARRAY(GetPartnerRewardReconciliation.value:"$")) PartnerRewardReconciliationData
		,LATERAL FLATTEN(TO_ARRAY(PartnerRewardReconciliationData.value:"$")) RewardReconciliation
	WHERE	GetPartnerRewardReconciliation.value like '<PartnerRewardReconciliationData>%'
	AND	(PartnerRewardReconciliationData.value like '<Abs:RewardReconciliation>%' )
	AND	(RewardReconciliation.value like '<Abs:PromoPriceAmt>%' )
) PromoPriceAmt on PromoPriceAmt.SEQ = RewardReconciliation.SEQ AND PromoPriceAmt.idx = RewardReconciliation.idx
	AND	PromoPriceAmt.SEQ1 = RewardReconciliation.SEQ1 AND PromoPriceAmt.idx1 = RewardReconciliation.idx1
LEFT JOIN
(
	SELECT
		XMLGET(RewardReconciliation.value, 'Abs:CurrencyExchangeRt'):"$"::string AS PurchDiscLimitAmt_CurrencyExchangeRt
		,XMLGET(RewardReconciliation.value, 'Abs:TransactionAmt'):"$"::string AS PurchDiscLimitAmt_TransactionAmt
		,XMLGET(RewardReconciliation.value, 'Abs:CurrencyCd'):"$"::string AS PurchDiscLimitAmt_CurrencyCd
		,XMLGET(RewardReconciliation.value, 'Abs:DecimalNbr'):"$"::string AS PurchDiscLimitAmt_DecimalNbr
		,GetPartnerRewardReconciliation.SEQ::integer as SEQ
		,GetPartnerRewardReconciliation.idx::integer as idx
		,PartnerRewardReconciliationData.SEQ::integer as SEQ1
		,PartnerRewardReconciliationData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetPartnerRewardReconciliation
		,LATERAL FLATTEN(TO_ARRAY(GetPartnerRewardReconciliation.value:"$")) PartnerRewardReconciliationData
		,LATERAL FLATTEN(TO_ARRAY(PartnerRewardReconciliationData.value:"$")) RewardReconciliation
	WHERE	GetPartnerRewardReconciliation.value like '<PartnerRewardReconciliationData>%'
	AND	(PartnerRewardReconciliationData.value like '<Abs:RewardReconciliation>%' )
	AND	(RewardReconciliation.value like '<Abs:PurchDiscLimitAmt>%' )
) PurchDiscLimitAmt on PurchDiscLimitAmt.SEQ = RewardReconciliation.SEQ AND PurchDiscLimitAmt.idx = RewardReconciliation.idx
	AND	PurchDiscLimitAmt.SEQ1 = RewardReconciliation.SEQ1 AND PurchDiscLimitAmt.idx1 = RewardReconciliation.idx1
LEFT JOIN
(
	SELECT
		XMLGET(RewardReconciliation.value, 'Abs:Code'):"$"::string AS PurchUOMCd_Code
		,XMLGET(RewardReconciliation.value, 'Abs:ShortDescription'):"$"::string AS PurchUOMCd_ShortDescription
		,XMLGET(RewardReconciliation.value, 'Abs:Description'):"$"::string AS PurchUOMCd_Description
		,GetPartnerRewardReconciliation.SEQ::integer as SEQ
		,GetPartnerRewardReconciliation.idx::integer as idx
		,PartnerRewardReconciliationData.SEQ::integer as SEQ1
		,PartnerRewardReconciliationData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetPartnerRewardReconciliation
		,LATERAL FLATTEN(TO_ARRAY(GetPartnerRewardReconciliation.value:"$")) PartnerRewardReconciliationData
		,LATERAL FLATTEN(TO_ARRAY(PartnerRewardReconciliationData.value:"$")) RewardReconciliation
	WHERE	GetPartnerRewardReconciliation.value like '<PartnerRewardReconciliationData>%'
	AND	(PartnerRewardReconciliationData.value like '<Abs:RewardReconciliation>%' )
	AND	(RewardReconciliation.value like '<Abs:PurchUOMCd>%' )
) PurchUOMCd on PurchUOMCd.SEQ = RewardReconciliation.SEQ AND PurchUOMCd.idx = RewardReconciliation.idx
	AND	PurchUOMCd.SEQ1 = RewardReconciliation.SEQ1 AND PurchUOMCd.idx1 = RewardReconciliation.idx1
LEFT JOIN
(
	SELECT
		XMLGET(RewardReconciliation.value, 'Abs:Code'):"$"::array AS ReconErrorTypeCd_Code
		,XMLGET(RewardReconciliation.value, 'Abs:ShortDescription'):"$"::string AS ReconErrorTypeCd_ShortDescription
		,XMLGET(RewardReconciliation.value, 'Abs:Description'):"$"::string AS ReconErrorTypeCd_Description
		,GetPartnerRewardReconciliation.SEQ::integer as SEQ
		,GetPartnerRewardReconciliation.idx::integer as idx
		,PartnerRewardReconciliationData.SEQ::integer as SEQ1
		,PartnerRewardReconciliationData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetPartnerRewardReconciliation
		,LATERAL FLATTEN(TO_ARRAY(GetPartnerRewardReconciliation.value:"$")) PartnerRewardReconciliationData
		,LATERAL FLATTEN(TO_ARRAY(PartnerRewardReconciliationData.value:"$")) RewardReconciliation
	WHERE	GetPartnerRewardReconciliation.value like '<PartnerRewardReconciliationData>%'
	AND	(PartnerRewardReconciliationData.value like '<Abs:RewardReconciliation>%' )
	AND	(RewardReconciliation.value like '<Abs:ReconErrorTypeCd>%' )
) ReconErrorTypeCd on ReconErrorTypeCd.SEQ = RewardReconciliation.SEQ AND ReconErrorTypeCd.idx = RewardReconciliation.idx
	AND	ReconErrorTypeCd.SEQ1 = RewardReconciliation.SEQ1 AND ReconErrorTypeCd.idx1 = RewardReconciliation.idx1
LEFT JOIN
(
	SELECT
		XMLGET(RewardReconciliation.value, 'Abs:TransactionAmt'):"$"::string AS RegularPriceAmt_TransactionAmt
		,XMLGET(RewardReconciliation.value, 'Abs:DecimalNbr'):"$"::string AS RegularPriceAmt_DecimalNbr
		,XMLGET(RewardReconciliation.value, 'Abs:CurrencyCd'):"$"::string AS RegularPriceAmt_CurrencyCd
		,XMLGET(RewardReconciliation.value, 'Abs:CurrencyExchangeRt'):"$"::string AS RegularPriceAmt_CurrencyExchangeRt
		,GetPartnerRewardReconciliation.SEQ::integer as SEQ
		,GetPartnerRewardReconciliation.idx::integer as idx
		,PartnerRewardReconciliationData.SEQ::integer as SEQ1
		,PartnerRewardReconciliationData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetPartnerRewardReconciliation
		,LATERAL FLATTEN(TO_ARRAY(GetPartnerRewardReconciliation.value:"$")) PartnerRewardReconciliationData
		,LATERAL FLATTEN(TO_ARRAY(PartnerRewardReconciliationData.value:"$")) RewardReconciliation
	WHERE	GetPartnerRewardReconciliation.value like '<PartnerRewardReconciliationData>%'
	AND	(PartnerRewardReconciliationData.value like '<Abs:RewardReconciliation>%' )
	AND	(RewardReconciliation.value like '<Abs:RegularPriceAmt>%' )
) RegularPriceAmt on RegularPriceAmt.SEQ = RewardReconciliation.SEQ AND RegularPriceAmt.idx = RewardReconciliation.idx
	AND	RegularPriceAmt.SEQ1 = RewardReconciliation.SEQ1 AND RegularPriceAmt.idx1 = RewardReconciliation.idx1
LEFT JOIN
(
	SELECT
		XMLGET(RewardReconciliation.value, 'Abs:CurrencyExchangeRt'):"$"::string AS SettlementAmt_CurrencyExchangeRt
		,XMLGET(RewardReconciliation.value, 'Abs:CurrencyCd'):"$"::string AS SettlementAmt_CurrencyCd
		,XMLGET(RewardReconciliation.value, 'Abs:DecimalNbr'):"$"::string AS SettlementAmt_DecimalNbr
		,XMLGET(RewardReconciliation.value, 'Abs:TransactionAmt'):"$"::string AS SettlementAmt_TransactionAmt
		,GetPartnerRewardReconciliation.SEQ::integer as SEQ
		,GetPartnerRewardReconciliation.idx::integer as idx
		,PartnerRewardReconciliationData.SEQ::integer as SEQ1
		,PartnerRewardReconciliationData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetPartnerRewardReconciliation
		,LATERAL FLATTEN(TO_ARRAY(GetPartnerRewardReconciliation.value:"$")) PartnerRewardReconciliationData
		,LATERAL FLATTEN(TO_ARRAY(PartnerRewardReconciliationData.value:"$")) RewardReconciliation
	WHERE	GetPartnerRewardReconciliation.value like '<PartnerRewardReconciliationData>%'
	AND	(PartnerRewardReconciliationData.value like '<Abs:RewardReconciliation>%' )
	AND	(RewardReconciliation.value like '<Abs:SettlementAmt>%' )
) SettlementAmt on SettlementAmt.SEQ = RewardReconciliation.SEQ AND SettlementAmt.idx = RewardReconciliation.idx
	AND	SettlementAmt.SEQ1 = RewardReconciliation.SEQ1 AND SettlementAmt.idx1 = RewardReconciliation.idx1
LEFT JOIN
(
	SELECT
		XMLGET(RewardReconciliation.value, 'Abs:ShortDescription'):"$"::string AS TenderTypeCd_ShortDescription
		,XMLGET(RewardReconciliation.value, 'Abs:Code'):"$"::string AS TenderTypeCd_Code
		,XMLGET(RewardReconciliation.value, 'Abs:Description'):"$"::string AS TenderTypeCd_Description
		,GetPartnerRewardReconciliation.SEQ::integer as SEQ
		,GetPartnerRewardReconciliation.idx::integer as idx
		,PartnerRewardReconciliationData.SEQ::integer as SEQ1
		,PartnerRewardReconciliationData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetPartnerRewardReconciliation
		,LATERAL FLATTEN(TO_ARRAY(GetPartnerRewardReconciliation.value:"$")) PartnerRewardReconciliationData
		,LATERAL FLATTEN(TO_ARRAY(PartnerRewardReconciliationData.value:"$")) RewardReconciliation
	WHERE	GetPartnerRewardReconciliation.value like '<PartnerRewardReconciliationData>%'
	AND	(PartnerRewardReconciliationData.value like '<Abs:RewardReconciliation>%' )
	AND	(RewardReconciliation.value like '<Abs:TenderTypeCd>%' )
) TenderTypeCd on TenderTypeCd.SEQ = RewardReconciliation.SEQ AND TenderTypeCd.idx = RewardReconciliation.idx
	AND	TenderTypeCd.SEQ1 = RewardReconciliation.SEQ1 AND TenderTypeCd.idx1 = RewardReconciliation.idx1
LEFT JOIN
(
	SELECT
		XMLGET(RewardReconciliation.value, 'Abs:TransactionAmt'):"$"::string AS TotalFuelPurchAmt_TransactionAmt
		,XMLGET(RewardReconciliation.value, 'Abs:DecimalNbr'):"$"::string AS TotalFuelPurchAmt_DecimalNbr
		,XMLGET(RewardReconciliation.value, 'Abs:CurrencyCd'):"$"::string AS TotalFuelPurchAmt_CurrencyCd
		,XMLGET(RewardReconciliation.value, 'Abs:CurrencyExchangeRt'):"$"::string AS TotalFuelPurchAmt_CurrencyExchangeRt
		,GetPartnerRewardReconciliation.SEQ::integer as SEQ
		,GetPartnerRewardReconciliation.idx::integer as idx
		,PartnerRewardReconciliationData.SEQ::integer as SEQ1
		,PartnerRewardReconciliationData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetPartnerRewardReconciliation
		,LATERAL FLATTEN(TO_ARRAY(GetPartnerRewardReconciliation.value:"$")) PartnerRewardReconciliationData
		,LATERAL FLATTEN(TO_ARRAY(PartnerRewardReconciliationData.value:"$")) RewardReconciliation
	WHERE	GetPartnerRewardReconciliation.value like '<PartnerRewardReconciliationData>%'
	AND	(PartnerRewardReconciliationData.value like '<Abs:RewardReconciliation>%' )
	AND	(RewardReconciliation.value like '<Abs:TotalFuelPurchAmt>%' )
) TotalFuelPurchAmt on TotalFuelPurchAmt.SEQ = RewardReconciliation.SEQ AND TotalFuelPurchAmt.idx = RewardReconciliation.idx
	AND	TotalFuelPurchAmt.SEQ1 = RewardReconciliation.SEQ1 AND TotalFuelPurchAmt.idx1 = RewardReconciliation.idx1
LEFT JOIN
(
	SELECT
		XMLGET(RewardReconciliation.value, 'Abs:CurrencyExchangeRt'):"$"::string AS TotalPurchaseAmt_CurrencyExchangeRt
		,XMLGET(RewardReconciliation.value, 'Abs:TransactionAmt'):"$"::string AS TotalPurchaseAmt_TransactionAmt
		,XMLGET(RewardReconciliation.value, 'Abs:CurrencyCd'):"$"::string AS TotalPurchaseAmt_CurrencyCd
		,XMLGET(RewardReconciliation.value, 'Abs:DecimalNbr'):"$"::string AS TotalPurchaseAmt_DecimalNbr
		,GetPartnerRewardReconciliation.SEQ::integer as SEQ
		,GetPartnerRewardReconciliation.idx::integer as idx
		,PartnerRewardReconciliationData.SEQ::integer as SEQ1
		,PartnerRewardReconciliationData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetPartnerRewardReconciliation
		,LATERAL FLATTEN(TO_ARRAY(GetPartnerRewardReconciliation.value:"$")) PartnerRewardReconciliationData
		,LATERAL FLATTEN(TO_ARRAY(PartnerRewardReconciliationData.value:"$")) RewardReconciliation
	WHERE	GetPartnerRewardReconciliation.value like '<PartnerRewardReconciliationData>%'
	AND	(PartnerRewardReconciliationData.value like '<Abs:RewardReconciliation>%' )
	AND	(RewardReconciliation.value like '<Abs:TotalPurchaseAmt>%' )
) TotalPurchaseAmt on TotalPurchaseAmt.SEQ = RewardReconciliation.SEQ AND TotalPurchaseAmt.idx = RewardReconciliation.idx
	AND	TotalPurchaseAmt.SEQ1 = RewardReconciliation.SEQ1 AND TotalPurchaseAmt.idx1 = RewardReconciliation.idx1
LEFT JOIN
(
	SELECT
		XMLGET(RewardReconciliation.value, 'Abs:DecimalNbr'):"$"::string AS TotalSavingValAmt_DecimalNbr
		,XMLGET(RewardReconciliation.value, 'Abs:CurrencyCd'):"$"::string AS TotalSavingValAmt_CurrencyCd
		,XMLGET(RewardReconciliation.value, 'Abs:CurrencyExchangeRt'):"$"::string AS TotalSavingValAmt_CurrencyExchangeRt
		,XMLGET(RewardReconciliation.value, 'Abs:TransactionAmt'):"$"::string AS TotalSavingValAmt_TransactionAmt
		,GetPartnerRewardReconciliation.SEQ::integer as SEQ
		,GetPartnerRewardReconciliation.idx::integer as idx
		,PartnerRewardReconciliationData.SEQ::integer as SEQ1
		,PartnerRewardReconciliationData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetPartnerRewardReconciliation
		,LATERAL FLATTEN(TO_ARRAY(GetPartnerRewardReconciliation.value:"$")) PartnerRewardReconciliationData
		,LATERAL FLATTEN(TO_ARRAY(PartnerRewardReconciliationData.value:"$")) RewardReconciliation
	WHERE	GetPartnerRewardReconciliation.value like '<PartnerRewardReconciliationData>%'
	AND	(PartnerRewardReconciliationData.value like '<Abs:RewardReconciliation>%' )
	AND	(RewardReconciliation.value like '<Abs:TotalSavingValAmt>%' )
) TotalSavingValAmt on TotalSavingValAmt.SEQ = RewardReconciliation.SEQ AND TotalSavingValAmt.idx = RewardReconciliation.idx
	AND	TotalSavingValAmt.SEQ1 = RewardReconciliation.SEQ1 AND TotalSavingValAmt.idx1 = RewardReconciliation.idx1
LEFT JOIN
(
	SELECT
		XMLGET(RewardReconciliation.value, 'Abs:TransactionAmt'):"$"::string AS TxnFeeAmt_TransactionAmt
		,XMLGET(RewardReconciliation.value, 'Abs:DecimalNbr'):"$"::string AS TxnFeeAmt_DecimalNbr
		,XMLGET(RewardReconciliation.value, 'Abs:CurrencyCd'):"$"::string AS TxnFeeAmt_CurrencyCd
		,XMLGET(RewardReconciliation.value, 'Abs:CurrencyExchangeRt'):"$"::string AS TxnFeeAmt_CurrencyExchangeRt
		,GetPartnerRewardReconciliation.SEQ::integer as SEQ
		,GetPartnerRewardReconciliation.idx::integer as idx
		,PartnerRewardReconciliationData.SEQ::integer as SEQ1
		,PartnerRewardReconciliationData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetPartnerRewardReconciliation
		,LATERAL FLATTEN(TO_ARRAY(GetPartnerRewardReconciliation.value:"$")) PartnerRewardReconciliationData
		,LATERAL FLATTEN(TO_ARRAY(PartnerRewardReconciliationData.value:"$")) RewardReconciliation
	WHERE	GetPartnerRewardReconciliation.value like '<PartnerRewardReconciliationData>%'
	AND	(PartnerRewardReconciliationData.value like '<Abs:RewardReconciliation>%' )
	AND	(RewardReconciliation.value like '<Abs:TxnFeeAmt>%' )
) TxnFeeAmt on TxnFeeAmt.SEQ = RewardReconciliation.SEQ AND TxnFeeAmt.idx = RewardReconciliation.idx
	AND	TxnFeeAmt.SEQ1 = RewardReconciliation.SEQ1 AND TxnFeeAmt.idx1 = RewardReconciliation.idx1
LEFT JOIN
(
	SELECT
		XMLGET(RewardReconciliation.value, 'Abs:CurrencyCd'):"$"::string AS TxnNetPymtAmt_CurrencyCd
		,XMLGET(RewardReconciliation.value, 'Abs:DecimalNbr'):"$"::string AS TxnNetPymtAmt_DecimalNbr
		,XMLGET(RewardReconciliation.value, 'Abs:CurrencyExchangeRt'):"$"::string AS TxnNetPymtAmt_CurrencyExchangeRt
		,XMLGET(RewardReconciliation.value, 'Abs:TransactionAmt'):"$"::string AS TxnNetPymtAmt_TransactionAmt
		,GetPartnerRewardReconciliation.SEQ::integer as SEQ
		,GetPartnerRewardReconciliation.idx::integer as idx
		,PartnerRewardReconciliationData.SEQ::integer as SEQ1
		,PartnerRewardReconciliationData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetPartnerRewardReconciliation
		,LATERAL FLATTEN(TO_ARRAY(GetPartnerRewardReconciliation.value:"$")) PartnerRewardReconciliationData
		,LATERAL FLATTEN(TO_ARRAY(PartnerRewardReconciliationData.value:"$")) RewardReconciliation
	WHERE	GetPartnerRewardReconciliation.value like '<PartnerRewardReconciliationData>%'
	AND	(PartnerRewardReconciliationData.value like '<Abs:RewardReconciliation>%' )
	AND	(RewardReconciliation.value like '<Abs:TxnNetPymtAmt>%' )
) TxnNetPymtAmt on TxnNetPymtAmt.SEQ = RewardReconciliation.SEQ AND TxnNetPymtAmt.idx = RewardReconciliation.idx
	AND	TxnNetPymtAmt.SEQ1 = RewardReconciliation.SEQ1 AND TxnNetPymtAmt.idx1 = RewardReconciliation.idx1
LEFT JOIN
(
	SELECT
		XMLGET(PartnerRewardReconciliationData.value, 'Abs:EffectiveDtTm'):"$"::string AS EffectiveDtTm
		,XMLGET(PartnerRewardReconciliationData.value, 'Abs:Description'):"$"::string AS StatusType_Description
		,GET(XMLGET(PartnerRewardReconciliationData.value, 'Abs:StatusCd'), '@Type')::string AS StatusCd_Type
		,XMLGET(PartnerRewardReconciliationData.value, 'Abs:StatusCd'):"$"::string AS StatusCd
		,GetPartnerRewardReconciliation.SEQ::integer as SEQ
		,GetPartnerRewardReconciliation.idx::integer as idx
	FROM	LEVEL_1_FLATTEN AS GetPartnerRewardReconciliation
		,LATERAL FLATTEN(TO_ARRAY(GetPartnerRewardReconciliation.value:"$")) PartnerRewardReconciliationData
	WHERE	GetPartnerRewardReconciliation.value like '<PartnerRewardReconciliationData>%'
	AND	(PartnerRewardReconciliationData.value like '<Abs:StatusType>%' )
) StatusType on StatusType.SEQ = PartnerRewardReconciliationData.SEQ AND StatusType.idx = PartnerRewardReconciliationData.idx
LEFT JOIN
(
	SELECT
		XMLGET(PartnerRewardReconciliationData.value, 'Abs:ReferenceNbr'):"$"::string AS ReferenceNbr
		,XMLGET(PartnerRewardReconciliationData.value, 'Abs:TransactionTs'):"$"::string AS TransactionTs
		,XMLGET(PartnerRewardReconciliationData.value, 'Abs:TransactionId'):"$"::string AS TransactionId
		,GetPartnerRewardReconciliation.SEQ::integer as SEQ
		,GetPartnerRewardReconciliation.idx::integer as idx
		,PartnerRewardReconciliationData.SEQ::integer as SEQ1
		,PartnerRewardReconciliationData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetPartnerRewardReconciliation
		,LATERAL FLATTEN(TO_ARRAY(GetPartnerRewardReconciliation.value:"$")) PartnerRewardReconciliationData
	WHERE	GetPartnerRewardReconciliation.value like '<PartnerRewardReconciliationData>%'
	AND	(PartnerRewardReconciliationData.value like '<Abs:TransactionType>%' )
) TransactionType on TransactionType.SEQ = PartnerRewardReconciliationData.SEQ AND TransactionType.idx = PartnerRewardReconciliationData.idx
LEFT JOIN
(
	SELECT
		XMLGET(TransactionType.value, 'Abs:AltTransactionId'):"$"::string AS AltTransactionId
		,XMLGET(TransactionType.value, 'Abs:AltTransactionTs'):"$"::string AS AltTransactionTs
		,GetPartnerRewardReconciliation.SEQ::integer as SEQ
		,GetPartnerRewardReconciliation.idx::integer as idx
		,PartnerRewardReconciliationData.SEQ::integer as SEQ1
		,PartnerRewardReconciliationData.index::integer as idx1
		,TransactionType.SEQ::integer as SEQ2
		,TransactionType.index::integer as idx2
	FROM	LEVEL_1_FLATTEN AS GetPartnerRewardReconciliation
		,LATERAL FLATTEN(TO_ARRAY(GetPartnerRewardReconciliation.value:"$")) PartnerRewardReconciliationData
		,LATERAL FLATTEN(TO_ARRAY(PartnerRewardReconciliationData.value:"$")) TransactionType
	WHERE	GetPartnerRewardReconciliation.value like '<PartnerRewardReconciliationData>%'
	AND	(PartnerRewardReconciliationData.value like '<Abs:TransactionType>%' )
	AND	(TransactionType.value like '<Abs:AltTransaction>%' )
) AltTransaction on AltTransaction.SEQ = TransactionType.SEQ AND AltTransaction.idx = TransactionType.idx
	AND	AltTransaction.SEQ1 = TransactionType.SEQ1 AND AltTransaction.idx1 = TransactionType.idx1
LEFT JOIN
(
	SELECT
		XMLGET(AltTransaction.value, 'Abs:Description'):"$"::string AS AltTransactionType_Description
		,XMLGET(AltTransaction.value, 'Abs:Code'):"$"::string AS AltTransactionType_Code
		,XMLGET(AltTransaction.value, 'Abs:ShortDescription'):"$"::string AS AltTransactionType_ShortDescription
		,GetPartnerRewardReconciliation.SEQ::integer as SEQ
		,GetPartnerRewardReconciliation.idx::integer as idx
		,PartnerRewardReconciliationData.SEQ::integer as SEQ1
		,PartnerRewardReconciliationData.index::integer as idx1
		,TransactionType.SEQ::integer as SEQ2
		,TransactionType.index::integer as idx2
	FROM	LEVEL_1_FLATTEN AS GetPartnerRewardReconciliation
		,LATERAL FLATTEN(TO_ARRAY(GetPartnerRewardReconciliation.value:"$")) PartnerRewardReconciliationData
		,LATERAL FLATTEN(TO_ARRAY(PartnerRewardReconciliationData.value:"$")) TransactionType
		,LATERAL FLATTEN(TO_ARRAY(TransactionType.value:"$")) AltTransaction
	WHERE	GetPartnerRewardReconciliation.value like '<PartnerRewardReconciliationData>%'
	AND	(PartnerRewardReconciliationData.value like '<Abs:TransactionType>%' )
	AND	(TransactionType.value like '<Abs:AltTransaction>%' )
	AND	(AltTransaction.value like '<Abs:AltTransactionType>%' )
) AltTransactionType on AltTransactionType.SEQ = AltTransaction.SEQ AND AltTransactionType.idx = AltTransaction.idx
	AND	AltTransactionType.SEQ1 = AltTransaction.SEQ1 AND AltTransactionType.idx1 = AltTransaction.idx1
	AND	AltTransactionType.SEQ2 = AltTransaction.SEQ2 AND AltTransactionType.idx2 = AltTransaction.idx2
LEFT JOIN
(
	SELECT
		XMLGET(TransactionType.value, 'Abs:ShortDescription'):"$"::string AS TransactionTypeCd_ShortDescription
		,XMLGET(TransactionType.value, 'Abs:Description'):"$"::string AS TransactionTypeCd_Description
		,XMLGET(TransactionType.value, 'Abs:Code'):"$"::string AS TransactionTypeCd_Code
		,GetPartnerRewardReconciliation.SEQ::integer as SEQ
		,GetPartnerRewardReconciliation.idx::integer as idx
		,PartnerRewardReconciliationData.SEQ::integer as SEQ1
		,PartnerRewardReconciliationData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetPartnerRewardReconciliation
		,LATERAL FLATTEN(TO_ARRAY(GetPartnerRewardReconciliation.value:"$")) PartnerRewardReconciliationData
		,LATERAL FLATTEN(TO_ARRAY(PartnerRewardReconciliationData.value:"$")) TransactionType
	WHERE	GetPartnerRewardReconciliation.value like '<PartnerRewardReconciliationData>%'
	AND	(PartnerRewardReconciliationData.value like '<Abs:TransactionType>%' )
	AND	(TransactionType.value like '<Abs:TransactionTypeCd>%' )
) TransactionTypeCd on TransactionTypeCd.SEQ = TransactionType.SEQ AND TransactionTypeCd.idx = TransactionType.idx
	AND	TransactionTypeCd.SEQ1 = TransactionType.SEQ1 AND TransactionTypeCd.idx1 = TransactionType.idx1`;

    try {
        snowflake.execute ( {sqlText: insert_into_flat_dml} );
    } catch (err) { 
        snowflake.execute ( {sqlText: sql_ins_rerun_tbl} ); 
        throw `Loading of table ${tgt_flat_tbl} Failed with error: ${err}`;   // Return a error message.
    }
$$
;

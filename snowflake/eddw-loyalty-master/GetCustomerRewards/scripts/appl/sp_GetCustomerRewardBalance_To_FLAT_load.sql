--liquibase formatted sql
--changeset SYSTEM:sp_GetCustomerRewardBalance_To_FLAT_load runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME_R>>;
use schema <<EDM_DB_NAME_R>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME_R>>.DW_APPL.SP_GETCUSTOMERREWARDBALANCE_TO_FLAT_LOAD()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
	 
   

    var cur_db = snowflake.execute( {sqlText: `Select current_database()`} ); 
	cur_db.next(); 
	var env = cur_db.getColumnValue(1); 
	env = env.split('_'); 
	env = env[env.length - 1]; 
	var env_tbl_nm = `EDM_Environment_Variable_${env}`; 
	var env_schema_nm = 'DW_R_MASTERDATA'; 
	var env_db_nm = `EDM_REFINED_${env}` 

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
    throw `Error while fetching data from EDM_Environment_Variable_${env}` 
	}
    var variant_nm = 'ESED_CustomerRewardBalance';
	var bod_nm = 'GetCustomerRewardBalance';
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
    ,GetCustomerRewardBalance.value as Value
    ,GetCustomerRewardBalance.SEQ::integer as SEQ
    ,GetCustomerRewardBalance.index::integer as idx
    FROM ${src_wrk_tbl} tbl
    ,LATERAL FLATTEN(tbl.SRC_XML:"$") GetCustomerRewardBalance
    )
     SELECT FILENAME
        ,BODNm
		,Document_Description
		,AlternateDocumentID
		,SourceApplicationCd
		,TargetApplicationCd
		,Note
		,GatewayNm
		,SenderId
		,ReceiverId
		,RoutingSystemNm
		,InternalFileTransferInd
		,InterchangeDate
		,InterchangeTime
		,ExternalTargetInd
		,MessageSequenceNbr
		,CreationDt
		,ExpectedMessageCnt
		,DocumentNm
		,InboundOutboundInd
		,DocumentID
		,PHIdataInd
		,PCIdataInd
		,PIIdataInd
		,BusinessSensitivityLevel_Code
		,BusinessSensitivityLevel_Description
		,BusinessSensitivityLevel_ShortDescription
		,DataClassificationLevel_Description
		,DataClassificationLevel_ShortDescription
		,DataClassificationLevel_Code
		,ActionTypeCd
		,RecordTypeCd
        ,ClubCardNbr
		,PhoneNbr
		,HouseholdId
		,BalanceUpdateTs
		,AccumulationTypCd
		,ClubDsc
		,ClubCd
		,PreferredSalutationCd
		,FormattedNm
		,NickNm
		,MiddleNm
		,FamilyNm
		,MaidenNm
		,GenerationAffixCd
		,QualificationAffixCd
		,GivenNm
		,TitleCd
		,InclusiveInd
		,StartTs
		,Duration
		,TimeZoneCd
		,EndTs
		,LoyaltyProgramDsc
		,LoyaltyProgramCd
		,RewardDollarPointsQty
		,RewardDollarStartTs
		,RewardDollarEndTs
		,RewardPeriodStartTs
		,RewardPeriodEndTs
		,RewardOriginDsc
		,RewardOriginCd
		,RewardTokenEndTs
		,RewardTokenStartTs
		,RewardTokenWilExpStartTs
		,RewardTokenWilExpEndTs
		,RewardTokenPointsQty
		,RewardTokenPointsExpireQty
		,CustomerNm_typeCode
		,CustomerNm_sequenceNbr
		,CustomerNm_preferredInd
		,Document_ReleaseId
		,Document_VersionId
		,Document_SystemEnvironmentCd
		,CURRENT_TIMESTAMP AS DW_CreateTs
		,RewardValidityEndTs
		,RewardValueQty
		,RewardTypeCd_Code
		,RewardTypeCd_Description
		,RewardTypeCd_ShortDescription		
		,ProgramType
		,ProgramDsc
		,ProgramValueQty
		,ProgramValidityEndTs
		,ProgramModifyTs
        
      FROM
	    (
	SELECT
		FILENAME
		,BODnm
		,GET(DocumentData.value, '@ReleaseId')::string AS Document_ReleaseId
		,GET(DocumentData.value, '@VersionId')::string AS Document_VersionId
		,GET(DocumentData.value, '@SystemEnvironmentCd')::string AS Document_SystemEnvironmentCd
		,XMLGET(DocumentData.value, 'Abs:Description'):"$"::string AS Document_Description
		,XMLGET(DocumentData.value, 'Abs:AlternateDocumentID'):"$"::string AS AlternateDocumentID
		,XMLGET(DocumentData.value, 'Abs:SourceApplicationCd'):"$"::string AS SourceApplicationCd
		,XMLGET(DocumentData.value, 'Abs:TargetApplicationCd'):"$"::string AS TargetApplicationCd
		,XMLGET(DocumentData.value, 'Abs:Note'):"$"::string AS Note
		,XMLGET(DocumentData.value, 'Abs:GatewayNm'):"$"::string AS GatewayNm
		,XMLGET(DocumentData.value, 'Abs:SenderId'):"$"::string AS SenderId
		,XMLGET(DocumentData.value, 'Abs:ReceiverId'):"$"::string AS ReceiverId
		,XMLGET(DocumentData.value, 'Abs:RoutingSystemNm'):"$"::string AS RoutingSystemNm
		,XMLGET(DocumentData.value, 'Abs:InternalFileTransferInd'):"$"::string AS InternalFileTransferInd
		,XMLGET(DocumentData.value, 'Abs:InterchangeDate'):"$"::string AS InterchangeDate
		,XMLGET(DocumentData.value, 'Abs:InterchangeTime'):"$"::string AS InterchangeTime
		,XMLGET(DocumentData.value, 'Abs:ExternalTargetInd'):"$"::string AS ExternalTargetInd
		,XMLGET(DocumentData.value, 'Abs:MessageSequenceNbr'):"$"::string AS MessageSequenceNbr
		,XMLGET(DocumentData.value, 'Abs:CreationDt'):"$"::string AS CreationDt
		,XMLGET(DocumentData.value, 'Abs:ExpectedMessageCnt'):"$"::string AS ExpectedMessageCnt
		,XMLGET(DocumentData.value, 'Abs:DocumentNm'):"$"::string AS DocumentNm
		,XMLGET(DocumentData.value, 'Abs:InboundOutboundInd'):"$"::string AS InboundOutboundInd
		,XMLGET(DocumentData.value, 'Abs:DocumentID'):"$"::string AS DocumentID
		,GetCustomerRewardBalance.SEQ::integer as SEQ
		,GetCustomerRewardBalance.idx::integer as idx
		,DocumentData.SEQ::integer as SEQ1
		,DocumentData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetCustomerRewardBalance
		,LATERAL FLATTEN(TO_ARRAY(GetCustomerRewardBalance.value:"$")) DocumentData
	WHERE	GetCustomerRewardBalance.value like '<DocumentData>%'
	AND	(DocumentData.value like '<Document>%' OR DocumentData.value like '<Document%ReleaseId%' OR DocumentData.value like '<Document%VersionId%' OR DocumentData.value like '<Document%SystemEnvironmentCd%' )
) Document 
LEFT JOIN
(
	SELECT
		XMLGET(Document.value, 'Abs:PHIdataInd'):"$"::string AS PHIdataInd
		,XMLGET(Document.value, 'Abs:PCIdataInd'):"$"::string AS PCIdataInd
		,XMLGET(Document.value, 'Abs:PIIdataInd'):"$"::string AS PIIdataInd
		,GetCustomerRewardBalance.SEQ::integer as SEQ
		,GetCustomerRewardBalance.idx::integer as idx
		,DocumentData.SEQ::integer as SEQ1
		,DocumentData.index::integer as idx1
		,Document.SEQ::integer as SEQ2
		,Document.index::integer as idx2
	FROM	LEVEL_1_FLATTEN AS GetCustomerRewardBalance
		,LATERAL FLATTEN(TO_ARRAY(GetCustomerRewardBalance.value:"$")) DocumentData
		,LATERAL FLATTEN(TO_ARRAY(DocumentData.value:"$")) Document
	WHERE	GetCustomerRewardBalance.value like '<DocumentData>%'
	AND	(DocumentData.value like '<Document>%' OR DocumentData.value like '<Document%ReleaseId%' OR DocumentData.value like '<Document%VersionId%' OR DocumentData.value like '<Document%SystemEnvironmentCd%' )
	AND	(Document.value like '<Abs:DataClassification>%' )
) DataClassification on DataClassification.SEQ = Document.SEQ AND DataClassification.idx = Document.idx
	AND	DataClassification.SEQ1 = Document.SEQ1 AND DataClassification.idx1 = Document.idx1
LEFT JOIN
(
	SELECT
		XMLGET(DataClassification.value, 'Abs:Code'):"$"::string AS BusinessSensitivityLevel_Code
		,XMLGET(DataClassification.value, 'Abs:Description'):"$"::string AS BusinessSensitivityLevel_Description
		,XMLGET(DataClassification.value, 'Abs:ShortDescription'):"$"::string AS BusinessSensitivityLevel_ShortDescription
		,GetCustomerRewardBalance.SEQ::integer as SEQ
		,GetCustomerRewardBalance.idx::integer as idx
		,DocumentData.SEQ::integer as SEQ1
		,DocumentData.index::integer as idx1
		,Document.SEQ::integer as SEQ2
		,Document.index::integer as idx2
	FROM	LEVEL_1_FLATTEN AS GetCustomerRewardBalance
		,LATERAL FLATTEN(TO_ARRAY(GetCustomerRewardBalance.value:"$")) DocumentData
		,LATERAL FLATTEN(TO_ARRAY(DocumentData.value:"$")) Document
		,LATERAL FLATTEN(TO_ARRAY(Document.value:"$")) DataClassification
	WHERE	GetCustomerRewardBalance.value like '<DocumentData>%'
	AND	(DocumentData.value like '<Document>%' OR DocumentData.value like '<Document%ReleaseId%' OR DocumentData.value like '<Document%VersionId%' OR DocumentData.value like '<Document%SystemEnvironmentCd%' )
	AND	(Document.value like '<Abs:DataClassification>%' )
	AND	(DataClassification.value like '<Abs:BusinessSensitivityLevel>%' )
) BusinessSensitivityLevel on BusinessSensitivityLevel.SEQ = DataClassification.SEQ AND BusinessSensitivityLevel.idx = DataClassification.idx
	AND	BusinessSensitivityLevel.SEQ1 = DataClassification.SEQ1 AND BusinessSensitivityLevel.idx1 = DataClassification.idx1
	AND	BusinessSensitivityLevel.SEQ2 = DataClassification.SEQ2 AND BusinessSensitivityLevel.idx2 = DataClassification.idx2
LEFT JOIN
(
	SELECT
		XMLGET(DataClassification.value, 'Abs:Description'):"$"::string AS DataClassificationLevel_Description
		,XMLGET(DataClassification.value, 'Abs:ShortDescription'):"$"::string AS DataClassificationLevel_ShortDescription
		,XMLGET(DataClassification.value, 'Abs:Code'):"$"::string AS DataClassificationLevel_Code
		,GetCustomerRewardBalance.SEQ::integer as SEQ
		,GetCustomerRewardBalance.idx::integer as idx
		,DocumentData.SEQ::integer as SEQ1
		,DocumentData.index::integer as idx1
		,Document.SEQ::integer as SEQ2
		,Document.index::integer as idx2
	FROM	LEVEL_1_FLATTEN AS GetCustomerRewardBalance
		,LATERAL FLATTEN(TO_ARRAY(GetCustomerRewardBalance.value:"$")) DocumentData
		,LATERAL FLATTEN(TO_ARRAY(DocumentData.value:"$")) Document
		,LATERAL FLATTEN(TO_ARRAY(Document.value:"$")) DataClassification
	WHERE	GetCustomerRewardBalance.value like '<DocumentData>%'
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
		,GetCustomerRewardBalance.SEQ::integer as SEQ
		,GetCustomerRewardBalance.idx::integer as idx
	FROM	LEVEL_1_FLATTEN AS GetCustomerRewardBalance
		,LATERAL FLATTEN(TO_ARRAY(GetCustomerRewardBalance.value:"$")) DocumentData
	WHERE	GetCustomerRewardBalance.value like '<DocumentData>%'
	AND	(DocumentData.value like '<DocumentAction>%' )
) DocumentAction on DocumentAction.SEQ = Document.SEQ AND DocumentAction.idx = Document.idx
LEFT JOIN
(
	SELECT 
		XMLGET(GetCustomerRewardBalance.value, 'Abs:ClubCardNbr'):"$"::string AS ClubCardNbr
		,XMLGET(GetCustomerRewardBalance.value, 'Abs:PhoneNbr'):"$"::string AS PhoneNbr
		,XMLGET(GetCustomerRewardBalance.value, 'Abs:HouseholdId'):"$"::string AS HouseholdId
		,XMLGET(GetCustomerRewardBalance.value, 'Abs:BalanceUpdateTs'):"$"::string AS BalanceUpdateTs
		,XMLGET(GetCustomerRewardBalance.value, 'Abs:AccumulationTypCd'):"$"::string AS AccumulationTypCd
		,GetCustomerRewardBalance.SEQ::integer as SEQ
		,GetCustomerRewardBalance.idx::integer as idx
	FROM	LEVEL_1_FLATTEN AS GetCustomerRewardBalance
	WHERE	GetCustomerRewardBalance.value like '<CustomerRewardData>%'
) CustomerRewardData on CustomerRewardData.SEQ = Document.SEQ

LEFT JOIN
(
	SELECT
		XMLGET(CustomerRewardData.value, 'Abs:ClubDsc'):"$"::string AS ClubDsc
		,XMLGET(CustomerRewardData.value, 'Abs:ClubCd'):"$"::string AS ClubCd
		,GetCustomerRewardBalance.SEQ::integer as SEQ
		,GetCustomerRewardBalance.idx::integer as idx
	FROM	LEVEL_1_FLATTEN AS GetCustomerRewardBalance
		,LATERAL FLATTEN(TO_ARRAY(GetCustomerRewardBalance.value:"$")) CustomerRewardData
	WHERE	GetCustomerRewardBalance.value like '<CustomerRewardData>%'
	AND	(CustomerRewardData.value like '<Abs:ClubInfo>%' )
) ClubInfo on ClubInfo.SEQ = CustomerRewardData.SEQ AND ClubInfo.idx = CustomerRewardData.idx
LEFT JOIN
(
	SELECT
		GET(CustomerRewardData.value, '@Abs:typeCode')::string AS CustomerNm_typeCode
		,GET(CustomerRewardData.value, '@Abs:sequenceNbr')::string AS CustomerNm_sequenceNbr
		,GET(CustomerRewardData.value, '@Abs:preferredInd')::string AS CustomerNm_preferredInd
		,XMLGET(CustomerRewardData.value, 'Abs:PreferredSalutationCd'):"$"::string AS PreferredSalutationCd
		,XMLGET(CustomerRewardData.value, 'Abs:FormattedNm'):"$"::string AS FormattedNm
		,XMLGET(CustomerRewardData.value, 'Abs:NickNm'):"$"::string AS NickNm
		,XMLGET(CustomerRewardData.value, 'Abs:MiddleNm'):"$"::string AS MiddleNm
		,XMLGET(CustomerRewardData.value, 'Abs:FamilyNm'):"$"::string AS FamilyNm
		,XMLGET(CustomerRewardData.value, 'Abs:MaidenNm'):"$"::string AS MaidenNm
		,XMLGET(CustomerRewardData.value, 'Abs:GenerationAffixCd'):"$"::string AS GenerationAffixCd
		,XMLGET(CustomerRewardData.value, 'Abs:QualificationAffixCd'):"$"::string AS QualificationAffixCd
		,XMLGET(CustomerRewardData.value, 'Abs:GivenNm'):"$"::string AS GivenNm
		,XMLGET(CustomerRewardData.value, 'Abs:TitleCd'):"$"::string AS TitleCd
		,GetCustomerRewardBalance.SEQ::integer as SEQ
		,GetCustomerRewardBalance.idx::integer as idx
		,CustomerRewardData.SEQ::integer as SEQ1
		,CustomerRewardData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetCustomerRewardBalance
		,LATERAL FLATTEN(TO_ARRAY(GetCustomerRewardBalance.value:"$")) CustomerRewardData
	WHERE	GetCustomerRewardBalance.value like '<CustomerRewardData>%'
	AND	(CustomerRewardData.value like '<Abs:CustomerNm>%' OR CustomerRewardData.value like '<Abs:CustomerNm%typeCode%' OR CustomerRewardData.value like '<Abs:CustomerNm%sequenceNbr%' OR CustomerRewardData.value like '<Abs:CustomerNm%preferredInd%' )
) CustomerNm on CustomerNm.SEQ = CustomerRewardData.SEQ AND CustomerNm.idx = CustomerRewardData.idx
LEFT JOIN
(
	SELECT
		XMLGET(CustomerNm.value, 'Abs:InclusiveInd'):"$"::string AS InclusiveInd
		,XMLGET(CustomerNm.value, 'Abs:StartTs'):"$"::string AS StartTs
		,XMLGET(CustomerNm.value, 'Abs:Duration'):"$"::string AS Duration
		,XMLGET(CustomerNm.value, 'Abs:TimeZoneCd'):"$"::string AS TimeZoneCd
		,XMLGET(CustomerNm.value, 'Abs:EndTs'):"$"::string AS EndTs
		,GetCustomerRewardBalance.SEQ::integer as SEQ
		,GetCustomerRewardBalance.idx::integer as idx
		,CustomerRewardData.SEQ::integer as SEQ1
		,CustomerRewardData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetCustomerRewardBalance
		,LATERAL FLATTEN(TO_ARRAY(GetCustomerRewardBalance.value:"$")) CustomerRewardData
		,LATERAL FLATTEN(TO_ARRAY(CustomerRewardData.value:"$")) CustomerNm
	WHERE	GetCustomerRewardBalance.value like '<CustomerRewardData>%'
	AND	(CustomerRewardData.value like '<Abs:CustomerNm>%' OR CustomerRewardData.value like '<Abs:CustomerNm%typeCode%' OR CustomerRewardData.value like '<Abs:CustomerNm%sequenceNbr%' OR CustomerRewardData.value like '<Abs:CustomerNm%preferredInd%' )
	AND	(CustomerNm.value like '<Abs:EffectiveTimePeriod>%' )
) EffectiveTimePeriod on EffectiveTimePeriod.SEQ = CustomerNm.SEQ AND EffectiveTimePeriod.idx = CustomerNm.idx
	AND	EffectiveTimePeriod.SEQ1 = CustomerNm.SEQ1 AND EffectiveTimePeriod.idx1 = CustomerNm.idx1
	
LEFT JOIN
(
	SELECT
		XMLGET(CustomerRewardData.value, 'Abs:LoyaltyProgramDsc'):"$"::string AS LoyaltyProgramDsc
		,XMLGET(CustomerRewardData.value, 'Abs:LoyaltyProgramCd'):"$"::string AS LoyaltyProgramCd
		,GetCustomerRewardBalance.SEQ::integer as SEQ
		,GetCustomerRewardBalance.idx::integer as idx
	FROM	LEVEL_1_FLATTEN AS GetCustomerRewardBalance
		,LATERAL FLATTEN(TO_ARRAY(GetCustomerRewardBalance.value:"$")) CustomerRewardData
	WHERE	GetCustomerRewardBalance.value like '<CustomerRewardData>%'
	AND	(CustomerRewardData.value like '<Abs:LoyaltyProgram>%' )
) LoyaltyProgram on LoyaltyProgram.SEQ = CustomerRewardData.SEQ AND LoyaltyProgram.idx = CustomerRewardData.idx
LEFT JOIN
(
	SELECT
		XMLGET(CustomerRewardData.value, 'Abs:RewardDollarPointsQty'):"$"::string AS RewardDollarPointsQty
		,XMLGET(CustomerRewardData.value, 'Abs:RewardDollarStartTs'):"$"::string AS RewardDollarStartTs
		,XMLGET(CustomerRewardData.value, 'Abs:RewardDollarEndTs'):"$"::string AS RewardDollarEndTs
		,GetCustomerRewardBalance.SEQ::integer as SEQ
		,GetCustomerRewardBalance.idx::integer as idx
	FROM	LEVEL_1_FLATTEN AS GetCustomerRewardBalance
		,LATERAL FLATTEN(TO_ARRAY(GetCustomerRewardBalance.value:"$")) CustomerRewardData
	WHERE	GetCustomerRewardBalance.value like '<CustomerRewardData>%'
	AND	(CustomerRewardData.value like '<Abs:RewardDollarPoints>%' )
) RewardDollarPoints on RewardDollarPoints.SEQ = CustomerRewardData.SEQ AND RewardDollarPoints.idx = CustomerRewardData.idx
LEFT JOIN
(
	SELECT
		XMLGET(CustomerRewardData.value, 'Abs:RewardPeriodStartTs'):"$"::string AS RewardPeriodStartTs
		,XMLGET(CustomerRewardData.value, 'Abs:RewardPeriodEndTs'):"$"::string AS RewardPeriodEndTs
		,GetCustomerRewardBalance.SEQ::integer as SEQ
		,GetCustomerRewardBalance.idx::integer as idx
	FROM	LEVEL_1_FLATTEN AS GetCustomerRewardBalance
		,LATERAL FLATTEN(TO_ARRAY(GetCustomerRewardBalance.value:"$")) CustomerRewardData
	WHERE	GetCustomerRewardBalance.value like '<CustomerRewardData>%'
	AND	(CustomerRewardData.value like '<Abs:RewardPeriod>%' )
) RewardPeriod on RewardPeriod.SEQ = CustomerRewardData.SEQ AND RewardPeriod.idx = CustomerRewardData.idx
LEFT JOIN
(
	SELECT
		XMLGET(CustomerRewardData.value, 'Abs:RewardOriginDsc'):"$"::string AS RewardOriginDsc
		,XMLGET(CustomerRewardData.value, 'Abs:RewardOriginCd'):"$"::string AS RewardOriginCd
		,GetCustomerRewardBalance.SEQ::integer as SEQ
		,GetCustomerRewardBalance.idx::integer as idx
	FROM	LEVEL_1_FLATTEN AS GetCustomerRewardBalance
		,LATERAL FLATTEN(TO_ARRAY(GetCustomerRewardBalance.value:"$")) CustomerRewardData
	WHERE	GetCustomerRewardBalance.value like '<CustomerRewardData>%'
	AND	(CustomerRewardData.value like '<Abs:RewardPointsOrigin>%' )
) RewardPointsOrigin on RewardPointsOrigin.SEQ = CustomerRewardData.SEQ AND RewardPointsOrigin.idx = CustomerRewardData.idx
LEFT JOIN
(
	SELECT
		XMLGET(CustomerRewardData.value, 'Abs:RewardValidityEndTs'):"$"::string AS RewardValidityEndTs
		,XMLGET(CustomerRewardData.value, 'Abs:RewardValueQty'):"$"::string AS RewardValueQty
		,GetCustomerRewardBalance.SEQ::integer as SEQ
		,GetCustomerRewardBalance.idx::integer as idx
		,CustomerRewardData.SEQ::integer as SEQ1
		,CustomerRewardData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetCustomerRewardBalance
		,LATERAL FLATTEN(TO_ARRAY(GetCustomerRewardBalance.value:"$")) CustomerRewardData
	WHERE	GetCustomerRewardBalance.value like '<CustomerRewardData>%'
	AND	(CustomerRewardData.value like '<Abs:CustomerRewardScoreCard>%' )
) CustomerRewardScoreCard on CustomerRewardScoreCard.SEQ = CustomerRewardData.SEQ AND CustomerRewardScoreCard.idx = CustomerRewardData.idx
LEFT JOIN
(
	SELECT
		XMLGET(CustomerRewardScoreCard.value, 'Abs:Code'):"$"::string AS RewardTypeCd_Code
		,XMLGET(CustomerRewardScoreCard.value, 'Abs:Description'):"$"::string AS RewardTypeCd_Description
		,XMLGET(CustomerRewardScoreCard.value, 'Abs:ShortDescription'):"$"::string AS RewardTypeCd_ShortDescription			
		,GetCustomerRewardBalance.SEQ::integer as SEQ
		,GetCustomerRewardBalance.idx::integer as idx
		,CustomerRewardData.SEQ::integer as SEQ1
		,CustomerRewardData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetCustomerRewardBalance
		,LATERAL FLATTEN(TO_ARRAY(GetCustomerRewardBalance.value:"$")) CustomerRewardData
		,LATERAL FLATTEN(TO_ARRAY(CustomerRewardData.value:"$")) CustomerRewardScoreCard
	WHERE	GetCustomerRewardBalance.value like '<CustomerRewardData>%'
	AND	(CustomerRewardData.value like '<Abs:CustomerRewardScoreCard>%' )
	AND	(CustomerRewardScoreCard.value like '<Abs:RewardType>%' )
) RewardTypeCd on RewardTypeCd.SEQ = CustomerRewardScoreCard.SEQ AND RewardTypeCd.idx = CustomerRewardScoreCard.idx
	AND	RewardTypeCd.SEQ1 = CustomerRewardScoreCard.SEQ1 AND RewardTypeCd.idx1 = CustomerRewardScoreCard.idx1

LEFT JOIN
(
	SELECT
		XMLGET(CustomerRewardData.value, 'Abs:RewardTokenEndTs'):"$"::string AS RewardTokenEndTs
		,XMLGET(CustomerRewardData.value, 'Abs:RewardTokenStartTs'):"$"::string AS RewardTokenStartTs
		,XMLGET(CustomerRewardData.value, 'Abs:RewardTokenWilExpStartTs'):"$"::string AS RewardTokenWilExpStartTs
		,XMLGET(CustomerRewardData.value, 'Abs:RewardTokenWilExpEndTs'):"$"::string AS RewardTokenWilExpEndTs
		,XMLGET(CustomerRewardData.value, 'Abs:RewardTokenPointsQty'):"$"::string AS RewardTokenPointsQty
		,XMLGET(CustomerRewardData.value, 'Abs:RewardTokenPointsExpireQty'):"$"::string AS RewardTokenPointsExpireQty
		,GetCustomerRewardBalance.SEQ::integer as SEQ
		,GetCustomerRewardBalance.idx::integer as idx
	FROM	LEVEL_1_FLATTEN AS GetCustomerRewardBalance
		,LATERAL FLATTEN(TO_ARRAY(GetCustomerRewardBalance.value:"$")) CustomerRewardData
	WHERE	GetCustomerRewardBalance.value like '<CustomerRewardData>%'
	AND	(CustomerRewardData.value like '<Abs:RewardTokenPoints>%' )
) RewardTokenPoints on RewardTokenPoints.SEQ = CustomerRewardData.SEQ AND RewardTokenPoints.idx = CustomerRewardData.idx
LEFT JOIN
(
	SELECT
		XMLGET(CustomerRewardData.value, 'Abs:ProgramModifyTs'):"$"::string AS ProgramModifyTs
		,XMLGET(CustomerRewardData.value, 'Abs:ProgramValueQty'):"$"::string AS ProgramValueQty
		,XMLGET(CustomerRewardData.value, 'Abs:ProgramDsc'):"$"::string AS ProgramDsc
		,XMLGET(CustomerRewardData.value, 'Abs:ProgramType'):"$"::string AS ProgramType
		,XMLGET(CustomerRewardData.value, 'Abs:ProgramValidityEndTs'):"$"::string AS ProgramValidityEndTs
		,GetCustomerRewardBalance.SEQ::integer as SEQ
		,GetCustomerRewardBalance.idx::integer as idx
	FROM	LEVEL_1_FLATTEN AS GetCustomerRewardBalance
		,LATERAL FLATTEN(TO_ARRAY(GetCustomerRewardBalance.value:"$")) CustomerRewardData
	WHERE	GetCustomerRewardBalance.value like '<CustomerRewardData>%'
	AND	(CustomerRewardData.value like '<Abs:CustomerProgramScoreCard>%' )
) CustomerProgramScoreCard on CustomerProgramScoreCard.SEQ = CustomerRewardData.SEQ AND CustomerProgramScoreCard.idx = CustomerRewardData.idx

`;

    try {
        snowflake.execute ( {sqlText: insert_into_flat_dml} );
    } catch (err) { 
        snowflake.execute ( {sqlText: sql_ins_rerun_tbl} ); 
        throw `Loading of table ${tgt_flat_tbl} Failed with error: ${err}`;   // Return a error message.
    }


$$;

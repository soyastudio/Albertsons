USE DATABASE EDM_REFINED_PRD;
USE SCHEMA DW_APPL;


CREATE OR REPLACE PROCEDURE SP_GETBUSINESSPARTNER_TO_FLAT_LOAD()
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
	var anlys_ref_db = metaparams['ANLYS_DB'];
	var anlys_ref_schema = metaparams['DATA_GOVRNC'];
	var anlys_wrk_schema = metaparams['A_STAGE'];
	} catch (err) { 
    throw `Error while fetching data from EDM_Environment_Variable_${env}`; 
	}
    var variant_nm = 'ESED_BusinessPartner';
	var bod_nm = 'GetBusinessPartner';
	var bodName = 'GetBusinessPartner';
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
    ,GetBusinessPartner.value as Value
    ,GetBusinessPartner.SEQ::integer as SEQ
    ,GetBusinessPartner.index::integer as idx
    FROM ${src_wrk_tbl} tbl
    ,LATERAL FLATTEN(tbl.SRC_XML:"$") GetBusinessPartner
    )
    SELECT DISTINCT FILENAME
          ,BODNm
          ,CustomerAccountNbr
		,InternalParnterInd
		,BusinessPartnerData_PartnerId
		,CustomerSiteNbr
		,VendorNbr
		,PartnerAuditData_UpdateTs
		,PartnerAuditData_UpdateUserId
		,PartnerAuditData_UpdateDtTm
		,PartnerAuditData_CreateUserId
		,PartnerAuditData_CreateTs
		,PartnerAuditData_CreateDtTm
		,PartnerEffectiveTimePeriod_FirstEffectiveTm
		,PartnerEffectiveTimePeriod_FirstEffectiveDt
		,PartnerEffectiveTimePeriod_LastEffectiveTm
		,PartnerEffectiveTimePeriod_LastEffectiveDt
		,PartnerProfile_PartnerNm
		,ContractId
		,ContractNm
		,ContractDsc
		,ContractStartDt
		,ContractEndDt
		,UserId
		,LastNm
		,FirstNm
		,ContractByType_CreateDtTm
		,ReasonType_Code
		,ReasonType_Description
		,ReasonType_ShortDescription
		,OrderLimitCnt
		,MaximumItemCnt
		,MinimumToteCnt
		,MaximumToteCnt
		,OrderAllocationPct
		,MileageNbr
		,MinimumItemCnt
		,NotesTxt
		,NotesTypeCd
		,PartnerProfile_OrganizationTypeCd
		,PartnerProfile_OrganizationValueTxt
		,Overrides_PartnerId
		,Overrides_PartnerNm
		,OverrideInd
		,OverrideReasonType_Description
		,OverrideReasonType_ShortDescription
		,OverrideReasonType_Code
		,OverrideType_ShortDescription
		,OverrideType_Description
		,OverrideType_Code
		,Address_StateNm
		,Address_FaxNbr
		,Address_PhoneNbr
		,Address_TimeZoneCd
		,Address_LatitudeDegree
		,Address_CountryNm
		,Address_CountryCd
		,Address_StateCd
		,Address_LongitudeDegree
		,Address_CountyCd
		,Address_CountyNm
		,Address_CityNm
		,Address_AddressLine5txt
		,Address_AddressLine4txt
		,Address_PostalZoneCd
		,Address_AddressLine3txt
		,Address_AddressLine2txt
		,Address_AddressLine1txt
		,Address_AddressUsageTypeCd
		,Contact_PhoneNbr
		,Contact_EmailAddresstxt
		,Contact_ContactNm
		,Contact_ShortDescription
		,Contact_Description
		,Contact_Code
		,PartnerProfileEffectiveTimePeriod_FirstEffectiveTm
		,PartnerProfileEffectiveTimePeriod_LastEffectiveDt
		,PartnerProfileEffectiveTimePeriod_LastEffectiveTm
		,PartnerProfileEffectiveTimePeriod_FirstEffectiveDt
		,PartnerTypeCd_ShortDescription
		,PartnerTypeCd_Description
		,PartnerTypeCd_Code
		,AreaTypeCd
		,AreaTypeValueTxt
		,Altitude
		,ServiceAreaCoordinateType_LongitudeDegree
		,ServiceAreaCoordinateType_LatitudeDegree
		,ServiceAreaType_ShortDescription
		,ServiceAreaType_Description
		,ServiceAreaType_Code
		,ServiceFeeAmt
		,ServiceFeeItemId
		,ServiceFeeTypeCd
		,ServiceFeeCategoryCd
		,ServiceLevelType_Code
		,ServiceLevelType_Description
		,ServiceLevelType_ShortDescription
		,ActivityType_Code
		,ActivityType_Description
		,ActivityType_ShortDescription
		,Status_StatusTypeCd
		,Status_Description
		,Status_EffectiveDtTm
		,PartnerParticipantId
		,PartnerSiteNm
		,PartnerSiteId
		,PartnerSiteActiveInd
		,PartnerSiteCommentTxt
		,PartnerSiteData_OrganizationValueTxt
		,PartnerSiteData_OrganizationTypeCd
		,PartnerSiteAuditData_CreateTs
		,PartnerSiteAuditData_UpdateUserId
		,PartnerSiteAuditData_UpdateTs
		,PartnerSiteAuditData_UpdateDtTm
		,PartnerSiteAuditData_CreateUserId
		,PartnerSiteAuditData_CreateDtTm
		,SiteAddress_AddressLine1txt
		,SiteAddress_AddressLine2txt
		,SiteAddress_AddressLine3txt
		,SiteAddress_LongitudeDegree
		,SiteAddress_LatitudeDegree
		,SiteAddress_StateCd
		,SiteAddress_StateNm
		,SiteAddress_CountryCd
		,SiteAddress_AddressLine4txt
		,SiteAddress_AddressUsageTypeCd
		,SiteAddress_CityNm
		,SiteAddress_CountyNm
		,SiteAddress_CountyCd
		,SiteAddress_PostalZoneCd
		,SiteAddress_CountryNm
		,SiteAddress_AddressLine5txt
		,SiteAddress_PhoneNbr
		,SiteAddress_TimeZoneCd
		,SiteAddress_FaxNbr
		,SiteContact_ContactNm
		,SiteContact_PhoneNbr
		,SiteContact_EmailAddresstxt
		,SiteContact_Code
		,SiteContact_Description
		,SiteContact_ShortDescription
		,PartnerSiteEffectiveTimePeriod_FirstEffectiveDt
		,PartnerSiteEffectiveTimePeriod_LastEffectiveDt
		,PartnerSiteEffectiveTimePeriod_LastEffectiveTm
		,PartnerSiteEffectiveTimePeriod_FirstEffectiveTm
		,PartnerSiteStatus_Description
		,PartnerSiteStatus_EffectiveDtTm
		,PartnerSiteStatus_StatusTypeCd
		,PartnerSiteTypeCd_Code
		,PartnerSiteTypeCd_Description
		,PartnerSiteTypeCd_ShortDescription
		,InboundOutboundInd
		,DocumentNm
		,CreationDt
		,Document_Description
		,SourceApplicationCd
		,TargetApplicationCd
		,Note
		,AlternateDocumentID
		,DocumentID
		,GatewayNm
		,SenderId
		,ReceiverId
		,RoutingSystemNm
		,InternalFileTransferInd
		,InterchangeDate
		,InterchangeTime
		,ExternalTargetInd
		,MessageSequenceNbr
		,ExpectedMessageCnt
		,PIIdataInd
		,PCIdataInd
		,PHIdataInd
		,BusinessSensitivityLevel_Code
		,BusinessSensitivityLevel_Description
		,BusinessSensitivityLevel_ShortDescription
		,DataClassificationLevel_Code
		,DataClassificationLevel_Description
		,DataClassificationLevel_ShortDescription
		,RecordTypeCd
		,ActionTypeCd
		,PartnerEffectiveTimePeriod_typeCode
		,PartnerProfileEffectiveTimePeriod_typeCode
		,Status_StatusTypeCd_Type
		,PartnerSiteEffectiveTimePeriod_typeCode
		,PartnerSiteStatus_StatusTypeCd_Type
		,Document_ReleaseId
		,Document_VersionId
		,Document_SystemEnvironmentCd
          ,CURRENT_TIMESTAMP AS DW_CreateTs
    FROM
	(
	SELECT
		GET(DocumentData.value, '@ReleaseId')::string AS Document_ReleaseId
		,GET(DocumentData.value, '@VersionId')::string AS Document_VersionId
		,GET(DocumentData.value, '@SystemEnvironmentCd')::string AS Document_SystemEnvironmentCd
		,XMLGET(DocumentData.value, 'Abs:InboundOutboundInd'):"$"::string AS InboundOutboundInd
		,XMLGET(DocumentData.value, 'Abs:DocumentNm'):"$"::string AS DocumentNm
		,XMLGET(DocumentData.value, 'Abs:CreationDt'):"$"::string AS CreationDt
		,XMLGET(DocumentData.value, 'Abs:Description'):"$"::string AS Document_Description
		,XMLGET(DocumentData.value, 'Abs:SourceApplicationCd'):"$"::string AS SourceApplicationCd
		,XMLGET(DocumentData.value, 'Abs:TargetApplicationCd'):"$"::string AS TargetApplicationCd
		,XMLGET(DocumentData.value, 'Abs:Note'):"$"::string AS Note
		,XMLGET(DocumentData.value, 'Abs:AlternateDocumentID'):"$"::string AS AlternateDocumentID
		,XMLGET(DocumentData.value, 'Abs:DocumentID'):"$"::string AS DocumentID
		,XMLGET(DocumentData.value, 'Abs:GatewayNm'):"$"::string AS GatewayNm
		,XMLGET(DocumentData.value, 'Abs:SenderId'):"$"::string AS SenderId
		,XMLGET(DocumentData.value, 'Abs:ReceiverId'):"$"::string AS ReceiverId
		,XMLGET(DocumentData.value, 'Abs:RoutingSystemNm'):"$"::string AS RoutingSystemNm
		,XMLGET(DocumentData.value, 'Abs:InternalFileTransferInd'):"$"::string AS InternalFileTransferInd
		,XMLGET(DocumentData.value, 'Abs:InterchangeDate'):"$"::string AS InterchangeDate
		,XMLGET(DocumentData.value, 'Abs:InterchangeTime'):"$"::string AS InterchangeTime
		,XMLGET(DocumentData.value, 'Abs:ExternalTargetInd'):"$"::string AS ExternalTargetInd
		,XMLGET(DocumentData.value, 'Abs:MessageSequenceNbr'):"$"::string AS MessageSequenceNbr
		,XMLGET(DocumentData.value, 'Abs:ExpectedMessageCnt'):"$"::string AS ExpectedMessageCnt
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
		,DocumentData.SEQ::integer as SEQ1
		,DocumentData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) DocumentData
	WHERE	GetBusinessPartner.value like '<DocumentData>%'
	AND	(DocumentData.value like '<Document>%' OR DocumentData.value like '<Document%ReleaseId%' OR DocumentData.value like '<Document%VersionId%' OR DocumentData.value like '<Document%SystemEnvironmentCd%' )
) Document 
LEFT JOIN
(
	SELECT
		XMLGET(Document.value, 'Abs:PIIdataInd'):"$"::string AS PIIdataInd
		,XMLGET(Document.value, 'Abs:PCIdataInd'):"$"::string AS PCIdataInd
		,XMLGET(Document.value, 'Abs:PHIdataInd'):"$"::string AS PHIdataInd
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
		,DocumentData.SEQ::integer as SEQ1
		,DocumentData.index::integer as idx1
		,Document.SEQ::integer as SEQ2
		,Document.index::integer as idx2
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) DocumentData
		,LATERAL FLATTEN(TO_ARRAY(DocumentData.value:"$")) Document
	WHERE	GetBusinessPartner.value like '<DocumentData>%'
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
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
		,DocumentData.SEQ::integer as SEQ1
		,DocumentData.index::integer as idx1
		,Document.SEQ::integer as SEQ2
		,Document.index::integer as idx2
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) DocumentData
		,LATERAL FLATTEN(TO_ARRAY(DocumentData.value:"$")) Document
		,LATERAL FLATTEN(TO_ARRAY(Document.value:"$")) DataClassification
	WHERE	GetBusinessPartner.value like '<DocumentData>%'
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
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
		,DocumentData.SEQ::integer as SEQ1
		,DocumentData.index::integer as idx1
		,Document.SEQ::integer as SEQ2
		,Document.index::integer as idx2
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) DocumentData
		,LATERAL FLATTEN(TO_ARRAY(DocumentData.value:"$")) Document
		,LATERAL FLATTEN(TO_ARRAY(Document.value:"$")) DataClassification
	WHERE	GetBusinessPartner.value like '<DocumentData>%'
	AND	(DocumentData.value like '<Document>%' OR DocumentData.value like '<Document%ReleaseId%' OR DocumentData.value like '<Document%VersionId%' OR DocumentData.value like '<Document%SystemEnvironmentCd%' )
	AND	(Document.value like '<Abs:DataClassification>%' )
	AND	(DataClassification.value like '<Abs:DataClassificationLevel>%' )
) DataClassificationLevel on DataClassificationLevel.SEQ = DataClassification.SEQ AND DataClassificationLevel.idx = DataClassification.idx
	AND	DataClassificationLevel.SEQ1 = DataClassification.SEQ1 AND DataClassificationLevel.idx1 = DataClassification.idx1
	AND	DataClassificationLevel.SEQ2 = DataClassification.SEQ2 AND DataClassificationLevel.idx2 = DataClassification.idx2
LEFT JOIN
(
	SELECT
		XMLGET(DocumentData.value, 'Abs:RecordTypeCd'):"$"::string AS RecordTypeCd
		,XMLGET(DocumentData.value, 'Abs:ActionTypeCd'):"$"::string AS ActionTypeCd
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) DocumentData
	WHERE	GetBusinessPartner.value like '<DocumentData>%'
	AND	(DocumentData.value like '<DocumentAction>%' )
) DocumentAction on DocumentAction.SEQ = Document.SEQ AND DocumentAction.idx = Document.idx
left join 
(
SELECT 
		FILENAME
		,BODnm
		,XMLGET(GetBusinessPartner.value, 'Abs:CustomerAccountNbr'):"$"::string AS CustomerAccountNbr
		,XMLGET(GetBusinessPartner.value, 'Abs:InternalParnterInd'):"$"::string AS InternalParnterInd
		,XMLGET(GetBusinessPartner.value, 'Abs:PartnerId'):"$"::string AS BusinessPartnerData_PartnerId
		,XMLGET(GetBusinessPartner.value, 'Abs:CustomerSiteNbr'):"$"::string AS CustomerSiteNbr
		,XMLGET(GetBusinessPartner.value, 'Abs:VendorNbr'):"$"::string AS VendorNbr
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
	WHERE	GetBusinessPartner.value like '<BusinessPartnerData>%'
) BusinessPartnerData on BusinessPartnerData.SEQ = Document.SEQ
LEFT JOIN
(
	SELECT
		XMLGET(BusinessPartnerData.value, 'Abs:UpdateTs'):"$"::string AS PartnerAuditData_UpdateTs
		,XMLGET(BusinessPartnerData.value, 'Abs:UpdateUserId'):"$"::string AS PartnerAuditData_UpdateUserId
		,XMLGET(BusinessPartnerData.value, 'Abs:UpdateDtTm'):"$"::string AS PartnerAuditData_UpdateDtTm
		,XMLGET(BusinessPartnerData.value, 'Abs:CreateUserId'):"$"::string AS PartnerAuditData_CreateUserId
		,XMLGET(BusinessPartnerData.value, 'Abs:CreateTs'):"$"::string AS PartnerAuditData_CreateTs
		,XMLGET(BusinessPartnerData.value, 'Abs:CreateDtTm'):"$"::string AS PartnerAuditData_CreateDtTm
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) BusinessPartnerData
	WHERE	GetBusinessPartner.value like '<BusinessPartnerData>%'
	AND	(BusinessPartnerData.value like '<Abs:PartnerAuditData>%' )
) PartnerAuditData on PartnerAuditData.SEQ = BusinessPartnerData.SEQ AND PartnerAuditData.idx = BusinessPartnerData.idx
LEFT JOIN
(
	SELECT
		GET(BusinessPartnerData.value, '@Abs:typeCode')::string AS PartnerEffectiveTimePeriod_typeCode
		,XMLGET(BusinessPartnerData.value, 'Abs:FirstEffectiveTm'):"$"::string AS PartnerEffectiveTimePeriod_FirstEffectiveTm
		,XMLGET(BusinessPartnerData.value, 'Abs:FirstEffectiveDt'):"$"::string AS PartnerEffectiveTimePeriod_FirstEffectiveDt
		,XMLGET(BusinessPartnerData.value, 'Abs:LastEffectiveTm'):"$"::string AS PartnerEffectiveTimePeriod_LastEffectiveTm
		,XMLGET(BusinessPartnerData.value, 'Abs:LastEffectiveDt'):"$"::string AS PartnerEffectiveTimePeriod_LastEffectiveDt
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) BusinessPartnerData
	WHERE	GetBusinessPartner.value like '<BusinessPartnerData>%'
	AND	(BusinessPartnerData.value like '<Abs:PartnerEffectiveTimePeriod>%' OR BusinessPartnerData.value like '<Abs:PartnerEffectiveTimePeriod%typeCode%' )
) PartnerEffectiveTimePeriod on PartnerEffectiveTimePeriod.SEQ = BusinessPartnerData.SEQ AND PartnerEffectiveTimePeriod.idx = BusinessPartnerData.idx
LEFT JOIN
(
	SELECT
		XMLGET(BusinessPartnerData.value, 'Abs:PartnerNm'):"$"::string AS PartnerProfile_PartnerNm
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
		,BusinessPartnerData.SEQ::integer as SEQ1
		,BusinessPartnerData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) BusinessPartnerData
	WHERE	GetBusinessPartner.value like '<BusinessPartnerData>%'
	AND	(BusinessPartnerData.value like '<Abs:PartnerProfile>%' )
) PartnerProfile on PartnerProfile.SEQ = BusinessPartnerData.SEQ AND PartnerProfile.idx = BusinessPartnerData.idx
LEFT JOIN
(
	SELECT
		XMLGET(PartnerProfile.value, 'Abs:ContractId'):"$"::string AS ContractId
		,XMLGET(PartnerProfile.value, 'Abs:ContractNm'):"$"::string AS ContractNm
		,XMLGET(PartnerProfile.value, 'Abs:ContractDsc'):"$"::string AS ContractDsc
		,XMLGET(PartnerProfile.value, 'Abs:ContractStartDt'):"$"::string AS ContractStartDt
		,XMLGET(PartnerProfile.value, 'Abs:ContractEndDt'):"$"::string AS ContractEndDt
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
		,BusinessPartnerData.SEQ::integer as SEQ1
		,BusinessPartnerData.index::integer as idx1
		,PartnerProfile.SEQ::integer as SEQ2
		,PartnerProfile.index::integer as idx2
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) BusinessPartnerData
		,LATERAL FLATTEN(TO_ARRAY(BusinessPartnerData.value:"$")) PartnerProfile
	WHERE	GetBusinessPartner.value like '<BusinessPartnerData>%'
	AND	(BusinessPartnerData.value like '<Abs:PartnerProfile>%' )
	AND	(PartnerProfile.value like '<Abs:BusinessContractType>%' )
) BusinessContractType on BusinessContractType.SEQ = PartnerProfile.SEQ AND BusinessContractType.idx = PartnerProfile.idx
	AND	BusinessContractType.SEQ1 = PartnerProfile.SEQ1 AND BusinessContractType.idx1 = PartnerProfile.idx1
LEFT JOIN
(
	SELECT
		XMLGET(BusinessContractType.value, 'Abs:UserId'):"$"::string AS UserId
		,XMLGET(BusinessContractType.value, 'Abs:LastNm'):"$"::string AS LastNm
		,XMLGET(BusinessContractType.value, 'Abs:FirstNm'):"$"::string AS FirstNm
		,XMLGET(BusinessContractType.value, 'Abs:CreateDtTm'):"$"::string AS ContractByType_CreateDtTm
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
		,BusinessPartnerData.SEQ::integer as SEQ1
		,BusinessPartnerData.index::integer as idx1
		,PartnerProfile.SEQ::integer as SEQ2
		,PartnerProfile.index::integer as idx2
		,BusinessContractType.SEQ::integer as SEQ3
		,BusinessContractType.index::integer as idx3
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) BusinessPartnerData
		,LATERAL FLATTEN(TO_ARRAY(BusinessPartnerData.value:"$")) PartnerProfile
		,LATERAL FLATTEN(TO_ARRAY(PartnerProfile.value:"$")) BusinessContractType
	WHERE	GetBusinessPartner.value like '<BusinessPartnerData>%'
	AND	(BusinessPartnerData.value like '<Abs:PartnerProfile>%' )
	AND	(PartnerProfile.value like '<Abs:BusinessContractType>%' )
	AND	(BusinessContractType.value like '<Abs:ContractByType>%' )
) ContractByType on ContractByType.SEQ = BusinessContractType.SEQ AND ContractByType.idx = BusinessContractType.idx
	AND	ContractByType.SEQ1 = BusinessContractType.SEQ1 AND ContractByType.idx1 = BusinessContractType.idx1
	AND	ContractByType.SEQ2 = BusinessContractType.SEQ2 AND ContractByType.idx2 = BusinessContractType.idx2
LEFT JOIN
(
	SELECT
		XMLGET(ContractByType.value, 'Abs:Code'):"$"::string AS ReasonType_Code
		,XMLGET(ContractByType.value, 'Abs:Description'):"$"::string AS ReasonType_Description
		,XMLGET(ContractByType.value, 'Abs:ShortDescription'):"$"::string AS ReasonType_ShortDescription
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
		,BusinessPartnerData.SEQ::integer as SEQ1
		,BusinessPartnerData.index::integer as idx1
		,PartnerProfile.SEQ::integer as SEQ2
		,PartnerProfile.index::integer as idx2
		,BusinessContractType.SEQ::integer as SEQ3
		,BusinessContractType.index::integer as idx3
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) BusinessPartnerData
		,LATERAL FLATTEN(TO_ARRAY(BusinessPartnerData.value:"$")) PartnerProfile
		,LATERAL FLATTEN(TO_ARRAY(PartnerProfile.value:"$")) BusinessContractType
		,LATERAL FLATTEN(TO_ARRAY(BusinessContractType.value:"$")) ContractByType
	WHERE	GetBusinessPartner.value like '<BusinessPartnerData>%'
	AND	(BusinessPartnerData.value like '<Abs:PartnerProfile>%' )
	AND	(PartnerProfile.value like '<Abs:BusinessContractType>%' )
	AND	(BusinessContractType.value like '<Abs:ContractByType>%' )
	AND	(ContractByType.value like '<Abs:ReasonType>%' )
) ReasonType on ReasonType.SEQ = ContractByType.SEQ AND ReasonType.idx = ContractByType.idx
	AND	ReasonType.SEQ1 = ContractByType.SEQ1 AND ReasonType.idx1 = ContractByType.idx1
	AND	ReasonType.SEQ2 = ContractByType.SEQ2 AND ReasonType.idx2 = ContractByType.idx2
	AND	ReasonType.SEQ3 = ContractByType.SEQ3 AND ReasonType.idx3 = ContractByType.idx3
LEFT JOIN
(
	SELECT
		XMLGET(PartnerProfile.value, 'Abs:OrderLimitCnt'):"$"::string AS OrderLimitCnt
		,XMLGET(PartnerProfile.value, 'Abs:MaximumItemCnt'):"$"::string AS MaximumItemCnt
		,XMLGET(PartnerProfile.value, 'Abs:MinimumToteCnt'):"$"::string AS MinimumToteCnt
		,XMLGET(PartnerProfile.value, 'Abs:MaximumToteCnt'):"$"::string AS MaximumToteCnt
		,XMLGET(PartnerProfile.value, 'Abs:OrderAllocationPct'):"$"::string AS OrderAllocationPct
		,XMLGET(PartnerProfile.value, 'Abs:MileageNbr'):"$"::string AS MileageNbr
		,XMLGET(PartnerProfile.value, 'Abs:MinimumItemCnt'):"$"::string AS MinimumItemCnt
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
		,BusinessPartnerData.SEQ::integer as SEQ1
		,BusinessPartnerData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) BusinessPartnerData
		,LATERAL FLATTEN(TO_ARRAY(BusinessPartnerData.value:"$")) PartnerProfile
	WHERE	GetBusinessPartner.value like '<BusinessPartnerData>%'
	AND	(BusinessPartnerData.value like '<Abs:PartnerProfile>%' )
	AND	(PartnerProfile.value like '<Abs:ContractThresholdType>%' )
) ContractThresholdType on ContractThresholdType.SEQ = PartnerProfile.SEQ AND ContractThresholdType.idx = PartnerProfile.idx
	AND	ContractThresholdType.SEQ1 = PartnerProfile.SEQ1 AND ContractThresholdType.idx1 = PartnerProfile.idx1
LEFT JOIN
(
	SELECT
		XMLGET(PartnerProfile.value, 'Abs:NotesTxt'):"$"::string AS NotesTxt
		,XMLGET(PartnerProfile.value, 'Abs:NotesTypeCd'):"$"::string AS NotesTypeCd
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
		,BusinessPartnerData.SEQ::integer as SEQ1
		,BusinessPartnerData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) BusinessPartnerData
		,LATERAL FLATTEN(TO_ARRAY(BusinessPartnerData.value:"$")) PartnerProfile
	WHERE	GetBusinessPartner.value like '<BusinessPartnerData>%'
	AND	(BusinessPartnerData.value like '<Abs:PartnerProfile>%' )
	AND	(PartnerProfile.value like '<Abs:NotesType>%' )
) NotesType on NotesType.SEQ = PartnerProfile.SEQ AND NotesType.idx = PartnerProfile.idx
	AND	NotesType.SEQ1 = PartnerProfile.SEQ1 AND NotesType.idx1 = PartnerProfile.idx1
LEFT JOIN
(
	SELECT
		XMLGET(PartnerProfile.value, 'Abs:OrganizationTypeCd'):"$"::string AS PartnerProfile_OrganizationTypeCd
		,XMLGET(PartnerProfile.value, 'Abs:OrganizationValueTxt'):"$"::string AS PartnerProfile_OrganizationValueTxt
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
		,BusinessPartnerData.SEQ::integer as SEQ1
		,BusinessPartnerData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) BusinessPartnerData
		,LATERAL FLATTEN(TO_ARRAY(BusinessPartnerData.value:"$")) PartnerProfile
	WHERE	GetBusinessPartner.value like '<BusinessPartnerData>%'
	AND	(BusinessPartnerData.value like '<Abs:PartnerProfile>%' )
	AND	(PartnerProfile.value like '<Abs:OrganizationType>%' )
) PartnerProfile_OrganizationType on PartnerProfile_OrganizationType.SEQ = PartnerProfile.SEQ AND PartnerProfile_OrganizationType.idx = PartnerProfile.idx
	AND	PartnerProfile_OrganizationType.SEQ1 = PartnerProfile.SEQ1 AND PartnerProfile_OrganizationType.idx1 = PartnerProfile.idx1
LEFT JOIN
(
	SELECT
		XMLGET(PartnerProfile.value, 'Abs:PartnerId'):"$"::string AS Overrides_PartnerId
		,XMLGET(PartnerProfile.value, 'Abs:PartnerNm'):"$"::string AS Overrides_PartnerNm
		,XMLGET(PartnerProfile.value, 'Abs:OverrideInd'):"$"::string AS OverrideInd
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
		,BusinessPartnerData.SEQ::integer as SEQ1
		,BusinessPartnerData.index::integer as idx1
		,PartnerProfile.SEQ::integer as SEQ2
		,PartnerProfile.index::integer as idx2
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) BusinessPartnerData
		,LATERAL FLATTEN(TO_ARRAY(BusinessPartnerData.value:"$")) PartnerProfile
	WHERE	GetBusinessPartner.value like '<BusinessPartnerData>%'
	AND	(BusinessPartnerData.value like '<Abs:PartnerProfile>%' )
	AND	(PartnerProfile.value like '<Abs:Overrides>%' )
) Overrides on Overrides.SEQ = PartnerProfile.SEQ AND Overrides.idx = PartnerProfile.idx
	AND	Overrides.SEQ1 = PartnerProfile.SEQ1 AND Overrides.idx1 = PartnerProfile.idx1
LEFT JOIN
(
	SELECT
		XMLGET(Overrides.value, 'Abs:Description'):"$"::string AS OverrideReasonType_Description
		,XMLGET(Overrides.value, 'Abs:ShortDescription'):"$"::string AS OverrideReasonType_ShortDescription
		,XMLGET(Overrides.value, 'Abs:Code'):"$"::string AS OverrideReasonType_Code
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
		,BusinessPartnerData.SEQ::integer as SEQ1
		,BusinessPartnerData.index::integer as idx1
		,PartnerProfile.SEQ::integer as SEQ2
		,PartnerProfile.index::integer as idx2
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) BusinessPartnerData
		,LATERAL FLATTEN(TO_ARRAY(BusinessPartnerData.value:"$")) PartnerProfile
		,LATERAL FLATTEN(TO_ARRAY(PartnerProfile.value:"$")) Overrides
	WHERE	GetBusinessPartner.value like '<BusinessPartnerData>%'
	AND	(BusinessPartnerData.value like '<Abs:PartnerProfile>%' )
	AND	(PartnerProfile.value like '<Abs:Overrides>%' )
	AND	(Overrides.value like '<Abs:OverrideReasonType>%' )
) OverrideReasonType on OverrideReasonType.SEQ = Overrides.SEQ AND OverrideReasonType.idx = Overrides.idx
	AND	OverrideReasonType.SEQ1 = Overrides.SEQ1 AND OverrideReasonType.idx1 = Overrides.idx1
	AND	OverrideReasonType.SEQ2 = Overrides.SEQ2 AND OverrideReasonType.idx2 = Overrides.idx2
LEFT JOIN
(
	SELECT
		XMLGET(Overrides.value, 'Abs:ShortDescription'):"$"::string AS OverrideType_ShortDescription
		,XMLGET(Overrides.value, 'Abs:Description'):"$"::string AS OverrideType_Description
		,XMLGET(Overrides.value, 'Abs:Code'):"$"::string AS OverrideType_Code
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
		,BusinessPartnerData.SEQ::integer as SEQ1
		,BusinessPartnerData.index::integer as idx1
		,PartnerProfile.SEQ::integer as SEQ2
		,PartnerProfile.index::integer as idx2
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) BusinessPartnerData
		,LATERAL FLATTEN(TO_ARRAY(BusinessPartnerData.value:"$")) PartnerProfile
		,LATERAL FLATTEN(TO_ARRAY(PartnerProfile.value:"$")) Overrides
	WHERE	GetBusinessPartner.value like '<BusinessPartnerData>%'
	AND	(BusinessPartnerData.value like '<Abs:PartnerProfile>%' )
	AND	(PartnerProfile.value like '<Abs:Overrides>%' )
	AND	(Overrides.value like '<Abs:OverrideType>%' )
) OverrideType on OverrideType.SEQ = Overrides.SEQ AND OverrideType.idx = Overrides.idx
	AND	OverrideType.SEQ1 = Overrides.SEQ1 AND OverrideType.idx1 = Overrides.idx1
	AND	OverrideType.SEQ2 = Overrides.SEQ2 AND OverrideType.idx2 = Overrides.idx2
LEFT JOIN
(
	SELECT
		XMLGET(PartnerContact.value, 'Abs:StateNm'):"$"::string AS Address_StateNm
		,XMLGET(PartnerContact.value, 'Abs:FaxNbr'):"$"::string AS Address_FaxNbr
		,XMLGET(PartnerContact.value, 'Abs:PhoneNbr'):"$"::string AS Address_PhoneNbr
		,XMLGET(PartnerContact.value, 'Abs:TimeZoneCd'):"$"::string AS Address_TimeZoneCd
		,XMLGET(PartnerContact.value, 'Abs:LatitudeDegree'):"$"::string AS Address_LatitudeDegree
		,XMLGET(PartnerContact.value, 'Abs:CountryNm'):"$"::string AS Address_CountryNm
		,XMLGET(PartnerContact.value, 'Abs:CountryCd'):"$"::string AS Address_CountryCd
		,XMLGET(PartnerContact.value, 'Abs:StateCd'):"$"::string AS Address_StateCd
		,XMLGET(PartnerContact.value, 'Abs:LongitudeDegree'):"$"::string AS Address_LongitudeDegree
		,XMLGET(PartnerContact.value, 'Abs:CountyCd'):"$"::string AS Address_CountyCd
		,XMLGET(PartnerContact.value, 'Abs:CountyNm'):"$"::string AS Address_CountyNm
		,XMLGET(PartnerContact.value, 'Abs:CityNm'):"$"::string AS Address_CityNm
		,XMLGET(PartnerContact.value, 'Abs:AddressLine5txt'):"$"::string AS Address_AddressLine5txt
		,XMLGET(PartnerContact.value, 'Abs:AddressLine4txt'):"$"::string AS Address_AddressLine4txt
		,XMLGET(PartnerContact.value, 'Abs:PostalZoneCd'):"$"::string AS Address_PostalZoneCd
		,XMLGET(PartnerContact.value, 'Abs:AddressLine3txt'):"$"::string AS Address_AddressLine3txt
		,XMLGET(PartnerContact.value, 'Abs:AddressLine2txt'):"$"::string AS Address_AddressLine2txt
		,XMLGET(PartnerContact.value, 'Abs:AddressLine1txt'):"$"::string AS Address_AddressLine1txt
		,XMLGET(PartnerContact.value, 'Abs:AddressUsageTypeCd'):"$"::string AS Address_AddressUsageTypeCd
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
		,BusinessPartnerData.SEQ::integer as SEQ1
		,BusinessPartnerData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) BusinessPartnerData
		,LATERAL FLATTEN(TO_ARRAY(BusinessPartnerData.value:"$")) PartnerProfile
		,LATERAL FLATTEN(TO_ARRAY(PartnerProfile.value:"$")) PartnerContact
	WHERE	GetBusinessPartner.value like '<BusinessPartnerData>%'
	AND	(BusinessPartnerData.value like '<Abs:PartnerProfile>%' )
	AND	(PartnerProfile.value like '<Abs:PartnerContact>%' )
	AND	(PartnerContact.value like '<Abs:Address>%' )
) Address on Address.SEQ = PartnerProfile.SEQ AND Address.idx = PartnerProfile.idx
	AND	Address.SEQ1 = PartnerProfile.SEQ1 AND Address.idx1 = PartnerProfile.idx1
LEFT JOIN
(
	SELECT
		XMLGET(PartnerContact.value, 'Abs:PhoneNbr'):"$"::string AS Contact_PhoneNbr
		,XMLGET(PartnerContact.value, 'Abs:EmailAddresstxt'):"$"::string AS Contact_EmailAddresstxt
		,XMLGET(PartnerContact.value, 'Abs:ContactNm'):"$"::string AS Contact_ContactNm
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
		,BusinessPartnerData.SEQ::integer as SEQ1
		,BusinessPartnerData.index::integer as idx1
		,PartnerProfile.SEQ::integer as SEQ2
		,PartnerProfile.index::integer as idx2
		,PartnerContact.SEQ::integer as SEQ3
		,PartnerContact.index::integer as idx3
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) BusinessPartnerData
		,LATERAL FLATTEN(TO_ARRAY(BusinessPartnerData.value:"$")) PartnerProfile
		,LATERAL FLATTEN(TO_ARRAY(PartnerProfile.value:"$")) PartnerContact
	WHERE	GetBusinessPartner.value like '<BusinessPartnerData>%'
	AND	(BusinessPartnerData.value like '<Abs:PartnerProfile>%' )
	AND	(PartnerProfile.value like '<Abs:PartnerContact>%' )
	AND	(PartnerContact.value like '<Abs:Contact>%' )
) Contact on Contact.SEQ = PartnerProfile.SEQ AND Contact.idx = PartnerProfile.idx
	AND	Contact.SEQ1 = PartnerProfile.SEQ1 AND Contact.idx1 = PartnerProfile.idx1
LEFT JOIN
(
	SELECT
		XMLGET(Contact.value, 'Abs:ShortDescription'):"$"::string AS Contact_ShortDescription
		,XMLGET(Contact.value, 'Abs:Description'):"$"::string AS Contact_Description
		,XMLGET(Contact.value, 'Abs:Code'):"$"::string AS Contact_Code
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
		,BusinessPartnerData.SEQ::integer as SEQ1
		,BusinessPartnerData.index::integer as idx1
		,PartnerProfile.SEQ::integer as SEQ2
		,PartnerProfile.index::integer as idx2
		,PartnerContact.SEQ::integer as SEQ3
		,PartnerContact.index::integer as idx3
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) BusinessPartnerData
		,LATERAL FLATTEN(TO_ARRAY(BusinessPartnerData.value:"$")) PartnerProfile
		,LATERAL FLATTEN(TO_ARRAY(PartnerProfile.value:"$")) PartnerContact
		,LATERAL FLATTEN(TO_ARRAY(PartnerContact.value:"$")) Contact
	WHERE	GetBusinessPartner.value like '<BusinessPartnerData>%'
	AND	(BusinessPartnerData.value like '<Abs:PartnerProfile>%' )
	AND	(PartnerProfile.value like '<Abs:PartnerContact>%' )
	AND	(PartnerContact.value like '<Abs:Contact>%' )
	AND	(Contact.value like '<Abs:ContactTypeCd>%' )
) Contact_ContactTypeCd on Contact_ContactTypeCd.SEQ = Contact.SEQ AND Contact_ContactTypeCd.idx = Contact.idx
	AND	Contact_ContactTypeCd.SEQ1 = Contact.SEQ1 AND Contact_ContactTypeCd.idx1 = Contact.idx1
	AND	Contact_ContactTypeCd.SEQ2 = Contact.SEQ2 AND Contact_ContactTypeCd.idx2 = Contact.idx2
	AND	Contact_ContactTypeCd.SEQ3 = Contact.SEQ3 AND Contact_ContactTypeCd.idx3 = Contact.idx3
LEFT JOIN
(
	SELECT
		GET(PartnerProfile.value, '@Abs:typeCode')::string AS PartnerProfileEffectiveTimePeriod_typeCode
		,XMLGET(PartnerProfile.value, 'Abs:FirstEffectiveTm'):"$"::string AS PartnerProfileEffectiveTimePeriod_FirstEffectiveTm
		,XMLGET(PartnerProfile.value, 'Abs:LastEffectiveDt'):"$"::string AS PartnerProfileEffectiveTimePeriod_LastEffectiveDt
		,XMLGET(PartnerProfile.value, 'Abs:LastEffectiveTm'):"$"::string AS PartnerProfileEffectiveTimePeriod_LastEffectiveTm
		,XMLGET(PartnerProfile.value, 'Abs:FirstEffectiveDt'):"$"::string AS PartnerProfileEffectiveTimePeriod_FirstEffectiveDt
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
		,BusinessPartnerData.SEQ::integer as SEQ1
		,BusinessPartnerData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) BusinessPartnerData
		,LATERAL FLATTEN(TO_ARRAY(BusinessPartnerData.value:"$")) PartnerProfile
	WHERE	GetBusinessPartner.value like '<BusinessPartnerData>%'
	AND	(BusinessPartnerData.value like '<Abs:PartnerProfile>%' )
	AND	(PartnerProfile.value like '<Abs:PartnerProfileEffectiveTimePeriod>%' OR PartnerProfile.value like '<Abs:PartnerProfileEffectiveTimePeriod%typeCode%' )
) PartnerProfileEffectiveTimePeriod on PartnerProfileEffectiveTimePeriod.SEQ = PartnerProfile.SEQ AND PartnerProfileEffectiveTimePeriod.idx = PartnerProfile.idx
	AND	PartnerProfileEffectiveTimePeriod.SEQ1 = PartnerProfile.SEQ1 AND PartnerProfileEffectiveTimePeriod.idx1 = PartnerProfile.idx1
LEFT JOIN
(
	SELECT
		XMLGET(PartnerProfile.value, 'Abs:ShortDescription'):"$"::string AS PartnerTypeCd_ShortDescription
		,XMLGET(PartnerProfile.value, 'Abs:Description'):"$"::string AS PartnerTypeCd_Description
		,XMLGET(PartnerProfile.value, 'Abs:Code'):"$"::string AS PartnerTypeCd_Code
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
		,BusinessPartnerData.SEQ::integer as SEQ1
		,BusinessPartnerData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) BusinessPartnerData
		,LATERAL FLATTEN(TO_ARRAY(BusinessPartnerData.value:"$")) PartnerProfile
	WHERE	GetBusinessPartner.value like '<BusinessPartnerData>%'
	AND	(BusinessPartnerData.value like '<Abs:PartnerProfile>%' )
	AND	(PartnerProfile.value like '<Abs:PartnerTypeCd>%' )
) PartnerTypeCd on PartnerTypeCd.SEQ = PartnerProfile.SEQ AND PartnerTypeCd.idx = PartnerProfile.idx
	AND	PartnerTypeCd.SEQ1 = PartnerProfile.SEQ1 AND PartnerTypeCd.idx1 = PartnerProfile.idx1
LEFT JOIN
(
	SELECT
		XMLGET(PartnerProfile.value, 'Abs:AreaTypeCd'):"$"::string AS AreaTypeCd
		,XMLGET(PartnerProfile.value, 'Abs:AreaTypeValueTxt'):"$"::string AS AreaTypeValueTxt
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
		,BusinessPartnerData.SEQ::integer as SEQ1
		,BusinessPartnerData.index::integer as idx1
		,PartnerProfile.SEQ::integer as SEQ2
		,PartnerProfile.index::integer as idx2
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) BusinessPartnerData
		,LATERAL FLATTEN(TO_ARRAY(BusinessPartnerData.value:"$")) PartnerProfile
	WHERE	GetBusinessPartner.value like '<BusinessPartnerData>%'
	AND	(BusinessPartnerData.value like '<Abs:PartnerProfile>%' )
	AND	(PartnerProfile.value like '<Abs:ServiceAreaLocationType>%' )
) ServiceAreaLocationType on ServiceAreaLocationType.SEQ = PartnerProfile.SEQ AND ServiceAreaLocationType.idx = PartnerProfile.idx
	AND	ServiceAreaLocationType.SEQ1 = PartnerProfile.SEQ1 AND ServiceAreaLocationType.idx1 = PartnerProfile.idx1
LEFT JOIN
(
	SELECT
		XMLGET(ServiceAreaLocationType.value, 'Abs:Altitude'):"$"::string AS Altitude
		,XMLGET(ServiceAreaLocationType.value, 'Abs:LongitudeDegree'):"$"::string AS ServiceAreaCoordinateType_LongitudeDegree
		,XMLGET(ServiceAreaLocationType.value, 'Abs:LatitudeDegree'):"$"::string AS ServiceAreaCoordinateType_LatitudeDegree
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
		,BusinessPartnerData.SEQ::integer as SEQ1
		,BusinessPartnerData.index::integer as idx1
		,PartnerProfile.SEQ::integer as SEQ2
		,PartnerProfile.index::integer as idx2
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) BusinessPartnerData
		,LATERAL FLATTEN(TO_ARRAY(BusinessPartnerData.value:"$")) PartnerProfile
		,LATERAL FLATTEN(TO_ARRAY(PartnerProfile.value:"$")) ServiceAreaLocationType
	WHERE	GetBusinessPartner.value like '<BusinessPartnerData>%'
	AND	(BusinessPartnerData.value like '<Abs:PartnerProfile>%' )
	AND	(PartnerProfile.value like '<Abs:ServiceAreaLocationType>%' )
	AND	(ServiceAreaLocationType.value like '<Abs:ServiceAreaCoordinateType>%' )
) ServiceAreaCoordinateType on ServiceAreaCoordinateType.SEQ = ServiceAreaLocationType.SEQ AND ServiceAreaCoordinateType.idx = ServiceAreaLocationType.idx
	AND	ServiceAreaCoordinateType.SEQ1 = ServiceAreaLocationType.SEQ1 AND ServiceAreaCoordinateType.idx1 = ServiceAreaLocationType.idx1
	AND	ServiceAreaCoordinateType.SEQ2 = ServiceAreaLocationType.SEQ2 AND ServiceAreaCoordinateType.idx2 = ServiceAreaLocationType.idx2
LEFT JOIN
(
	SELECT
		XMLGET(PartnerProfile.value, 'Abs:ShortDescription'):"$"::string AS ServiceAreaType_ShortDescription
		,XMLGET(PartnerProfile.value, 'Abs:Description'):"$"::string AS ServiceAreaType_Description
		,XMLGET(PartnerProfile.value, 'Abs:Code'):"$"::string AS ServiceAreaType_Code
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
		,BusinessPartnerData.SEQ::integer as SEQ1
		,BusinessPartnerData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) BusinessPartnerData
		,LATERAL FLATTEN(TO_ARRAY(BusinessPartnerData.value:"$")) PartnerProfile
	WHERE	GetBusinessPartner.value like '<BusinessPartnerData>%'
	AND	(BusinessPartnerData.value like '<Abs:PartnerProfile>%' )
	AND	(PartnerProfile.value like '<Abs:ServiceAreaType>%' )
) ServiceAreaType on ServiceAreaType.SEQ = PartnerProfile.SEQ AND ServiceAreaType.idx = PartnerProfile.idx
	AND	ServiceAreaType.SEQ1 = PartnerProfile.SEQ1 AND ServiceAreaType.idx1 = PartnerProfile.idx1
LEFT JOIN
(
	SELECT
		XMLGET(PartnerProfile.value, 'Abs:ServiceFeeAmt'):"$"::string AS ServiceFeeAmt
		,XMLGET(PartnerProfile.value, 'Abs:ServiceFeeItemId'):"$"::string AS ServiceFeeItemId
		,XMLGET(PartnerProfile.value, 'Abs:ServiceFeeTypeCd'):"$"::string AS ServiceFeeTypeCd
		,XMLGET(PartnerProfile.value, 'Abs:ServiceFeeCategoryCd'):"$"::string AS ServiceFeeCategoryCd
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
		,BusinessPartnerData.SEQ::integer as SEQ1
		,BusinessPartnerData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) BusinessPartnerData
		,LATERAL FLATTEN(TO_ARRAY(BusinessPartnerData.value:"$")) PartnerProfile
	WHERE	GetBusinessPartner.value like '<BusinessPartnerData>%'
	AND	(BusinessPartnerData.value like '<Abs:PartnerProfile>%' )
	AND	(PartnerProfile.value like '<Abs:ServiceFeeType>%' )
) ServiceFeeType on ServiceFeeType.SEQ = PartnerProfile.SEQ AND ServiceFeeType.idx = PartnerProfile.idx
	AND	ServiceFeeType.SEQ1 = PartnerProfile.SEQ1 AND ServiceFeeType.idx1 = PartnerProfile.idx1
LEFT JOIN
(
	SELECT
		XMLGET(PartnerProfile.value, 'Abs:Code'):"$"::string AS ServiceLevelType_Code
		,XMLGET(PartnerProfile.value, 'Abs:Description'):"$"::string AS ServiceLevelType_Description
		,XMLGET(PartnerProfile.value, 'Abs:ShortDescription'):"$"::string AS ServiceLevelType_ShortDescription
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
		,BusinessPartnerData.SEQ::integer as SEQ1
		,BusinessPartnerData.index::integer as idx1
		,PartnerProfile.SEQ::integer as SEQ2
		,PartnerProfile.index::integer as idx2
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) BusinessPartnerData
		,LATERAL FLATTEN(TO_ARRAY(BusinessPartnerData.value:"$")) PartnerProfile
	WHERE	GetBusinessPartner.value like '<BusinessPartnerData>%'
	AND	(BusinessPartnerData.value like '<Abs:PartnerProfile>%' )
	AND	(PartnerProfile.value like '<Abs:ServiceLevelType>%' )
) ServiceLevelType on ServiceLevelType.SEQ = PartnerProfile.SEQ AND ServiceLevelType.idx = PartnerProfile.idx
	AND	ServiceLevelType.SEQ1 = PartnerProfile.SEQ1 AND ServiceLevelType.idx1 = PartnerProfile.idx1
LEFT JOIN
(
	SELECT
		XMLGET(ServiceLevelType.value, 'Abs:Code'):"$"::string AS ActivityType_Code
		,XMLGET(ServiceLevelType.value, 'Abs:Description'):"$"::string AS ActivityType_Description
		,XMLGET(ServiceLevelType.value, 'Abs:ShortDescription'):"$"::string AS ActivityType_ShortDescription
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
		,BusinessPartnerData.SEQ::integer as SEQ1
		,BusinessPartnerData.index::integer as idx1
		,PartnerProfile.SEQ::integer as SEQ2
		,PartnerProfile.index::integer as idx2
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) BusinessPartnerData
		,LATERAL FLATTEN(TO_ARRAY(BusinessPartnerData.value:"$")) PartnerProfile
		,LATERAL FLATTEN(TO_ARRAY(PartnerProfile.value:"$")) ServiceLevelType
	WHERE	GetBusinessPartner.value like '<BusinessPartnerData>%'
	AND	(BusinessPartnerData.value like '<Abs:PartnerProfile>%' )
	AND	(PartnerProfile.value like '<Abs:ServiceLevelType>%' )
	AND	(ServiceLevelType.value like '<Abs:ActivityType>%' )
) ActivityType on ActivityType.SEQ = ServiceLevelType.SEQ AND ActivityType.idx = ServiceLevelType.idx
	AND	ActivityType.SEQ1 = ServiceLevelType.SEQ1 AND ActivityType.idx1 = ServiceLevelType.idx1
	AND	ActivityType.SEQ2 = ServiceLevelType.SEQ2 AND ActivityType.idx2 = ServiceLevelType.idx2
LEFT JOIN
(
	SELECT
		GET(XMLGET(PartnerProfile.value, 'Abs:StatusTypeCd'), '@Type')::string AS Status_StatusTypeCd_Type
		,XMLGET(PartnerProfile.value, 'Abs:StatusTypeCd'):"$"::string AS Status_StatusTypeCd
		,XMLGET(PartnerProfile.value, 'Abs:Description'):"$"::string AS Status_Description
		,XMLGET(PartnerProfile.value, 'Abs:EffectiveDtTm'):"$"::string AS Status_EffectiveDtTm
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
		,BusinessPartnerData.SEQ::integer as SEQ1
		,BusinessPartnerData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) BusinessPartnerData
		,LATERAL FLATTEN(TO_ARRAY(BusinessPartnerData.value:"$")) PartnerProfile
	WHERE	GetBusinessPartner.value like '<BusinessPartnerData>%'
	AND	(BusinessPartnerData.value like '<Abs:PartnerProfile>%' )
	AND	(PartnerProfile.value like '<Abs:Status>%' )
) Status on Status.SEQ = PartnerProfile.SEQ AND Status.idx = PartnerProfile.idx
	AND	Status.SEQ1 = PartnerProfile.SEQ1 AND Status.idx1 = PartnerProfile.idx1
LEFT JOIN
(
	SELECT
		XMLGET(BusinessPartnerData.value, 'Abs:PartnerParticipantId'):"$"::string AS PartnerParticipantId
		,XMLGET(BusinessPartnerData.value, 'Abs:PartnerSiteNm'):"$"::string AS PartnerSiteNm
		,XMLGET(BusinessPartnerData.value, 'Abs:PartnerSiteId'):"$"::string AS PartnerSiteId
		,XMLGET(BusinessPartnerData.value, 'Abs:PartnerSiteActiveInd'):"$"::string AS PartnerSiteActiveInd
		,XMLGET(BusinessPartnerData.value, 'Abs:PartnerSiteCommentTxt'):"$"::string AS PartnerSiteCommentTxt
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
		,BusinessPartnerData.SEQ::integer as SEQ1
		,BusinessPartnerData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) BusinessPartnerData
	WHERE	GetBusinessPartner.value like '<BusinessPartnerData>%'
	AND	(BusinessPartnerData.value like '<Abs:PartnerSiteData>%' )
) PartnerSiteData on PartnerSiteData.SEQ = BusinessPartnerData.SEQ AND PartnerSiteData.idx = BusinessPartnerData.idx
LEFT JOIN
(
	SELECT
		XMLGET(PartnerSiteData.value, 'Abs:OrganizationValueTxt'):"$"::string AS PartnerSiteData_OrganizationValueTxt
		,XMLGET(PartnerSiteData.value, 'Abs:OrganizationTypeCd'):"$"::string AS PartnerSiteData_OrganizationTypeCd
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
		,BusinessPartnerData.SEQ::integer as SEQ1
		,BusinessPartnerData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) BusinessPartnerData
		,LATERAL FLATTEN(TO_ARRAY(BusinessPartnerData.value:"$")) PartnerSiteData
	WHERE	GetBusinessPartner.value like '<BusinessPartnerData>%'
	AND	(BusinessPartnerData.value like '<Abs:PartnerSiteData>%' )
	AND	(PartnerSiteData.value like '<Abs:OrganizationType>%' )
) PartnerSiteData_OrganizationType on PartnerSiteData_OrganizationType.SEQ = PartnerSiteData.SEQ AND PartnerSiteData_OrganizationType.idx = PartnerSiteData.idx
	AND	PartnerSiteData_OrganizationType.SEQ1 = PartnerSiteData.SEQ1 AND PartnerSiteData_OrganizationType.idx1 = PartnerSiteData.idx1
LEFT JOIN
(
	SELECT
		XMLGET(PartnerSiteData.value, 'Abs:CreateTs'):"$"::string AS PartnerSiteAuditData_CreateTs
		,XMLGET(PartnerSiteData.value, 'Abs:UpdateUserId'):"$"::string AS PartnerSiteAuditData_UpdateUserId
		,XMLGET(PartnerSiteData.value, 'Abs:UpdateTs'):"$"::string AS PartnerSiteAuditData_UpdateTs
		,XMLGET(PartnerSiteData.value, 'Abs:UpdateDtTm'):"$"::string AS PartnerSiteAuditData_UpdateDtTm
		,XMLGET(PartnerSiteData.value, 'Abs:CreateUserId'):"$"::string AS PartnerSiteAuditData_CreateUserId
		,XMLGET(PartnerSiteData.value, 'Abs:CreateDtTm'):"$"::string AS PartnerSiteAuditData_CreateDtTm
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
		,BusinessPartnerData.SEQ::integer as SEQ1
		,BusinessPartnerData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) BusinessPartnerData
		,LATERAL FLATTEN(TO_ARRAY(BusinessPartnerData.value:"$")) PartnerSiteData
	WHERE	GetBusinessPartner.value like '<BusinessPartnerData>%'
	AND	(BusinessPartnerData.value like '<Abs:PartnerSiteData>%' )
	AND	(PartnerSiteData.value like '<Abs:PartnerSiteAuditData>%' )
) PartnerSiteAuditData on PartnerSiteAuditData.SEQ = PartnerSiteData.SEQ AND PartnerSiteAuditData.idx = PartnerSiteData.idx
	AND	PartnerSiteAuditData.SEQ1 = PartnerSiteData.SEQ1 AND PartnerSiteAuditData.idx1 = PartnerSiteData.idx1
LEFT JOIN
(
	SELECT
		XMLGET(PartnerSiteContact.value, 'Abs:AddressLine1txt'):"$"::string AS SiteAddress_AddressLine1txt
		,XMLGET(PartnerSiteContact.value, 'Abs:AddressLine2txt'):"$"::string AS SiteAddress_AddressLine2txt
		,XMLGET(PartnerSiteContact.value, 'Abs:AddressLine3txt'):"$"::string AS SiteAddress_AddressLine3txt
		,XMLGET(PartnerSiteContact.value, 'Abs:LongitudeDegree'):"$"::string AS SiteAddress_LongitudeDegree
		,XMLGET(PartnerSiteContact.value, 'Abs:LatitudeDegree'):"$"::string AS SiteAddress_LatitudeDegree
		,XMLGET(PartnerSiteContact.value, 'Abs:StateCd'):"$"::string AS SiteAddress_StateCd
		,XMLGET(PartnerSiteContact.value, 'Abs:StateNm'):"$"::string AS SiteAddress_StateNm
		,XMLGET(PartnerSiteContact.value, 'Abs:CountryCd'):"$"::string AS SiteAddress_CountryCd
		,XMLGET(PartnerSiteContact.value, 'Abs:AddressLine4txt'):"$"::string AS SiteAddress_AddressLine4txt
		,XMLGET(PartnerSiteContact.value, 'Abs:AddressUsageTypeCd'):"$"::string AS SiteAddress_AddressUsageTypeCd
		,XMLGET(PartnerSiteContact.value, 'Abs:CityNm'):"$"::string AS SiteAddress_CityNm
		,XMLGET(PartnerSiteContact.value, 'Abs:CountyNm'):"$"::string AS SiteAddress_CountyNm
		,XMLGET(PartnerSiteContact.value, 'Abs:CountyCd'):"$"::string AS SiteAddress_CountyCd
		,XMLGET(PartnerSiteContact.value, 'Abs:PostalZoneCd'):"$"::string AS SiteAddress_PostalZoneCd
		,XMLGET(PartnerSiteContact.value, 'Abs:CountryNm'):"$"::string AS SiteAddress_CountryNm
		,XMLGET(PartnerSiteContact.value, 'Abs:AddressLine5txt'):"$"::string AS SiteAddress_AddressLine5txt
		,XMLGET(PartnerSiteContact.value, 'Abs:PhoneNbr'):"$"::string AS SiteAddress_PhoneNbr
		,XMLGET(PartnerSiteContact.value, 'Abs:TimeZoneCd'):"$"::string AS SiteAddress_TimeZoneCd
		,XMLGET(PartnerSiteContact.value, 'Abs:FaxNbr'):"$"::string AS SiteAddress_FaxNbr
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
		,BusinessPartnerData.SEQ::integer as SEQ1
		,BusinessPartnerData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) BusinessPartnerData
		,LATERAL FLATTEN(TO_ARRAY(BusinessPartnerData.value:"$")) PartnerSiteData
		,LATERAL FLATTEN(TO_ARRAY(PartnerSiteData.value:"$")) PartnerSiteContact
	WHERE	GetBusinessPartner.value like '<BusinessPartnerData>%'
	AND	(BusinessPartnerData.value like '<Abs:PartnerSiteData>%' )
	AND	(PartnerSiteData.value like '<Abs:PartnerSiteContact>%' )
	AND	(PartnerSiteContact.value like '<Abs:SiteAddress>%' )
) SiteAddress on SiteAddress.SEQ = PartnerSiteData.SEQ AND SiteAddress.idx = PartnerSiteData.idx
	AND	SiteAddress.SEQ1 = PartnerSiteData.SEQ1 AND SiteAddress.idx1 = PartnerSiteData.idx1
LEFT JOIN
(
	SELECT
		XMLGET(PartnerSiteContact.value, 'Abs:ContactNm'):"$"::string AS SiteContact_ContactNm
		,XMLGET(PartnerSiteContact.value, 'Abs:PhoneNbr'):"$"::string AS SiteContact_PhoneNbr
		,XMLGET(PartnerSiteContact.value, 'Abs:EmailAddresstxt'):"$"::string AS SiteContact_EmailAddresstxt
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
		,BusinessPartnerData.SEQ::integer as SEQ1
		,BusinessPartnerData.index::integer as idx1
		,PartnerSiteData.SEQ::integer as SEQ2
		,PartnerSiteData.index::integer as idx2
		,PartnerSiteContact.SEQ::integer as SEQ3
		,PartnerSiteContact.index::integer as idx3
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) BusinessPartnerData
		,LATERAL FLATTEN(TO_ARRAY(BusinessPartnerData.value:"$")) PartnerSiteData
		,LATERAL FLATTEN(TO_ARRAY(PartnerSiteData.value:"$")) PartnerSiteContact
	WHERE	GetBusinessPartner.value like '<BusinessPartnerData>%'
	AND	(BusinessPartnerData.value like '<Abs:PartnerSiteData>%' )
	AND	(PartnerSiteData.value like '<Abs:PartnerSiteContact>%' )
	AND	(PartnerSiteContact.value like '<Abs:SiteContact>%' )
) SiteContact on SiteContact.SEQ = PartnerSiteData.SEQ AND SiteContact.idx = PartnerSiteData.idx
	AND	SiteContact.SEQ1 = PartnerSiteData.SEQ1 AND SiteContact.idx1 = PartnerSiteData.idx1
LEFT JOIN
(
	SELECT
		XMLGET(SiteContact.value, 'Abs:Code'):"$"::string AS SiteContact_Code
		,XMLGET(SiteContact.value, 'Abs:Description'):"$"::string AS SiteContact_Description
		,XMLGET(SiteContact.value, 'Abs:ShortDescription'):"$"::string AS SiteContact_ShortDescription
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
		,BusinessPartnerData.SEQ::integer as SEQ1
		,BusinessPartnerData.index::integer as idx1
		,PartnerSiteData.SEQ::integer as SEQ2
		,PartnerSiteData.index::integer as idx2
		,PartnerSiteContact.SEQ::integer as SEQ3
		,PartnerSiteContact.index::integer as idx3
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) BusinessPartnerData
		,LATERAL FLATTEN(TO_ARRAY(BusinessPartnerData.value:"$")) PartnerSiteData
		,LATERAL FLATTEN(TO_ARRAY(PartnerSiteData.value:"$")) PartnerSiteContact
		,LATERAL FLATTEN(TO_ARRAY(PartnerSiteContact.value:"$")) SiteContact
	WHERE	GetBusinessPartner.value like '<BusinessPartnerData>%'
	AND	(BusinessPartnerData.value like '<Abs:PartnerSiteData>%' )
	AND	(PartnerSiteData.value like '<Abs:PartnerSiteContact>%' )
	AND	(PartnerSiteContact.value like '<Abs:SiteContact>%' )
	AND	(SiteContact.value like '<Abs:ContactTypeCd>%' )
) SiteContact_ContactTypeCd on SiteContact_ContactTypeCd.SEQ = SiteContact.SEQ AND SiteContact_ContactTypeCd.idx = SiteContact.idx
	AND	SiteContact_ContactTypeCd.SEQ1 = SiteContact.SEQ1 AND SiteContact_ContactTypeCd.idx1 = SiteContact.idx1
	AND	SiteContact_ContactTypeCd.SEQ2 = SiteContact.SEQ2 AND SiteContact_ContactTypeCd.idx2 = SiteContact.idx2
	AND	SiteContact_ContactTypeCd.SEQ3 = SiteContact.SEQ3 AND SiteContact_ContactTypeCd.idx3 = SiteContact.idx3
LEFT JOIN
(
	SELECT
		GET(PartnerSiteData.value, '@Abs:typeCode')::string AS PartnerSiteEffectiveTimePeriod_typeCode
		,XMLGET(PartnerSiteData.value, 'Abs:FirstEffectiveDt'):"$"::string AS PartnerSiteEffectiveTimePeriod_FirstEffectiveDt
		,XMLGET(PartnerSiteData.value, 'Abs:LastEffectiveDt'):"$"::string AS PartnerSiteEffectiveTimePeriod_LastEffectiveDt
		,XMLGET(PartnerSiteData.value, 'Abs:LastEffectiveTm'):"$"::string AS PartnerSiteEffectiveTimePeriod_LastEffectiveTm
		,XMLGET(PartnerSiteData.value, 'Abs:FirstEffectiveTm'):"$"::string AS PartnerSiteEffectiveTimePeriod_FirstEffectiveTm
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
		,BusinessPartnerData.SEQ::integer as SEQ1
		,BusinessPartnerData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) BusinessPartnerData
		,LATERAL FLATTEN(TO_ARRAY(BusinessPartnerData.value:"$")) PartnerSiteData
	WHERE	GetBusinessPartner.value like '<BusinessPartnerData>%'
	AND	(BusinessPartnerData.value like '<Abs:PartnerSiteData>%' )
	AND	(PartnerSiteData.value like '<Abs:PartnerSiteEffectiveTimePeriod>%' OR PartnerSiteData.value like '<Abs:PartnerSiteEffectiveTimePeriod%typeCode%' )
) PartnerSiteEffectiveTimePeriod on PartnerSiteEffectiveTimePeriod.SEQ = PartnerSiteData.SEQ AND PartnerSiteEffectiveTimePeriod.idx = PartnerSiteData.idx
	AND	PartnerSiteEffectiveTimePeriod.SEQ1 = PartnerSiteData.SEQ1 AND PartnerSiteEffectiveTimePeriod.idx1 = PartnerSiteData.idx1
LEFT JOIN
(
	SELECT
		XMLGET(PartnerSiteData.value, 'Abs:Description'):"$"::string AS PartnerSiteStatus_Description
		,XMLGET(PartnerSiteData.value, 'Abs:EffectiveDtTm'):"$"::string AS PartnerSiteStatus_EffectiveDtTm
		,GET(XMLGET(PartnerSiteData.value, 'Abs:StatusTypeCd'), '@Type')::string AS PartnerSiteStatus_StatusTypeCd_Type
		,XMLGET(PartnerSiteData.value, 'Abs:StatusTypeCd'):"$"::string AS PartnerSiteStatus_StatusTypeCd
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
		,BusinessPartnerData.SEQ::integer as SEQ1
		,BusinessPartnerData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) BusinessPartnerData
		,LATERAL FLATTEN(TO_ARRAY(BusinessPartnerData.value:"$")) PartnerSiteData
	WHERE	GetBusinessPartner.value like '<BusinessPartnerData>%'
	AND	(BusinessPartnerData.value like '<Abs:PartnerSiteData>%' )
	AND	(PartnerSiteData.value like '<Abs:PartnerSiteStatus>%' )
) PartnerSiteStatus on PartnerSiteStatus.SEQ = PartnerSiteData.SEQ AND PartnerSiteStatus.idx = PartnerSiteData.idx
	AND	PartnerSiteStatus.SEQ1 = PartnerSiteData.SEQ1 AND PartnerSiteStatus.idx1 = PartnerSiteData.idx1
LEFT JOIN
(
	SELECT
		XMLGET(PartnerSiteData.value, 'Abs:Code'):"$"::string AS PartnerSiteTypeCd_Code
		,XMLGET(PartnerSiteData.value, 'Abs:Description'):"$"::string AS PartnerSiteTypeCd_Description
		,XMLGET(PartnerSiteData.value, 'Abs:ShortDescription'):"$"::string AS PartnerSiteTypeCd_ShortDescription
		,GetBusinessPartner.SEQ::integer as SEQ
		,GetBusinessPartner.idx::integer as idx
		,BusinessPartnerData.SEQ::integer as SEQ1
		,BusinessPartnerData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetBusinessPartner
		,LATERAL FLATTEN(TO_ARRAY(GetBusinessPartner.value:"$")) BusinessPartnerData
		,LATERAL FLATTEN(TO_ARRAY(BusinessPartnerData.value:"$")) PartnerSiteData
	WHERE	GetBusinessPartner.value like '<BusinessPartnerData>%'
	AND	(BusinessPartnerData.value like '<Abs:PartnerSiteData>%' )
	AND	(PartnerSiteData.value like '<Abs:PartnerSiteTypeCd>%' )
) PartnerSiteTypeCd on PartnerSiteTypeCd.SEQ = PartnerSiteData.SEQ AND PartnerSiteTypeCd.idx = PartnerSiteData.idx
	AND	PartnerSiteTypeCd.SEQ1 = PartnerSiteData.SEQ1 AND PartnerSiteTypeCd.idx1 = PartnerSiteData.idx1
`;

    try {
        snowflake.execute ( {sqlText: insert_into_flat_dml} );
    } catch (err) { 
        snowflake.execute ( {sqlText: sql_ins_rerun_tbl} ); 
        throw `Loading of table ${tgt_flat_tbl} Failed with error: ${err}`;   // Return a error message.
    }
// EDM Remediation sp call starts
function execEdmRemProc(rem_proc_nm, rem_params) 
    {
		try {
			 ret_obj = snowflake.execute (
						{sqlText: "call " + rem_proc_nm + "("+ rem_params +")"  }
						);
             ret_obj.next();
             ret_msg = ret_obj.getColumnValue(1);            

			}
		catch (err)  {
			return "Error executing stored procedure "+ rem_proc_nm + "("+ rem_params +")" + err;   // Return a error message.
			}
		return ret_msg;
	}
	
	var sub_proc_list = ['SP_EDM_Remediation_Recon_P1load']		
	 
for (index = 0; index < sub_proc_list.length; index++) 
    {
            rem_proc_nm = sub_proc_list[index];
               rem_params = "'"+ bodName +"','"+ src_wrk_tbl +"','"+ anlys_ref_db +"','"+ anlys_ref_schema +"','"+ anlys_wrk_schema +"'"; 
            return_msg = execEdmRemProc(rem_proc_nm, rem_params);
           
			/*
			if (return_msg)
            {
                snowflake.execute (
                        {sqlText: sql_ins_rerun_tbl }
                        );
                throw return_msg;
            }
			*/
    }

	// EDM Remediation sp call ends	
$$;
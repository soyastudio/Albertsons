--liquibase formatted sql
--changeset SYSTEM:sp_GetRewardTransaction_To_FLAT_load runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME_R>>;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE SP_GETREWARDTRANSACTION_TO_FLAT_LOAD()
RETURNS VARCHAR(16777216)
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
    var variant_nm = 'ESED_RewardTransaction';
	var bod_nm = 'GetRewardTransaction';
    var src_tbl = `${ref_db}.${app_schema}.${variant_nm}_R_STREAM`;
    var src_wrk_tbl = `${ref_db}.${wrk_schema}.${variant_nm}_wrk`;
    var src_rerun_tbl = `${ref_db}.${wrk_schema}.${variant_nm}_Rerun`;
    var tgt_flat_tbl = `${ref_db}.${ref_schema}.${bod_nm}_FLAT`;
	
	var bodName = 'GetRewardTransaction';	
	
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
    ,GetRewardTransaction.value as Value
    ,GetRewardTransaction.SEQ::integer as SEQ
    ,GetRewardTransaction.index::integer as idx
    FROM ${src_wrk_tbl} tbl
    ,LATERAL FLATTEN(tbl.SRC_XML:"$") GetRewardTransaction
    )
    SELECT FILENAME
          ,BODNm
          ,DocumentID
		,TargetApplicationCd
		,SourceApplicationCd
		,Description
		,InternalFileTransferInd
		,DocumentNm
		,InboundOutboundInd
		,AlternateDocumentID
		,CreationDt
		,PHIdataInd
		,PCIdataInd
		,PIIdataInd
		,BusinessSensitivityLevel_Code
		,DataClassificationLevel_Code
		,ActionTypeCd
		,RecordTypeCd
		,TransactionDetailTxt
		,CustomerId
		,HouseholdId
		,UpdateTs
		,CreateTs
		,CreateUserId
		,UpdateUserId
		,CustomerTierCd_Code
		,LoyaltyProgramCd
		,RewardDollarStartTs
		,RewardDollarEndTs
		,RewardDollarPointsQty
		,RewardOriginCd
		,RewardOriginTs
		,StatusCd
		,TransactionId
		,AltTransactionId
		,TransactionTypeCd_Code
		,Document_ReleaseId
		,Document_VersionId
		,Document_SystemEnvironmentCd
          ,CURRENT_TIMESTAMP AS DW_CreateTs
		,BeforeTransactionSnapshot
		,AfterTransactionSnapshot
    FROM
    (
	SELECT 
		FILENAME
		,BODnm
		,GET(DocumentData.value, '@ReleaseId')::string AS Document_ReleaseId
		,GET(DocumentData.value, '@VersionId')::string AS Document_VersionId
		,GET(DocumentData.value, '@SystemEnvironmentCd')::string AS Document_SystemEnvironmentCd
		,XMLGET(DocumentData.value, 'Abs:DocumentID'):"$"::string AS DocumentID
		,XMLGET(DocumentData.value, 'Abs:TargetApplicationCd'):"$"::string AS TargetApplicationCd
		,XMLGET(DocumentData.value, 'Abs:SourceApplicationCd'):"$"::string AS SourceApplicationCd
		,XMLGET(DocumentData.value, 'Abs:Description'):"$"::string AS Description
		,XMLGET(DocumentData.value, 'Abs:InternalFileTransferInd'):"$"::string AS InternalFileTransferInd
		,XMLGET(DocumentData.value, 'Abs:DocumentNm'):"$"::string AS DocumentNm
		,XMLGET(DocumentData.value, 'Abs:InboundOutboundInd'):"$"::string AS InboundOutboundInd
		,XMLGET(DocumentData.value, 'Abs:AlternateDocumentID'):"$"::string AS AlternateDocumentID
		,XMLGET(DocumentData.value, 'Abs:CreationDt'):"$"::string AS CreationDt
		,GetRewardTransaction.SEQ::integer as SEQ
		,GetRewardTransaction.idx::integer as idx
		,DocumentData.SEQ::integer as SEQ1
		,DocumentData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetRewardTransaction
		,LATERAL FLATTEN(TO_ARRAY(GetRewardTransaction.value:"$")) DocumentData
	WHERE	GetRewardTransaction.value like '<DocumentData>%'
	AND	(DocumentData.value like '<Document>%' OR DocumentData.value like '<Document%ReleaseId%' OR DocumentData.value like '<Document%VersionId%' OR DocumentData.value like '<Document%SystemEnvironmentCd%' )
) Document
LEFT JOIN
(
	SELECT
		XMLGET(Document.value, 'Abs:PHIdataInd'):"$"::string AS PHIdataInd
		,XMLGET(Document.value, 'Abs:PCIdataInd'):"$"::string AS PCIdataInd
		,XMLGET(Document.value, 'Abs:PIIdataInd'):"$"::string AS PIIdataInd
		,GetRewardTransaction.SEQ::integer as SEQ
		,GetRewardTransaction.idx::integer as idx
		,DocumentData.SEQ::integer as SEQ1
		,DocumentData.index::integer as idx1
		,Document.SEQ::integer as SEQ2
		,Document.index::integer as idx2
	FROM	LEVEL_1_FLATTEN AS GetRewardTransaction
		,LATERAL FLATTEN(TO_ARRAY(GetRewardTransaction.value:"$")) DocumentData
		,LATERAL FLATTEN(TO_ARRAY(DocumentData.value:"$")) Document
	WHERE	GetRewardTransaction.value like '<DocumentData>%'
	AND	(DocumentData.value like '<Document>%' OR DocumentData.value like '<Document%ReleaseId%' OR DocumentData.value like '<Document%VersionId%' OR DocumentData.value like '<Document%SystemEnvironmentCd%' )
	AND	(Document.value like '<Abs:DataClassification>%' )
) DataClassification on DataClassification.SEQ = Document.SEQ AND DataClassification.idx = Document.idx
	AND	DataClassification.SEQ1 = Document.SEQ1 AND DataClassification.idx1 = Document.idx1
LEFT JOIN
(
	SELECT
		XMLGET(DataClassification.value, 'Abs:Code'):"$"::string AS BusinessSensitivityLevel_Code
		,GetRewardTransaction.SEQ::integer as SEQ
		,GetRewardTransaction.idx::integer as idx
		,DocumentData.SEQ::integer as SEQ1
		,DocumentData.index::integer as idx1
		,Document.SEQ::integer as SEQ2
		,Document.index::integer as idx2
	FROM	LEVEL_1_FLATTEN AS GetRewardTransaction
		,LATERAL FLATTEN(TO_ARRAY(GetRewardTransaction.value:"$")) DocumentData
		,LATERAL FLATTEN(TO_ARRAY(DocumentData.value:"$")) Document
		,LATERAL FLATTEN(TO_ARRAY(Document.value:"$")) DataClassification
	WHERE	GetRewardTransaction.value like '<DocumentData>%'
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
		,GetRewardTransaction.SEQ::integer as SEQ
		,GetRewardTransaction.idx::integer as idx
		,DocumentData.SEQ::integer as SEQ1
		,DocumentData.index::integer as idx1
		,Document.SEQ::integer as SEQ2
		,Document.index::integer as idx2
	FROM	LEVEL_1_FLATTEN AS GetRewardTransaction
		,LATERAL FLATTEN(TO_ARRAY(GetRewardTransaction.value:"$")) DocumentData
		,LATERAL FLATTEN(TO_ARRAY(DocumentData.value:"$")) Document
		,LATERAL FLATTEN(TO_ARRAY(Document.value:"$")) DataClassification
	WHERE	GetRewardTransaction.value like '<DocumentData>%'
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
		,GetRewardTransaction.SEQ::integer as SEQ
		,GetRewardTransaction.idx::integer as idx
	FROM	LEVEL_1_FLATTEN AS GetRewardTransaction
		,LATERAL FLATTEN(TO_ARRAY(GetRewardTransaction.value:"$")) DocumentData
	WHERE	GetRewardTransaction.value like '<DocumentData>%'
	AND	(DocumentData.value like '<DocumentAction>%' )
) DocumentAction on DocumentAction.SEQ = Document.SEQ AND DocumentAction.idx = Document.idx
LEFT JOIN
(
	SELECT
		XMLGET(GetRewardTransaction.value, 'Abs:TransactionDetailTxt'):"$"::string AS TransactionDetailTxt
		,XMLGET(GetRewardTransaction.value, 'Abs:CustomerId'):"$"::string AS CustomerId
		,XMLGET(GetRewardTransaction.value, 'Abs:HouseholdId'):"$"::string AS HouseholdId
		,GetRewardTransaction.SEQ::integer as SEQ
		,GetRewardTransaction.idx::integer as idx
	FROM	LEVEL_1_FLATTEN AS GetRewardTransaction
	WHERE	GetRewardTransaction.value like '<RewardTransactionData>%'
) RewardTransactionData on RewardTransactionData.SEQ = Document.SEQ
LEFT JOIN
(
	SELECT
		XMLGET(RewardTransactionData.value, 'Abs:UpdateTs'):"$"::string AS UpdateTs
		,XMLGET(RewardTransactionData.value, 'Abs:CreateTs'):"$"::string AS CreateTs
		,XMLGET(RewardTransactionData.value, 'Abs:CreateUserId'):"$"::string AS CreateUserId
		,XMLGET(RewardTransactionData.value, 'Abs:UpdateUserId'):"$"::string AS UpdateUserId
		,GetRewardTransaction.SEQ::integer as SEQ
		,GetRewardTransaction.idx::integer as idx
	FROM	LEVEL_1_FLATTEN AS GetRewardTransaction
		,LATERAL FLATTEN(TO_ARRAY(GetRewardTransaction.value:"$")) RewardTransactionData
	WHERE	GetRewardTransaction.value like '<RewardTransactionData>%'
	AND	(RewardTransactionData.value like '<Abs:AuditData>%' )
) AuditData on AuditData.SEQ = RewardTransactionData.SEQ AND AuditData.idx = RewardTransactionData.idx
LEFT JOIN
(
	SELECT
		XMLGET(RewardTransactionData.value, 'Abs:Code'):"$"::string AS CustomerTierCd_Code
		,GetRewardTransaction.SEQ::integer as SEQ
		,GetRewardTransaction.idx::integer as idx
	FROM	LEVEL_1_FLATTEN AS GetRewardTransaction
		,LATERAL FLATTEN(TO_ARRAY(GetRewardTransaction.value:"$")) RewardTransactionData
	WHERE	GetRewardTransaction.value like '<RewardTransactionData>%'
	AND	(RewardTransactionData.value like '<Abs:CustomerTierCd>%' )
) CustomerTierCd on CustomerTierCd.SEQ = RewardTransactionData.SEQ AND CustomerTierCd.idx = RewardTransactionData.idx
LEFT JOIN
(
	SELECT
		XMLGET(RewardTransactionData.value, 'Abs:LoyaltyProgramCd'):"$"::string AS LoyaltyProgramCd
		,GetRewardTransaction.SEQ::integer as SEQ
		,GetRewardTransaction.idx::integer as idx
	FROM	LEVEL_1_FLATTEN AS GetRewardTransaction
		,LATERAL FLATTEN(TO_ARRAY(GetRewardTransaction.value:"$")) RewardTransactionData
	WHERE	GetRewardTransaction.value like '<RewardTransactionData>%'
	AND	(RewardTransactionData.value like '<Abs:LoyaltyProgram>%' )
) LoyaltyProgram on LoyaltyProgram.SEQ = RewardTransactionData.SEQ AND LoyaltyProgram.idx = RewardTransactionData.idx
LEFT JOIN
(
	SELECT
		XMLGET(RewardTransactionData.value, 'Abs:RewardDollarStartTs'):"$"::string AS RewardDollarStartTs
		,XMLGET(RewardTransactionData.value, 'Abs:RewardDollarEndTs'):"$"::string AS RewardDollarEndTs
		,XMLGET(RewardTransactionData.value, 'Abs:RewardDollarPointsQty'):"$"::string AS RewardDollarPointsQty
		,GetRewardTransaction.SEQ::integer as SEQ
		,GetRewardTransaction.idx::integer as idx
	FROM	LEVEL_1_FLATTEN AS GetRewardTransaction
		,LATERAL FLATTEN(TO_ARRAY(GetRewardTransaction.value:"$")) RewardTransactionData
	WHERE	GetRewardTransaction.value like '<RewardTransactionData>%'
	AND	(RewardTransactionData.value like '<Abs:RewardDollarPoints>%' )
) RewardDollarPoints on RewardDollarPoints.SEQ = RewardTransactionData.SEQ AND RewardDollarPoints.idx = RewardTransactionData.idx
LEFT JOIN
(
	SELECT
		XMLGET(RewardTransactionData.value, 'Abs:RewardOriginCd'):"$"::string AS RewardOriginCd
		,XMLGET(RewardTransactionData.value, 'Abs:RewardOriginTs'):"$"::string AS RewardOriginTs
		,GetRewardTransaction.SEQ::integer as SEQ
		,GetRewardTransaction.idx::integer as idx
	FROM	LEVEL_1_FLATTEN AS GetRewardTransaction
		,LATERAL FLATTEN(TO_ARRAY(GetRewardTransaction.value:"$")) RewardTransactionData
	WHERE	GetRewardTransaction.value like '<RewardTransactionData>%'
	AND	(RewardTransactionData.value like '<Abs:RewardPointsOrigin>%' )
) RewardPointsOrigin on RewardPointsOrigin.SEQ = RewardTransactionData.SEQ AND RewardPointsOrigin.idx = RewardTransactionData.idx
LEFT JOIN
(
	SELECT
		XMLGET(RewardTransactionData.value, 'Abs:StatusCd'):"$"::string AS StatusCd
		,GetRewardTransaction.SEQ::integer as SEQ
		,GetRewardTransaction.idx::integer as idx
	FROM	LEVEL_1_FLATTEN AS GetRewardTransaction
		,LATERAL FLATTEN(TO_ARRAY(GetRewardTransaction.value:"$")) RewardTransactionData
	WHERE	GetRewardTransaction.value like '<RewardTransactionData>%'
	AND	(RewardTransactionData.value like '<Abs:StatusType>%' )
) StatusType on StatusType.SEQ = RewardTransactionData.SEQ AND StatusType.idx = RewardTransactionData.idx

LEFT JOIN
(
	SELECT
		XMLGET(RewardTransactionData.value, 'Abs:BeforeTransactionSnapshot'):"$"::STRING AS BeforeTransactionSnapshot
		,XMLGET(RewardTransactionData.value, 'Abs:AfterTransactionSnapshot'):"$"::STRING AS AfterTransactionSnapshot
		,GetRewardTransaction.SEQ::integer as SEQ
		,GetRewardTransaction.idx::integer as idx
	FROM	LEVEL_1_FLATTEN AS GetRewardTransaction
		,LATERAL FLATTEN(TO_ARRAY(GetRewardTransaction.value:"$")) RewardTransactionData
	WHERE	GetRewardTransaction.value like '<RewardTransactionData>%'
	AND	(RewardTransactionData.value like '<Abs:TransactionAuditLogData>%' )
) TransactionAuditLogData on TransactionAuditLogData.SEQ = RewardTransactionData.SEQ AND TransactionAuditLogData.idx = RewardTransactionData.idx

LEFT JOIN
(
	SELECT
		XMLGET(RewardTransactionData.value, 'Abs:TransactionId'):"$"::string AS TransactionId
		,GetRewardTransaction.SEQ::integer as SEQ
		,GetRewardTransaction.idx::integer as idx
		,RewardTransactionData.SEQ::integer as SEQ1
		,RewardTransactionData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetRewardTransaction
		,LATERAL FLATTEN(TO_ARRAY(GetRewardTransaction.value:"$")) RewardTransactionData
	WHERE	GetRewardTransaction.value like '<RewardTransactionData>%'
	AND	(RewardTransactionData.value like '<Abs:TransactionType>%' )
) TransactionType on TransactionType.SEQ = RewardTransactionData.SEQ AND TransactionType.idx = RewardTransactionData.idx
LEFT JOIN
(
	SELECT
		XMLGET(TransactionType.value, 'Abs:AltTransactionId'):"$"::string AS AltTransactionId
		,GetRewardTransaction.SEQ::integer as SEQ
		,GetRewardTransaction.idx::integer as idx
		,RewardTransactionData.SEQ::integer as SEQ1
		,RewardTransactionData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetRewardTransaction
		,LATERAL FLATTEN(TO_ARRAY(GetRewardTransaction.value:"$")) RewardTransactionData
		,LATERAL FLATTEN(TO_ARRAY(RewardTransactionData.value:"$")) TransactionType
	WHERE	GetRewardTransaction.value like '<RewardTransactionData>%'
	AND	(RewardTransactionData.value like '<Abs:TransactionType>%' )
	AND	(TransactionType.value like '<Abs:AltTransaction>%' )
) AltTransaction on AltTransaction.SEQ = TransactionType.SEQ AND AltTransaction.idx = TransactionType.idx
	AND	AltTransaction.SEQ1 = TransactionType.SEQ1 AND AltTransaction.idx1 = TransactionType.idx1
LEFT JOIN
(
	SELECT
		XMLGET(TransactionType.value, 'Abs:Code'):"$"::string AS TransactionTypeCd_Code
		,GetRewardTransaction.SEQ::integer as SEQ
		,GetRewardTransaction.idx::integer as idx
		,RewardTransactionData.SEQ::integer as SEQ1
		,RewardTransactionData.index::integer as idx1
	FROM	LEVEL_1_FLATTEN AS GetRewardTransaction
		,LATERAL FLATTEN(TO_ARRAY(GetRewardTransaction.value:"$")) RewardTransactionData
		,LATERAL FLATTEN(TO_ARRAY(RewardTransactionData.value:"$")) TransactionType
	WHERE	GetRewardTransaction.value like '<RewardTransactionData>%'
	AND	(RewardTransactionData.value like '<Abs:TransactionType>%' )
	AND	(TransactionType.value like '<Abs:TransactionTypeCd>%' )
) TransactionTypeCd on TransactionTypeCd.SEQ = TransactionType.SEQ AND TransactionTypeCd.idx = TransactionType.idx
	AND	TransactionTypeCd.SEQ1 = TransactionType.SEQ1 AND TransactionTypeCd.idx1 = TransactionType.idx1`;

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

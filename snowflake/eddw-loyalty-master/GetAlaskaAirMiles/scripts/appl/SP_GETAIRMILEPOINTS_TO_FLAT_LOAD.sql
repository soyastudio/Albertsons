
--liquibase formatted sql
--changeset SYSTEM:SP_GETAIRMILEPOINTS_To_BIM_load_Air_Mile_Points_DETAIL runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME_R>>;
use schema <<EDM_DB_NAME_R>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME_R>>.DW_APPL.SP_GETAIRMILEPOINTS_TO_FLAT_LOAD()
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
    var variant_nm = 'ESED_AirMilePoints';
	var bod_nm = 'GetAirMilePoints';
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
					with LVL_1_FLATTEN as
				(select  
					tbl.filename as filename
					,tbl.src_avro as src_avro
				from ${src_wrk_tbl} tbl
				,LATERAL FLATTEN(tbl.SRC_AVRO) profile
				)
	SELECT DISTINCT 
       filename
	  ,current_timestamp() as DW_CREATE_TS
	  ,profile.src_avro:AirMilePointsData:AirMilePointsSourceType:Code::string as AirMilePointsSourceType_Code
      ,profile.src_avro:AirMilePointsData:AirMilePointsSourceType:Description::string as AirMilePointsSourceType_Description
	  ,profile.src_avro:AirMilePointsData:AirMilePointsSourceType:ShortDescription::string as AirMilePointsSourceType_ShortDescription
	  ,Summary_Type.value:FileNm::string as AirMilePointsSummary_FileNm
	  ,Summary_Type.value:LinkURL::string as AirMilePointsSummary_LinkURL
	  ,profile.src_avro:AirMilePointsData:AirMilePointsSummary:AuditData:CreateDtTm::string as AirMilePointsSummary_CreateDtTm
	  ,profile.src_avro:AirMilePointsData:AirMilePointsSummary:AuditData:CreateTs::string as AirMilePointsSummary_CreateTs
	  ,profile.src_avro:AirMilePointsData:AirMilePointsSummary:AuditData:CreateUserId::string as AirMilePointsSummary_CreateUserId
	  ,profile.src_avro:AirMilePointsData:AirMilePointsSummary:AuditData:UpdateDtTm::string as AirMilePointsSummary_UpdateDtTm
	  ,profile.src_avro:AirMilePointsData:AirMilePointsSummary:AuditData:UpdateTs::string as AirMilePointsSummary_UpdateTs
	  ,profile.src_avro:AirMilePointsData:AirMilePointsSummary:AuditData:UpdateUserId::string as AirMilePointsSummary_UpdateUserId
	  ,profile.src_avro:AirMilePointsData:AirMilePointsSummary:BatchEndDt::string as AirMilePointsSummary_BatchEndDt
	  ,profile.src_avro:AirMilePointsData:AirMilePointsSummary:BatchId::string as AirMilePointsSummary_BatchId
	  ,profile.src_avro:AirMilePointsData:AirMilePointsSummary:BatchStartDt::string as AirMilePointsSummary_BatchStartDt
	  ,profile.src_avro:AirMilePointsData:AirMilePointsSummary:RecordCnt::string as AirMilePointsSummary_RecordCnt
	  ,profile.src_avro:AirMilePointsData:AirMilePointsSummary:RejectedRecordCnt::string as AirMilePointsSummary_RejectedRecordCnt
	  ,profile.src_avro:AirMilePointsData:AirMilePointsSummary:TotalAirMilePointsQty::string as AirMilePointsSummary_TotalAirMilePointsQty
	  ,profile.src_avro:AirMilePointsData:AirMilePointsSummary:TotalRejectedAirMilePointsQty::string as AirMilePointsSummary_TotalRejectedAirMilePointsQty
	  ,profile.src_avro:DocumentData:Document:AlternateDocumentID as AlternateDocumentID
	  ,profile.src_avro:DocumentData:Document:CreationDt as CreationDt
	  ,profile.src_avro:DocumentData:Document:DataClassification:BusinessSensitivityLevel:Code::string as BusinessSensitivityLevel_Code
	  ,profile.src_avro:DocumentData:Document:DataClassification:BusinessSensitivityLevel:Description::string as BusinessSensitivityLevel_Description
	  ,profile.src_avro:DocumentData:Document:DataClassification:BusinessSensitivityLevel:ShortDescription::string as BusinessSensitivityLevel_ShortDescription
	  ,profile.src_avro:DocumentData:Document:DataClassification:DataClassificationLevel:Code::string as DataClassificationLevel_Code
	  ,profile.src_avro:DocumentData:Document:DataClassification:DataClassificationLevel:Description::string as DataClassificationLevel_Description
	  ,profile.src_avro:DocumentData:Document:DataClassification:DataClassificationLevel:ShortDescription::string as DataClassificationLevel_ShortDescription
	  ,profile.src_avro:DocumentData:Document:DataClassification:PCIdataInd::string as PCIdataInd
	  ,profile.src_avro:DocumentData:Document:DataClassification:PHIdataInd::string as PHIdataInd
	  ,profile.src_avro:DocumentData:Document:DataClassification:PIIdataInd::string as PIIdataInd
	  ,Summary_desc.value::string as Summary_desc
	  ,profile.src_avro:DocumentData:Document:DocumentID::string as DocumentID
	  ,profile.src_avro:DocumentData:Document:DocumentNm::string as DocumentNm
	  ,profile.src_avro:DocumentData:Document:ExpectedMessageCnt::string as ExpectedMessageCnt
	  ,profile.src_avro:DocumentData:Document:ExternalTargetInd::string as ExternalTargetInd
	  ,profile.src_avro:DocumentData:Document:GatewayNm::string as GatewayNm
	  ,profile.src_avro:DocumentData:Document:InboundOutboundInd::string as InboundOutboundInd
	  ,profile.src_avro:DocumentData:Document:InterchangeDate::string as InterchangeDate
	  ,profile.src_avro:DocumentData:Document:InterchangeTime::string as IntrchangeTime
	  ,profile.src_avro:DocumentData:Document:InternalFileTransferInd::string as InternalFileTransferInd
	  ,profile.src_avro:DocumentData:Document:MessageSequenceNbr::string as MessageSequenceNbr
	  ,profile.src_avro:DocumentData:Document:Note::string as Note
	  ,profile.src_avro:DocumentData:Document:ReceiverId::string as ReceiverId
	  ,profile.src_avro:DocumentData:Document:ReleaseId::string as ReleaseId
	  ,profile.src_avro:DocumentData:Document:RoutingSystemNm::string as RoutingSystemNm
	  ,profile.src_avro:DocumentData:Document:SenderId::string as SenderId
	  ,profile.src_avro:DocumentData:Document:SystemEnvironmentCd::string as SystemEnvironmentCd
	  ,profile.src_avro:DocumentData:Document:TargetApplicationCd::string as TargetApplicationCd
	  ,profile.src_avro:DocumentData:Document:VersionId::string as VersionId
	  ,profile.src_avro:DocumentData:DocumentAction:ActionTypeCd::string as ActionTypeCd
	  ,profile.src_avro:DocumentData:DocumentAction:RecordTypeCd::string as RecordTypeCd
	  ,Summary_Appcode.value::string as SourceApplicationCd
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:AirMileProgram:AirMileProgramId::string as AirMileProgramId
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:AirMileProgram:AirMileProgramNm::string as AirMileProgramNm
      ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:AirMileProgram:AirMileTier:AirMilePointQty::string as AirMilePointQty
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:AirMileProgram:AirMileTier:AirMileTierNm::string as AirMileTierNm
	  ,Detail_Type.value:FileNm::string as AirMilePointsDetail_FileNm
	  ,Detail_Type.value:LinkURL::string as AirMilePointsDetail_LinkURL
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:AuditData:CreateDtTm::string as AirMilePointsDetail_CreateDtTm
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:AuditData:CreateTs::string as AirMilePointsDetail_CreateTs
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:AuditData:CreateUserId::string as AirMilePointsDetail_CreateUserId
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:AuditData:UpdateDtTm::string as AirMilePointsDetail_UpdateDtTm
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:AuditData:UpdateTs::string as AirMilePointsDetail_UpdateTs
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:AuditData:UpdateUserId::string as AirMilePointsDetail_UpdateUserId
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:BatchId::string as AirMilePointsDetail_BatchId
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:CustomerNm:FamilyNm::string as AirMilePointsDetail_FamilyNm
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:CustomerNm:FormattedNm::string as AirMilePointsDetail_FormattedNm
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:CustomerNm:GenerationAffixCd::string as AirMilePointsDetail_GenerationAffixCd
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:CustomerNm:GivenNm::string as AirMilePointsDetail_GivenNm
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:CustomerNm:MaidenNm::string as AirMilePointsDetail_MaidenNm
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:CustomerNm:MiddleNm::string as AirMilePointsDetail_MiddleNm
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:CustomerNm:NickNm::string as AirMilePointsDetail_NickNm
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:CustomerNm:PreferredSalutationCd::string as AirMilePointsDetail_PreferredSalutationCd
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:CustomerNm:QualificationAffixCd::string as AirMilePointsDetail_QualificationAffixCd
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:CustomerNm:TitleCd::string as AirMilePointsDetail_TitleCd
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:HouseholdId::string as AirMilePointsDetail_HouseholdId
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:RecordType:Code::string as AirMilePointsDetail_RecordType_Code
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:RecordType:Description::string as AirMilePointsDetail_RecordType_Description
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:RecordType:ShortDescription::string as AirMilePointsDetail_RecordType_ShortDescription
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:TransactionType:AltTransaction:AltTransactionDtTs::string as AltTransactionDtTs
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:TransactionType:AltTransaction:AltTransactionId::string as AltTransactionId
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:TransactionType:AltTransaction:AltTransactionType:Code::string as AltTransactionType_Code
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:TransactionType:AltTransaction:AltTransactionType:Description::string as AltTransactionType_Description
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:TransactionType:AltTransaction:AltTransactionType:ShortDescription::string as AltTransactionType_ShortDescription
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:TransactionType:ReferenceNbr::string as ReferenceNbr
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:TransactionType:TransactionDt::string as TransactionDt
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:TransactionType:TransactionId::string as TransactionId
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:TransactionType:TransactionReasonCd:Code::string as TransactionReasonCd_Code
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:TransactionType:TransactionReasonCd:Description::string as TransactionReasonCd_Description
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:TransactionType:TransactionReasonCd:ShortDescription::string as TransactionReasonCd_ShortDescription
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:TransactionType:TransactionTs::string as TransactionTs
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:TransactionType:TransactionTypeCd:Code::string as TransactionTypeCd_Code
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:TransactionType:TransactionTypeCd:Description::string as TransactionTypeCd_Description
	  ,profile.src_avro:AirMilePointsData:AirMilePointsDetail:TransactionType:TransactionTypeCd:ShortDescription::string as TransactionTypeCd_ShortDescription
from LVL_1_FLATTEN profile
,LATERAL FLATTEN(input => profile.src_avro:AirMilePointsData:AirMilePointsSummary:AttachmentType , outer => TRUE ) as Summary_Type
,LATERAL FLATTEN(input => profile.src_avro:DocumentData:Document:Description , outer => TRUE ) as Summary_desc
,LATERAL FLATTEN(input => profile.src_avro:DocumentData:Document:SourceApplicationCd , outer => TRUE ) as Summary_Appcode
,LATERAL FLATTEN(input => profile.src_avro:AirMilePointsData:AirMilePointsDetail:AttachmentType, outer => TRUE ) as Detail_Type`;

    try {
        snowflake.execute ( {sqlText: insert_into_flat_dml} );
    } catch (err) { 
        snowflake.execute ( {sqlText: sql_ins_rerun_tbl} ); 
        throw `Loading of table ${tgt_flat_tbl} Failed with error: ${err}`;   // Return a error message.
    }
$$;
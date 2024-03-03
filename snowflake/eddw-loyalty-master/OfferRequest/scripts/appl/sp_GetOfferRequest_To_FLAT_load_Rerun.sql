--liquibase formatted sql
--changeset SYSTEM:SP_GETOFFERREQUEST_TO_FLAT_LOAD_RERUN_Beh runOnChange:true splitStatements:false OBJECT_TYPE:SP

use database EDM_REFINED_PRD;
use schema EDM_REFINED_PRD.DW_APPL;

drop procedure if exists EDM_REFINED_PRD.dw_r_product.SP_GETOFFERREQUEST_TO_FLAT_LOAD_RERUN();

CREATE OR REPLACE PROCEDURE SP_GETOFFERREQUEST_TO_FLAT_LOAD_RERUN()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS 
$$
   // Global Variable Declaration
  var wrk_schema = "DW_R_STAGE";
    var ref_db = "EDM_REFINED_PRD";
    var ref_schema = "DW_R_PRODUCT";
    var appl_schema = "DW_APPL";
    var src_tbl = ref_db + "." + "DW_R_PRODUCT" +".ESED_OfferRequest_R_Stream";
    var src_wrk_tbl = ref_db + "." + wrk_schema + ".ESED_OfferRequest_wrk";
    var src_rerun_tbl = ref_db + "." + wrk_schema + ".ESED_OfferRequest_Rerun";
    var tgt_flat_tbl = ref_db + "." + ref_schema + ".GetOfferRequest_Flat";
	var split_logic_tbl_1 = ref_db + "." + wrk_schema + ".split_logic_tbl_1";
	var split_logic_tbl_2 = ref_db + "." + wrk_schema + ".split_logic_tbl_2";

var sql_empty_wrk_tbl = `TRUNCATE TABLE `+ src_wrk_tbl +` `;
	try {
        snowflake.execute ({sqlText: sql_empty_wrk_tbl });
  }
  catch (err) { 
    throw "Truncation of wrk table "+ src_wrk_tbl +" Failed with error: " + err;   // Return a error message.
  }
	
	
// persist stream data in work table for the current transaction, includes data from previous failed run
var sql_crt_src_wrk_tbl = `insert into `+ src_wrk_tbl +` 
                            select * from `+ src_tbl +`
                            UNION ALL
                            select * from `+ src_rerun_tbl;
  try {
      snowflake.execute ({ sqlText: sql_crt_src_wrk_tbl });
  }
  catch (err)  {
    throw "Creation of Source Work table "+ src_wrk_tbl +" Failed with error: " + err;   // Return a error message.
  }
  
// Empty the rerun queue table
var sql_empty_rerun_tbl = `TRUNCATE TABLE `+ src_rerun_tbl;
try {
        snowflake.execute ({sqlText: sql_empty_rerun_tbl });
  }
  catch (err) {
    throw "Truncation of rerun queue table "+ src_rerun_tbl +" Failed with error: " + err;   // Return a error message.
  }

// query to load rerun queue table when encountered a failure
var sql_ins_rerun_tbl = `insert into `+ src_rerun_tbl + ` DATA_RETENTION_TIME_IN_DAYS = 0 AS
           SELECT * FROM `+ src_wrk_tbl;
		   
var insert_split_logic_tbl_1 = `CREATE OR REPLACE TABLE ` + split_logic_tbl_1 + ` AS
           WITH LEVEL_1_FLATTEN AS (
   SELECT tbl.SRC_XML:"@"::string AS BODNm
,tbl.FILENAME AS FILENAME
      ,GetOfferRequest.value as Value
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.index::integer as idx
   FROM ` + src_wrk_tbl + ` tbl
 ,LATERAL FLATTEN(tbl.SRC_XML:"$") GetOfferRequest
)
           SELECT DISTINCT FILENAME
,BODNM
,DOCUMENT_RELEASEID
,DOCUMENT_VERSIONID
,DOCUMENT_SYSTEMENVIRONMENTCD
,DOCUMENTID
,EXTERNALTARGETIND
,INTERCHANGETIME
,INTERCHANGEDATE
,INTERNALFILETRANSFERIND
,ROUTINGSYSTEMNM
,RECEIVERID
,GATEWAYNM
,SENDERID
,TARGETAPPLICATIONCD
,SOURCEAPPLICATIONCD
,DOCUMENT_DESCRIPTION
,CREATIONDT
,DOCUMENTNM
,INBOUNDOUTBOUNDIND
,ALTERNATEDOCUMENTID
,NOTE
,PHIDATAIND
,PIIDATAIND
,PCIDATAIND
,BUSINESSSENSITIVITYLEVEL_SHORTDESCRIPTION
,BUSINESSSENSITIVITYLEVEL_DESCRIPTION
,BUSINESSSENSITIVITYLEVEL_CODE
,DATACLASSIFICATIONLEVEL_SHORTDESCRIPTION
,DATACLASSIFICATIONLEVEL_DESCRIPTION
,DATACLASSIFICATIONLEVEL_CODE
,ACTIONTYPECD
,RECORDTYPECD
,TRIGGERID
,SAVINGSVALUETXT
,OFFERREQUESTDATA_DISCLAIMERTXT
,OFFERREQUESTDATA_IMAGEID
,OFFERREQUESTREFERENCEID_QUALIFIERCD
,OFFERREQUESTREFERENCEID
,BUSINESSJUSTIFICATIONTXT
,OFFERREQUESTTYPECD
,OFFERITEMDSC
,OFFERREQUESTCOMMENTTXT
,VERSIONQTY
,TIERQTY
,PRODUCTQTY
,STOREGROUPQTY
,CUSTOMERSEGMENTINFOTXT
,DELETEDOFFERTXT
,BRANDINFOTXT
,OFFERREQUESTID
,SIZEDSC
,DEPARTMENTID
,DEPARTMENTNM
,OFFERNM
,OFFERREQUESTDSC
,OFFERFLAGDSC
,UPCQTYTXT
,OFFEREFFECTIVEDAY
,OFFEREFFECTIVEDAY_QUALIFIER
,ADVERTISEMENTTYPE_SHORTDESCRIPTION
,ADVERTISEMENTTYPE_DESCRIPTION
,ADVERTISEMENTTYPE_CODE
,ECOMMPROGRAMTYPENAME
,ECOMMPROGRAMTYPECODE
,ALLOCATIONTYPECD_SHORTDESCRIPTION
,ALLOCATIONTYPECD_DESCRIPTION
,ALLOCATIONTYPECD_CODE
,LINKURL_QUALIFIER
,LINKURL
,FILENM_QUALIFIER
,FILENM
,IDNBR
,MANUFACTURERTYPE_DESCRIPTION
,IDTXT
,MANUFACTURERTYPE_SHORTDESCRIPTION
,DELIVERYCHANNELTYPEDSC
,DELIVERYCHANNELTYPECD
,OFFEREFFECTIVETM_TIMEZONECD
,ENDTM
,STARTTM
,GROUPCD
,GROUPNM
,GROUPID
,SUBGROUPNM
,SUBGROUPID
,SUBGROUPCD
,OFFERORGANIZATIONREGIONID
,OFFERORGANIZATIONREGIONNM
,OFFERPERIODTYPE_DISPLAYSTARTDT
,OFFERPERIODTYPE_DISPLAYENDDT
,OFFERSTARTDT
,OFFERENDDT
,TESTSTARTDT
,TESTENDDT
,SOURCESYSTEMID
,APPLICATIONID
,UPDATEDAPPLICATIONID
,FIRSTNM
,LASTNM
,USERTYPECD
,UPDATETS
,USERUPDATE_TIMEZONECD
,CREATETS
,USERID
,OFFERRESTRICTIONTYPE_LIMITWT
,USAGELIMITTYPETXT
,USAGELIMITPERIODNBR
,USAGELIMITNBR
,OFFERRESTRICTIONTYPE_LIMITQTY
,OFFERRESTRICTIONTYPE_LIMITAMT
,OFFERRESTRICTIONTYPE_LIMITVOL
,RESTRICTIONTYPE_DESCRIPTION
,RESTRICTIONTYPE_CODE
,RESTRICTIONTYPE_SHORTDESCRIPTION
,OFFERRESTRICTIONTYPE_UOMNM
,OFFERRESTRICTIONTYPE_UOMCD
,NAME
,PROMOTIONPROGRAMTYPE_CODE
,PROGRAMSUBTYPECODE
,PROGRAMSUBTYPENAME
,CHANGEDETAILCHANGETYPECD
,CHANGEDETAILCHANGETYPEQTY
,CHANGEDETAILCHANGETYPEDSC
,CHANGEDETAILCHANGECATEGORYCD
,CHANGEDETAILCHANGECATEGORYQTY
,CHANGEDETAILCHANGECATEGORYDSC
,CHANGEDETAILREASONTYPECD
,CHANGEDETAILREASONTYPEDSC
,CHANGEDETAILCOMMENTTXT
,CHANGEBYTYPEUSERID
,CHANGEBYTYPEFIRSTNM
,CHANGEBYTYPELASTNM
,CHANGEBYTYPECHANGEBYDTTM
,NOPAENDDT
,BILLEDIND
,NOPASTARTDT
,VENDORPROMOTIONID
,ALLOWANCETYPE_SHORTDESCRIPTION
,ALLOWANCETYPE_CODE
,ALLOWANCETYPE_DESCRIPTION
,BILLINGOPTIONTYPE_DESCRIPTION
,BILLINGOPTIONTYPE_SHORTDESCRIPTION
,BILLINGOPTIONTYPE_CODE
,NOPAASSIGNSTATUS_STATUSTYPECD_TYPE
,NOPAASSIGNSTATUS_STATUSTYPECD
,NOPAASSIGNSTATUS_EFFECTIVEDTTM
,NOPAASSIGNSTATUS_DESCRIPTION
,OFFERBANKNM
,OFFERBANKID
,OFFERBANKTYPECD
,TEMPLATEID
,TEMPLATENM
,FULFILLMENTCHANNELTYPECD
,FULFILLMENTCHANNELIND
,FULFILLMENTCHANNELDSC
,TEMPLATEREVIEWSTATUSFLAGCD
,REVIEWCHECKLISTIND
,TEMPLATESTATUSCD
,PROMOTIONPROGRAM_NAME
,PROMOTIONPROGRAM_CODE
,PROMOTIONPERIODNM
,PROMOTIONPERIODID
,PROMOTIONWEEKID
,PROMOTIONSTARTDT
,PROMOTIONENDDT
,REQUIREDQTY
,REQUIREDIND
,REQUIREMENTTYPECD
,REFUNDABLEIND
,CHARGEBACKDEPARTMENT_DEPARTMENTNM
,CHARGEBACKDEPARTMENT_DEPARTMENTID
,EcommValidWithOtherOffersInd
,EcommValidForFirstTimeCustomerInd
,EcommAutoApplyPromoInd
,EcommOfferEligibleOrderCnt
,EcommPromoCd
,PromotionSubProgramCode
,EcommBehaviorCd
,InitialSubscriptionOfferInd
,OfferTemplateStatusInd
,DynamicOfferInd
,DaysToRedeemOfferCnt
,DOCUMENT.SEQ as SEQ
, DOCUMENT.idx as IDX
, AttachedOfferType.seq1 as SEQ1
, AttachedOfferType.idx1 as IDX1 
, AttachedOfferType.value as AttachedOfferType_value 
     FROM
   (
SELECT
FILENAME
,BODnm
,GET(DocumentData.value, '@ReleaseId')::string AS Document_ReleaseId
,GET(DocumentData.value, '@VersionId')::string AS Document_VersionId
,GET(DocumentData.value, '@SystemEnvironmentCd')::string AS Document_SystemEnvironmentCd
,XMLGET(DocumentData.value, 'Abs:DocumentID'):"$"::string AS DocumentID
,XMLGET(DocumentData.value, 'Abs:ExternalTargetInd'):"$"::string AS ExternalTargetInd
,XMLGET(DocumentData.value, 'Abs:InterchangeTime'):"$"::string AS InterchangeTime
,XMLGET(DocumentData.value, 'Abs:InterchangeDate'):"$"::string AS InterchangeDate
,XMLGET(DocumentData.value, 'Abs:InternalFileTransferInd'):"$"::string AS InternalFileTransferInd
,XMLGET(DocumentData.value, 'Abs:RoutingSystemNm'):"$"::string AS RoutingSystemNm
,XMLGET(DocumentData.value, 'Abs:ReceiverId'):"$"::string AS ReceiverId
,XMLGET(DocumentData.value, 'Abs:GatewayNm'):"$"::string AS GatewayNm
,XMLGET(DocumentData.value, 'Abs:SenderId'):"$"::string AS SenderId
,XMLGET(DocumentData.value, 'Abs:TargetApplicationCd'):"$"::string AS TargetApplicationCd
,XMLGET(DocumentData.value, 'Abs:SourceApplicationCd'):"$"::string AS SourceApplicationCd
,XMLGET(DocumentData.value, 'Abs:Description'):"$"::string AS Document_Description
,XMLGET(DocumentData.value, 'Abs:CreationDt'):"$"::string AS CreationDt
,XMLGET(DocumentData.value, 'Abs:DocumentNm'):"$"::string AS DocumentNm
,XMLGET(DocumentData.value, 'Abs:InboundOutboundInd'):"$"::string AS InboundOutboundInd
,XMLGET(DocumentData.value, 'Abs:AlternateDocumentID'):"$"::string AS AlternateDocumentID
,XMLGET(DocumentData.value, 'Abs:Note'):"$"::string AS Note
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,DocumentData.SEQ::integer as SEQ1
,DocumentData.index::integer as idx1
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) DocumentData
WHERE GetOfferRequest.value like '<DocumentData>%'
AND (DocumentData.value like '<Document>%'
          OR DocumentData.value like '<Document%ReleaseId%'
          OR DocumentData.value like '<Document%VersionId%'
          OR DocumentData.value like '<Document%SystemEnvironmentCd%' )
) Document
LEFT JOIN
(
SELECT
XMLGET(Document.value, 'Abs:PHIdataInd'):"$"::string AS PHIdataInd
,XMLGET(Document.value, 'Abs:PIIdataInd'):"$"::string AS PIIdataInd
,XMLGET(Document.value, 'Abs:PCIdataInd'):"$"::string AS PCIdataInd
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,DocumentData.SEQ::integer as SEQ1
,DocumentData.index::integer as idx1
,Document.SEQ::integer as SEQ2
,Document.index::integer as idx2
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) DocumentData
,LATERAL FLATTEN(TO_ARRAY(DocumentData.value:"$")) Document
WHERE GetOfferRequest.value like '<DocumentData>%'
AND (DocumentData.value like '<Document>%' OR DocumentData.value like '<Document%ReleaseId%' OR DocumentData.value like '<Document%VersionId%' OR DocumentData.value like '<Document%SystemEnvironmentCd%' )
AND (Document.value like '<Abs:DataClassification>%' )
) DataClassification on DataClassification.SEQ = Document.SEQ AND DataClassification.idx = Document.idx
AND DataClassification.SEQ1 = Document.SEQ1 AND DataClassification.idx1 = Document.idx1
LEFT JOIN
(
SELECT
XMLGET(DataClassification.value, 'Abs:ShortDescription'):"$"::string AS BusinessSensitivityLevel_ShortDescription
,XMLGET(DataClassification.value, 'Abs:Description'):"$"::string AS BusinessSensitivityLevel_Description
,XMLGET(DataClassification.value, 'Abs:Code'):"$"::string AS BusinessSensitivityLevel_Code
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,DocumentData.SEQ::integer as SEQ1
,DocumentData.index::integer as idx1
,Document.SEQ::integer as SEQ2
,Document.index::integer as idx2
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) DocumentData
,LATERAL FLATTEN(TO_ARRAY(DocumentData.value:"$")) Document
,LATERAL FLATTEN(TO_ARRAY(Document.value:"$")) DataClassification
WHERE GetOfferRequest.value like '<DocumentData>%'
AND (DocumentData.value like '<Document>%' OR DocumentData.value like '<Document%ReleaseId%' OR DocumentData.value like '<Document%VersionId%' OR DocumentData.value like '<Document%SystemEnvironmentCd%' )
AND (Document.value like '<Abs:DataClassification>%' )
AND (DataClassification.value like '<Abs:BusinessSensitivityLevel>%' )
) BusinessSensitivityLevel on BusinessSensitivityLevel.SEQ = DataClassification.SEQ AND BusinessSensitivityLevel.idx = DataClassification.idx
AND BusinessSensitivityLevel.SEQ1 = DataClassification.SEQ1 AND BusinessSensitivityLevel.idx1 = DataClassification.idx1
AND BusinessSensitivityLevel.SEQ2 = DataClassification.SEQ2 AND BusinessSensitivityLevel.idx2 = DataClassification.idx2
LEFT JOIN
(
SELECT
XMLGET(DataClassification.value, 'Abs:ShortDescription'):"$"::string AS DataClassificationLevel_ShortDescription
,XMLGET(DataClassification.value, 'Abs:Description'):"$"::string AS DataClassificationLevel_Description
,XMLGET(DataClassification.value, 'Abs:Code'):"$"::string AS DataClassificationLevel_Code
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,DocumentData.SEQ::integer as SEQ1
,DocumentData.index::integer as idx1
,Document.SEQ::integer as SEQ2
,Document.index::integer as idx2
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) DocumentData
,LATERAL FLATTEN(TO_ARRAY(DocumentData.value:"$")) Document
,LATERAL FLATTEN(TO_ARRAY(Document.value:"$")) DataClassification
WHERE GetOfferRequest.value like '<DocumentData>%'
AND (DocumentData.value like '<Document>%' OR DocumentData.value like '<Document%ReleaseId%' OR DocumentData.value like '<Document%VersionId%' OR DocumentData.value like '<Document%SystemEnvironmentCd%' )
AND (Document.value like '<Abs:DataClassification>%' )
AND (DataClassification.value like '<Abs:DataClassificationLevel>%' )
) DataClassificationLevel on DataClassificationLevel.SEQ = DataClassification.SEQ AND DataClassificationLevel.idx = DataClassification.idx
AND DataClassificationLevel.SEQ1 = DataClassification.SEQ1 AND DataClassificationLevel.idx1 = DataClassification.idx1
AND DataClassificationLevel.SEQ2 = DataClassification.SEQ2 AND DataClassificationLevel.idx2 = DataClassification.idx2
LEFT JOIN
(
SELECT
XMLGET(DocumentData.value, 'Abs:ActionTypeCd'):"$"::string AS ActionTypeCd
,XMLGET(DocumentData.value, 'Abs:RecordTypeCd'):"$"::string AS RecordTypeCd
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) DocumentData
WHERE GetOfferRequest.value like '<DocumentData>%'
AND (DocumentData.value like '<DocumentAction>%' )
) DocumentAction on DocumentAction.SEQ = Document.SEQ AND DocumentAction.idx = Document.idx
LEFT JOIN
(
SELECT
  XMLGET(GetOfferRequest.value, 'Abs:TriggerId'):"$"::string AS TriggerId
 ,XMLGET(GetOfferRequest.value, 'Abs:SavingsValueTxt'):"$"::string AS SavingsValueTxt
 ,XMLGET(GetOfferRequest.value, 'Abs:DisclaimerTxt'):"$"::string AS OfferRequestData_DisclaimerTxt
 ,XMLGET(GetOfferRequest.value, 'Abs:ImageId'):"$"::string AS OfferRequestData_ImageId
      //,GET(XMLGET(GetOfferRequest.value, 'Abs:OfferEffectiveDay'), '@Qualifier')::string AS OfferEffectiveDay_Qualifier
      // ,XMLGET(GetOfferRequest.value, 'Abs:OfferEffectiveDay'):"$"::string AS OfferEffectiveDay
 //,GET(days.value, '@Qualifier')::string AS OfferEffectiveDay_Qualifier
 //,GET(days.value, '$')::string AS OfferEffectiveDay
 ,GET(XMLGET(GetOfferRequest.value, 'Abs:OfferRequestReferenceId'), '@QualifierCd')::string AS OfferRequestReferenceId_QualifierCd
 ,XMLGET(GetOfferRequest.value, 'Abs:OfferRequestReferenceId'):"$"::string AS OfferRequestReferenceId
 ,XMLGET(GetOfferRequest.value, 'Abs:BusinessJustificationTxt'):"$"::string AS BusinessJustificationTxt
 ,XMLGET(GetOfferRequest.value, 'Abs:OfferRequestTypeCd'):"$"::string AS OfferRequestTypeCd
 ,XMLGET(GetOfferRequest.value, 'Abs:OfferItemDsc'):"$"::string AS OfferItemDsc
 ,XMLGET(GetOfferRequest.value, 'Abs:OfferRequestCommentTxt'):"$"::string AS OfferRequestCommentTxt
 ,XMLGET(GetOfferRequest.value, 'Abs:VersionQty'):"$"::string AS VersionQty
 ,XMLGET(GetOfferRequest.value, 'Abs:TierQty'):"$"::string AS TierQty
 ,XMLGET(GetOfferRequest.value, 'Abs:ProductQty'):"$"::string AS ProductQty
 ,XMLGET(GetOfferRequest.value, 'Abs:StoreGroupQty'):"$"::string AS StoreGroupQty
 ,XMLGET(GetOfferRequest.value, 'Abs:CustomerSegmentInfoTxt'):"$"::string AS CustomerSegmentInfoTxt
 ,XMLGET(GetOfferRequest.value, 'Abs:DeletedOfferTxt'):"$"::string AS DeletedOfferTxt
 ,XMLGET(GetOfferRequest.value, 'Abs:BrandInfoTxt'):"$"::string AS BrandInfoTxt
 ,XMLGET(GetOfferRequest.value, 'Abs:OfferRequestId'):"$"::string AS OfferRequestId
 ,XMLGET(GetOfferRequest.value, 'Abs:SizeDsc'):"$"::string AS SizeDsc
 ,XMLGET(GetOfferRequest.value, 'Abs:DepartmentId'):"$"::string AS DepartmentId
 ,XMLGET(GetOfferRequest.value, 'Abs:DepartmentNm'):"$"::string AS DepartmentNm
 ,XMLGET(GetOfferRequest.value, 'Abs:OfferNm'):"$"::string AS OfferNm
 ,XMLGET(GetOfferRequest.value, 'Abs:OfferRequestDsc'):"$"::string AS OfferRequestDsc
 ,XMLGET(GetOfferRequest.value, 'Abs:OfferFlagDsc'):"$"::string AS OfferFlagDsc
 ,XMLGET(GetOfferRequest.value, 'Abs:UPCQtyTxt'):"$"::string AS UPCQtyTxt
 ,XMLGET(GetOfferRequest.value, 'Abs:OfferQualificationBehaviorCd'):"$"::string AS EcommBehaviorCd
 ,XMLGET(GetOfferRequest.value, 'Abs:OfferTemplateStatusInd'):"$"::string AS OfferTemplateStatusInd 
 ,XMLGET(GetOfferRequest.value, 'Abs:DynamicOfferInd'):"$"::string AS DynamicOfferInd 
 ,XMLGET(GetOfferRequest.value, 'Abs:DaysToRedeemOfferCnt'):"$"::string AS DaysToRedeemOfferCnt 
 ,GetOfferRequest.SEQ::integer as SEQ
 ,GetOfferRequest.idx::integer as idx
FROM    LEVEL_1_FLATTEN AS GetOfferRequest
// ,LATERAL FLATTEN(GetOfferRequest.value:"$") as days
WHERE   GetOfferRequest.value like '<OfferRequestData>%'
// AND days.value like '<Abs:OfferEffectiveDay%'
) OfferRequestData on OfferRequestData.SEQ = Document.SEQ
LEFT JOIN
(
SELECT
XMLGET(OfferRequestData.value, 'Abs:DayOfWkInd'):"$"::string AS OfferEffectiveDay
,XMLGET(OfferRequestData.value, 'Abs:DayOfWk'):"$"::string AS OfferEffectiveDay_Qualifier
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:OfferEffectiveDay>%' )
)Days on Days.seq = OfferRequestData.seq
LEFT JOIN
(
SELECT
XMLGET(OfferRequestData.value, 'Abs:ShortDescription'):"$"::string AS AdvertisementType_ShortDescription
,XMLGET(OfferRequestData.value, 'Abs:Description'):"$"::string AS AdvertisementType_Description
,XMLGET(OfferRequestData.value, 'Abs:Code'):"$"::string AS AdvertisementType_Code
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AdvertisementType>%' )
) AdvertisementType on AdvertisementType.SEQ = OfferRequestData.SEQ AND AdvertisementType.idx = OfferRequestData.idx
LEFT JOIN
(
SELECT
XMLGET(OfferRequestData.value, 'Abs:ValidWithOtherOffersInd'):"$"::string AS EcommValidWithOtherOffersInd
,XMLGET(OfferRequestData.value, 'Abs:ValidForFirstTimeCustomerInd'):"$"::string AS EcommValidForFirstTimeCustomerInd
,XMLGET(OfferRequestData.value, 'Abs:AutoApplyPromoInd'):"$"::string AS EcommAutoApplyPromoInd
,XMLGET(OfferRequestData.value, 'Abs:OfferEligibleOrderCnt'):"$"::string AS EcommOfferEligibleOrderCnt
,XMLGET(OfferRequestData.value, 'Abs:PromoCd'):"$"::string AS EcommPromoCd
,XMLGET(OfferRequestData.value, 'Abs:InitialSubscriptionOfferInd'):"$"::string AS InitialSubscriptionOfferInd
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) EcommPromotionProgramType
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:EcommPromotionProgramType>%' )
)EcommPromotionProgramType on EcommPromotionProgramType.SEQ = OfferRequestData.SEQ AND EcommPromotionProgramType.idx = OfferRequestData.idx
LEFT JOIN
(
SELECT
 XMLGET(EcommPromotionProgramType.value, 'Abs:Name'):"$"::string AS EcommProgramTypeName
,XMLGET(EcommPromotionProgramType.value, 'Abs:Code'):"$"::string AS EcommProgramTypeCode
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) EcommPromotionProgramType
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:EcommPromotionProgramType>%' )
AND (EcommPromotionProgramType.value like '<Abs:EcommProgramType>%' )
) EcommProgramType on EcommProgramType.SEQ = EcommPromotionProgramType.SEQ AND EcommProgramType.idx = EcommPromotionProgramType.idx
--AND EcommProgramType.SEQ1 = EcommPromotionProgramType.SEQ1 AND EcommProgramType.idx1 = EcommPromotionProgramType.idx1
-----------------------------------------------------------------------
LEFT JOIN
(
SELECT
XMLGET(OfferRequestData.value, 'Abs:ShortDescription'):"$"::string AS AllocationTypeCd_ShortDescription
,XMLGET(OfferRequestData.value, 'Abs:Description'):"$"::string AS AllocationTypeCd_Description
,XMLGET(OfferRequestData.value, 'Abs:Code'):"$"::string AS AllocationTypeCd_Code
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AllocationTypeCd>%' )
) AllocationTypeCd on AllocationTypeCd.SEQ = OfferRequestData.SEQ AND AllocationTypeCd.idx = OfferRequestData.idx
LEFT JOIN
(
SELECT
GET(XMLGET(OfferRequestData.value, 'Abs:LinkURL'), '@Qualifier')::string AS LinkURL_Qualifier
,XMLGET(OfferRequestData.value, 'Abs:LinkURL'):"$"::string AS LinkURL
,GET(XMLGET(OfferRequestData.value, 'Abs:FileNm'), '@Qualifier')::string AS FileNm_Qualifier
,XMLGET(OfferRequestData.value, 'Abs:FileNm'):"$"::string AS FileNm
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AttachmentType>%' )
) AttachmentType on AttachmentType.SEQ = OfferRequestData.SEQ AND AttachmentType.idx = OfferRequestData.idx
LEFT JOIN
(
SELECT
XMLGET(OfferRequestData.value, 'Abs:IdNbr'):"$"::string AS IdNbr
,XMLGET(OfferRequestData.value, 'Abs:Description'):"$"::string AS ManufacturerType_Description
,XMLGET(OfferRequestData.value, 'Abs:IdTxt'):"$"::string AS IdTxt
,XMLGET(OfferRequestData.value, 'Abs:ShortDescription'):"$"::string AS ManufacturerType_ShortDescription
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:ManufacturerType>%' )
) ManufacturerType on ManufacturerType.SEQ = OfferRequestData.SEQ AND ManufacturerType.idx = OfferRequestData.idx
LEFT JOIN
(
SELECT
XMLGET(OfferRequestData.value, 'Abs:DeliveryChannelTypeDsc'):"$"::string AS DeliveryChannelTypeDsc
,XMLGET(OfferRequestData.value, 'Abs:DeliveryChannelTypeCd'):"$"::string AS DeliveryChannelTypeCd
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:OfferDeliveryChannelType>%' )
) OfferDeliveryChannelType on OfferDeliveryChannelType.SEQ = OfferRequestData.SEQ AND OfferDeliveryChannelType.idx = OfferRequestData.idx
LEFT JOIN
(
SELECT
XMLGET(OfferRequestData.value, 'Abs:TimeZoneCd'):"$"::string AS OfferEffectiveTm_TimeZoneCd
,XMLGET(OfferRequestData.value, 'Abs:EndTm'):"$"::string AS EndTm
,XMLGET(OfferRequestData.value, 'Abs:StartTm'):"$"::string AS StartTm
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:OfferEffectiveTm>%' )
) OfferEffectiveTm on OfferEffectiveTm.SEQ = OfferRequestData.SEQ AND OfferEffectiveTm.idx = OfferRequestData.idx
LEFT JOIN
(
SELECT
XMLGET(OfferOrganization.value, 'Abs:GroupCd'):"$"::string AS GroupCd
,XMLGET(OfferOrganization.value, 'Abs:GroupNm'):"$"::string AS GroupNm
,XMLGET(OfferOrganization.value, 'Abs:GroupId'):"$"::string AS GroupId
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,OfferOrganization.SEQ::integer as SEQ2
,OfferOrganization.index::integer as idx2
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) OfferOrganization
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:OfferOrganization>%' )
AND (OfferOrganization.value like '<Abs:Group>%' )
) OfferOrgGroup on OfferOrgGroup.SEQ = OfferRequestData.SEQ AND OfferOrgGroup.idx = OfferRequestData.idx
LEFT JOIN
(
SELECT
XMLGET(OfferOrgGroup.value, 'Abs:SubGroupNm'):"$"::string AS SubGroupNm
,XMLGET(OfferOrgGroup.value, 'Abs:SubGroupId'):"$"::string AS SubGroupId
,XMLGET(OfferOrgGroup.value, 'Abs:SubGroupCd'):"$"::string AS SubGroupCd
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,OfferOrganization.SEQ::integer as SEQ2
,OfferOrganization.index::integer as idx2
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) OfferOrganization
,LATERAL FLATTEN(TO_ARRAY(OfferOrganization.value:"$")) OfferOrgGroup
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:OfferOrganization>%' )
AND (OfferOrganization.value like '<Abs:Group>%' )
AND (OfferOrgGroup.value like '<Abs:SubGroup>%' )
) SubGroup on SubGroup.SEQ = OfferOrgGroup.SEQ AND SubGroup.idx = OfferOrgGroup.idx
AND SubGroup.SEQ1 = OfferOrgGroup.SEQ1 AND SubGroup.idx1 = OfferOrgGroup.idx1
AND SubGroup.SEQ2 = OfferOrgGroup.SEQ2 AND SubGroup.idx2 = OfferOrgGroup.idx2
LEFT JOIN
(
SELECT
XMLGET(OfferOrganization.value, 'Abs:RegionId'):"$"::string AS OfferOrganizationregionid
,XMLGET(OfferOrganization.value, 'Abs:RegionNm'):"$"::string AS OfferOrganizationregionnm
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,OfferOrganization.SEQ::integer as SEQ2
,OfferOrganization.index::integer as idx2
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) OfferOrganization
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:OfferOrganization>%' )
AND (OfferOrganization.value like '<Abs:OfferRegion>%' )
) offerRegion on offerRegion.SEQ = OfferOrgGroup.SEQ AND offerRegion.idx = OfferOrgGroup.idx
AND offerRegion.SEQ1 = OfferOrgGroup.SEQ1 AND offerRegion.idx1 = OfferOrgGroup.idx1
LEFT JOIN
(
SELECT
XMLGET(OfferRequestData.value, 'Abs:DisplayStartDt'):"$"::string AS OfferPeriodType_DisplayStartDt
,XMLGET(OfferRequestData.value, 'Abs:DisplayEndDt'):"$"::string AS OfferPeriodType_DisplayEndDt
,XMLGET(OfferRequestData.value, 'Abs:OfferStartDt'):"$"::string AS OfferStartDt
,XMLGET(OfferRequestData.value, 'Abs:OfferEndDt'):"$"::string AS OfferEndDt
,XMLGET(OfferRequestData.value, 'Abs:TestStartDt'):"$"::string AS TestStartDt
,XMLGET(OfferRequestData.value, 'Abs:TestEndDt'):"$"::string AS TestEndDt
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:OfferPeriodType>%' )
) OfferPeriodType on OfferPeriodType.SEQ = OfferRequestData.SEQ AND OfferPeriodType.idx = OfferRequestData.idx
LEFT JOIN
(
SELECT
XMLGET(OfferRequestData.value, 'Abs:SourceSystemId'):"$"::string AS SourceSystemId
,XMLGET(OfferRequestData.value, 'Abs:ApplicationId'):"$"::string AS ApplicationId
,XMLGET(OfferRequestData.value, 'Abs:UpdatedApplicationId'):"$"::string AS UpdatedApplicationId
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:OfferRequestSource>%' )
) OfferRequestSource on OfferRequestSource.SEQ = OfferRequestData.SEQ AND OfferRequestSource.idx = OfferRequestData.idx
LEFT JOIN
(
SELECT
XMLGET(OfferRequestSource.value, 'Abs:FirstNm'):"$"::string AS FirstNm
,XMLGET(OfferRequestSource.value, 'Abs:LastNm'):"$"::string AS LastNm
,XMLGET(OfferRequestSource.value, 'Abs:UserTypeCd'):"$"::string AS UserTypeCd
,XMLGET(OfferRequestSource.value, 'Abs:UpdateTs'):"$"::string AS UpdateTs
,XMLGET(OfferRequestSource.value, 'Abs:TimeZoneCd'):"$"::string AS UserUpdate_TimeZoneCd
,XMLGET(OfferRequestSource.value, 'Abs:CreateTs'):"$"::string AS CreateTs
,XMLGET(OfferRequestSource.value, 'Abs:UserId'):"$"::string AS UserId
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) OfferRequestSource
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:OfferRequestSource>%' )
AND (OfferRequestSource.value like '<Abs:UserUpdate>%' )
) UserUpdate on UserUpdate.SEQ = OfferRequestSource.SEQ AND UserUpdate.idx = OfferRequestSource.idx
AND UserUpdate.SEQ1 = OfferRequestSource.SEQ1 AND UserUpdate.idx1 = OfferRequestSource.idx1
LEFT JOIN
(
SELECT
XMLGET(OfferRequestData.value, 'Abs:LimitWt'):"$"::string AS OfferRestrictionType_LimitWt
,XMLGET(OfferRequestData.value, 'Abs:UsageLimitTypeTxt'):"$"::string AS UsageLimitTypeTxt
,XMLGET(OfferRequestData.value, 'Abs:UsageLimitPeriodNbr'):"$"::string AS UsageLimitPeriodNbr
        ,XMLGET(OfferRequestData.value, 'Abs:UsageLimitNbr'):"$"::string AS UsageLimitNbr
,XMLGET(OfferRequestData.value, 'Abs:LimitQty'):"$"::string AS OfferRestrictionType_LimitQty
,XMLGET(OfferRequestData.value, 'Abs:LimitAmt'):"$"::string AS OfferRestrictionType_LimitAmt
,XMLGET(OfferRequestData.value, 'Abs:LimitVol'):"$"::string AS OfferRestrictionType_LimitVol
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:OfferRestrictionType>%' )
) OfferRestrictionType on OfferRestrictionType.SEQ = OfferRequestData.SEQ AND OfferRestrictionType.idx = OfferRequestData.idx
LEFT JOIN
(
SELECT
XMLGET(OfferRestrictionType.value, 'Abs:Description'):"$"::string AS RestrictionType_Description
,XMLGET(OfferRestrictionType.value, 'Abs:Code'):"$"::string AS RestrictionType_Code
,XMLGET(OfferRestrictionType.value, 'Abs:ShortDescription'):"$"::string AS RestrictionType_ShortDescription
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) OfferRestrictionType
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:OfferRestrictionType>%' )
AND (OfferRestrictionType.value like '<Abs:RestrictionType>%' )
) RestrictionType on RestrictionType.SEQ = OfferRestrictionType.SEQ AND RestrictionType.idx = OfferRestrictionType.idx
AND RestrictionType.SEQ1 = OfferRestrictionType.SEQ1 AND RestrictionType.idx1 = OfferRestrictionType.idx1
LEFT JOIN
(
SELECT
XMLGET(OfferRestrictionType.value, 'Abs:UOMNm'):"$"::string AS OfferRestrictionType_UOMNm
,XMLGET(OfferRestrictionType.value, 'Abs:UOMCd'):"$"::string AS OfferRestrictionType_UOMCd
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) OfferRestrictionType
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:OfferRestrictionType>%' )
AND (OfferRestrictionType.value like '<Abs:UOM>%' )
) OfferRestrictionType_UOM on OfferRestrictionType_UOM.SEQ = OfferRestrictionType.SEQ AND OfferRestrictionType_UOM.idx = OfferRestrictionType.idx
AND OfferRestrictionType_UOM.SEQ1 = OfferRestrictionType.SEQ1 AND OfferRestrictionType_UOM.idx1 = OfferRestrictionType.idx1
----------------------------------------
LEFT JOIN
(
SELECT
XMLGET(OfferRequestData.value, 'Abs:Name'):"$"::string AS Name
,XMLGET(OfferRequestData.value, 'Abs:Code'):"$"::string AS PromotionProgramType_Code
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
        ,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:PromotionProgramType>%' )
) PromotionProgramType on PromotionProgramType.SEQ = OfferRequestData.SEQ AND PromotionProgramType.idx = OfferRequestData.idx
LEFT JOIN
(
SELECT
XMLGET(OfferRequestData.value, 'Abs:Code'):"$"::string AS PromotionSubProgramCode
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
        ,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:PromotionSubProgram>%' )
) PromotionSubProgram on PromotionSubProgram.SEQ = OfferRequestData.SEQ AND PromotionSubProgram.idx = OfferRequestData.idx
----------------------------------
LEFT JOIN
(
SELECT DISTINCT
XMLGET(PromotionProgramType.value, 'Abs:Code'):"$"::string AS Programsubtypecode
,XMLGET(PromotionProgramType.value, 'Abs:Name'):"$"::string AS Programsubtypename
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) PromotionProgramType
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:PromotionProgramType>%' )
AND (PromotionProgramType.value like '<Abs:ProgramSubType>%')
) PromotionProgramSubType on PromotionProgramSubType.SEQ = PromotionProgramType.SEQ AND PromotionProgramSubType.idx = PromotionProgramType.idx
and PromotionProgramSubType.SEQ1=PromotionProgramType.SEQ1 AND PromotionProgramSubType.idx1 = PromotionProgramType.idx1
LEFT JOIN
(
SELECT
XMLGET(OfferRequestChangeDetail.value, 'Abs:ChangeTypeCd'):"$"::string AS ChangeDetailChangeTypeCd
,XMLGET(OfferRequestChangeDetail.value, 'Abs:ChangeTypeQty'):"$"::string AS ChangeDetailChangeTypeQty
,XMLGET(OfferRequestChangeDetail.value, 'Abs:ChangeTypeDsc'):"$"::string AS ChangeDetailChangeTypeDsc
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,OfferRequestChangeDetail.SEQ::integer as SEQ2
,OfferRequestChangeDetail.index::integer as idx2
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) OfferRequestChangeDetail
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:OfferRequestChangeDetail>%' )
AND (OfferRequestChangeDetail.value like '<Abs:ChangeType>%' )
) ChangeType on ChangeType.SEQ = OfferRequestData.SEQ AND ChangeType.idx = OfferRequestData.idx
LEFT JOIN
(
SELECT
XMLGET(OfferRequestChangeDetail.value, 'Abs:ChangeCategoryCd'):"$"::string AS ChangeDetailChangeCategoryCd
,XMLGET(OfferRequestChangeDetail.value, 'Abs:ChangeCategoryQty'):"$"::string AS ChangeDetailChangeCategoryQty
,XMLGET(OfferRequestChangeDetail.value, 'Abs:ChangeCategoryDsc'):"$"::string AS ChangeDetailChangeCategoryDsc
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,OfferRequestChangeDetail.SEQ::integer as SEQ2
,OfferRequestChangeDetail.index::integer as idx2
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) OfferRequestChangeDetail
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:OfferRequestChangeDetail>%' )
AND (OfferRequestChangeDetail.value like '<Abs:ChangeCategory>%' )
) ChangeCategory on ChangeCategory.SEQ = ChangeType.SEQ AND ChangeCategory.idx = ChangeType.idx
AND ChangeCategory.SEQ1 = ChangeType.SEQ1 AND ChangeCategory.idx1 = ChangeType.idx1
LEFT JOIN
(
SELECT
XMLGET(OfferRequestChangeDetail.value, 'Abs:ReasonTypeCd'):"$"::string AS ChangeDetailReasonTypeCd
,XMLGET(OfferRequestChangeDetail.value, 'Abs:ReasonTypeDsc'):"$"::string AS ChangeDetailReasonTypeDsc
,XMLGET(OfferRequestChangeDetail.value, 'Abs:CommentTxt'):"$"::string AS ChangeDetailCommentTxt
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,OfferRequestChangeDetail.SEQ::integer as SEQ2
,OfferRequestChangeDetail.index::integer as idx2
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) OfferRequestChangeDetail
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:OfferRequestChangeDetail>%' )
AND (OfferRequestChangeDetail.value like '<Abs:ReasonType>%' )
) ReasonType on ReasonType.SEQ = ChangeCategory.SEQ AND ReasonType.idx = ChangeCategory.idx
AND ReasonType.SEQ1 = ChangeCategory.SEQ1 AND ReasonType.idx1 = ChangeCategory.idx1
LEFT JOIN
(
SELECT
XMLGET(OfferRequestChangeDetail.value, 'Abs:UserId'):"$"::string AS ChangeByTypeUserId
,XMLGET(OfferRequestChangeDetail.value, 'Abs:FirstNm'):"$"::string AS ChangeByTypeFirstNm
,XMLGET(OfferRequestChangeDetail.value, 'Abs:LastNm'):"$"::string AS ChangeByTypeLastNm
,XMLGET(OfferRequestChangeDetail.value, 'Abs:ChangeByDtTm'):"$"::string AS ChangeByTypeChangeByDtTm
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,OfferRequestChangeDetail.SEQ::integer as SEQ2
,OfferRequestChangeDetail.index::integer as idx2
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) OfferRequestChangeDetail
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:OfferRequestChangeDetail>%' )
AND (OfferRequestChangeDetail.value like '<Abs:ChangeByType>%' )
) ChangeByType on ReasonType.SEQ = ChangeByType.SEQ AND ReasonType.idx = ChangeByType.idx
AND ReasonType.SEQ1 = ChangeByType.SEQ1 AND ReasonType.idx1 = ChangeByType.idx1
    LEFT JOIN
(
SELECT DISTINCT
XMLGET(OfferRequestData.value, 'Abs:NOPAEndDt'):"$"::string AS NOPAEndDt
,XMLGET(OfferRequestData.value, 'Abs:BilledInd'):"$"::string AS BilledInd
,XMLGET(OfferRequestData.value, 'Abs:NOPAStartDt'):"$"::string AS NOPAStartDt
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) vendor
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:VendorPromotionType>%' )
) VendorPromotionType on VendorPromotionType.SEQ = OfferRequestData.SEQ AND VendorPromotionType.idx = OfferRequestData.idx
LEFT JOIN
(
SELECT DISTINCT
GET(vendor.value, '$')::string AS VendorPromotionId
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) vendor
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:VendorPromotionType>%' )
AND (vendor.value like '<Abs:VendorPromotionId>%')
) VendorPromotionId on VendorPromotionId.SEQ = VendorPromotionType.SEQ AND VendorPromotionId.idx = VendorPromotionType.idx
AND VendorPromotionId.SEQ1 = VendorPromotionType.SEQ1 AND VendorPromotionId.idx1 = VendorPromotionType.idx1
LEFT JOIN
(
SELECT
XMLGET(VendorPromotionType.value, 'Abs:ShortDescription'):"$"::string AS AllowanceType_ShortDescription
,XMLGET(VendorPromotionType.value, 'Abs:Code'):"$"::string AS AllowanceType_Code
,XMLGET(VendorPromotionType.value, 'Abs:Description'):"$"::string AS AllowanceType_Description
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) VendorPromotionType
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:VendorPromotionType>%' )
AND (VendorPromotionType.value like '<Abs:AllowanceType>%' )
) AllowanceType on AllowanceType.SEQ = VendorPromotionType.SEQ AND AllowanceType.idx = VendorPromotionType.idx
AND AllowanceType.SEQ1 = VendorPromotionType.SEQ1 AND AllowanceType.idx1 = VendorPromotionType.idx1
LEFT JOIN
(
SELECT
XMLGET(VendorPromotionType.value, 'Abs:Description'):"$"::string AS BillingOptionType_Description
,XMLGET(VendorPromotionType.value, 'Abs:ShortDescription'):"$"::string AS BillingOptionType_ShortDescription
,XMLGET(VendorPromotionType.value, 'Abs:Code'):"$"::string AS BillingOptionType_Code
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) VendorPromotionType
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:VendorPromotionType>%' )
AND (VendorPromotionType.value like '<Abs:BillingOptionType>%' )
) BillingOptionType on BillingOptionType.SEQ = VendorPromotionType.SEQ AND BillingOptionType.idx = VendorPromotionType.idx
AND BillingOptionType.SEQ1 = VendorPromotionType.SEQ1 AND BillingOptionType.idx1 = VendorPromotionType.idx1
LEFT JOIN
(
SELECT
GET(XMLGET(VendorPromotionType.value, 'Abs:StatusTypeCd'), '@Type')::string AS NOPAAssignStatus_StatusTypeCd_Type
,XMLGET(VendorPromotionType.value, 'Abs:StatusTypeCd'):"$"::string AS NOPAAssignStatus_StatusTypeCd
,XMLGET(VendorPromotionType.value, 'Abs:EffectiveDtTm'):"$"::string AS NOPAAssignStatus_EffectiveDtTm
,XMLGET(VendorPromotionType.value, 'Abs:Description'):"$"::string AS NOPAAssignStatus_Description
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) VendorPromotionType
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:VendorPromotionType>%' )
AND (VendorPromotionType.value like '<Abs:NOPAAssignStatus>%' )
) NOPAAssignStatus on NOPAAssignStatus.SEQ = VendorPromotionType.SEQ AND NOPAAssignStatus.idx = VendorPromotionType.idx
AND NOPAAssignStatus.SEQ1 = VendorPromotionType.SEQ1 AND NOPAAssignStatus.idx1 = VendorPromotionType.idx1
LEFT JOIN
(
SELECT
XMLGET(OfferRequestData.value, 'Abs:OfferBankNm'):"$"::string AS OfferBankNm
,XMLGET(OfferRequestData.value, 'Abs:OfferBankId'):"$"::string AS OfferBankId
,XMLGET(OfferRequestData.value, 'Abs:OfferBankTypeCd'):"$"::string AS OfferBankTypeCd
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:OfferBank>%' )
) OfferBank on OfferBank.SEQ = OfferRequestData.SEQ AND OfferBank.idx = OfferRequestData.idx  
LEFT JOIN
(
SELECT
XMLGET(OfferRequestData.value, 'Abs:TemplateId'):"$"::string AS TemplateId
,XMLGET(OfferRequestData.value, 'Abs:TemplateNm'):"$"::string AS TemplateNm
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:OfferRequestTemplate>%' )
) OfferRequestTemplate on OfferRequestTemplate.SEQ = OfferRequestData.SEQ AND OfferRequestTemplate.idx = OfferRequestData.idx
  LEFT JOIN
(
SELECT
XMLGET(OfferRequestData.value, 'Abs:FulfillmentChannelTypeCd'):"$"::string AS FulfillmentChannelTypeCd
,XMLGET(OfferRequestData.value, 'Abs:FulfillmentChannelInd'):"$"::string AS FulfillmentChannelInd
,XMLGET(OfferRequestData.value, 'Abs:FulfillmentChannelDsc'):"$"::string AS FulfillmentChannelDsc
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:OfferFulfillmentChannel>%' )
) OfferFulfillmentChannel on OfferFulfillmentChannel.SEQ = OfferRequestData.SEQ AND OfferFulfillmentChannel.idx = OfferRequestData.idx
  LEFT JOIN
(
SELECT
XMLGET(OfferRequestData.value, 'Abs:ReviewChecklistTypeCd'):"$"::string AS TemplateReviewStatusFlagCd
,XMLGET(OfferRequestData.value, 'Abs:ReviewChecklistInd'):"$"::string AS ReviewChecklistInd
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:OfferReviewChecklist>%' )
) OfferReviewChecklist on OfferReviewChecklist.SEQ = OfferRequestData.SEQ AND OfferReviewChecklist.idx = OfferRequestData.idx
  LEFT JOIN
(
SELECT
XMLGET(OfferRequestData.value, 'Abs:StatusTypeCd'):"$"::string AS TemplateStatusCd
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:OfferRequestTemplateStatus>%' )
) OfferRequestTemplateStatus on OfferRequestTemplateStatus.SEQ = OfferRequestData.SEQ AND OfferRequestTemplateStatus.idx = OfferRequestData.idx
LEFT JOIN
(
SELECT
XMLGET(OfferRequestData.value, 'Abs:Name'):"$"::string AS PromotionProgram_Name
,XMLGET(OfferRequestData.value, 'Abs:Code'):"$"::string AS PromotionProgram_Code
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:PromotionProgram>%' )
) PromotionProgram on PromotionProgram.SEQ = OfferRequestData.SEQ AND PromotionProgram.idx = OfferRequestData.idx
LEFT JOIN
(
SELECT
XMLGET(OfferRequestData.value, 'Abs:PromotionPeriodNm'):"$"::string AS PromotionPeriodNm
,XMLGET(OfferRequestData.value, 'Abs:PromotionPeriodId'):"$"::string AS PromotionPeriodId
,XMLGET(OfferRequestData.value, 'Abs:PromotionWeekId'):"$"::string AS PromotionWeekId
,XMLGET(OfferRequestData.value, 'Abs:PromotionStartDt'):"$"::string AS PromotionStartDt
,XMLGET(OfferRequestData.value, 'Abs:PromotionEndDt'):"$"::string AS PromotionEndDt
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:PromotionPeriodType>%' )
) PromotionPeriodType on PromotionPeriodType.SEQ = OfferRequestData.SEQ AND PromotionPeriodType.idx = OfferRequestData.idx
LEFT JOIN
(
SELECT
XMLGET(OfferRequestData.value, 'Abs:RequiredQty'):"$"::string AS RequiredQty
,XMLGET(OfferRequestData.value, 'Abs:RequiredInd'):"$"::string AS RequiredInd
,XMLGET(OfferRequestData.value, 'Abs:RequirementTypeCd'):"$"::string AS RequirementTypeCd
,XMLGET(OfferRequestData.value, 'Abs:RefundableInd'):"$"::string AS RefundableInd  
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:RequirementType>%' )
) RequirementType on RequirementType.SEQ = OfferRequestData.SEQ AND RequirementType.idx = OfferRequestData.idx
    LEFT JOIN
(
SELECT
XMLGET(OfferRequestData.value, 'Abs:DepartmentNm'):"$"::string AS ChargeBackDepartment_DepartmentNm
,XMLGET(OfferRequestData.value, 'Abs:DepartmentId'):"$"::string AS ChargeBackDepartment_DepartmentId
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:ChargeBackDepartment>%' )
) ChargeBackDepartment on ChargeBackDepartment.SEQ = OfferRequestData.SEQ AND ChargeBackDepartment.idx = OfferRequestData.idx
LEFT JOIN
(
SELECT
XMLGET(OfferRequestData.value, 'Abs:StoreGroupVersionId'):"$"::string AS AttachedOfferType_StoreGroupVersionId
,XMLGET(OfferRequestData.value, 'Abs:DisplayOrderNbr'):"$"::string AS AttachedOfferType_DisplayOrderNbr
,XMLGET(OfferRequestData.value, 'Abs:AttachedOfferTypeId'):"$"::string AS AttachedOfferTypeId
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,OfferRequestData.value as value 
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
) AttachedOfferType on AttachedOfferType.SEQ = OfferRequestData.SEQ AND AttachedOfferType.idx = OfferRequestData.idx
;`

try {
        snowflake.execute ({sqlText: insert_split_logic_tbl_1 });
  }
  catch (err) { 
    throw "insert into wrk table "+ split_logic_tbl_1 +" Failed with error: " + err;   // Return a error message.
  }

var insert_split_logic_tbl_2 = `CREATE OR REPLACE TABLE ` + split_logic_tbl_2 + ` AS 
           WITH LEVEL_1_FLATTEN AS (
   SELECT tbl.SRC_XML:"@"::string AS BODNm
,tbl.FILENAME AS FILENAME
      ,GetOfferRequest.value as Value
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.index::integer as idx
   FROM ` + src_wrk_tbl + ` tbl
 ,LATERAL FLATTEN(tbl.SRC_XML:"$") GetOfferRequest
)
SELECT DISTINCT FILENAME
,BODNM
,DOCUMENT_RELEASEID
,DOCUMENT_VERSIONID
,DOCUMENT_SYSTEMENVIRONMENTCD
,DOCUMENTID
,EXTERNALTARGETIND
,INTERCHANGETIME
,INTERCHANGEDATE
,INTERNALFILETRANSFERIND
,ROUTINGSYSTEMNM
,RECEIVERID
,GATEWAYNM
,SENDERID
,TARGETAPPLICATIONCD
,SOURCEAPPLICATIONCD
,DOCUMENT_DESCRIPTION
,CREATIONDT
,DOCUMENTNM
,INBOUNDOUTBOUNDIND
,ALTERNATEDOCUMENTID
,NOTE
,TRIGGERID
,SAVINGSVALUETXT
,OFFERREQUESTDATA_DISCLAIMERTXT
,OFFERREQUESTDATA_IMAGEID
,OFFERREQUESTREFERENCEID_QUALIFIERCD
,OFFERREQUESTREFERENCEID
,BUSINESSJUSTIFICATIONTXT
,OFFERREQUESTTYPECD
,OFFERITEMDSC
,OFFERREQUESTCOMMENTTXT
,VERSIONQTY
,TIERQTY
,PRODUCTQTY
,STOREGROUPQTY
,CUSTOMERSEGMENTINFOTXT
,DELETEDOFFERTXT
,BRANDINFOTXT
,OFFERREQUESTID
,SIZEDSC
,DEPARTMENTID
,DEPARTMENTNM
,OFFERNM
,OFFERREQUESTDSC
,OFFERFLAGDSC
,UPCQTYTXT
,ATTACHEDOFFERTYPE_STOREGROUPVERSIONID
,ATTACHEDOFFERTYPE_DISPLAYORDERNBR
,ATTACHEDOFFERTYPEID
,ATTACHEDOFFER_DISCOUNTID
,ATTACHEDOFFER_DISCOUNTVERSIONID
,ATTACHEDOFFER_PRODUCTGROUPVERSIONID
,ATTACHEDOFFER_STOREGROUPVERSIONID
,DISTINCTID
,OFFERID
,REFERENCEOFFERID
,ATTACHEDOFFER_INSTANTWINVERSIONID
,ATTACHEDOFFER_OFFERRANKNBR
,OFFERREQUESTSTATUS_TIMEZONECD
,OFFERREQUESTSTATUS_EFFECTIVEDTTM
,OFFERREQUESTSTATUS_DESCRIPTION
,OFFERREQUESTSTATUS_STATUSTYPECD_TYPE
,OFFERREQUESTSTATUS_STATUSTYPECD
,OFFERREQUESTSTATUSTYPECD
,APPLIEDIND
,PROGRAMNM
,STATUS_STATUSTYPECD_TYPE
,STATUS_STATUSTYPECD
,STATUS_DESCRIPTION
,STATUS_EFFECTIVEDTTM
,FREQUENCYDSC
,INSTANTWINPROGRAMID
,PRIZEITEMQTY
,INSTANTWINVERSIONID
,PODDETAILTYPE_DISCLAIMERTXT
,PODDETAILTYPE_DISPLAYENDDT
,PODDETAILTYPE_DISPLAYSTARTDT
,HEADLINETXT
,PRICEINFOTXT
,OFFERDSC
,PODDETAILTYPE_HEADLINESUBTXT
,PODDETAILTYPE_ITEMQTY
,CUSTOMERFRIENDLYCATEGORY_DESCRIPTION
,CUSTOMERFRIENDLYCATEGORY_SHORTDESCRIPTION
,CUSTOMERFRIENDLYCATEGORY_CODE
,DISPLAYIMAGE_IMAGEID
,IMAGETYPECD
,EVENTID
,EVENTNM
,DISCOUNTVERSION_DISCOUNTVERSIONID
,AIRMILEPROGRAMNM
,AIRMILEPROGRAMID
,AIRMILEPOINTQTY
,AIRMILETIERNM
,DISCOUNT_DISCOUNTID
,CHARGEBACKDEPARTMENTNM
,CHARGEBACKDEPARTMENTID
,EXCLUDEDPRODUCTGROUPNM
,EXCLUDEDPRODUCTGROUPID
,INCLUDEDPRODUCTGROUPNM
,INCLUDEDPRODUCTGROUPID
,BENEFITVALUEQTY
,DISPLAYORDERNBR
,BENEFITVALUETYPE_CODE
,BENEFITVALUETYPE_DESCRIPTION
,BENEFITVALUETYPE_SHORTDESCRIPTION
,DISCOUNTUPTOQTY
,RECEIPTTXT
,REWARDQTY
,DISCOUNTAMT
,TIERLEVELNBR
,LIMITTYPE_LIMITQTY
,LIMITTYPE_LIMITWT
,LIMITTYPE_LIMITVOL
,LIMITTYPE_LIMITAMT
,LIMITTYPE_UOMCD
,LIMITTYPE_UOMNM
,DISCOUNTTYPE_SHORTDESCRIPTION
,DISCOUNTTYPE_DESCRIPTION
,DISCOUNTTYPE_CODE
,PRODUCTGROUP_DISPLAYORDERNBR
,ITEMQTY
,GIFTCARDIND
,ANYPRODUCTIND
,INHERITEDIND
,CONJUNCTIONDSC
,MINIMUMPURCHASEAMT
,MAXIMUMPURCHASEAMT
,UNIQUEITEMIND
,PRODUCTGROUP_PRODUCTGROUPNM
,PRODUCTGROUPDSC
,PRODUCTGROUP_PRODUCTGROUPID
,PRODUCTGROUP_PRODUCTGROUPVERSIONID
,EXCLUDEDPRODUCTGROUP_PRODUCTGROUPNM
,EXCLUDEDPRODUCTGROUP_PRODUCTGROUPID
,TIERLEVELAMT
,TIERLEVELID
,PRODUCTGROUP_UOMCD
,PRODUCTGROUP_UOMNM
,UOMDSC
,PROTOTYPE_DESCRIPTION
,PROTOTYPE_CODE
,PROTOTYPE_SHORTDESCRIPTION
,STOREGROUPID
,STOREGROUPNM
,STOREGROUPDSC
,STOREGROUPTYPE_CODE
,STOREGROUPTYPE_DESCRIPTION
,STOREGROUPTYPE_SHORTDESCRIPTION
,TAGDSC
,LOYALTYPGMTAGIND
,TAGAMT
,TAGNBR
,CORPORATEITEMCD
,UPC_QUALIFIER
,UPCNBR
,UPCTXT
,UPCDSC
,REPRESENTATIVESTATUS_STATUSTYPECD_TYPE
,REPRESENTATIVESTATUS_STATUSTYPECD
,EFFECTIVEENDDT
,REPRESENTATIVESTATUS_DESCRIPTION
,REPRESENTATIVESTATUS_EFFECTIVEDTTM
,STATUSREASON_SHORTDESCRIPTION
,STATUSREASON_DESCRIPTION
,STATUSREASON_CODE
,ITEMOFFERPRICEAMT
,EFFECTIVEENDTS
,EFFECTIVESTARTTS
,ITEMOFFERPRICE_UOMNM
,ITEMOFFERPRICE_UOMCD
,OFFERDETAIL_SHORTDESCRIPTION
,OFFERDETAIL_DESCRIPTION
,OFFERDETAIL_CODE
,PODDETAILTYPE_UOMCD
,PODDETAILTYPE_UOMNM
,SHOPPINGLISTCATEGORY_DESCRIPTION
,SHOPPINGLISTCATEGORY_SHORTDESCRIPTION
,SHOPPINGLISTCATEGORY_CODE
,PODCATEGORY_DESCRIPTION
,PODCATEGORY_SHORTDESCRIPTION
,PODCATEGORY_CODE
,PODDetailType_VendorNm
,PODDetailType_Land
,PODDetailType_Space
,PODDetailType_Slot
,EcommBehaviorCd
, DOCUMENT.SEQ as SEQ
, DOCUMENT.idx as IDX
, AttachedOfferType.seq1 as SEQ1
, AttachedOfferType.idx1 as IDX1 
 from 
(
SELECT
FILENAME
,BODnm
,GET(DocumentData.value, '@ReleaseId')::string AS Document_ReleaseId
,GET(DocumentData.value, '@VersionId')::string AS Document_VersionId
,GET(DocumentData.value, '@SystemEnvironmentCd')::string AS Document_SystemEnvironmentCd
,XMLGET(DocumentData.value, 'Abs:DocumentID'):"$"::string AS DocumentID
,XMLGET(DocumentData.value, 'Abs:ExternalTargetInd'):"$"::string AS ExternalTargetInd
,XMLGET(DocumentData.value, 'Abs:InterchangeTime'):"$"::string AS InterchangeTime
,XMLGET(DocumentData.value, 'Abs:InterchangeDate'):"$"::string AS InterchangeDate
,XMLGET(DocumentData.value, 'Abs:InternalFileTransferInd'):"$"::string AS InternalFileTransferInd
,XMLGET(DocumentData.value, 'Abs:RoutingSystemNm'):"$"::string AS RoutingSystemNm
,XMLGET(DocumentData.value, 'Abs:ReceiverId'):"$"::string AS ReceiverId
,XMLGET(DocumentData.value, 'Abs:GatewayNm'):"$"::string AS GatewayNm
,XMLGET(DocumentData.value, 'Abs:SenderId'):"$"::string AS SenderId
,XMLGET(DocumentData.value, 'Abs:TargetApplicationCd'):"$"::string AS TargetApplicationCd
,XMLGET(DocumentData.value, 'Abs:SourceApplicationCd'):"$"::string AS SourceApplicationCd
,XMLGET(DocumentData.value, 'Abs:Description'):"$"::string AS Document_Description
,XMLGET(DocumentData.value, 'Abs:CreationDt'):"$"::string AS CreationDt
,XMLGET(DocumentData.value, 'Abs:DocumentNm'):"$"::string AS DocumentNm
,XMLGET(DocumentData.value, 'Abs:InboundOutboundInd'):"$"::string AS InboundOutboundInd
,XMLGET(DocumentData.value, 'Abs:AlternateDocumentID'):"$"::string AS AlternateDocumentID
,XMLGET(DocumentData.value, 'Abs:Note'):"$"::string AS Note
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,DocumentData.SEQ::integer as SEQ1
,DocumentData.index::integer as idx1
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) DocumentData
WHERE GetOfferRequest.value like '<DocumentData>%'
AND (DocumentData.value like '<Document>%'
          OR DocumentData.value like '<Document%ReleaseId%'
          OR DocumentData.value like '<Document%VersionId%'
          OR DocumentData.value like '<Document%SystemEnvironmentCd%' )
) Document
LEFT JOIN
(
SELECT
  XMLGET(GetOfferRequest.value, 'Abs:TriggerId'):"$"::string AS TriggerId
 ,XMLGET(GetOfferRequest.value, 'Abs:SavingsValueTxt'):"$"::string AS SavingsValueTxt
 ,XMLGET(GetOfferRequest.value, 'Abs:DisclaimerTxt'):"$"::string AS OfferRequestData_DisclaimerTxt
 ,XMLGET(GetOfferRequest.value, 'Abs:ImageId'):"$"::string AS OfferRequestData_ImageId
      //,GET(XMLGET(GetOfferRequest.value, 'Abs:OfferEffectiveDay'), '@Qualifier')::string AS OfferEffectiveDay_Qualifier
      // ,XMLGET(GetOfferRequest.value, 'Abs:OfferEffectiveDay'):"$"::string AS OfferEffectiveDay
 //,GET(days.value, '@Qualifier')::string AS OfferEffectiveDay_Qualifier
 //,GET(days.value, '$')::string AS OfferEffectiveDay
 ,GET(XMLGET(GetOfferRequest.value, 'Abs:OfferRequestReferenceId'), '@QualifierCd')::string AS OfferRequestReferenceId_QualifierCd
 ,XMLGET(GetOfferRequest.value, 'Abs:OfferRequestReferenceId'):"$"::string AS OfferRequestReferenceId
 ,XMLGET(GetOfferRequest.value, 'Abs:BusinessJustificationTxt'):"$"::string AS BusinessJustificationTxt
 ,XMLGET(GetOfferRequest.value, 'Abs:OfferRequestTypeCd'):"$"::string AS OfferRequestTypeCd
 ,XMLGET(GetOfferRequest.value, 'Abs:OfferItemDsc'):"$"::string AS OfferItemDsc
 ,XMLGET(GetOfferRequest.value, 'Abs:OfferRequestCommentTxt'):"$"::string AS OfferRequestCommentTxt
 ,XMLGET(GetOfferRequest.value, 'Abs:VersionQty'):"$"::string AS VersionQty
 ,XMLGET(GetOfferRequest.value, 'Abs:TierQty'):"$"::string AS TierQty
 ,XMLGET(GetOfferRequest.value, 'Abs:ProductQty'):"$"::string AS ProductQty
 ,XMLGET(GetOfferRequest.value, 'Abs:StoreGroupQty'):"$"::string AS StoreGroupQty
 ,XMLGET(GetOfferRequest.value, 'Abs:CustomerSegmentInfoTxt'):"$"::string AS CustomerSegmentInfoTxt
 ,XMLGET(GetOfferRequest.value, 'Abs:DeletedOfferTxt'):"$"::string AS DeletedOfferTxt
 ,XMLGET(GetOfferRequest.value, 'Abs:BrandInfoTxt'):"$"::string AS BrandInfoTxt
 ,XMLGET(GetOfferRequest.value, 'Abs:OfferRequestId'):"$"::string AS OfferRequestId
 ,XMLGET(GetOfferRequest.value, 'Abs:SizeDsc'):"$"::string AS SizeDsc
 ,XMLGET(GetOfferRequest.value, 'Abs:DepartmentId'):"$"::string AS DepartmentId
 ,XMLGET(GetOfferRequest.value, 'Abs:DepartmentNm'):"$"::string AS DepartmentNm
 ,XMLGET(GetOfferRequest.value, 'Abs:OfferNm'):"$"::string AS OfferNm
 ,XMLGET(GetOfferRequest.value, 'Abs:OfferRequestDsc'):"$"::string AS OfferRequestDsc
 ,XMLGET(GetOfferRequest.value, 'Abs:OfferFlagDsc'):"$"::string AS OfferFlagDsc
 ,XMLGET(GetOfferRequest.value, 'Abs:UPCQtyTxt'):"$"::string AS UPCQtyTxt 
 ,XMLGET(GetOfferRequest.value, 'Abs:EcommBehaviorCd'):"$"::string AS EcommBehaviorCd 
 
 ,GetOfferRequest.SEQ::integer as SEQ
 ,GetOfferRequest.idx::integer as idx
FROM    LEVEL_1_FLATTEN AS GetOfferRequest
// ,LATERAL FLATTEN(GetOfferRequest.value:"$") as days
WHERE   GetOfferRequest.value like '<OfferRequestData>%'
// AND days.value like '<Abs:OfferEffectiveDay%'
) OfferRequestData on OfferRequestData.SEQ = Document.SEQ
LEFT JOIN
(
SELECT
XMLGET(OfferRequestData.value, 'Abs:StoreGroupVersionId'):"$"::string AS AttachedOfferType_StoreGroupVersionId
,XMLGET(OfferRequestData.value, 'Abs:DisplayOrderNbr'):"$"::string AS AttachedOfferType_DisplayOrderNbr
,XMLGET(OfferRequestData.value, 'Abs:AttachedOfferTypeId'):"$"::string AS AttachedOfferTypeId
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
) AttachedOfferType on AttachedOfferType.SEQ = OfferRequestData.SEQ AND AttachedOfferType.idx = OfferRequestData.idx
LEFT JOIN
(
select distinct * from 
(SELECT
XMLGET(AttachedOfferType.value, 'Abs:DiscountId'):"$"::string AS AttachedOffer_DiscountId
,XMLGET(AttachedOfferType.value, 'Abs:DiscountVersionId'):"$"::string AS AttachedOffer_DiscountVersionId
,XMLGET(AttachedOfferType.value, 'Abs:ProductGroupVersionId'):"$"::string AS AttachedOffer_ProductGroupVersionId
,XMLGET(AttachedOfferType.value, 'Abs:StoreGroupVersionId'):"$"::string AS AttachedOffer_StoreGroupVersionId
,XMLGET(AttachedOfferType.value, 'Abs:DistinctId'):"$"::string AS DistinctId
,XMLGET(AttachedOfferType.value, 'Abs:OfferId'):"$"::string AS OfferId
,XMLGET(AttachedOfferType.value, 'Abs:ReferenceOfferId'):"$"::string AS ReferenceOfferId
        ,XMLGET(AttachedOfferType.value, 'Abs:InstantWinVersionId'):"$"::string AS AttachedOffer_InstantWinVersionId
        ,XMLGET(AttachedOfferType.value, 'Abs:OfferRankNbr'):"$"::string AS AttachedOffer_OfferRankNbr
		
		,XMLGET(OfferRequestStatus.value, 'Abs:TimeZoneCd'):"$"::string AS OfferRequestStatus_TimeZoneCd
,XMLGET(OfferRequestStatus.value, 'Abs:EffectiveDtTm'):"$"::string AS OfferRequestStatus_EffectiveDtTm
,XMLGET(OfferRequestStatus.value, 'Abs:Description'):"$"::string AS OfferRequestStatus_Description
,GET(XMLGET(OfferRequestStatus.value, 'Abs:StatusTypeCd'), '@Type')::string AS OfferRequestStatus_StatusTypeCd_Type
,XMLGET(OfferRequestStatus.value, 'Abs:StatusTypeCd'):"$"::string AS OfferRequestStatus_StatusTypeCd
,XMLGET(OfferRequestStatus.value, 'Abs:OfferRequestStatusTypeCd'):"$"::string AS OfferRequestStatusTypeCd
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,AttachedOfferType.SEQ::integer as SEQ2
,AttachedOfferType.index::integer as idx2
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestStatus
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) AttachedOfferType
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestStatus.value like '<Abs:OfferRequestStatus>%' )
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
AND (AttachedOfferType.value like '<Abs:AttachedOffer>%' ))
where (ReferenceOfferId like '%-D%' and OFFERREQUESTSTATUSTYPECD = 'digital')
or (ReferenceOfferId like '%-ND%' and OFFERREQUESTSTATUSTYPECD = 'nonDigital')
or (OFFERREQUESTSTATUSTYPECD = 'offerTemplate')
) AttachedOffer on AttachedOffer.SEQ = AttachedOfferType.SEQ AND AttachedOffer.idx = AttachedOfferType.idx
LEFT JOIN
(
SELECT
XMLGET(AttachedOffer.value, 'Abs:AppliedInd'):"$"::string AS AppliedInd
,XMLGET(AttachedOffer.value, 'Abs:ProgramNm'):"$"::string AS ProgramNm
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,AttachedOfferType.SEQ::integer as SEQ2
,AttachedOfferType.index::integer as idx2
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestStatus
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) AttachedOfferType
,LATERAL FLATTEN(TO_ARRAY(AttachedOfferType.value:"$")) AttachedOffer
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestStatus.value like '<Abs:OfferRequestStatus>%' )
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
AND (AttachedOfferType.value like '<Abs:AttachedOffer>%' )
AND (AttachedOffer.value like '<Abs:AppliedProgram>%' )
) AppliedProgram on AppliedProgram.SEQ = AttachedOffer.SEQ AND AppliedProgram.idx = AttachedOffer.idx
AND AppliedProgram.SEQ1 = AttachedOffer.SEQ1 AND AppliedProgram.idx1 = AttachedOffer.idx1
AND AppliedProgram.SEQ2 = AttachedOffer.SEQ2 AND AppliedProgram.idx2 = AttachedOffer.idx2
LEFT JOIN
(
SELECT
GET(XMLGET(AttachedOffer.value, 'Abs:StatusTypeCd'), '@Type')::string AS Status_StatusTypeCd_Type
,XMLGET(AttachedOffer.value, 'Abs:StatusTypeCd'):"$"::string AS Status_StatusTypeCd
,XMLGET(AttachedOffer.value, 'Abs:Description'):"$"::string AS Status_Description
,XMLGET(AttachedOffer.value, 'Abs:EffectiveDtTm'):"$"::string AS Status_EffectiveDtTm
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,AttachedOfferType.SEQ::integer as SEQ2
,AttachedOfferType.index::integer as idx2
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestStatus
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) AttachedOfferType
,LATERAL FLATTEN(TO_ARRAY(AttachedOfferType.value:"$")) AttachedOffer
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestStatus.value like '<Abs:OfferRequestStatus>%' )
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
AND (AttachedOfferType.value like '<Abs:AttachedOffer>%' )
AND (AttachedOffer.value like '<Abs:Status>%' )
) Status on Status.SEQ = AttachedOffer.SEQ AND Status.idx = AttachedOffer.idx
AND Status.SEQ1 = AttachedOffer.SEQ1 AND Status.idx1 = AttachedOffer.idx1
AND Status.SEQ2 = AttachedOffer.SEQ2 AND Status.idx2 = AttachedOffer.idx2
LEFT JOIN
(
SELECT
XMLGET(AttachedOfferType.value, 'Abs:FrequencyDsc'):"$"::string AS FrequencyDsc
,XMLGET(AttachedOfferType.value, 'Abs:InstantWinProgramId'):"$"::string AS InstantWinProgramId
,XMLGET(AttachedOfferType.value, 'Abs:PrizeItemQty'):"$"::string AS PrizeItemQty
,XMLGET(AttachedOfferType.value, 'Abs:InstantWinVersionId'):"$"::string AS InstantWinVersionId
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) AttachedOfferType
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
AND (AttachedOfferType.value like '<Abs:InstantWinProgramType>%' )
) InstantWinProgramType on InstantWinProgramType.SEQ = AttachedOfferType.SEQ AND InstantWinProgramType.idx = AttachedOfferType.idx
AND InstantWinProgramType.SEQ1 = AttachedOfferType.SEQ1 AND InstantWinProgramType.idx1 = AttachedOfferType.idx1
LEFT JOIN
(
SELECT
XMLGET(AttachedOfferType.value, 'Abs:DisclaimerTxt'):"$"::string AS PODDetailType_DisclaimerTxt
,XMLGET(AttachedOfferType.value, 'Abs:DisplayEndDt'):"$"::string AS PODDetailType_DisplayEndDt
,XMLGET(AttachedOfferType.value, 'Abs:DisplayStartDt'):"$"::string AS PODDetailType_DisplayStartDt
,XMLGET(AttachedOfferType.value, 'Abs:HeadlineTxt'):"$"::string AS HeadlineTxt
,XMLGET(AttachedOfferType.value, 'Abs:PriceInfoTxt'):"$"::string AS PriceInfoTxt
,XMLGET(AttachedOfferType.value, 'Abs:OfferDsc'):"$"::string AS OfferDsc
  ,XMLGET(AttachedOfferType.value, 'Abs:HeadlineSubTxt'):"$"::string AS PODDetailType_HeadlineSubTxt
,XMLGET(AttachedOfferType.value, 'Abs:ItemQty'):"$"::string AS PODDetailType_ItemQty
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,AttachedOfferType.SEQ::integer as SEQ2
,AttachedOfferType.index::integer as idx2
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) AttachedOfferType
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
AND (AttachedOfferType.value like '<Abs:PODDetailType>%' )
) PODDetailType on PODDetailType.SEQ = AttachedOfferType.SEQ AND PODDetailType.idx = AttachedOfferType.idx
AND PODDetailType.SEQ1 = AttachedOfferType.SEQ1 AND PODDetailType.idx1 = AttachedOfferType.idx1
LEFT JOIN
(
SELECT
XMLGET(PODDetailType.value, 'Abs:Description'):"$"::string AS CustomerFriendlyCategory_Description
,XMLGET(PODDetailType.value, 'Abs:ShortDescription'):"$"::string AS CustomerFriendlyCategory_ShortDescription
,XMLGET(PODDetailType.value, 'Abs:Code'):"$"::string AS CustomerFriendlyCategory_Code
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,AttachedOfferType.SEQ::integer as SEQ2
,AttachedOfferType.index::integer as idx2
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) AttachedOfferType
,LATERAL FLATTEN(TO_ARRAY(AttachedOfferType.value:"$")) PODDetailType
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
AND (AttachedOfferType.value like '<Abs:PODDetailType>%' )
AND (PODDetailType.value like '<Abs:CustomerFriendlyCategory>%' )
) CustomerFriendlyCategory on CustomerFriendlyCategory.SEQ = PODDetailType.SEQ AND CustomerFriendlyCategory.idx = PODDetailType.idx
AND CustomerFriendlyCategory.SEQ1 = PODDetailType.SEQ1 AND CustomerFriendlyCategory.idx1 = PODDetailType.idx1
AND CustomerFriendlyCategory.SEQ2 = PODDetailType.SEQ2 AND CustomerFriendlyCategory.idx2 = PODDetailType.idx2
LEFT JOIN
(
SELECT
XMLGET(PODDetailType.value, 'Abs:ImageId'):"$"::string AS DisplayImage_ImageId
,XMLGET(PODDetailType.value, 'Abs:ImageTypeCd'):"$"::string AS ImageTypeCd
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,AttachedOfferType.SEQ::integer as SEQ2
,AttachedOfferType.index::integer as idx2
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) AttachedOfferType
,LATERAL FLATTEN(TO_ARRAY(AttachedOfferType.value:"$")) PODDetailType
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
AND (AttachedOfferType.value like '<Abs:PODDetailType>%' )
AND (PODDetailType.value like '<Abs:DisplayImage>%' )
) DisplayImage on DisplayImage.SEQ = PODDetailType.SEQ AND DisplayImage.idx = PODDetailType.idx
AND DisplayImage.SEQ1 = PODDetailType.SEQ1 AND DisplayImage.idx1 = PODDetailType.idx1
AND DisplayImage.SEQ2 = PODDetailType.SEQ2 AND DisplayImage.idx2 = PODDetailType.idx2
LEFT JOIN
(
SELECT
XMLGET(PODDetailType.value, 'Abs:EventId'):"$"::string AS EventId
,XMLGET(PODDetailType.value, 'Abs:EventNm'):"$"::string AS EventNm
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,AttachedOfferType.SEQ::integer as SEQ2
,AttachedOfferType.index::integer as idx2
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) AttachedOfferType
,LATERAL FLATTEN(TO_ARRAY(AttachedOfferType.value:"$")) PODDetailType
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
AND (AttachedOfferType.value like '<Abs:PODDetailType>%' )
AND (PODDetailType.value like '<Abs:SpecialEventType>%' )
) SpecialEventType on SpecialEventType.SEQ = PODDetailType.SEQ AND SpecialEventType.idx = PODDetailType.idx
AND SpecialEventType.SEQ1 = PODDetailType.SEQ1 AND SpecialEventType.idx1 = PODDetailType.idx1
AND SpecialEventType.SEQ2 = PODDetailType.SEQ2 AND SpecialEventType.idx2 = PODDetailType.idx2
LEFT JOIN
(
SELECT
XMLGET(ProductGroupVersion.value, 'Abs:DiscountVersionId'):"$"::string AS DiscountVersion_DiscountVersionId
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,AttachedOfferType.SEQ::integer as SEQ2
,AttachedOfferType.index::integer as idx2
,ProductGroupVersion.SEQ::integer as SEQ3
,ProductGroupVersion.index::integer as idx3
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) AttachedOfferType
,LATERAL FLATTEN(TO_ARRAY(AttachedOfferType.value:"$")) ProductGroupVersion
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
AND (AttachedOfferType.value like '<Abs:ProductGroupVersion>%' )
AND (ProductGroupVersion.value like '<Abs:DiscountVersion>%' )
) DiscountVersion on DiscountVersion.SEQ = AttachedOfferType.SEQ AND DiscountVersion.idx = AttachedOfferType.idx
AND DiscountVersion.SEQ1 = AttachedOfferType.SEQ1 AND DiscountVersion.idx1 = AttachedOfferType.idx1
LEFT JOIN
(
SELECT
XMLGET(DiscountVersion.value, 'Abs:AirMileProgramNm'):"$"::string AS AirMileProgramNm
,XMLGET(DiscountVersion.value, 'Abs:AirMileProgramId'):"$"::string AS AirMileProgramId
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,AttachedOfferType.SEQ::integer as SEQ2
,AttachedOfferType.index::integer as idx2
,ProductGroupVersion.SEQ::integer as SEQ3
,ProductGroupVersion.index::integer as idx3
,DiscountVersion.SEQ::integer as SEQ4
,DiscountVersion.index::integer as idx4
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) AttachedOfferType
,LATERAL FLATTEN(TO_ARRAY(AttachedOfferType.value:"$")) ProductGroupVersion
,LATERAL FLATTEN(TO_ARRAY(ProductGroupVersion.value:"$")) DiscountVersion
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
AND (AttachedOfferType.value like '<Abs:ProductGroupVersion>%' )
AND (ProductGroupVersion.value like '<Abs:DiscountVersion>%' )
AND (DiscountVersion.value like '<Abs:AirMileProgram>%' )
) AirMileProgram on AirMileProgram.SEQ = DiscountVersion.SEQ AND AirMileProgram.idx = DiscountVersion.idx
AND AirMileProgram.SEQ1 = DiscountVersion.SEQ1 AND AirMileProgram.idx1 = DiscountVersion.idx1
AND AirMileProgram.SEQ2 = DiscountVersion.SEQ2 AND AirMileProgram.idx2 = DiscountVersion.idx2
AND AirMileProgram.SEQ3 = DiscountVersion.SEQ3 AND AirMileProgram.idx3 = DiscountVersion.idx3
AND     AttachedOffer.AttachedOffer_DiscountId = AirMileProgram.AirMileProgramId
LEFT JOIN
(
SELECT
XMLGET(AirMileProgram.value, 'Abs:AirMilePointQty'):"$"::string AS AirMilePointQty
,XMLGET(AirMileProgram.value, 'Abs:AirMileTierNm'):"$"::string AS AirMileTierNm
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,AttachedOfferType.SEQ::integer as SEQ2
,AttachedOfferType.index::integer as idx2
,ProductGroupVersion.SEQ::integer as SEQ3
,ProductGroupVersion.index::integer as idx3
,DiscountVersion.SEQ::integer as SEQ4
,DiscountVersion.index::integer as idx4
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) AttachedOfferType
,LATERAL FLATTEN(TO_ARRAY(AttachedOfferType.value:"$")) ProductGroupVersion
,LATERAL FLATTEN(TO_ARRAY(ProductGroupVersion.value:"$")) DiscountVersion
,LATERAL FLATTEN(TO_ARRAY(DiscountVersion.value:"$")) AirMileProgram
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
AND (AttachedOfferType.value like '<Abs:ProductGroupVersion>%' )
AND (ProductGroupVersion.value like '<Abs:DiscountVersion>%' )
AND (DiscountVersion.value like '<Abs:AirMileProgram>%' )
AND (AirMileProgram.value like '<Abs:AirMileTier>%' )
) AirMileTier on AirMileTier.SEQ = AirMileProgram.SEQ AND AirMileTier.idx = AirMileProgram.idx
AND AirMileTier.SEQ1 = AirMileProgram.SEQ1 AND AirMileTier.idx1 = AirMileProgram.idx1
AND AirMileTier.SEQ2 = AirMileProgram.SEQ2 AND AirMileTier.idx2 = AirMileProgram.idx2
AND AirMileTier.SEQ3 = AirMileProgram.SEQ3 AND AirMileTier.idx3 = AirMileProgram.idx3
AND AirMileTier.SEQ4 = AirMileProgram.SEQ4 AND AirMileTier.idx4 = AirMileProgram.idx4
LEFT JOIN
(
SELECT
XMLGET(DiscountVersion.value, 'Abs:DiscountId'):"$"::string AS Discount_DiscountId
,XMLGET(DiscountVersion.value, 'Abs:ChargebackDepartmentNm'):"$"::string AS ChargebackDepartmentNm
,XMLGET(DiscountVersion.value, 'Abs:ChargebackDepartmentId'):"$"::string AS ChargebackDepartmentId
,XMLGET(DiscountVersion.value, 'Abs:ExcludedProductGroupNm'):"$"::string AS ExcludedProductGroupNm
,XMLGET(DiscountVersion.value, 'Abs:ExcludedProductGroupId'):"$"::string AS ExcludedProductGroupId
,XMLGET(DiscountVersion.value, 'Abs:IncludedProductgroupNm'):"$"::string AS IncludedProductgroupNm
,XMLGET(DiscountVersion.value, 'Abs:IncludedProductGroupId'):"$"::string AS IncludedProductGroupId
,XMLGET(DiscountVersion.value, 'Abs:BenefitValueQty'):"$"::string AS BenefitValueQty
  ,XMLGET(DiscountVersion.value, 'Abs:DisplayOrderNbr'):"$"::string AS DisplayOrderNbr
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,AttachedOfferType.SEQ::integer as SEQ2
,AttachedOfferType.index::integer as idx2
,ProductGroupVersion.SEQ::integer as SEQ3
,ProductGroupVersion.index::integer as idx3
,DiscountVersion.SEQ::integer as SEQ4
,DiscountVersion.index::integer as idx4
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) AttachedOfferType
,LATERAL FLATTEN(TO_ARRAY(AttachedOfferType.value:"$")) ProductGroupVersion
,LATERAL FLATTEN(TO_ARRAY(ProductGroupVersion.value:"$")) DiscountVersion
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
AND (AttachedOfferType.value like '<Abs:ProductGroupVersion>%' )
AND (ProductGroupVersion.value like '<Abs:DiscountVersion>%' )
AND (DiscountVersion.value like '<Abs:Discount>%' )
) Discount on Discount.SEQ = DiscountVersion.SEQ AND Discount.idx = DiscountVersion.idx
AND Discount.SEQ1 = DiscountVersion.SEQ1 AND Discount.idx1 = DiscountVersion.idx1
AND Discount.SEQ2 = DiscountVersion.SEQ2 AND Discount.idx2 = DiscountVersion.idx2
AND Discount.SEQ3 = DiscountVersion.SEQ3 AND Discount.idx3 = DiscountVersion.idx3
AND AttachedOffer.AttachedOffer_DiscountId=Discount.Discount_DiscountId
LEFT JOIN
(
SELECT
XMLGET(Discount.value, 'Abs:Code'):"$"::string AS BenefitValueType_Code
,XMLGET(Discount.value, 'Abs:Description'):"$"::string AS BenefitValueType_Description
,XMLGET(Discount.value, 'Abs:ShortDescription'):"$"::string AS BenefitValueType_ShortDescription
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,AttachedOfferType.SEQ::integer as SEQ2
,AttachedOfferType.index::integer as idx2
,ProductGroupVersion.SEQ::integer as SEQ3
,ProductGroupVersion.index::integer as idx3
,DiscountVersion.SEQ::integer as SEQ4
,DiscountVersion.index::integer as idx4
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) AttachedOfferType
,LATERAL FLATTEN(TO_ARRAY(AttachedOfferType.value:"$")) ProductGroupVersion
,LATERAL FLATTEN(TO_ARRAY(ProductGroupVersion.value:"$")) DiscountVersion
,LATERAL FLATTEN(TO_ARRAY(DiscountVersion.value:"$")) Discount
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
AND (AttachedOfferType.value like '<Abs:ProductGroupVersion>%' )
AND (ProductGroupVersion.value like '<Abs:DiscountVersion>%' )
AND (DiscountVersion.value like '<Abs:Discount>%' )
AND (Discount.value like '<Abs:BenefitValueType>%' )
) BenefitValueType on BenefitValueType.SEQ = Discount.SEQ AND BenefitValueType.idx = Discount.idx
AND BenefitValueType.SEQ1 = Discount.SEQ1 AND BenefitValueType.idx1 = Discount.idx1
AND BenefitValueType.SEQ2 = Discount.SEQ2 AND BenefitValueType.idx2 = Discount.idx2
AND BenefitValueType.SEQ3 = Discount.SEQ3 AND BenefitValueType.idx3 = Discount.idx3
AND BenefitValueType.SEQ4 = Discount.SEQ4 AND BenefitValueType.idx4 = Discount.idx4
LEFT JOIN
(
SELECT
XMLGET(Discount.value, 'Abs:DiscountUptoQty'):"$"::string AS DiscountUptoQty
,XMLGET(Discount.value, 'Abs:ReceiptTxt'):"$"::string AS ReceiptTxt
,XMLGET(Discount.value, 'Abs:RewardQty'):"$"::string AS RewardQty
,XMLGET(Discount.value, 'Abs:DiscountAmt'):"$"::string AS DiscountAmt
,XMLGET(Discount.value, 'Abs:TierLevelnbr'):"$"::string AS TierLevelnbr
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,AttachedOfferType.SEQ::integer as SEQ2
,AttachedOfferType.index::integer as idx2
,ProductGroupVersion.SEQ::integer as SEQ3
,ProductGroupVersion.index::integer as idx3
,DiscountVersion.SEQ::integer as SEQ4
,DiscountVersion.index::integer as idx4
,Discount.SEQ::integer as SEQ5
,Discount.index::integer as idx5
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) AttachedOfferType
,LATERAL FLATTEN(TO_ARRAY(AttachedOfferType.value:"$")) ProductGroupVersion
,LATERAL FLATTEN(TO_ARRAY(ProductGroupVersion.value:"$")) DiscountVersion
,LATERAL FLATTEN(TO_ARRAY(DiscountVersion.value:"$")) Discount
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
AND (AttachedOfferType.value like '<Abs:ProductGroupVersion>%' )
AND (ProductGroupVersion.value like '<Abs:DiscountVersion>%' )
AND (DiscountVersion.value like '<Abs:Discount>%' )
AND (Discount.value like '<Abs:DiscountTier>%' )
) DiscountTier on DiscountTier.SEQ = Discount.SEQ AND DiscountTier.idx = Discount.idx
AND DiscountTier.SEQ1 = Discount.SEQ1 AND DiscountTier.idx1 = Discount.idx1
AND DiscountTier.SEQ2 = Discount.SEQ2 AND DiscountTier.idx2 = Discount.idx2
AND DiscountTier.SEQ3 = Discount.SEQ3 AND DiscountTier.idx3 = Discount.idx3
AND DiscountTier.SEQ4 = Discount.SEQ4 AND DiscountTier.idx4 = Discount.idx4
LEFT JOIN
(
SELECT
XMLGET(DiscountTier.value, 'Abs:LimitQty'):"$"::string AS LimitType_LimitQty
,XMLGET(DiscountTier.value, 'Abs:LimitWt'):"$"::string AS LimitType_LimitWt
,XMLGET(DiscountTier.value, 'Abs:LimitVol'):"$"::string AS LimitType_LimitVol
,XMLGET(DiscountTier.value, 'Abs:LimitAmt'):"$"::string AS LimitType_LimitAmt
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,AttachedOfferType.SEQ::integer as SEQ2
,AttachedOfferType.index::integer as idx2
,ProductGroupVersion.SEQ::integer as SEQ3
,ProductGroupVersion.index::integer as idx3
,DiscountVersion.SEQ::integer as SEQ4
,DiscountVersion.index::integer as idx4
,Discount.SEQ::integer as SEQ5
,Discount.index::integer as idx5
,DiscountTier.SEQ::integer as SEQ6
,DiscountTier.index::integer as idx6
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) AttachedOfferType
,LATERAL FLATTEN(TO_ARRAY(AttachedOfferType.value:"$")) ProductGroupVersion
,LATERAL FLATTEN(TO_ARRAY(ProductGroupVersion.value:"$")) DiscountVersion
,LATERAL FLATTEN(TO_ARRAY(DiscountVersion.value:"$")) Discount
,LATERAL FLATTEN(TO_ARRAY(Discount.value:"$")) DiscountTier
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
AND (AttachedOfferType.value like '<Abs:ProductGroupVersion>%' )
AND (ProductGroupVersion.value like '<Abs:DiscountVersion>%' )
AND (DiscountVersion.value like '<Abs:Discount>%' )
AND (Discount.value like '<Abs:DiscountTier>%' )
AND (DiscountTier.value like '<Abs:LimitType>%' )
) LimitType on LimitType.SEQ = DiscountTier.SEQ AND LimitType.idx = DiscountTier.idx
AND LimitType.SEQ1 = DiscountTier.SEQ1 AND LimitType.idx1 = DiscountTier.idx1
AND LimitType.SEQ2 = DiscountTier.SEQ2 AND LimitType.idx2 = DiscountTier.idx2
AND LimitType.SEQ3 = DiscountTier.SEQ3 AND LimitType.idx3 = DiscountTier.idx3
AND LimitType.SEQ4 = DiscountTier.SEQ4 AND LimitType.idx4 = DiscountTier.idx4
AND LimitType.SEQ5 = DiscountTier.SEQ5 AND LimitType.idx5 = DiscountTier.idx5
LEFT JOIN
(
SELECT
XMLGET(LimitType.value, 'Abs:UOMCd'):"$"::string AS LimitType_UOMCd
,XMLGET(LimitType.value, 'Abs:UOMNm'):"$"::string AS LimitType_UOMNm
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,AttachedOfferType.SEQ::integer as SEQ2
,AttachedOfferType.index::integer as idx2
,ProductGroupVersion.SEQ::integer as SEQ3
,ProductGroupVersion.index::integer as idx3
,DiscountVersion.SEQ::integer as SEQ4
,DiscountVersion.index::integer as idx4
,Discount.SEQ::integer as SEQ5
,Discount.index::integer as idx5
,DiscountTier.SEQ::integer as SEQ6
,DiscountTier.index::integer as idx6
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) AttachedOfferType
,LATERAL FLATTEN(TO_ARRAY(AttachedOfferType.value:"$")) ProductGroupVersion
,LATERAL FLATTEN(TO_ARRAY(ProductGroupVersion.value:"$")) DiscountVersion
,LATERAL FLATTEN(TO_ARRAY(DiscountVersion.value:"$")) Discount
,LATERAL FLATTEN(TO_ARRAY(Discount.value:"$")) DiscountTier
,LATERAL FLATTEN(TO_ARRAY(DiscountTier.value:"$")) LimitType
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
AND (AttachedOfferType.value like '<Abs:ProductGroupVersion>%' )
AND (ProductGroupVersion.value like '<Abs:DiscountVersion>%' )
AND (DiscountVersion.value like '<Abs:Discount>%' )
AND (Discount.value like '<Abs:DiscountTier>%' )
AND (DiscountTier.value like '<Abs:LimitType>%' )
AND (LimitType.value like '<Abs:UOM>%' )
) LimitType_UOM on LimitType_UOM.SEQ = LimitType.SEQ AND LimitType_UOM.idx = LimitType.idx
AND LimitType_UOM.SEQ1 = LimitType.SEQ1 AND LimitType_UOM.idx1 = LimitType.idx1
AND LimitType_UOM.SEQ2 = LimitType.SEQ2 AND LimitType_UOM.idx2 = LimitType.idx2
AND LimitType_UOM.SEQ3 = LimitType.SEQ3 AND LimitType_UOM.idx3 = LimitType.idx3
AND LimitType_UOM.SEQ4 = LimitType.SEQ4 AND LimitType_UOM.idx4 = LimitType.idx4
AND LimitType_UOM.SEQ5 = LimitType.SEQ5 AND LimitType_UOM.idx5 = LimitType.idx5
AND LimitType_UOM.SEQ6 = LimitType.SEQ6 AND LimitType_UOM.idx6 = LimitType.idx6
LEFT JOIN
(
SELECT
XMLGET(Discount.value, 'Abs:ShortDescription'):"$"::string AS DiscountType_ShortDescription
,XMLGET(Discount.value, 'Abs:Description'):"$"::string AS DiscountType_Description
,XMLGET(Discount.value, 'Abs:Code'):"$"::string AS DiscountType_Code
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,AttachedOfferType.SEQ::integer as SEQ2
,AttachedOfferType.index::integer as idx2
,ProductGroupVersion.SEQ::integer as SEQ3
,ProductGroupVersion.index::integer as idx3
,DiscountVersion.SEQ::integer as SEQ4
,DiscountVersion.index::integer as idx4
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) AttachedOfferType
,LATERAL FLATTEN(TO_ARRAY(AttachedOfferType.value:"$")) ProductGroupVersion
,LATERAL FLATTEN(TO_ARRAY(ProductGroupVersion.value:"$")) DiscountVersion
,LATERAL FLATTEN(TO_ARRAY(DiscountVersion.value:"$")) Discount
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
AND (AttachedOfferType.value like '<Abs:ProductGroupVersion>%' )
AND (ProductGroupVersion.value like '<Abs:DiscountVersion>%' )
AND (DiscountVersion.value like '<Abs:Discount>%' )
AND (Discount.value like '<Abs:DiscountType>%' )
) DiscountType on DiscountType.SEQ = Discount.SEQ AND DiscountType.idx = Discount.idx
AND DiscountType.SEQ1 = Discount.SEQ1 AND DiscountType.idx1 = Discount.idx1
AND DiscountType.SEQ2 = Discount.SEQ2 AND DiscountType.idx2 = Discount.idx2
AND DiscountType.SEQ3 = Discount.SEQ3 AND DiscountType.idx3 = Discount.idx3
AND DiscountType.SEQ4 = Discount.SEQ4 AND DiscountType.idx4 = Discount.idx4
LEFT JOIN
(
SELECT
XMLGET(ProductGroupVersion.value, 'Abs:DisplayOrderNbr'):"$"::string AS ProductGroup_DisplayOrderNbr
,XMLGET(ProductGroupVersion.value, 'Abs:ItemQty'):"$"::string AS ItemQty
,XMLGET(ProductGroupVersion.value, 'Abs:GiftCardInd'):"$"::string AS GiftCardInd
,XMLGET(ProductGroupVersion.value, 'Abs:AnyProductInd'):"$"::string AS AnyProductInd
,XMLGET(ProductGroupVersion.value, 'Abs:InheritedInd'):"$"::string AS InheritedInd
,XMLGET(ProductGroupVersion.value, 'Abs:ConjunctionDsc'):"$"::string AS ConjunctionDsc
,XMLGET(ProductGroupVersion.value, 'Abs:MinimumPurchaseAmt'):"$"::string AS MinimumPurchaseAmt
,XMLGET(ProductGroupVersion.value, 'Abs:MaximumPurchaseAmt'):"$"::string AS MaximumPurchaseAmt
,XMLGET(ProductGroupVersion.value, 'Abs:UniqueItemInd'):"$"::string AS UniqueItemInd
,XMLGET(ProductGroupVersion.value, 'Abs:ProductGroupNm'):"$"::string AS ProductGroup_ProductGroupNm
,XMLGET(ProductGroupVersion.value, 'Abs:ProductGroupDsc'):"$"::string AS ProductGroupDsc
,XMLGET(ProductGroupVersion.value, 'Abs:ProductGroupId'):"$"::string AS ProductGroup_ProductGroupId
,XMLGET(ProductGroupVersion.value, 'Abs:ProductGroupVersionId'):"$"::string AS ProductGroup_ProductGroupVersionId
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,AttachedOfferType.SEQ::integer as SEQ2
,AttachedOfferType.index::integer as idx2
,ProductGroupVersion.SEQ::integer as SEQ3
,ProductGroupVersion.index::integer as idx3
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) AttachedOfferType
,LATERAL FLATTEN(TO_ARRAY(AttachedOfferType.value:"$")) ProductGroupVersion
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
AND (AttachedOfferType.value like '<Abs:ProductGroupVersion>%' )
AND (ProductGroupVersion.value like '<Abs:ProductGroup>%' )
) ProductGroup on ProductGroup.SEQ = AttachedOfferType.SEQ AND ProductGroup.idx = AttachedOfferType.idx
AND ProductGroup.SEQ1 = AttachedOfferType.SEQ1 AND ProductGroup.idx1 = AttachedOfferType.idx1
//Added below section to pick BUY record correctly for an offer, for Meal deal all Buys should be attached to each offer
AND (OfferRequestData.OfferRequestTypeCd = 'MEAL_DEAL'
OR  (OfferRequestData.OfferRequestTypeCd <> 'MEAL_DEAL' AND AttachedOffer.AttachedOffer_ProductGroupVersionId=ProductGroup.ProductGroup_ProductGroupVersionId))
LEFT JOIN
(
SELECT
XMLGET(ProductGroup.value, 'Abs:ProductGroupNm'):"$"::string AS ExcludedProductGroup_ProductGroupNm
,XMLGET(ProductGroup.value, 'Abs:ProductGroupId'):"$"::string AS ExcludedProductGroup_ProductGroupId
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,AttachedOfferType.SEQ::integer as SEQ2
,AttachedOfferType.index::integer as idx2
,ProductGroupVersion.SEQ::integer as SEQ3
,ProductGroupVersion.index::integer as idx3
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) AttachedOfferType
,LATERAL FLATTEN(TO_ARRAY(AttachedOfferType.value:"$")) ProductGroupVersion
,LATERAL FLATTEN(TO_ARRAY(ProductGroupVersion.value:"$")) ProductGroup
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
AND (AttachedOfferType.value like '<Abs:ProductGroupVersion>%' )
AND (ProductGroupVersion.value like '<Abs:ProductGroup>%' )
AND (ProductGroup.value like '<Abs:ExcludedProductGroup>%' )
) ExcludedProductGroup on ExcludedProductGroup.SEQ = ProductGroup.SEQ AND ExcludedProductGroup.idx = ProductGroup.idx
AND ExcludedProductGroup.SEQ1 = ProductGroup.SEQ1 AND ExcludedProductGroup.idx1 = ProductGroup.idx1
AND ExcludedProductGroup.SEQ2 = ProductGroup.SEQ2 AND ExcludedProductGroup.idx2 = ProductGroup.idx2
AND ExcludedProductGroup.SEQ3 = ProductGroup.SEQ3 AND ExcludedProductGroup.idx3 = ProductGroup.idx3
LEFT JOIN
(
SELECT
XMLGET(ProductGroup.value, 'Abs:TierLevelAmt'):"$"::string AS TierLevelAmt
,XMLGET(ProductGroup.value, 'Abs:TierLevelId'):"$"::string AS TierLevelId
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,AttachedOfferType.SEQ::integer as SEQ2
,AttachedOfferType.index::integer as idx2
,ProductGroupVersion.SEQ::integer as SEQ3
,ProductGroupVersion.index::integer as idx3
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) AttachedOfferType
,LATERAL FLATTEN(TO_ARRAY(AttachedOfferType.value:"$")) ProductGroupVersion
,LATERAL FLATTEN(TO_ARRAY(ProductGroupVersion.value:"$")) ProductGroup
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
AND (AttachedOfferType.value like '<Abs:ProductGroupVersion>%' )
AND (ProductGroupVersion.value like '<Abs:ProductGroup>%' )
AND (ProductGroup.value like '<Abs:ProductGroupTier>%' )
) ProductGroupTier on ProductGroupTier.SEQ = ProductGroup.SEQ AND ProductGroupTier.idx = ProductGroup.idx
AND ProductGroupTier.SEQ1 = ProductGroup.SEQ1 AND ProductGroupTier.idx1 = ProductGroup.idx1
AND ProductGroupTier.SEQ2 = ProductGroup.SEQ2 AND ProductGroupTier.idx2 = ProductGroup.idx2
AND ProductGroupTier.SEQ3 = ProductGroup.SEQ3 AND ProductGroupTier.idx3 = ProductGroup.idx3
LEFT JOIN
(
SELECT
XMLGET(ProductGroup.value, 'Abs:UOMCd'):"$"::string AS ProductGroup_UOMCd
,XMLGET(ProductGroup.value, 'Abs:UOMNm'):"$"::string AS ProductGroup_UOMNm
,XMLGET(ProductGroup.value, 'Abs:UOMDsc'):"$"::string AS UOMDsc
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,AttachedOfferType.SEQ::integer as SEQ2
,AttachedOfferType.index::integer as idx2
,ProductGroupVersion.SEQ::integer as SEQ3
,ProductGroupVersion.index::integer as idx3
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) AttachedOfferType
,LATERAL FLATTEN(TO_ARRAY(AttachedOfferType.value:"$")) ProductGroupVersion
,LATERAL FLATTEN(TO_ARRAY(ProductGroupVersion.value:"$")) ProductGroup
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
AND (AttachedOfferType.value like '<Abs:ProductGroupVersion>%' )
AND (ProductGroupVersion.value like '<Abs:ProductGroup>%' )
AND (ProductGroup.value like '<Abs:UOM>%' )
) ProductGroup_UOM on ProductGroup_UOM.SEQ = ProductGroup.SEQ AND ProductGroup_UOM.idx = ProductGroup.idx
AND ProductGroup_UOM.SEQ1 = ProductGroup.SEQ1 AND ProductGroup_UOM.idx1 = ProductGroup.idx1
AND ProductGroup_UOM.SEQ2 = ProductGroup.SEQ2 AND ProductGroup_UOM.idx2 = ProductGroup.idx2
AND ProductGroup_UOM.SEQ3 = ProductGroup.SEQ3 AND ProductGroup_UOM.idx3 = ProductGroup.idx3
LEFT JOIN
(
SELECT
XMLGET(AttachedOfferType.value, 'Abs:Description'):"$"::string AS ProtoType_Description
,XMLGET(AttachedOfferType.value, 'Abs:Code'):"$"::string AS ProtoType_Code
,XMLGET(AttachedOfferType.value, 'Abs:ShortDescription'):"$"::string AS ProtoType_ShortDescription
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) AttachedOfferType
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
AND (AttachedOfferType.value like '<Abs:ProtoType>%' )
) ProtoType on ProtoType.SEQ = AttachedOfferType.SEQ AND ProtoType.idx = AttachedOfferType.idx
AND ProtoType.SEQ1 = AttachedOfferType.SEQ1 AND ProtoType.idx1 = AttachedOfferType.idx1
LEFT JOIN
(
SELECT
XMLGET(AttachedOfferType.value, 'Abs:StoreGroupId'):"$"::string AS StoreGroupId
,XMLGET(AttachedOfferType.value, 'Abs:StoreGroupNm'):"$"::string AS StoreGroupNm
,XMLGET(AttachedOfferType.value, 'Abs:StoreGroupDsc'):"$"::string AS StoreGroupDsc
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,AttachedOfferType.SEQ::integer as SEQ2
,AttachedOfferType.index::integer as idx2
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) AttachedOfferType
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
AND (AttachedOfferType.value like '<Abs:StoreGroup>%' )
) StoreGroup on StoreGroup.SEQ = AttachedOfferType.SEQ AND StoreGroup.idx = AttachedOfferType.idx
AND StoreGroup.SEQ1 = AttachedOfferType.SEQ1 AND StoreGroup.idx1 = AttachedOfferType.idx1
LEFT JOIN
(
SELECT
XMLGET(StoreGroup.value, 'Abs:Code'):"$"::string AS StoreGroupType_Code
,XMLGET(StoreGroup.value, 'Abs:Description'):"$"::string AS StoreGroupType_Description
,XMLGET(StoreGroup.value, 'Abs:ShortDescription'):"$"::string AS StoreGroupType_ShortDescription
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,AttachedOfferType.SEQ::integer as SEQ2
,AttachedOfferType.index::integer as idx2
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) AttachedOfferType
,LATERAL FLATTEN(TO_ARRAY(AttachedOfferType.value:"$")) StoreGroup
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
AND (AttachedOfferType.value like '<Abs:StoreGroup>%' )
AND (StoreGroup.value like '<Abs:StoreGroupType>%' )
) StoreGroupType on StoreGroupType.SEQ = StoreGroup.SEQ AND StoreGroupType.idx = StoreGroup.idx
AND StoreGroupType.SEQ1 = StoreGroup.SEQ1 AND StoreGroupType.idx1 = StoreGroup.idx1
AND StoreGroupType.SEQ2 = StoreGroup.SEQ2 AND StoreGroupType.idx2 = StoreGroup.idx2
LEFT JOIN
(
SELECT
XMLGET(AttachedOfferType.value, 'Abs:TagDsc'):"$"::string AS TagDsc
,XMLGET(AttachedOfferType.value, 'Abs:LoyaltyPgmTagInd'):"$"::string AS LoyaltyPgmTagInd
,XMLGET(AttachedOfferType.value, 'Abs:TagAmt'):"$"::string AS TagAmt
,XMLGET(AttachedOfferType.value, 'Abs:TagNbr'):"$"::string AS TagNbr
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) AttachedOfferType
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
AND (AttachedOfferType.value like '<Abs:StoreTagType>%' )
) StoreTagType on StoreTagType.SEQ = AttachedOfferType.SEQ AND StoreTagType.idx = AttachedOfferType.idx
AND StoreTagType.SEQ1 = AttachedOfferType.SEQ1 AND StoreTagType.idx1 = AttachedOfferType.idx1
LEFT JOIN
(
SELECT
XMLGET(ProductGroup.value, 'Abs:CorporateItemCd'):"$"::string AS CorporateItemCd
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,AttachedOfferType.SEQ::integer as SEQ2
,AttachedOfferType.index::integer as idx2
,ProductGroupVersion.SEQ::integer as SEQ3
,ProductGroupVersion.index::integer as idx3
,ProductGroup.SEQ::integer as SEQ4
,ProductGroup.index::integer as idx4
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) AttachedOfferType
,LATERAL FLATTEN(TO_ARRAY(AttachedOfferType.value:"$")) ProductGroupVersion
,LATERAL FLATTEN(TO_ARRAY(ProductGroupVersion.value:"$")) ProductGroup
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
AND (AttachedOfferType.value like '<Abs:ProductGroupVersion>%' )
AND (ProductGroupVersion.value like '<Abs:ProductGroup>%' )
AND (ProductGroup.value like '<Abs:ProductGroupItemType>%' )
) ProductGroupItemType on ProductGroupItemType.SEQ = ProductGroup.SEQ AND ProductGroupItemType.idx = ProductGroup.idx
AND ProductGroupItemType.SEQ1 = ProductGroup.SEQ1 AND ProductGroupItemType.idx1 = ProductGroup.idx1
AND ProductGroupItemType.SEQ2 = ProductGroup.SEQ2 AND ProductGroupItemType.idx2 = ProductGroup.idx2
AND ProductGroupItemType.SEQ3 = ProductGroup.SEQ3 AND ProductGroupItemType.idx3 = ProductGroup.idx3
    LEFT JOIN
(
SELECT
GET(ProductGroupItemType.value, '@Qualifier')::string AS UPC_Qualifier
,XMLGET(ProductGroupItemType.value, 'Abs:UPCNbr'):"$"::string AS UPCNbr
,XMLGET(ProductGroupItemType.value, 'Abs:UPCTxt'):"$"::string AS UPCTxt
,XMLGET(ProductGroupItemType.value, 'Abs:UPCDsc'):"$"::string AS UPCDsc
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,AttachedOfferType.SEQ::integer as SEQ2
,AttachedOfferType.index::integer as idx2
,ProductGroupVersion.SEQ::integer as SEQ3
,ProductGroupVersion.index::integer as idx3
,ProductGroup.SEQ::integer as SEQ4
,ProductGroup.index::integer as idx4
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) AttachedOfferType
,LATERAL FLATTEN(TO_ARRAY(AttachedOfferType.value:"$")) ProductGroupVersion
,LATERAL FLATTEN(TO_ARRAY(ProductGroupVersion.value:"$")) ProductGroup
,LATERAL FLATTEN(TO_ARRAY(ProductGroup.value:"$")) ProductGroupItemType
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
AND (AttachedOfferType.value like '<Abs:ProductGroupVersion>%' )
AND (ProductGroupVersion.value like '<Abs:ProductGroup>%' )
AND (ProductGroup.value like '<Abs:ProductGroupItemType>%' )
AND (ProductGroupItemType.value like '<Abs:UPC>%' OR ProductGroupItemType.value like '<Abs:UPC%Qualifier%' )
) UPC on UPC.SEQ = ProductGroupItemType.SEQ AND UPC.idx = ProductGroupItemType.idx
AND UPC.SEQ1 = ProductGroupItemType.SEQ1 AND UPC.idx1 = ProductGroupItemType.idx1
AND UPC.SEQ2 = ProductGroupItemType.SEQ2 AND UPC.idx2 = ProductGroupItemType.idx2
AND UPC.SEQ3 = ProductGroupItemType.SEQ3 AND UPC.idx3 = ProductGroupItemType.idx3
AND UPC.SEQ4 = ProductGroupItemType.SEQ4 AND UPC.idx4 = ProductGroupItemType.idx4
    LEFT JOIN
(
SELECT
GET(XMLGET(ProductGroupItemType.value, 'Abs:StatusTypeCd'), '@Type')::string AS RepresentativeStatus_StatusTypeCd_Type
,XMLGET(ProductGroupItemType.value, 'Abs:StatusTypeCd'):"$"::string AS RepresentativeStatus_StatusTypeCd
,XMLGET(ProductGroupItemType.value, 'Abs:EffectiveEndDt'):"$"::string AS EffectiveEndDt
,XMLGET(ProductGroupItemType.value, 'Abs:Description'):"$"::string AS RepresentativeStatus_Description
,XMLGET(ProductGroupItemType.value, 'Abs:EffectiveDtTm'):"$"::string AS RepresentativeStatus_EffectiveDtTm
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,AttachedOfferType.SEQ::integer as SEQ2
,AttachedOfferType.index::integer as idx2
,ProductGroupVersion.SEQ::integer as SEQ3
,ProductGroupVersion.index::integer as idx3
,ProductGroup.SEQ::integer as SEQ4
,ProductGroup.index::integer as idx4
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) AttachedOfferType
,LATERAL FLATTEN(TO_ARRAY(AttachedOfferType.value:"$")) ProductGroupVersion
,LATERAL FLATTEN(TO_ARRAY(ProductGroupVersion.value:"$")) ProductGroup
,LATERAL FLATTEN(TO_ARRAY(ProductGroup.value:"$")) ProductGroupItemType
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
AND (AttachedOfferType.value like '<Abs:ProductGroupVersion>%' )
AND (ProductGroupVersion.value like '<Abs:ProductGroup>%' )
AND (ProductGroup.value like '<Abs:ProductGroupItemType>%' )
AND (ProductGroupItemType.value like '<Abs:RepresentativeStatus>%' )
) RepresentativeStatus on RepresentativeStatus.SEQ = ProductGroupItemType.SEQ AND RepresentativeStatus.idx = ProductGroupItemType.idx
AND RepresentativeStatus.SEQ1 = ProductGroupItemType.SEQ1 AND RepresentativeStatus.idx1 = ProductGroupItemType.idx1
AND RepresentativeStatus.SEQ2 = ProductGroupItemType.SEQ2 AND RepresentativeStatus.idx2 = ProductGroupItemType.idx2
AND RepresentativeStatus.SEQ3 = ProductGroupItemType.SEQ3 AND RepresentativeStatus.idx3 = ProductGroupItemType.idx3
AND RepresentativeStatus.SEQ4 = ProductGroupItemType.SEQ4 AND RepresentativeStatus.idx4 = ProductGroupItemType.idx4
    LEFT JOIN
(
SELECT
XMLGET(ProductGroupItemType.value, 'Abs:ShortDescription'):"$"::string AS StatusReason_ShortDescription
,XMLGET(ProductGroupItemType.value, 'Abs:Description'):"$"::string AS StatusReason_Description
,XMLGET(ProductGroupItemType.value, 'Abs:Code'):"$"::string AS StatusReason_Code
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,AttachedOfferType.SEQ::integer as SEQ2
,AttachedOfferType.index::integer as idx2
,ProductGroupVersion.SEQ::integer as SEQ3
,ProductGroupVersion.index::integer as idx3
,ProductGroup.SEQ::integer as SEQ4
,ProductGroup.index::integer as idx4
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) AttachedOfferType
,LATERAL FLATTEN(TO_ARRAY(AttachedOfferType.value:"$")) ProductGroupVersion
,LATERAL FLATTEN(TO_ARRAY(ProductGroupVersion.value:"$")) ProductGroup
,LATERAL FLATTEN(TO_ARRAY(ProductGroup.value:"$")) ProductGroupItemType
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
AND (AttachedOfferType.value like '<Abs:ProductGroupVersion>%' )
AND (ProductGroupVersion.value like '<Abs:ProductGroup>%' )
AND (ProductGroup.value like '<Abs:ProductGroupItemType>%' )
AND (ProductGroupItemType.value like '<Abs:StatusReason>%' )
) StatusReason on StatusReason.SEQ = ProductGroupItemType.SEQ AND StatusReason.idx = ProductGroupItemType.idx
AND StatusReason.SEQ1 = ProductGroupItemType.SEQ1 AND StatusReason.idx1 = ProductGroupItemType.idx1
AND StatusReason.SEQ2 = ProductGroupItemType.SEQ2 AND StatusReason.idx2 = ProductGroupItemType.idx2
AND StatusReason.SEQ3 = ProductGroupItemType.SEQ3 AND StatusReason.idx3 = ProductGroupItemType.idx3
AND StatusReason.SEQ4 = ProductGroupItemType.SEQ4 AND StatusReason.idx4 = ProductGroupItemType.idx4
    LEFT JOIN
(
SELECT
XMLGET(ProductGroupItemType.value, 'Abs:ItemOfferPriceAmt'):"$"::string AS ItemOfferPriceAmt
,XMLGET(ProductGroupItemType.value, 'Abs:EffectiveEndTs'):"$"::string AS EffectiveEndTs
,XMLGET(ProductGroupItemType.value, 'Abs:EffectiveStartTs'):"$"::string AS EffectiveStartTs
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,AttachedOfferType.SEQ::integer as SEQ2
,AttachedOfferType.index::integer as idx2
,ProductGroupVersion.SEQ::integer as SEQ3
,ProductGroupVersion.index::integer as idx3
,ProductGroup.SEQ::integer as SEQ4
,ProductGroup.index::integer as idx4
,ProductGroupItemType.SEQ::integer as SEQ5
,ProductGroupItemType.index::integer as idx5
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) AttachedOfferType
,LATERAL FLATTEN(TO_ARRAY(AttachedOfferType.value:"$")) ProductGroupVersion
,LATERAL FLATTEN(TO_ARRAY(ProductGroupVersion.value:"$")) ProductGroup
,LATERAL FLATTEN(TO_ARRAY(ProductGroup.value:"$")) ProductGroupItemType
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
AND (AttachedOfferType.value like '<Abs:ProductGroupVersion>%' )
AND (ProductGroupVersion.value like '<Abs:ProductGroup>%' )
AND (ProductGroup.value like '<Abs:ProductGroupItemType>%' )
AND (ProductGroupItemType.value like '<Abs:ItemOfferPrice>%' )
) ItemOfferPrice on ItemOfferPrice.SEQ = ProductGroupItemType.SEQ AND ItemOfferPrice.idx = ProductGroupItemType.idx
AND ItemOfferPrice.SEQ1 = ProductGroupItemType.SEQ1 AND ItemOfferPrice.idx1 = ProductGroupItemType.idx1
AND ItemOfferPrice.SEQ2 = ProductGroupItemType.SEQ2 AND ItemOfferPrice.idx2 = ProductGroupItemType.idx2
AND ItemOfferPrice.SEQ3 = ProductGroupItemType.SEQ3 AND ItemOfferPrice.idx3 = ProductGroupItemType.idx3
AND ItemOfferPrice.SEQ4 = ProductGroupItemType.SEQ4 AND ItemOfferPrice.idx4 = ProductGroupItemType.idx4
    LEFT JOIN
(
SELECT
XMLGET(ItemOfferPrice.value, 'Abs:UOMNm'):"$"::string AS ItemOfferPrice_UOMNm
,XMLGET(ItemOfferPrice.value, 'Abs:UOMCd'):"$"::string AS ItemOfferPrice_UOMCd
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,AttachedOfferType.SEQ::integer as SEQ2
,AttachedOfferType.index::integer as idx2
,ProductGroupVersion.SEQ::integer as SEQ3
,ProductGroupVersion.index::integer as idx3
,ProductGroup.SEQ::integer as SEQ4
,ProductGroup.index::integer as idx4
,ProductGroupItemType.SEQ::integer as SEQ5
,ProductGroupItemType.index::integer as idx5
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) AttachedOfferType
,LATERAL FLATTEN(TO_ARRAY(AttachedOfferType.value:"$")) ProductGroupVersion
,LATERAL FLATTEN(TO_ARRAY(ProductGroupVersion.value:"$")) ProductGroup
,LATERAL FLATTEN(TO_ARRAY(ProductGroup.value:"$")) ProductGroupItemType
,LATERAL FLATTEN(TO_ARRAY(ProductGroupItemType.value:"$")) ItemOfferPrice
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
AND (AttachedOfferType.value like '<Abs:ProductGroupVersion>%' )
AND (ProductGroupVersion.value like '<Abs:ProductGroup>%' )
AND (ProductGroup.value like '<Abs:ProductGroupItemType>%' )
AND (ProductGroupItemType.value like '<Abs:ItemOfferPrice>%' )
AND (ItemOfferPrice.value like '<Abs:UOM>%' )
) ItemOfferPrice_UOM on ItemOfferPrice_UOM.SEQ = ItemOfferPrice.SEQ AND ItemOfferPrice_UOM.idx = ItemOfferPrice.idx
AND ItemOfferPrice_UOM.SEQ1 = ItemOfferPrice.SEQ1 AND ItemOfferPrice_UOM.idx1 = ItemOfferPrice.idx1
AND ItemOfferPrice_UOM.SEQ2 = ItemOfferPrice.SEQ2 AND ItemOfferPrice_UOM.idx2 = ItemOfferPrice.idx2
AND ItemOfferPrice_UOM.SEQ3 = ItemOfferPrice.SEQ3 AND ItemOfferPrice_UOM.idx3 = ItemOfferPrice.idx3
AND ItemOfferPrice_UOM.SEQ4 = ItemOfferPrice.SEQ4 AND ItemOfferPrice_UOM.idx4 = ItemOfferPrice.idx4
AND ItemOfferPrice_UOM.SEQ5 = ItemOfferPrice.SEQ5 AND ItemOfferPrice_UOM.idx5 = ItemOfferPrice.idx5
    LEFT JOIN
(
SELECT
XMLGET(PODDetailType.value, 'Abs:ShortDescription'):"$"::string AS OfferDetail_ShortDescription
,XMLGET(PODDetailType.value, 'Abs:Description'):"$"::string AS OfferDetail_Description
,XMLGET(PODDetailType.value, 'Abs:Code'):"$"::string AS OfferDetail_Code
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,AttachedOfferType.SEQ::integer as SEQ2
,AttachedOfferType.index::integer as idx2
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) AttachedOfferType
,LATERAL FLATTEN(TO_ARRAY(AttachedOfferType.value:"$")) PODDetailType
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
AND (AttachedOfferType.value like '<Abs:PODDetailType>%' )
AND (PODDetailType.value like '<Abs:OfferDetail>%' )
) OfferDetail on OfferDetail.SEQ = PODDetailType.SEQ AND OfferDetail.idx = PODDetailType.idx
AND OfferDetail.SEQ1 = PODDetailType.SEQ1 AND OfferDetail.idx1 = PODDetailType.idx1
AND OfferDetail.SEQ2 = PODDetailType.SEQ2 AND OfferDetail.idx2 = PODDetailType.idx2
LEFT JOIN
(
SELECT
XMLGET(PODDetailType.value, 'Abs:UOMCd'):"$"::string AS PODDetailType_UOMCd
,XMLGET(PODDetailType.value, 'Abs:UOMNm'):"$"::string AS PODDetailType_UOMNm
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,AttachedOfferType.SEQ::integer as SEQ2
,AttachedOfferType.index::integer as idx2
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) AttachedOfferType
,LATERAL FLATTEN(TO_ARRAY(AttachedOfferType.value:"$")) PODDetailType
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
AND (AttachedOfferType.value like '<Abs:PODDetailType>%' )
AND (PODDetailType.value like '<Abs:UOM>%' )
) PODDetailType_UOM on PODDetailType_UOM.SEQ = PODDetailType.SEQ AND PODDetailType_UOM.idx = PODDetailType.idx
AND PODDetailType_UOM.SEQ1 = PODDetailType.SEQ1 AND PODDetailType_UOM.idx1 = PODDetailType.idx1
AND PODDetailType_UOM.SEQ2 = PODDetailType.SEQ2 AND PODDetailType_UOM.idx2 = PODDetailType.idx2
LEFT JOIN
(
SELECT
XMLGET(PODDetailType.value, 'Abs:VendorNm'):"$"::string AS PODDetailType_VendorNm
,XMLGET(PODDetailType.value, 'Abs:Land'):"$"::string AS PODDetailType_Land
,XMLGET(PODDetailType.value, 'Abs:Space'):"$"::string AS PODDetailType_Space
,XMLGET(PODDetailType.value, 'Abs:Slot'):"$"::string AS PODDetailType_Slot
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,AttachedOfferType.SEQ::integer as SEQ2
,AttachedOfferType.index::integer as idx2
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) AttachedOfferType
,LATERAL FLATTEN(TO_ARRAY(AttachedOfferType.value:"$")) PODDetailType
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
AND (AttachedOfferType.value like '<Abs:PODDetailType>%' )
AND (PODDetailType.value like '<Abs:GamingOffer>%' )
) GamingOffer on GamingOffer.SEQ = PODDetailType.SEQ AND GamingOffer.idx = PODDetailType.idx
AND GamingOffer.SEQ1 = PODDetailType.SEQ1 AND GamingOffer.idx1 = PODDetailType.idx1
AND GamingOffer.SEQ2 = PODDetailType.SEQ2 AND GamingOffer.idx2 = PODDetailType.idx2
    LEFT JOIN
	
(
SELECT
XMLGET(PODDetailType.value, 'Abs:Description'):"$"::string AS ShoppingListCategory_Description
,XMLGET(PODDetailType.value, 'Abs:ShortDescription'):"$"::string AS ShoppingListCategory_ShortDescription
,XMLGET(PODDetailType.value, 'Abs:Code'):"$"::string AS ShoppingListCategory_Code
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,AttachedOfferType.SEQ::integer as SEQ2
,AttachedOfferType.index::integer as idx2
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) AttachedOfferType
,LATERAL FLATTEN(TO_ARRAY(AttachedOfferType.value:"$")) PODDetailType
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
AND (AttachedOfferType.value like '<Abs:PODDetailType>%' )
AND (PODDetailType.value like '<Abs:ShoppingListCategory>%' )
) ShoppingListCategory on ShoppingListCategory.SEQ = PODDetailType.SEQ AND ShoppingListCategory.idx = PODDetailType.idx
AND ShoppingListCategory.SEQ1 = PODDetailType.SEQ1 AND ShoppingListCategory.idx1 = PODDetailType.idx1
AND ShoppingListCategory.SEQ2 = PODDetailType.SEQ2 AND ShoppingListCategory.idx2 = PODDetailType.idx2
    LEFT JOIN
(
SELECT
XMLGET(PODDetailType.value, 'Abs:Description'):"$"::string AS PODCategory_Description
,XMLGET(PODDetailType.value, 'Abs:ShortDescription'):"$"::string AS PODCategory_ShortDescription
,XMLGET(PODDetailType.value, 'Abs:Code'):"$"::string AS PODCategory_Code
,GetOfferRequest.SEQ::integer as SEQ
,GetOfferRequest.idx::integer as idx
,OfferRequestData.SEQ::integer as SEQ1
,OfferRequestData.index::integer as idx1
,AttachedOfferType.SEQ::integer as SEQ2
,AttachedOfferType.index::integer as idx2
FROM LEVEL_1_FLATTEN AS GetOfferRequest
,LATERAL FLATTEN(TO_ARRAY(GetOfferRequest.value:"$")) OfferRequestData
,LATERAL FLATTEN(TO_ARRAY(OfferRequestData.value:"$")) AttachedOfferType
,LATERAL FLATTEN(TO_ARRAY(AttachedOfferType.value:"$")) PODDetailType
WHERE GetOfferRequest.value like '<OfferRequestData>%'
AND (OfferRequestData.value like '<Abs:AttachedOfferType>%' )
AND (AttachedOfferType.value like '<Abs:PODDetailType>%' )
AND (PODDetailType.value like '<Abs:PODCategory>%' )
) PODCategory on PODCategory.SEQ = PODDetailType.SEQ AND PODCategory.idx = PODDetailType.idx
AND PODCategory.SEQ1 = PODDetailType.SEQ1 AND PODCategory.idx1 = PODDetailType.idx1
AND PODCategory.SEQ2 = PODDetailType.SEQ2 AND PODCategory.idx2 = PODDetailType.idx2;`

try {
        snowflake.execute ({sqlText: insert_split_logic_tbl_2 });
  }
  catch (err) { 
    throw "insert into wrk table "+ split_logic_tbl_2 +" Failed with error: " + err;   // Return a error message.
  }

var insert_into_flat_dml =`INSERT INTO ` + tgt_flat_tbl + `
           SELECT
 DISTINCT
     a.FILENAME
         ,a.BODNm
         ,a.DocumentID
,a.ExternalTargetInd
,a.InterchangeTime
,a.InterchangeDate
,a.InternalFileTransferInd
,a.RoutingSystemNm
,a.ReceiverId
,a.GatewayNm
,a.SenderId
,a.TargetApplicationCd
,a.SourceApplicationCd
,a.Document_Description
,a.CreationDt
,a.DocumentNm
,a.InboundOutboundInd
,a.AlternateDocumentID
,a.Note
,PHIdataInd
,PIIdataInd
,PCIdataInd
,BusinessSensitivityLevel_ShortDescription
,BusinessSensitivityLevel_Description
,BusinessSensitivityLevel_Code
,DataClassificationLevel_ShortDescription
,DataClassificationLevel_Description
,DataClassificationLevel_Code
,ActionTypeCd
,RecordTypeCd
,a.TriggerId
,a.SavingsValueTxt
,a.OfferRequestData_DisclaimerTxt
,a.OfferRequestData_ImageId
,OfferEffectiveDay
,a.OfferRequestReferenceId
,a.BusinessJustificationTxt
,a.OfferRequestTypeCd
,a.OfferItemDsc
,a.OfferRequestCommentTxt
,a.VersionQty
,a.TierQty
,a.ProductQty
,a.StoreGroupQty
,a.CustomerSegmentInfoTxt
,a.DeletedOfferTxt
,a.BrandInfoTxt
,a.OfferRequestId
,a.SizeDsc
,a.DepartmentId
,a.DepartmentNm
,a.OfferNm
,a.OfferRequestDsc
,AdvertisementType_ShortDescription
,AdvertisementType_Description
,AdvertisementType_Code
,AttachedOfferType_StoreGroupVersionId
,AttachedOfferType_DisplayOrderNbr
,AttachedOfferTypeId
,AttachedOffer_DiscountId
,AttachedOffer_DiscountVersionId
,AttachedOffer_ProductGroupVersionId
,AttachedOffer_StoreGroupVersionId
,DistinctId
,OfferId
,ReferenceOfferId
,AppliedInd
,ProgramNm
,Status_StatusTypeCd
,Status_Description
,Status_EffectiveDtTm
,FrequencyDsc
,InstantWinProgramId
,PrizeItemQty
,InstantWinVersionId
,PODDetailType_DisclaimerTxt
,PODDetailType_DisplayEndDt
,PODDetailType_DisplayStartDt
,HeadlineTxt
,PriceInfoTxt
,OfferDsc
,CustomerFriendlyCategory_Description
,CustomerFriendlyCategory_ShortDescription
,CustomerFriendlyCategory_Code
,DisplayImage_ImageId
,ImageTypeCd
,EventId
,EventNm
,DiscountVersion_DiscountVersionId
,AirMileProgramNm
,AirMileProgramId
,AirMilePointQty
,AirMileTierNm
,Discount_DiscountId
,ChargebackDepartmentNm
,ChargebackDepartmentId
,ExcludedProductGroupNm
,ExcludedProductGroupId
,IncludedProductgroupNm
,IncludedProductGroupId
,BenefitValueQty
,BenefitValueType_Code
,BenefitValueType_Description
,BenefitValueType_ShortDescription
,DiscountUptoQty
,ReceiptTxt
,RewardQty
,DiscountAmt
,TierLevelnbr
,LimitType_LimitQty
,LimitType_LimitWt
,LimitType_LimitVol
,LimitType_LimitAmt
,LimitType_UOMCd
,LimitType_UOMNm
,DiscountType_ShortDescription
,DiscountType_Description
,DiscountType_Code
,ProductGroup_DisplayOrderNbr
,ItemQty
,GiftCardInd
,AnyProductInd
,InheritedInd
,ConjunctionDsc
,MinimumPurchaseAmt
,MaximumPurchaseAmt
,UniqueItemInd
,ProductGroup_ProductGroupNm
,ProductGroupDsc
,ProductGroup_ProductGroupId
,ProductGroup_ProductGroupVersionId
,ExcludedProductGroup_ProductGroupNm
,ExcludedProductGroup_ProductGroupId
,TierLevelAmt
,TierLevelId
,ProductGroup_UOMCd
,ProductGroup_UOMNm
,UOMDsc
,ProtoType_Description
,ProtoType_Code
,ProtoType_ShortDescription
,StoreGroupId
,StoreGroupNm
,StoreGroupDsc
,StoreGroupType_Code
,StoreGroupType_Description
,StoreGroupType_ShortDescription
,TagDsc
,LoyaltyPgmTagInd
,TagAmt
,TagNbr
,LinkURL
,FileNm
,IdNbr
,ManufacturerType_Description
,IdTxt
,ManufacturerType_ShortDescription
,DeliveryChannelTypeDsc
,DeliveryChannelTypeCd
,OfferEffectiveTm_TimeZoneCd
,EndTm
,StartTm
,GroupCd
,GroupNm
,GroupId
,SubGroupNm
,SubGroupId
,SubGroupCd
,OfferPeriodType_DisplayStartDt
,OfferPeriodType_DisplayEndDt
,OfferStartDt
,OfferEndDt
,TestStartDt
,TestEndDt
,SourceSystemId
,ApplicationId
,UpdatedApplicationId
,FirstNm
,LastNm
,UserTypeCd
,UpdateTs
,UserUpdate_TimeZoneCd
,CreateTs
,UserId
,OfferRequestStatus_TimeZoneCd
,OfferRequestStatus_EffectiveDtTm
,OfferRequestStatus_Description
,OfferRequestStatus_StatusTypeCd
,OfferRequestStatusTypeCd
,OfferRestrictionType_LimitWt
,UsageLimitTypeTxt
,OfferRestrictionType_LimitQty
,OfferRestrictionType_LimitAmt
,OfferRestrictionType_LimitVol
,RestrictionType_Description
,RestrictionType_Code
,RestrictionType_ShortDescription
,OfferRestrictionType_UOMNm
,OfferRestrictionType_UOMCd
,Name
,PromotionProgramType_Code
,NOPAEndDt
,BilledInd
,NOPAStartDt
,VendorPromotionId
,AllowanceType_ShortDescription
,AllowanceType_Code
,AllowanceType_Description
,BillingOptionType_Description
,BillingOptionType_ShortDescription
,BillingOptionType_Code
,NOPAAssignStatus_StatusTypeCd
,NOPAAssignStatus_EffectiveDtTm
,NOPAAssignStatus_Description
,a.Document_ReleaseId
,a.Document_VersionId
,a.Document_SystemEnvironmentCd
,OfferEffectiveDay_Qualifier
,a.OfferRequestReferenceId_QualifierCd
,Status_StatusTypeCd_Type
,LinkURL_Qualifier
,FileNm_Qualifier
,OfferRequestStatus_StatusTypeCd_Type
,NOPAAssignStatus_StatusTypeCd_Type
         ,CURRENT_TIMESTAMP AS DW_CreateTs
        ,OfferBankNm string    //new fields 052721
,OfferBankId string
,OfferBankTypeCd string
        ,TemplateId string
,TemplateNm string
        ,PromotionProgram_Name
        ,PromotionProgram_Code
        ,PromotionPeriodNm
,PromotionPeriodId
,PromotionWeekId
,PromotionStartDt
,PromotionEndDt
        ,RequiredQty
,RequiredInd
,RequirementTypeCd
        ,CorporateItemCd
        ,UPC_Qualifier
        ,UPCNbr
,UPCTxt
,UPCDsc
        ,RepresentativeStatus_StatusTypeCd_Type
,RepresentativeStatus_StatusTypeCd
,EffectiveEndDt
,RepresentativeStatus_Description
,RepresentativeStatus_EffectiveDtTm
        ,StatusReason_ShortDescription
,StatusReason_Description
,StatusReason_Code
        ,ItemOfferPriceAmt
,EffectiveEndTs
,EffectiveStartTs
        ,ItemOfferPrice_UOMNm
   ,ItemOfferPrice_UOMCd
        ,OfferDetail_ShortDescription
,OfferDetail_Description
,OfferDetail_Code
        ,PODDetailType_UOMCd
,PODDetailType_UOMNm
        ,ShoppingListCategory_Description
,ShoppingListCategory_ShortDescription
,ShoppingListCategory_Code
        ,PODCategory_Description
,PODCategory_ShortDescription
,PODCategory_Code
        ,ChargeBackDepartment_DepartmentNm
,ChargeBackDepartment_DepartmentId
        ,AttachedOffer_InstantWinVersionId
        ,AttachedOffer_OfferRankNbr
        ,PODDetailType_HeadlineSubTxt
,PODDetailType_ItemQty
        ,DisplayOrderNbr
        ,UsageLimitNbr
        ,OfferOrganizationregionid
        ,OfferOrganizationregionnm
        ,Programsubtypecode
        ,Programsubtypename
,ChangeDetailChangeCategoryCd  
,ChangeDetailChangeCategoryQty  
,ChangeDetailChangeCategoryDsc  
,ChangeDetailReasonTypeCd      
,ChangeDetailReasonTypeDsc      
,ChangeDetailCommentTxt        
,ChangeDetailChangeTypeCd      
,ChangeDetailChangeTypeQty      
,ChangeDetailChangeTypeDsc      
,ChangeByTypeUserId            
,ChangeByTypeFirstNm            
,ChangeByTypeLastNm            
,ChangeByTypeChangeByDtTm      
,AllocationTypeCd_Code          
,AllocationTypeCd_Description  
,AllocationTypeCd_ShortDescription
        ,TemplateStatusCd
        ,TemplateReviewStatusFlagCd
		,FulfillmentChannelTypeCd
		,FulfillmentChannelInd
		,FulfillmentChannelDsc
		,ReviewChecklistInd
		,UsageLimitPeriodNbr
		,a.OfferFlagDsc
		,a.UPCQtyTxt
		,RefundableInd
		,EcommProgramTypeName
		,EcommProgramTypeCode
		,EcommValidWithOtherOffersInd
		,EcommValidForFirstTimeCustomerInd
		,EcommAutoApplyPromoInd
		,EcommOfferEligibleOrderCnt
		,EcommPromoCd
		,PODDetailType_VendorNm
		,PODDetailType_Land
		,PODDetailType_Space
		,PODDetailType_Slot
		,PromotionSubProgramCode
		,a.EcommBehaviorCd
		,a.InitialSubscriptionOfferInd
		,a.OfferTemplateStatusInd
		,a.DynamicOfferInd
		,a.DaysToRedeemOfferCnt
     FROM
   ` + split_logic_tbl_1 + ` a  left join ` + split_logic_tbl_2 + ` b on a.filename = b.filename and a.offerrequestid = b.offerrequestid
   and a.CreationDt = b.CreationDt
  /* and a.seq=b.seq and a.seq1=b.seq1 and a.idx=b.idx and a.idx1=b.idx1 */
   ;`
     
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

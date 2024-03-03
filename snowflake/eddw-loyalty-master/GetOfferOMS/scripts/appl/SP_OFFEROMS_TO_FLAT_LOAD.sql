--liquibase formatted sql
--changeset SYSTEM:SP_OFFEROMS_TO_FLAT_LOAD runOnChange:true splitStatements:false OBJECT_TYPE:SP

USE DATABASE <<EDM_DB_NAME_R>>;
USE SCHEMA DW_APPL;

CREATE OR REPLACE PROCEDURE SP_OFFEROMS_TO_FLAT_LOAD()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS 
$$
// Global Variable Declaration
    var wrk_schema = "DW_R_STAGE";
    var ref_db = "EDM_REFINED_PRD";
    var ref_schema = "DW_R_PRODUCT";
	var appl_schema = "DW_APPL";
    var src_tbl = ref_db + "." + appl_schema + ".ESED_OfferOMS_R_STREAM";
    var src_wrk_tbl = ref_db + "." + wrk_schema + ".ESED_OfferOMS_wrk";
	var src_rerun_tbl = ref_db + "." + wrk_schema + ".ESED_OfferOMS_Rerun";
    var tgt_flat_tbl = ref_db + "." + ref_schema + ".OfferOMS_Flat";
	
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
        snowflake.execute (
            {sqlText: sql_crt_src_wrk_tbl  }
            );
        }
    catch (err)  {
        throw "inserting into src wrk table "+ src_wrk_tbl +" Failed with error: " + err;   // Return a error message.
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
    var sql_ins_rerun_tbl = `insert into  `+ src_rerun_tbl+`  SELECT * FROM `+ src_wrk_tbl +``;

    var insert_into_flat_dml =`INSERT INTO `+ tgt_flat_tbl +`	
			with LVL_1_FLATTEN as
			(select 
			tbl.filename as filename
			,tbl.src_json as src_json
			,offer.value as value
			,offer.seq as seq
			from `+ src_wrk_tbl +` tbl
			,LATERAL FLATTEN(tbl.SRC_JSON) offer
			)
			select 
			distinct
			filename
			,PayloadPart
			,PageNum
			,TotalPages
			,EntityId
			,PayLoadType
			,EntityType
			,SourceAction
			,payload_id
			,payload_externalOfferId
			,payload_offerRequestId
			,payload_aggregatorOfferId
			,payload_manufacturerId
			,payload_manufacturerOfferRefCd
			,payload_providerName
			,payload_events_additionalProperties
			,payload_hiddenEvents_additionalProperties
			,payload_programCode
			,payload_programCodeDesc
			,payload_subProgram
			,payload_subProgramDesc
			,payload_deliveryChannel
			,payload_deliveryChannelDesc
			,payload_status
			,payload_statusDesc
			,payload_priceTitle
			,payload_priceValue
			,payload_savingsValueText
			,payload_titleDescription_additionalProperties
			,payload_productDescription_additionalProperties
			,payload_disclaimerText
			,payload_description
			,payload_printTags
			,payload_productImageId
			,payload_priceCode
			,payload_time
			,payload_year
			,payload_productCd
			,payload_isEmployeeOffer
			,payload_isDefaultAllocationOffer
			,payload_programType
			,payload_shouldReportRedemptions
			,payload_createdTs
			,payload_createdApplicationId
			,payload_createdUserId
			,payload_lastUpdatedApplicationId
			,payload_lastUpdatedUserId
			,payload_lastUpdatedTs
			,payload_displayEffectiveStartDate
			,payload_displayEffectiveEndDate
			,payload_effectiveStartDate
			,payload_effectiveEndDate
			,payload_testEffectiveStartDate
			,payload_testEffectiveEndDate
			,payload_postalCodes
			,payload_terminals
			,payload_excludedTerminals
			,payload_qualificationUnitType
			,payload_qualificationUnitSubType
			,payload_benefitValueType
			,payload_usageLimitTypePerUser
			,payload_pluTriggerBarcode
			,payload_copientCategory
			,payload_engine
			,payload_priority
			,payload_tiers
			,payload_sendOutboundData
			,payload_chargebackVendor
			,payload_autoTransferable
			,payload_enableIssuance
			,payload_deferEvaluationUntilEOS
			,payload_enableImpressionReporting
			,payload_limitEligibilityFrequency
			,payload_isApplicableToJ4U
			,payload_customerSegment
			,payload_assignment_userId
			,payload_assignment_firstName
			,payload_assignment_lastName
			,payload_qualificationProductGroups
			,payload_qualificationCustomergroups
			,payload_qualificationPointsGroups
			,payload_testStoreGroups
			,payload_testCustomerGroups
			,payload_benefit_benefitValueType
			,payload_benefit_benefitValueDesc
			,payload_benefit_benefitValue
			,payload_benefit_discount
			,payload_benefit_points
			,payload_benefit_groupMemberShip_customerGroupName
			,payload_benefit_printedMessage_message
			,payload_benefit_printedMessage_isApplicableForNotifications
			,payload_benefit_cashierMessage_cashierMessageTiers
			,payload_benefit_cashierMessage_isApplicableForNotifications
			,payload_qualificationStoreGroups_podStoreGroups
			,payload_qualificationStoreGroups_nonDigitalRedemptionStoreGroups
			,payload_qualificationStoreGroups_digitalRedemptionStoreGroups
			,payload_qualificationProductDisQualifier
			,payload_qualificationDay_monday
			,payload_qualificationDay_tuesday
			,payload_qualificationDay_wednesday
			,payload_qualificationDay_thursday
			,payload_qualificationDay_friday
			,payload_qualificationDay_saturday
			,payload_qualificationDay_sunday
			,payload_qualificationTime_start
			,payload_qualificationTime_end
			,payload_qualificationEnterpriseInstantWin_numberOfPrizes
			,payload_qualificationEnterpriseInstantWin_frequency
			,payload_qualificationTriggerCodes
			,payload_qualificationAttributes
			,payload_nopaNumbers
			,payload_offerName
			,payload_adType
			,payload_offerProtoType
			,payload_offerPrototypeDesc
			,payload_storeGroupVersionId
			,payload_storeTag_printJ4uTagEnabled
			,payload_storeTag_multiple
			,payload_storeTag_amount
			,payload_storeTag_comments
			,payload_requestedRemovalForAll
			,payload_removedOn
			,payload_removedUnclippedOn
			,payload_removedForAllOn
			,payload_brandNSize
			,payload_createdUser_userId
			,payload_createdUser_firstName
			,payload_createdUser_lastName
			,payload_updatedUser_userId
			,payload_updatedUser_firstName
			,payload_updatedUser_lastName
			,payload_firstUpdateToRedemptionEngine
			,payload_lastUpdateToRedemptionEngine
			,payload_firstUpdateToJ4U
			,payload_lastUpdateToJ4U
			,payload_offerRequestorGroup
			,payload_headLine
			,payload_isPODApproved
			,payload_podUsageLimitTypePerUser
			,payload_podReferenceOfferId
			,payload_ivieImageId
			,payload_vehicleNm
			,payload_adPageNbr
			,payload_adModNbr
			,payload_ecomDesc
			,payload_requestedUser_userId
			,payload_requestedUser_firstName
			,payload_requestedUser_lastName
			,payload_isPrimaryPODOffer
			,lastUpdateTs
			,current_timestamp() as DW_CREATE_TS
			,PAYLOAD_PRODUCT_DESCPRODDSC1
			,PAYLOAD_PRODUCT_DESCPRODDSC2
			,PAYLOAD_TITLE_DESCTITLEDSC1
			,PAYLOAD_TITLE_DESCTITLEDSC2
			,PAYLOAD_TITLE_DESCTITLEDSC3
			,payload_redemptionSystemId
			,payload_events
			,payload_regionIdMap
			,payload_inEmail
			,payload_submittedDate
			,payload_hiddenevents
			,payload_qualificationStoreGroups_redemptionStoreGroups
			,payload_adBugText
			,PAYLOAD_PRODUCT_DESCPRODDSC3
			,payload_benefit_groupMemberShip_customerGroupId
            ,payload_categories_additionalProperties
			,payload_primaryCategory_additionalProperties
			,payload_allocationCode
			,payload_allocationCodeName
			,payload_usageLimitPerUser
			,payload_customPeriod
			,payload_customType
			,QUALIFICATIONPRODUCTDISQUALIFIERNAME
			,ISDISPLAYIMMEDIATE
			,payload_offerFlag
			,payload_qualificationTenderTypes
			,payload_fulfillmentChannel
			,payload_ecommPromoType
			,payload_order
            ,payload_headLine2
			,payload_usageLimitPerOffer
			,payload_refundableRewards
			,payload_multiClipLimit
			,payload_points
			,payload_houseHoldTargeting
			,payload_programSubType
			,payload_ecommPromoCode 
            ,payload_validWithOtherOffer
			,payload_orderCount
			,payload_firstTimeCustomerOnly
			,payload_autoApplyPromoCode
			,payload_notCombinableWith
			,payload_land 
            ,payload_space
            ,payload_slot 
			,payload_subProgramCode
            ,payload_qualificationBehavior
			,payload_initialSubscriptionOffer
			,payload_isDynamicOffer
			,payload_DaysToRedeem
			,payload_ad_clippable
			,payload_ad_applicableOnline
			,payload_ad_displayable
			
			
			
			from
			(select filename
			,offer.src_json:payloadPart::string as PayloadPart
			,offer.src_json:pageNum::string as PageNum
			,offer.src_json:totalPages::string as totalPages
			,offer.src_json:entityId::string as entityId
			,offer.src_json:payLoadType::string as payLoadType
			,offer.src_json:entityType::string as entityType
			,offer.src_json:sourceAction::string as sourceAction
			,offer.src_json:payload:id::string as payload_id
			,offer.src_json:payload:externalOfferId::string  as payload_externalOfferId
			,offer.src_json:payload:offerRequestId::string  as payload_offerRequestId
			,offer.src_json:payload:aggregatorOfferId::string as payload_aggregatorOfferId
			,offer.src_json:payload:manufacturerId::string as payload_manufacturerId
			,offer.src_json:payload:manufacturerOfferRefCd::string as payload_manufacturerOfferRefCd
			,offer.src_json:payload:providerName::string as payload_providerName
			,offer.src_json:payload:events:additionalProperties::string as payload_events_additionalProperties
			,offer.src_json:payload:hiddenEvents:additionalProperties::string as payload_hiddenEvents_additionalProperties
			,offer.src_json:payload:programCode::string as payload_programCode 
			,offer.src_json:payload:programCodeDesc::string as payload_programCodeDesc
			,offer.src_json:payload:subProgram::string as payload_subProgram
			,offer.src_json:payload:subProgramDesc::string as payload_subProgramDesc
			,offer.src_json:payload:deliveryChannel::string as payload_deliveryChannel
			,offer.src_json:payload:deliveryChannelDesc::string as payload_deliveryChannelDesc
			,offer.src_json:payload:status::string as payload_status
			,offer.src_json:payload:statusDesc::string as payload_statusDesc
			,offer.src_json:payload:priceTitle::string as payload_priceTitle
			,offer.src_json:payload:priceValue::string as payload_priceValue
			,offer.src_json:payload:savingsValueText::string as payload_savingsValueText
			,offer.src_json:payload:titleDescription:additionalProperties::string as payload_titleDescription_additionalProperties
			,offer.src_json:payload:productDescription:additionalProperties::string as payload_productDescription_additionalProperties
			,offer.src_json:payload:disclaimerText::string as payload_disclaimerText
			,offer.src_json:payload:description::string as payload_description
			,offer.src_json:payload:printTags::string as payload_printTags
			,offer.src_json:payload:productImageId::string as payload_productImageId
			,offer.src_json:payload:priceCode::string as payload_priceCode
			,offer.src_json:payload:time::string as payload_time
			,offer.src_json:payload:year::string as payload_year
			,offer.src_json:payload:productCd::string as payload_productCd
			,offer.src_json:payload:isEmployeeOffer::string as payload_isEmployeeOffer
			,offer.src_json:payload:isDefaultAllocationOffer::string as payload_isDefaultAllocationOffer
			,offer.src_json:payload:programType::string as payload_programType
			,offer.src_json:payload:shouldReportRedemptions::string as payload_shouldReportRedemptions
			,offer.src_json:payload:createdTs::string as payload_createdTs
			,offer.src_json:payload:createdApplicationId::string as payload_createdApplicationId
			,offer.src_json:payload:createdUserId::string as payload_createdUserId
			,offer.src_json:payload:lastUpdatedApplicationId::string as payload_lastUpdatedApplicationId
			,offer.src_json:payload:lastUpdatedUserId::string as payload_lastUpdatedUserId
			,offer.src_json:payload:lastUpdatedTs::string as payload_lastUpdatedTs
			,offer.src_json:payload:displayEffectiveStartDate::string as payload_displayEffectiveStartDate
			,offer.src_json:payload:displayEffectiveEndDate::string as payload_displayEffectiveEndDate
			,offer.src_json:payload:effectiveStartDate::string as payload_effectiveStartDate
			,offer.src_json:payload:effectiveEndDate::string as payload_effectiveEndDate
			,offer.src_json:payload:testEffectiveStartDate::string as payload_testEffectiveStartDate
			,offer.src_json:payload:testEffectiveEndDate::string as payload_testEffectiveEndDate
			,offer.src_json:payload:postalCodes::array as payload_postalCodes
			,offer.src_json:payload:terminals::array as payload_terminals
			,offer.src_json:payload:excludedTerminals::array as payload_excludedTerminals
			,offer.src_json:payload:qualificationUnitType::string as payload_qualificationUnitType
			,offer.src_json:payload:qualificationUnitSubType::string as payload_qualificationUnitSubType
			,offer.src_json:payload:benefitValueType::string as payload_benefitValueType
			,offer.src_json:payload:usageLimitTypePerUser::string as payload_usageLimitTypePerUser
			,offer.src_json:payload:usageLimitPerUser::string as payload_usageLimitPerUser
			,offer.src_json:payload:pluTriggerBarcode::string as payload_pluTriggerBarcode
			,offer.src_json:payload:copientCategory::string as payload_copientCategory
			,offer.src_json:payload:engine::string as payload_engine
			,offer.src_json:payload:priority::string as payload_priority
			,offer.src_json:payload:tiers::string as payload_tiers
			,offer.src_json:payload:sendOutboundData::string as payload_sendOutboundData
			,offer.src_json:payload:chargebackVendor::string as payload_chargebackVendor
			,offer.src_json:payload:autoTransferable::string as payload_autoTransferable
			,offer.src_json:payload:enableIssuance::string as payload_enableIssuance
			,offer.src_json:payload:deferEvaluationUntilEOS::string as payload_deferEvaluationUntilEOS
			,offer.src_json:payload:enableImpressionReporting::string as payload_enableImpressionReporting
			,offer.src_json:payload:limitEligibilityFrequency::string as payload_limitEligibilityFrequency
			,offer.src_json:payload:isApplicableToJ4U::string as payload_isApplicableToJ4U
			,offer.src_json:payload:customerSegment::string  as payload_customerSegment
			,offer.src_json:payload:assignment:userId::string as payload_assignment_userId
			,offer.src_json:payload:assignment:firstName::string as payload_assignment_firstName
			,offer.src_json:payload:assignment:lastName::string as payload_assignment_lastName
			,offer.src_json:payload:qualificationProductGroups::array as payload_qualificationProductGroups
			,offer.src_json:payload:qualificationCustomerGroups::array as payload_qualificationCustomergroups
			,offer.src_json:payload:qualificationPointsGroups::array as payload_qualificationPointsGroups
			,offer.src_json:payload:testStoreGroupIds::array as payload_testStoreGroups
			,offer.src_json:payload:testCustomerGroupIds::array as payload_testCustomerGroups
			,offer.src_json:payload:benefit:benefitValueType::string as payload_benefit_benefitValueType
			,offer.src_json:payload:benefit:benefitValueDesc::string as payload_benefit_benefitValueDesc
			,offer.src_json:payload:benefit:benefitValue::string as payload_benefit_benefitValue
			,offer.src_json:payload:benefit:discount::array as payload_benefit_discount
			,offer.src_json:payload:benefit:points::array as payload_benefit_points
			,offer.src_json:payload:benefit:groupMemberShip:customerGroupName::string as payload_benefit_groupMemberShip_customerGroupName
			,offer.src_json:payload:benefit:printedMessage:message::array as payload_benefit_printedMessage_message
			,offer.src_json:payload:benefit:printedMessage:isApplicableForNotifications::string as payload_benefit_printedMessage_isApplicableForNotifications
			,offer.src_json:payload:benefit:cashierMessage:cashierMessageTiers::array as payload_benefit_cashierMessage_cashierMessageTiers
			,offer.src_json:payload:benefit:cashierMessage:isApplicableForNotifications::string as payload_benefit_cashierMessage_isApplicableForNotifications
			,offer.src_json:payload:qualificationStoreGroups:podStoreGroups::array as payload_qualificationStoreGroups_podStoreGroups
			,offer.src_json:payload:qualificationStoreGroups:nonDigitalRedemptionStoreGroups::array as payload_qualificationStoreGroups_nonDigitalRedemptionStoreGroups
			,offer.src_json:payload:qualificationStoreGroups:digitalRedemptionStoreGroups::array as payload_qualificationStoreGroups_digitalRedemptionStoreGroups
			,offer.src_json:payload:qualificationProductDisQualifier::string as payload_qualificationProductDisQualifier
			,offer.src_json:payload:qualificationDay:monday::string as payload_qualificationDay_monday
			,offer.src_json:payload:qualificationDay:tuesday::string as payload_qualificationDay_tuesday
			,offer.src_json:payload:qualificationDay:wednesday::string as payload_qualificationDay_wednesday
			,offer.src_json:payload:qualificationDay:thursday::string as payload_qualificationDay_thursday
			,offer.src_json:payload:qualificationDay:friday::string as payload_qualificationDay_friday
			,offer.src_json:payload:qualificationDay:saturday::string as payload_qualificationDay_saturday
			,offer.src_json:payload:qualificationDay:sunday::string as payload_qualificationDay_sunday
			,offer.src_json:payload:qualificationTime:start::string as payload_qualificationTime_start
			,offer.src_json:payload:qualificationTime:end::string as payload_qualificationTime_end
			,offer.src_json:payload:qualificationEnterpriseInstantWin:numberOfPrizes::string as payload_qualificationEnterpriseInstantWin_numberOfPrizes
			,offer.src_json:payload:qualificationEnterpriseInstantWin:frequency::string as payload_qualificationEnterpriseInstantWin_frequency
			,offer.src_json:payload:qualificationTriggerCodes::array as payload_qualificationTriggerCodes
			,offer.src_json:payload:qualificationAttributes::array as payload_qualificationAttributes
			,offer.src_json:payload:nopaNumbers::array as payload_nopaNumbers
			,offer.src_json:payload:offerName::string as payload_offerName
			,offer.src_json:payload:adType::string as payload_adType
			,offer.src_json:payload:offerProtoType::string as payload_offerProtoType
			,offer.src_json:payload:offerPrototypeDesc::string as payload_offerPrototypeDesc
			,offer.src_json:payload:storeGroupVersionId::string as payload_storeGroupVersionId
			,offer.src_json:payload:storeTag:printJ4uTagEnabled::string as payload_storeTag_printJ4uTagEnabled
			,offer.src_json:payload:storeTag:multiple::string as payload_storeTag_multiple
			,offer.src_json:payload:storeTag:amount::string as payload_storeTag_amount
			,offer.src_json:payload:storeTag:comments::string as payload_storeTag_comments
			,offer.src_json:payload:requestedRemovalForAll::string as payload_requestedRemovalForAll
			,offer.src_json:payload:removedOn::string as payload_removedOn
			,offer.src_json:payload:removedUnclippedOn::string as payload_removedUnclippedOn
			,offer.src_json:payload:removedForAllOn::string as payload_removedForAllOn
			,offer.src_json:payload:brandNSize::string as payload_brandNSize
			,offer.src_json:payload:createdUser:userId::string as payload_createdUser_userId
			,offer.src_json:payload:createdUser:firstName::string as payload_createdUser_firstName
			,offer.src_json:payload:createdUser:lastName::string as payload_createdUser_lastName
			,offer.src_json:payload:updatedUser:userId::string as payload_updatedUser_userId
			,offer.src_json:payload:updatedUser:firstName::string as payload_updatedUser_firstName
			,offer.src_json:payload:updatedUser:lastName::string as payload_updatedUser_lastName
			,offer.src_json:payload:firstUpdateToRedemptionEngine::string as payload_firstUpdateToRedemptionEngine
			,offer.src_json:payload:lastUpdateToRedemptionEngine::string as payload_lastUpdateToRedemptionEngine
			,offer.src_json:payload:firstUpdateToJ4U::string as payload_firstUpdateToJ4U
			,offer.src_json:payload:lastUpdateToJ4U::string as payload_lastUpdateToJ4U
			,offer.src_json:payload:offerRequestorGroup::string as payload_offerRequestorGroup
			,offer.src_json:payload:headLine::string as payload_headLine
			,offer.src_json:payload:isPODApproved::string as payload_isPODApproved
			,offer.src_json:payload:podUsageLimitTypePerUser::string as payload_podUsageLimitTypePerUser
			,offer.src_json:payload:podReferenceOfferId::string as payload_podReferenceOfferId
			,offer.src_json:payload:ivieImageId::string as payload_ivieImageId
			,offer.src_json:payload:vehicleNm::string as payload_vehicleNm
			,offer.src_json:payload:adPageNbr::string as payload_adPageNbr
			,offer.src_json:payload:adModNbr::string as payload_adModNbr
			,offer.src_json:payload:ecomDesc::string as payload_ecomDesc
			,offer.src_json:payload:requestedUser:userId::string as payload_requestedUser_userId
			,offer.src_json:payload:requestedUser:firstName::string as payload_requestedUser_firstName
			,offer.src_json:payload:requestedUser:lastName::string as payload_requestedUser_lastName
			,offer.src_json:payload:isPrimaryPODOffer::string as payload_isPrimaryPODOffer
			,offer.src_json:lastUpdateTs::string as lastUpdateTs
			,offer.src_json:payload:productDescription:product_descprodDsc1::string as PAYLOAD_PRODUCT_DESCPRODDSC1
			,offer.src_json:payload:productDescription:product_descprodDsc2::string as PAYLOAD_PRODUCT_DESCPRODDSC2
			,offer.src_json:payload:titleDescription:title_desctitleDsc1::string as PAYLOAD_TITLE_DESCTITLEDSC1
			,offer.src_json:payload:titleDescription:title_desctitleDsc2::string as PAYLOAD_TITLE_DESCTITLEDSC2
			,offer.src_json:payload:titleDescription:title_desctitleDsc3::string as PAYLOAD_TITLE_DESCTITLEDSC3
	        ,offer.src_json:payload:redemptionSystemId as payload_redemptionSystemId
			,offer.src_json:payload:events::variant as payload_events
			,offer.src_json:payload:regionIdMap::variant as payload_regionIdMap
			,offer.src_json:payload:inEmail::string as payload_inEmail
			,offer.src_json:payload:submittedDate::string as payload_submittedDate
			,offer.src_json:payload:hiddenEvents::variant as payload_hiddenevents
			,offer.src_json:payload:qualificationStoreGroups:redemptionStoreGroups::variant as payload_qualificationStoreGroups_redemptionStoreGroups
			,offer.src_json:payload:adBugText::string as payload_adBugText
			,offer.src_json:payload:productDescription:product_descprodDsc3::string as PAYLOAD_PRODUCT_DESCPRODDSC3
			,offer.src_json:payload:benefit:groupMemberShip:customerGroupId::string as payload_benefit_groupMemberShip_customerGroupId
            ,offer.src_json:payload:categories::variant as payload_categories_additionalProperties
			,offer.src_json:payload:primaryCategory::variant as payload_primaryCategory_additionalProperties
			,offer.src_json:payload:allocationCode::string as payload_allocationCode
			,offer.src_json:payload:allocationCodeName::string as payload_allocationCodeName
			,offer.src_json:payload:customPeriod::string as payload_customPeriod
			,offer.src_json:payload:customType::string as payload_customType
			,offer.src_json:payload:isDisplayImmediate::string as ISDISPLAYIMMEDIATE
			,offer.src_json:payload:qualificationProductDisQualifierName::string as QUALIFICATIONPRODUCTDISQUALIFIERNAME
			,offer.src_json:payload:offerFlag::variant as payload_offerFlag
			,offer.src_json:payload:qualificationTenderTypes::array as payload_qualificationTenderTypes
			,offer.src_json:payload:fulfillmentChannel::variant as payload_fulfillmentChannel
			,offer.src_json:payload:ecommPromoType::string as payload_ecommPromoType
			,offer.src_json:payload:order::string as payload_order
            ,offer.src_json:payload:headLine2::string as payload_headLine2
			,offer.src_json:payload:usageLimitPerOffer::string as payload_usageLimitPerOffer
			,offer.src_json:payload:refundableRewards::string as payload_refundableRewards              
			,offer.src_json:payload:multiClipLimit::string as payload_multiClipLimit 
			,offer.src_json:payload:points::string as payload_points     -- This column added for RX- HEALTH OUTBOUND.       
			,offer.src_json:payload:houseHoldTargeting::variant as payload_houseHoldTargeting -- This column added  to load for HOUSEHOLD_TARGETING_GROUP and HOUSEHOLD_TARGETING_GROUP_ITEM table . 
			,offer.src_json:payload:programSubType::string as payload_programSubType     -- This column added for RX- HEALTH OUTBOUND. 
			,offer.src_json:payload:ecommPromoCode::string as payload_ecommPromoCode 
            ,offer.src_json:payload:validWithOtherOffer::string as payload_validWithOtherOffer
			,offer.src_json:payload:orderCount::string as payload_orderCount
			,offer.src_json:payload:firstTimeCustomerOnly::string as payload_firstTimeCustomerOnly
			,offer.src_json:payload:autoApplyPromoCode::string as payload_autoApplyPromoCode
			,offer.src_json:payload:notCombinableWith::variant as payload_notCombinableWith
			,offer.src_json:payload:land::string as payload_land
			,offer.src_json:payload:space::string as payload_space
			,offer.src_json:payload:slot::string as payload_slot
			,offer.src_json:payload:subProgramCode::string as payload_subProgramCode
			,offer.src_json:payload:qualificationBehavior::string as payload_qualificationBehavior
			,offer.src_json:payload:initialSubscriptionOffer::string as payload_initialSubscriptionOffer
			,offer.src_json:payload:isDynamicOffer::string as payload_isDynamicOffer
			,offer.src_json:payload:daysToRedeem::string as payload_DaysToRedeem
			
			,offer.src_json:payload:advertisement:clippable::BOOLEAN as payload_ad_clippable
			,offer.src_json:payload:advertisement:applicableOnline::BOOLEAN as payload_ad_applicableOnline
			,offer.src_json:payload:advertisement:displayable::BOOLEAN as payload_ad_displayable
			
			from lvl_1_flatten offer) as offer;`
			
			
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

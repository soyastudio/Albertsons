--liquibase formatted sql
--changeset SYSTEM:OFFEROMS_FLAT runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view OFFEROMS_FLAT(
	FILENAME COMMENT 'Source filename',
	PAYLOADPART COMMENT 'payload part from oms source file',
	PAGENUM COMMENT 'page number from oms source file',
	TOTALPAGES COMMENT 'total pages in source file',
	ENTITYID COMMENT 'Internal Offer Id',
	PAYLOADTYPE COMMENT 'payload type in oms offer',
	ENTITYTYPE COMMENT 'entity type in oms offer',
	SOURCEACTION COMMENT 'source action like update or delete in oms offer payload',
	PAYLOAD_ID COMMENT 'entity id/payload id in oms offer',
	PAYLOAD_EXTERNALOFFERID COMMENT 'External Ofer Id. Ex: Offers from TOGM, OMS and EMOM.',
	PAYLOAD_OFFERREQUESTID COMMENT 'Identifier of the offer request',
	PAYLOAD_AGGREGATOROFFERID COMMENT 'Offer Aggregate Identifier',
	PAYLOAD_MANUFACTURERID COMMENT 'Offer Manufacturer Identifier',
	PAYLOAD_MANUFACTUREROFFERREFCD COMMENT 'Offer reference code of the manufacturer',
	PAYLOAD_PROVIDERNAME COMMENT 'Name of the Offer Provider',
	PAYLOAD_CATEGORIES_ADDITIONALPROPERTIES COMMENT 'Category of the offer. Defines the higher categorization of an offer',
	PAYLOAD_PRIMARYCATEGORY_ADDITIONALPROPERTIES COMMENT 'Primary Category of the offer',
	PAYLOAD_EVENTS_ADDITIONALPROPERTIES COMMENT 'additional properties of the offer',
	PAYLOAD_HIDDENEVENTS_ADDITIONALPROPERTIES COMMENT 'Hidden events additional properties of the offer',
	PAYLOAD_PROGRAMCODE COMMENT 'Offer Program Code',
	PAYLOAD_PROGRAMCODEDESC COMMENT 'Offer Program Code Description',
	PAYLOAD_SUBPROGRAM COMMENT 'Subprogram Name of the offer',
	PAYLOAD_SUBPROGRAMDESC COMMENT 'Subprogram description of the offer',
	PAYLOAD_DELIVERYCHANNEL COMMENT 'Offer Delivery Channel Code',
	PAYLOAD_DELIVERYCHANNELDESC COMMENT 'Offer Delivery Channel Description',
	PAYLOAD_STATUS COMMENT 'Status code of the offer',
	PAYLOAD_STATUSDESC COMMENT 'Status of the offer. Defines the different stages of an offer and its effective date.',
	PAYLOAD_PRICETITLE COMMENT 'Offer Price Title text',
	PAYLOAD_PRICEVALUE COMMENT 'Offer Price Value Text',
	PAYLOAD_SAVINGSVALUETEXT COMMENT 'Savings value of the offer when used',
	PAYLOAD_TITLEDESCRIPTION_ADDITIONALPROPERTIES COMMENT 'Description of the Offer Title',
	PAYLOAD_PRODUCTDESCRIPTION_ADDITIONALPROPERTIES COMMENT 'Description of the product on the offer',
	PAYLOAD_DISCLAIMERTEXT COMMENT 'Offer Disclaimer Text',
	PAYLOAD_DESCRIPTION COMMENT 'long description of the offer',
	PAYLOAD_PRINTTAGS COMMENT 'Indicator if the offer tags can be printable',
	PAYLOAD_PRODUCTIMAGEID COMMENT 'Identifier of the Product Image on the Offer',
	PAYLOAD_PRICECODE COMMENT 'Offer Price Code',
	PAYLOAD_TIME COMMENT 'Time Text',
	PAYLOAD_YEAR COMMENT 'Year Text',
	PAYLOAD_PRODUCTCD COMMENT 'Product code of the product on the offer',
	PAYLOAD_ISEMPLOYEEOFFER COMMENT 'Indicator if the offer is for employees',
	PAYLOAD_ISDEFAULTALLOCATIONOFFER COMMENT 'Indicator if the offer has been allocated by default',
	PAYLOAD_PROGRAMTYPE COMMENT 'Program Type of the Offer',
	PAYLOAD_SHOULDREPORTREDEMPTIONS COMMENT 'Indicator if the offer redemptions has to be reported for the offer',
	PAYLOAD_CREATEDTS COMMENT 'Offer Created Timestamp',
	PAYLOAD_CREATEDAPPLICATIONID COMMENT 'Created Application Identifier',
	PAYLOAD_CREATEDUSERID COMMENT 'Identifier of the person who created the offer',
	PAYLOAD_LASTUPDATEDAPPLICATIONID COMMENT 'Identifier of the application which last updated the offer',
	PAYLOAD_LASTUPDATEDUSERID COMMENT 'Offer last updated by User Identifier',
	PAYLOAD_LASTUPDATEDTS COMMENT 'Offer last updated timestamp',
	PAYLOAD_DISPLAYEFFECTIVESTARTDATE COMMENT 'Date from when the offer will be displayed',
	PAYLOAD_DISPLAYEFFECTIVEENDDATE COMMENT 'Date till when the offer will be displayed',
	PAYLOAD_EFFECTIVESTARTDATE COMMENT 'Offer Effective Start Date',
	PAYLOAD_EFFECTIVEENDDATE COMMENT 'Offer Effective end date',
	PAYLOAD_TESTEFFECTIVESTARTDATE COMMENT 'Date from when the offer test is effective',
	PAYLOAD_TESTEFFECTIVEENDDATE COMMENT 'Date till when the offers test is effective',
	PAYLOAD_POSTALCODES COMMENT 'postal codes of the offer',
	PAYLOAD_TERMINALS COMMENT 'Terminals of the offer',
	PAYLOAD_EXCLUDEDTERMINALS COMMENT 'Excluded Terminals of the offer',
	PAYLOAD_QUALIFICATIONUNITTYPE COMMENT 'Offer Qualification Unit Type description',
	PAYLOAD_QUALIFICATIONUNITSUBTYPE COMMENT 'Offer Qualification Unit subtype description',
	PAYLOAD_BENEFITVALUETYPE COMMENT 'Type of the benefit value of offer',
	PAYLOAD_USAGELIMITTYPEPERUSER COMMENT 'Usage Limit for the Offer',
	PAYLOAD_PLUTRIGGERBARCODE COMMENT 'Barcode Value of the PLU Trigger',
	PAYLOAD_COPIENTCATEGORY COMMENT 'Copient Category Description',
	PAYLOAD_ENGINE COMMENT 'Name of the engine that the offer was originated from',
	PAYLOAD_PRIORITY COMMENT 'Priority of the offer. Used for receipt message ordering.',
	PAYLOAD_TIERS COMMENT 'Offer Tier Code',
	PAYLOAD_SENDOUTBOUNDDATA COMMENT 'Outbound data description of the offer',
	PAYLOAD_CHARGEBACKVENDOR COMMENT 'Chargeback Vendor Name',
	PAYLOAD_AUTOTRANSFERABLE COMMENT 'Indicator if the offer is auto transferrable',
	PAYLOAD_ENABLEISSUANCE COMMENT 'Indicator if issuance is enabled on the offer',
	PAYLOAD_DEFEREVALUATIONUNTILEOS COMMENT 'Indicator if the Offer is Deferred for Evaluation',
	PAYLOAD_ENABLEIMPRESSIONREPORTING COMMENT 'Indicator if the impression reporting is enabled on the offer',
	PAYLOAD_LIMITELIGIBILITYFREQUENCY COMMENT 'Eligibility frequency limit of the offer',
	PAYLOAD_ISAPPLICABLETOJ4U COMMENT 'Indicator if the offer is applicable to J4U',
	PAYLOAD_CUSTOMERSEGMENT COMMENT 'Offer Customer Segment Description',
	PAYLOAD_ASSIGNMENT_USERID COMMENT 'User Identifier of the Person who assigned the offer',
	PAYLOAD_ASSIGNMENT_FIRSTNAME COMMENT 'First Name of the person who assigned the offer',
	PAYLOAD_ASSIGNMENT_LASTNAME COMMENT 'Last Name of the person who assigned the offer',
	PAYLOAD_QUALIFICATIONPRODUCTGROUPS COMMENT 'Qualifiation product discqualifier text',
	PAYLOAD_QUALIFICATIONCUSTOMERGROUPS COMMENT 'Qualifiation customer discqualifier text',
	PAYLOAD_QUALIFICATIONPOINTSGROUPS COMMENT 'Qualifiation points disqualifier text',
	PAYLOAD_TESTSTOREGROUPS COMMENT 'oms offer test store groups',
	PAYLOAD_TESTCUSTOMERGROUPS COMMENT 'oms offer test customer groups',
	PAYLOAD_BENEFIT_BENEFITVALUETYPE COMMENT 'oms offer benefit value type',
	PAYLOAD_BENEFIT_BENEFITVALUEDESC COMMENT 'osm offer benefit value description',
	PAYLOAD_BENEFIT_BENEFITVALUE COMMENT 'oms offer benefit value',
	PAYLOAD_BENEFIT_DISCOUNT COMMENT 'oms offer(Discount)',
	PAYLOAD_BENEFIT_POINTS COMMENT 'oms offer(points)',
	PAYLOAD_BENEFIT_GROUPMEMBERSHIP_CUSTOMERGROUPNAME COMMENT 'Customer group name in oms offer',
	PAYLOAD_BENEFIT_PRINTEDMESSAGE_MESSAGE COMMENT 'printed message in oms offer',
	PAYLOAD_BENEFIT_PRINTEDMESSAGE_ISAPPLICABLEFORNOTIFICATIONS COMMENT 'Indicator if the printed message notifications are enabled for the offer',
	PAYLOAD_BENEFIT_CASHIERMESSAGE_CASHIERMESSAGETIERS COMMENT 'cashier tier messages in oms offer',
	PAYLOAD_BENEFIT_CASHIERMESSAGE_ISAPPLICABLEFORNOTIFICATIONS COMMENT 'Indicator for the cashiers message notification',
	PAYLOAD_QUALIFICATIONSTOREGROUPS_PODSTOREGROUPS COMMENT 'pod store groups in payload',
	PAYLOAD_QUALIFICATIONSTOREGROUPS_NONDIGITALREDEMPTIONSTOREGROUPS COMMENT 'non-digital stores in oms offer',
	PAYLOAD_QUALIFICATIONSTOREGROUPS_DIGITALREDEMPTIONSTOREGROUPS COMMENT 'digital stores in oms offer',
	PAYLOAD_QUALIFICATIONPRODUCTDISQUALIFIER COMMENT 'Qualifiation product disqualifier text',
	PAYLOAD_QUALIFICATIONDAY_MONDAY COMMENT 'Indicator if the qualification day of the offer is Monday',
	PAYLOAD_QUALIFICATIONDAY_TUESDAY COMMENT 'Indicator if the qualification day of the offer is Tuesday',
	PAYLOAD_QUALIFICATIONDAY_WEDNESDAY COMMENT 'Indicator if the qualification day of the offer is Wednesday',
	PAYLOAD_QUALIFICATIONDAY_THURSDAY COMMENT 'Indicator if the qualification day of the offer is Thursday',
	PAYLOAD_QUALIFICATIONDAY_FRIDAY COMMENT 'Indicator if the qualification day of the offer is Friday',
	PAYLOAD_QUALIFICATIONDAY_SATURDAY COMMENT 'Indicator if the qualification day of the offer is Saturday',
	PAYLOAD_QUALIFICATIONDAY_SUNDAY COMMENT 'Indicator if the qualification day of the offer is Sunday',
	PAYLOAD_QUALIFICATIONTIME_START COMMENT 'Offer Qualificaiton start time text',
	PAYLOAD_QUALIFICATIONTIME_END COMMENT 'Offer Qualification end time',
	PAYLOAD_QUALIFICATIONENTERPRISEINSTANTWIN_NUMBEROFPRIZES COMMENT 'Offer Qualification instant win prizes quantity',
	PAYLOAD_QUALIFICATIONENTERPRISEINSTANTWIN_FREQUENCY COMMENT 'Offer Qualification Instant Win frequency',
	PAYLOAD_QUALIFICATIONTRIGGERCODES COMMENT 'Barcode Value of the PLU Trigger',
	PAYLOAD_QUALIFICATIONATTRIBUTES COMMENT 'payload qualification attributes',
	PAYLOAD_NOPANUMBERS COMMENT 'Nopa numbers in oms offer payload',
	PAYLOAD_OFFERNAME COMMENT 'Name of the Offer',
	PAYLOAD_ADTYPE COMMENT 'Type of the Advertisement',
	PAYLOAD_OFFERPROTOTYPE COMMENT 'Prototype code of the offer',
	PAYLOAD_OFFERPROTOTYPEDESC COMMENT 'Prototype description of the offer',
	PAYLOAD_STOREGROUPVERSIONID COMMENT 'Offer Version Identifier at the store group level',
	PAYLOAD_STORETAG_PRINTJ4UTAGENABLED COMMENT 'Indicator if the J4U tag is printed on the store tag',
	PAYLOAD_STORETAG_MULTIPLE COMMENT 'Store tag number',
	PAYLOAD_STORETAG_AMOUNT COMMENT 'Offer Amount on the store tag of the offer',
	PAYLOAD_STORETAG_COMMENTS COMMENT 'Comments on the store tag',
	PAYLOAD_REQUESTEDREMOVALFORALL COMMENT 'Indicator if there is a request for the offer has to be removed for all',
	PAYLOAD_REMOVEDON COMMENT 'Timestamp when the offer is removed',
	PAYLOAD_REMOVEDUNCLIPPEDON COMMENT 'Timestamp when the offer has been unclipped',
	PAYLOAD_REMOVEDFORALLON COMMENT 'Timestamp when the offer has to be removed for all',
	PAYLOAD_BRANDNSIZE COMMENT 'Offer Brand size description',
	PAYLOAD_CREATEDUSER_USERID COMMENT 'User Id of the person who created the offer',
	PAYLOAD_CREATEDUSER_FIRSTNAME COMMENT 'First Name of the person who created the offer',
	PAYLOAD_CREATEDUSER_LASTNAME COMMENT 'last name of the person who created the offer',
	PAYLOAD_UPDATEDUSER_USERID COMMENT 'Offer last updated by User Identifier',
	PAYLOAD_UPDATEDUSER_FIRSTNAME COMMENT 'First name of the user who updated the offer',
	PAYLOAD_UPDATEDUSER_LASTNAME COMMENT 'Last name of the user who updated the offer',
	PAYLOAD_FIRSTUPDATETOREDEMPTIONENGINE COMMENT 'Timestamp when the offer was first updated to redemption engine',
	PAYLOAD_LASTUPDATETOREDEMPTIONENGINE COMMENT 'Timestamp when the offer was last updated to redemption engine',
	PAYLOAD_FIRSTUPDATETOJ4U COMMENT 'Timestamp when the offer was first updated to J4U',
	PAYLOAD_LASTUPDATETOJ4U COMMENT 'Timestamp when the offer was last updated to J4U',
	PAYLOAD_OFFERREQUESTORGROUP COMMENT 'Group code of the offer requestor',
	PAYLOAD_HEADLINE COMMENT 'Offer Headline text',
	PAYLOAD_ISPODAPPROVED COMMENT 'Indicator if the offer is approved by POD',
	PAYLOAD_PODUSAGELIMITTYPEPERUSER COMMENT 'Usage Limit of the Of the Offer per User at POD level',
	PAYLOAD_PODREFERENCEOFFERID COMMENT 'Offer Identifier of the POD reference',
	PAYLOAD_IVIEIMAGEID COMMENT 'Identifier of the Coupon Image',
	PAYLOAD_VEHICLENM COMMENT 'Vehicle Name text of offer',
	PAYLOAD_ADPAGENBR COMMENT 'Page Number of the Offer Advertisement',
	PAYLOAD_ADMODNBR COMMENT 'Advertisement MOD Number',
	PAYLOAD_ECOMDESC COMMENT 'Online Description of the offer',
	PAYLOAD_REQUESTEDUSER_USERID COMMENT 'User Identifier of the user who requested the offer',
	PAYLOAD_REQUESTEDUSER_FIRSTNAME COMMENT 'First name of the user who requested the offer',
	PAYLOAD_REQUESTEDUSER_LASTNAME COMMENT 'Last name of the user who requested the offer',
	PAYLOAD_ISPRIMARYPODOFFER COMMENT 'Indicator if the offer is a Primary POD Offer',
	LASTUPDATETS COMMENT 'The timestamp the record was inserted.',
	DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
	PAYLOAD_PRODUCT_DESCPRODDSC1 COMMENT 'Additional description 1 of the product on the offer',
	PAYLOAD_PRODUCT_DESCPRODDSC2 COMMENT 'Additional description 2 of the product on the offer',
	PAYLOAD_TITLE_DESCTITLEDSC1 COMMENT 'Additional Description 1 of the offer title',
	PAYLOAD_TITLE_DESCTITLEDSC2 COMMENT 'Additional Description 2 of the offer title',
	PAYLOAD_TITLE_DESCTITLEDSC3 COMMENT 'Additional Description 3 of the offer title',
	PAYLOAD_REDEMPTIONSYSTEMID COMMENT 'Identifier of the system which redeemed the offer',
	PAYLOAD_USAGELIMITPERUSER COMMENT 'Usage Limit for the Offer',
	PAYLOAD_CUSTOMPERIOD COMMENT 'Usage custom period of the offer',
	PAYLOAD_CUSTOMTYPE COMMENT 'Custom Usage type of the offer',
	QUALIFICATIONPRODUCTDISQUALIFIERNAME COMMENT 'Disqualified product group name (id of the disqualified product group)',
	ISDISPLAYIMMEDIATE COMMENT 'used for cashier message, if true, then Display Immediately; if false Display at EOS',
	PAYLOAD_QUALIFICATIONSTOREGROUPS_REDEMPTIONSTOREGROUPS COMMENT 'Redemption store group details in Offer',
	PAYLOAD_QUALIFICATIONTENDERTYPES COMMENT 'Tender type details eg: Tiers and values in OMS offer payload',
	PAYLOAD_FULFILLMENTCHANNEL COMMENT 'EPE Team purpose that Fulfillment channel codes have been added with 4 fields',
	PAYLOAD_ECOMMPROMOTYPE COMMENT 'EPE Team purpose that Schedule and Save Fields have been added',
	PAYLOAD_ECOMMPROMOCODE COMMENT 'Name of a Ecomm promotion program code',
	PAYLOAD_VALIDWITHOTHEROFFER COMMENT 'Promo code can be used (is combinable) with other promo codes',
	PAYLOAD_ORDERCOUNT COMMENT 'Count of number of Orderson which promotion is valid/Number of orders or the order number on which the promo ',
	PAYLOAD_FIRSTTIMECUSTOMERONLY COMMENT 'Promo code can only apply on a customer''s first order.',
	PAYLOAD_AUTOAPPLYPROMOCODE COMMENT 'Promo code can be auto applied without the customer entering it on the site.',
	PAYLOAD_NOTCOMBINABLEWITH COMMENT 'Promo code cannot be used (not combinable) with other promo codes',
	PAYLOAD_SUBPROGRAMCODE COMMENT 'Divisional Games code added',
	PAYLOAD_QUALIFICATIONBEHAVIOR COMMENT 'Behavior code added',
	PAYLOAD_INITIALSUBSCRIPTIONOFFER COMMENT 'Initial Subscription schedule and save indicator',
	PAYLOAD_ISDYNAMICOFFER COMMENT 'Dynamic offer indicator',
	PAYLOAD_DAYSTOREDEEM COMMENT 'Days to redeem count'
) COMMENT='VIEW for OMS Offer Confirmed Flat table'
 as 
SELect
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
,payload_categories_additionalProperties 
,payload_primaryCategory_additionalProperties 
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
,DW_CREATE_TS 
,PAYLOAD_PRODUCT_DESCPRODDSC1 
,PAYLOAD_PRODUCT_DESCPRODDSC2 
,PAYLOAD_TITLE_DESCTITLEDSC1  
,PAYLOAD_TITLE_DESCTITLEDSC2  
,PAYLOAD_TITLE_DESCTITLEDSC3  
,payload_redemptionSystemId  
,payload_usageLimitPerUser
,payload_customPeriod
,payload_customType 
,QUALIFICATIONPRODUCTDISQUALIFIERNAME
,ISDISPLAYIMMEDIATE
,payload_qualificationStoreGroups_redemptionStoreGroups
,PAYLOAD_QUALIFICATIONTENDERTYPES
,PAYLOAD_FULFILLMENTCHANNEL
,PAYLOAD_ECOMMPROMOTYPE
,payload_ecommPromoCode
,payload_validWithOtherOffer 
,payload_orderCount 
,payload_firstTimeCustomerOnly 
,payload_autoApplyPromoCode 
,payload_notCombinableWith
,PAYLOAD_SUBPROGRAMCODE
,PAYLOAD_QUALIFICATIONBEHAVIOR
,PAYLOAD_INITIALSUBSCRIPTIONOFFER
,PAYLOAD_ISDYNAMICOFFER
,PAYLOAD_DAYSTOREDEEM

from <<EDM_DB_NAME>>.DW_C_PRODUCT.OfferOMS_Flat;

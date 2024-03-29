--liquibase formatted sql
--changeset SYSTEM:GetOffer_Flat runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_PRODUCT;

create or replace TABLE GETOFFER_FLAT (
	FILENAME VARCHAR(16777216),
	INCENTIVEID VARCHAR(16777216),
	ENGINETYPENM VARCHAR(16777216),
	DEPLOYIND VARCHAR(16777216),
	DEFERDEPLOYIND VARCHAR(16777216),
	LASTUPDATETS VARCHAR(16777216),
	GENERAL_IDENTIFICATION_OFFERNM VARCHAR(16777216),
	GENERAL_IDENTIFICATION_OFFERDSC VARCHAR(16777216),
	GENERAL_IDENTIFICATION_CATEGORYDSC VARCHAR(16777216),
	GENERAL_IDENTIFICATION_OFFEREXTERNALID VARCHAR(16777216),
	GENERAL_IDENTIFICATION_VENDORCOUPONCD VARCHAR(16777216),
	GENERAL_PRIORITY_PRIORITYNM VARCHAR(16777216),
	GENERAL_PRIORITY_FOOTERPRIORITYMSGTXT VARCHAR(16777216),
	GENERAL_DATES_TESTING_TESTINGSTARTDT VARCHAR(16777216),
	GENERAL_DATES_TESTING_TESTINGENDDT VARCHAR(16777216),
	GENERAL_DATES_ELIGIBILITY_ELIGIBILITYSTARTDT VARCHAR(16777216),
	GENERAL_DATES_ELIGIBILITY_ELIGIBILITYENDDT VARCHAR(16777216),
	GENERAL_DATES_PRODUCTION_PRODUCTIONSTARTDT VARCHAR(16777216),
	GENERAL_DATES_PRODUCTION_PRODUCTIONENDDT VARCHAR(16777216),
	GENERAL_LIMITS_ELIGIBILITY_ELIGIBILITYFREQUENCYNM VARCHAR(16777216),
	GENERAL_LIMITS_ELIGIBILITY_LIMITNBR VARCHAR(16777216),
	GENERAL_LIMITS_ELIGIBILITY_PERIODQTY VARCHAR(16777216),
	GENERAL_LIMITS_ELIGIBILITY_QUANTITYTYPE VARCHAR(16777216),
	GENERAL_LIMITS_REWARD_REWARDSFREQUENCYNM VARCHAR(16777216),
	GENERAL_LIMITS_REWARD_LIMITNBR VARCHAR(16777216),
	GENERAL_LIMITS_REWARD_PERIODQTY VARCHAR(16777216),
	GENERAL_LIMITS_REWARD_QUANTITYTYPE VARCHAR(16777216),
	GENERAL_INBOUNDOUTBOUND_CHARGEBACKVENDORID VARCHAR(16777216),
	GENERAL_INBOUNDOUTBOUND_CREATIONSOURCENM VARCHAR(16777216),
	GENERAL_INBOUNDOUTBOUND_OUTBOUNDSOURCENM VARCHAR(16777216),
	GENERAL_EMPLOYEES_EMPLOYEEELIGIBILITYTYPNM VARCHAR(16777216),
	GENERAL_ADVANCED_EOSDEFERIND VARCHAR(16777216),
	GENERAL_ADVANCED_ISSUANCEIND VARCHAR(16777216),
	GENERAL_ADVANCED_MANUFACTURERCOUPONIND VARCHAR(16777216),
	GENERAL_ADVANCED_REPORTING_IMPRESSIONIND VARCHAR(16777216),
	GENERAL_ADVANCED_REPORTING_REDEMPTIONIND VARCHAR(16777216),
	NOTIFICATIONS_PRINTEDMESSAGE_PRINTEDMESSAGETXT VARCHAR(16777216),
	NOTIFICATIONS_CASHIERMESSAGE_MESSAGELINE1TXT VARCHAR(16777216),
	NOTIFICATIONS_CASHIERMESSAGE_MESSAGELINE2TXT VARCHAR(16777216),
	NOTIFICATIONS_CASHIERMESSAGE_BEEPTYPE VARCHAR(16777216),
	NOTIFICATIONS_CASHIERMESSAGE_BEEPDURATIONSEC VARCHAR(16777216),
	NOTIFICATIONS_CASHIERMESSAGE_DISPLAYIMMEDIATELYIND VARCHAR(16777216),
	NOTIFICATIONS_ACCUMULATIONPRINTEDMESSAGE_ACUMULATIONPRINTEDMESSAGEIND VARCHAR(16777216),
	LOCATIONS_STOREGROUPS_EXCLUDED_STOREGROUPNM VARCHAR(16777216),
	LOCATIONS_STOREGROUPS_INCLUDED ARRAY,
	LOCATIONS_TERMINALS_INCLUDED ARRAY,
	LOCATIONS_TERMINALS_EXCLUDED ARRAY,
	CONDITIONS_ENTERPRISEINSTANTWIN_PRIZENBR VARCHAR(16777216),
	CONDITIONS_ENTERPRISEINSTANTWIN_PRIZEFREQUENCYNBR VARCHAR(16777216),
	CONDITIONS_DAY_MONDAYIND VARCHAR(16777216),
	CONDITIONS_DAY_TUESDAYIND VARCHAR(16777216),
	CONDITIONS_DAY_WEDNESDAYIND VARCHAR(16777216),
	CONDITIONS_DAY_THURSDAYIND VARCHAR(16777216),
	CONDITIONS_DAY_FRIDAYIND VARCHAR(16777216),
	CONDITIONS_DAY_SATURDAYIND VARCHAR(16777216),
	CONDITIONS_DAY_SUNDAYIND VARCHAR(16777216),
	CONDITIONS_DISQUALIFIERPRODUCTGROUPNM VARCHAR(16777216),
	CONDITIONS_STOREDVALUE_PROGRAMNM VARCHAR(16777216),
	CONDITIONS_STOREDVALUE_TIERS ARRAY,
	CONDITIONS_TENDERS ARRAY,
	CONDITIONS_TIMES ARRAY,
	CONDITIONS_TRIGGERCODES ARRAY,
	CONDITIONS_CUSTOMERS_INCLUDED ARRAY,
	CONDITIONS_CUSTOMERS_EXCLUDED ARRAY,
	CONDITIONS_PRODUCTS ARRAY,
	CONDITIONS_POINTS_PROGRAMNM VARCHAR(16777216),
	CONDITIONS_POINTS_TIERS ARRAY,
	CONDITIONS_CARDTYPES ARRAY,
	CONDITIONS_ATTRIBUTES ARRAY,
	REWARDS_DISCOUNT_PRODUCTGROUP_DISCOUNTTYPE VARCHAR(16777216),
	REWARDS_DISCOUNT_PRODUCTGROUP_INCPRODUCTGROUPNM VARCHAR(16777216),
	REWARDS_DISCOUNT_PRODUCTGROUP_EXCPRODUCTGROUPNM VARCHAR(16777216),
	REWARDS_DISCOUNT_CHARGEBACKDEPARTMENT_DEPARTMENTNM VARCHAR(16777216),
	REWARDS_DISCOUNT_CHARGEBACKDEPARTMENT_DEPARTMENTID VARCHAR(16777216),
	REWARDS_DISCOUNT_SCORECARD_SCORECARDNM VARCHAR(16777216),
	REWARDS_DISCOUNT_SCORECARD_SCORECARDENABLEIND VARCHAR(16777216),
	REWARDS_DISCOUNT_SCORECARD_SCORECARDLINETXT VARCHAR(16777216),
	REWARDS_DISCOUNT_ADVANCED_COMPUTEDISCOUNTIND VARCHAR(16777216),
	REWARDS_DISCOUNT_ADVANCED_BESTDEALIND VARCHAR(16777216),
	REWARDS_DISCOUNT_ADVANCED_ALLOWNEGATIVEIND VARCHAR(16777216),
	REWARDS_DISCOUNT_ADVANCED_FLEXNEGATIVEIND VARCHAR(16777216),
	REWARDS_DISCOUNT_DISTRIBUTION_DISTRIBUTIONTYPE VARCHAR(16777216),
	REWARDS_DISCOUNT_DISTRIBUTION_TIERS ARRAY,
	REWARDS_CASHIERMESSAGE_TIERS ARRAY,
	REWARDS_CASHIERMESSAGE_DISPLAYIMMEDIATELYIND VARCHAR(16777216),
	REWARDS_POINTS ARRAY,
	REWARDS_PRINTEDMESSAGE_TIERS ARRAY,
	REWARDS_GROUPMEMBERSHIP_TIERS ARRAY,
	REWARDS_FRANKINGMESSAGE_TIERS ARRAY,
	REWARDS_STOREDVALUE_PROGRAMNM VARCHAR(16777216),
	REWARDS_STOREDVALUE_SCORECARD_SCORECARDENABLEDIND VARCHAR(16777216),
	REWARDS_STOREDVALUE_SCORECARD_SCORECARDNM VARCHAR(16777216),
	REWARDS_STOREDVALUE_SCORECARD_SCORECARDTXT VARCHAR(16777216),
	REWARDS_STOREDVALUE_TIERS ARRAY,
	PRODUCTGROUP_PRODUCTGROUPNM VARCHAR(16777216),
	PRODUCTGROUP_PRODUCTGROUPDSC VARCHAR(16777216),
	PRODUCTGROUP_LASTUPDATETS VARCHAR(16777216),
	PRODUCT_GROUPS_PRODUCTS ARRAY,
	STOREGROUP_LASTUPDATETS VARCHAR(16777216),
	STOREGROUP_STOREGROUPNM VARCHAR(16777216),
	STOREGROUPS_STORES ARRAY,
	DW_CREATE_TS TIMESTAMP_LTZ(9)
);

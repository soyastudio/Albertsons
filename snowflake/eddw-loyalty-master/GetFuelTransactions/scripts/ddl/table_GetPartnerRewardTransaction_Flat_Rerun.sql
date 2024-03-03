--liquibase formatted sql
--changeset SYSTEM:GetPartnerRewardTransaction_Flat_Rerun runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME_R>>;
use schema <<EDM_DB_NAME_R>>.DW_R_STAGE;

create or replace TABLE GETPARTNERREWARDTRANSACTION_FLAT_RERUN (
	FILENAME VARCHAR(16777216),
	BODNM VARCHAR(16777216),
	DOCUMENTID VARCHAR(16777216),
	EXPECTEDMESSAGECNT VARCHAR(16777216),
	EXTERNALTARGETIND VARCHAR(16777216),
	INTERCHANGETIME VARCHAR(16777216),
	INTERCHANGEDATE VARCHAR(16777216),
	INTERNALFILETRANSFERIND VARCHAR(16777216),
	ROUTINGSYSTEMNM VARCHAR(16777216),
	RECEIVERID VARCHAR(16777216),
	SENDERID VARCHAR(16777216),
	MESSAGESEQUENCENBR VARCHAR(16777216),
	NOTE VARCHAR(16777216),
	TARGETAPPLICATIONCD VARCHAR(16777216),
	SOURCEAPPLICATIONCD VARCHAR(16777216),
	DOCUMENT_DESCRIPTION VARCHAR(16777216),
	CREATIONDT VARCHAR(16777216),
	DOCUMENTNM VARCHAR(16777216),
	INBOUNDOUTBOUNDIND VARCHAR(16777216),
	ALTERNATEDOCUMENTID VARCHAR(16777216),
	GATEWAYNM VARCHAR(16777216),
	PIIDATAIND VARCHAR(16777216),
	PCIDATAIND VARCHAR(16777216),
	PHIDATAIND VARCHAR(16777216),
	BUSINESSSENSITIVITYLEVEL_SHORTDESCRIPTION VARCHAR(16777216),
	BUSINESSSENSITIVITYLEVEL_CODE VARCHAR(16777216),
	BUSINESSSENSITIVITYLEVEL_DESCRIPTION VARCHAR(16777216),
	DATACLASSIFICATIONLEVEL_SHORTDESCRIPTION VARCHAR(16777216),
	DATACLASSIFICATIONLEVEL_DESCRIPTION VARCHAR(16777216),
	DATACLASSIFICATIONLEVEL_CODE VARCHAR(16777216),
	ACTIONTYPECD VARCHAR(16777216),
	RECORDTYPECD VARCHAR(16777216),
	PARTNERID VARCHAR(16777216),
	CREATEDTTM VARCHAR(16777216),
	UPDATEDTTM VARCHAR(16777216),
	CREATEUSERID VARCHAR(16777216),
	CREATETS VARCHAR(16777216),
	UPDATETS VARCHAR(16777216),
	UPDATEUSERID VARCHAR(16777216),
	CUSTOMERDATA_DIVISIONID VARCHAR(16777216),
	HOUSEHOLDID VARCHAR(16777216),
	CLUBCARDNBR VARCHAR(16777216),
	OLDCLUBCARDNBR VARCHAR(16777216),
	PHONENBR VARCHAR(16777216),
	CUSTOMERACCOUNTNBR VARCHAR(16777216),
	CUSTOMERID VARCHAR(16777216),
	GENERATIONAFFIXCD VARCHAR(16777216),
	MAIDENNM VARCHAR(16777216),
	FAMILYNM VARCHAR(16777216),
	MIDDLENM VARCHAR(16777216),
	NICKNM VARCHAR(16777216),
	GIVENNM VARCHAR(16777216),
	TITLECD VARCHAR(16777216),
	PREFERREDSALUTATIONCD VARCHAR(16777216),
	FORMATTEDNM VARCHAR(16777216),
	QUALIFICATIONAFFIXCD VARCHAR(16777216),
	ENDTS VARCHAR(16777216),
	TIMEZONECD VARCHAR(16777216),
	DURATION VARCHAR(16777216),
	STARTTS VARCHAR(16777216),
	INCLUSIVEIND VARCHAR(16777216),
	CUSTOMERTYPE_SHORTDESCRIPTION VARCHAR(16777216),
	CUSTOMERTYPE_DESCRIPTION VARCHAR(16777216),
	CUSTOMERTYPE_CODE VARCHAR(16777216),
	POSTALZONECD VARCHAR(16777216),
	PARTNERDATA_DIVISIONID VARCHAR(16777216),
	PARTNERSITEID VARCHAR(16777216),
	PARTNERPARTICIPANTID VARCHAR(16777216),
	TOTALPURCHQTY VARCHAR(16777216),
	REWARDTOKENOFFEREDQTY VARCHAR(16777216),
	REWARDMSGID VARCHAR(16777216),
	FUELPUMPID VARCHAR(16777216),
	REGISTERID VARCHAR(16777216),
	PURCHDISCLIMITQTY VARCHAR(16777216),
	DISCOUNTAMT_CURRENCYEXCHANGERT VARCHAR(16777216),
	DISCOUNTAMT_CURRENCYCD VARCHAR(16777216),
	DISCOUNTAMT_DECIMALNBR VARCHAR(16777216),
	DISCOUNTAMT_TRANSACTIONAMT VARCHAR(16777216),
	FUELGRADECD_DESCRIPTION VARCHAR(16777216),
	FUELGRADECD_CODE VARCHAR(16777216),
	FUELGRADECD_SHORTDESCRIPTION VARCHAR(16777216),
	NONFUELPURCHAMT_CURRENCYCD VARCHAR(16777216),
	NONFUELPURCHAMT_TRANSACTIONAMT VARCHAR(16777216),
	NONFUELPURCHAMT_DECIMALNBR VARCHAR(16777216),
	NONFUELPURCHAMT_CURRENCYEXCHANGERT VARCHAR(16777216),
	PURCHDISCLIMITAMT_CURRENCYCD VARCHAR(16777216),
	PURCHDISCLIMITAMT_DECIMALNBR VARCHAR(16777216),
	PURCHDISCLIMITAMT_CURRENCYEXCHANGERT VARCHAR(16777216),
	PURCHDISCLIMITAMT_TRANSACTIONAMT VARCHAR(16777216),
	UOMCD VARCHAR(16777216),
	UOMNM VARCHAR(16777216),
	TENDERTYPECD_CODE VARCHAR(16777216),
	TENDERTYPECD_DESCRIPTION VARCHAR(16777216),
	TENDERTYPECD_SHORTDESCRIPTION VARCHAR(16777216),
	TOTALFUELPURCHAMT_TRANSACTIONAMT VARCHAR(16777216),
	TOTALFUELPURCHAMT_DECIMALNBR VARCHAR(16777216),
	TOTALFUELPURCHAMT_CURRENCYCD VARCHAR(16777216),
	TOTALFUELPURCHAMT_CURRENCYEXCHANGERT VARCHAR(16777216),
	TOTALPURCHASEAMT_CURRENCYEXCHANGERT VARCHAR(16777216),
	TOTALPURCHASEAMT_DECIMALNBR VARCHAR(16777216),
	TOTALPURCHASEAMT_TRANSACTIONAMT VARCHAR(16777216),
	TOTALPURCHASEAMT_CURRENCYCD VARCHAR(16777216),
	TOTALSAVINGSVALAMT_TRANSACTIONAMT VARCHAR(16777216),
	TOTALSAVINGSVALAMT_DECIMALNBR VARCHAR(16777216),
	TOTALSAVINGSVALAMT_CURRENCYCD VARCHAR(16777216),
	TOTALSAVINGSVALAMT_CURRENCYEXCHANGERT VARCHAR(16777216),
	EXCEPTIONTXNTS VARCHAR(16777216),
	EXCEPTIONMSGID VARCHAR(16777216),
	EXCEPTIONTYPECD_CODE VARCHAR(16777216),
	EXCEPTIONTYPECD_SHORTDESCRIPTION VARCHAR(16777216),
	EXCEPTIONTYPECD_DESCRIPTION VARCHAR(16777216),
	STATUSCD VARCHAR(16777216),
	STATUSTYPE_DESCRIPTION VARCHAR(16777216),
	EFFECTIVEDTTM VARCHAR(16777216),
	TRANSACTIONTS VARCHAR(16777216),
	REFERENCENBR VARCHAR(16777216),
	TRANSACTIONID VARCHAR(16777216),
	ALTTRANSACTIONID VARCHAR(16777216),
	ALTTRANSACTIONTS VARCHAR(16777216),
	ALTTRANSACTIONTYPE_SHORTDESCRIPTION VARCHAR(16777216),
	ALTTRANSACTIONTYPE_CODE VARCHAR(16777216),
	ALTTRANSACTIONTYPE_DESCRIPTION VARCHAR(16777216),
	TRANSACTIONTYPECD_DESCRIPTION VARCHAR(16777216),
	TRANSACTIONTYPECD_CODE VARCHAR(16777216),
	TRANSACTIONTYPECD_SHORTDESCRIPTION VARCHAR(16777216),
	DOCUMENT_RELEASEID VARCHAR(16777216),
	DOCUMENT_VERSIONID VARCHAR(16777216),
	DOCUMENT_SYSTEMENVIRONMENTCD VARCHAR(16777216),
	CUSTOMERNM_TYPECODE VARCHAR(16777216),
	CUSTOMERNM_SEQUENCENBR VARCHAR(16777216),
	CUSTOMERNM_PREFERREDIND VARCHAR(16777216),
	STATUSCD_TYPE VARCHAR(16777216),
	DW_CREATETS TIMESTAMP_LTZ(9),
	METADATA$ACTION VARCHAR(6),
	METADATA$ISUPDATE BOOLEAN,
	METADATA$ROW_ID VARCHAR(40)
);
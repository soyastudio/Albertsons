--liquibase formatted sql
--changeset SYSTEM:RETAIL_CUSTOMER_HH_LOYALTY_PROGRAM runOnChange:true splitStatements:false OBJECT_TYPE:TABLE

use database <<EDM_DB_NAME>>;
use schema DW_STAGE;
	


create or replace TABLE RETAIL_CUSTOMER_HH_LOYALTY_PROGRAM cluster by (LOYALTY_PROGRAM_CARD_NBR,HOUSEHOLD_ID)(
	HOUSEHOLD_ID NUMBER(38,0),
	LOYALTY_PROGRAM_CARD_NBR NUMBER(38,0)
);


alter table  RETAIL_CUSTOMER_HH_LOYALTY_PROGRAM   cluster by (LOYALTY_PROGRAM_CARD_NBR,HOUSEHOLD_ID);

   
 alter table  CLIP_DETAILS_TMP_DIGITAL   cluster by (HOUSEHOLD_ID,OFFER_ID);
 
 

--liquibase formatted sql
--changeset SYSTEM:RETAIL_CUSTOMER_HH_LOYALTY_PROGRAM_REPROCESS runOnChange:true splitStatements:false OBJECT_TYPE:TABLE

use database <<EDM_DB_NAME>>;
use schema DW_STAGE;
	


create or replace TABLE RETAIL_CUSTOMER_HH_LOYALTY_PROGRAM_REPROCESS cluster by (LOYALTY_PROGRAM_CARD_NBR,HOUSEHOLD_ID)(
	HOUSEHOLD_ID NUMBER(38,0),
	LOYALTY_PROGRAM_CARD_NBR NUMBER(38,0)
);


alter table  RETAIL_CUSTOMER_HH_LOYALTY_PROGRAM_REPROCESS   cluster by (LOYALTY_PROGRAM_CARD_NBR,HOUSEHOLD_ID);

   
 alter table  clip_details_tmp_digital_Reprocess   cluster by (HOUSEHOLD_ID,OFFER_ID);

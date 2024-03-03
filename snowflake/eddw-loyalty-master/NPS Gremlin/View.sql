
USE DATABASE EDM_VIEWS_PRD;
USE SCHEMA DW_VIEWS;

create or replace view Txn_NPS_Survey_Faulty_Data(
	TXN_ID COMMENT 'Transaction Id', 
	STORE_ID COMMENT 'Store number in which transaction takes place', 
	REGISTER_NBR COMMENT 'Register number of the store', 
	TXN_DTE COMMENT 'Transaction date', 
	TXN_TM COMMENT 'Transaction date/time'
) COMMENT='VIEW for Txn_NPS_Survey_Faulty_Data'
 as
		SELECT 
			  TXN_ID 
			  ,STORE_ID 
			  ,REGISTER_NBR 
			  ,TXN_DTE 
			  ,TXN_TM 
		FROM EDM_CONFIRMED_PRD.DW_C_TRANSACTION.Txn_NPS_Survey_Faulty_Data;

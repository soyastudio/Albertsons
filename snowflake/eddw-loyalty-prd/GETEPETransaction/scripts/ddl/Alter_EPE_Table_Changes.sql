
--liquibase formatted sql
--changeset SYSTEM:tables runOnChange:true splitStatements:false OBJECT_TYPE:table

USE Database <<EDM_DB_NAME_R>>;
USE SChema dw_r_retailsale;

 ALTER TABLE <<EDM_DB_NAME_R>>.dw_r_retailsale.EPETransaction_Flat drop COLUMN if exists
 TxnLevel_ProgramType ;
Alter table <<EDM_DB_NAME_R>>.DW_R_Retailsale.EPETransaction_Flat
add Column TxnLevel_ProgramType String;



ALTER TABLE <<EDM_DB_NAME>>.DW_C_RETAILSALE.Epe_Transaction_Header_Savings drop COLUMN if exists
 Program_Type_Cd   ;
  
ALTER TABLE <<EDM_DB_NAME>>.DW_C_RETAILSALE.Epe_Transaction_Item_Savings drop COLUMN if exists
 Program_Type_Cd   ;

ALTER TABLE <<EDM_DB_NAME>>.DW_C_RETAILSALE.Epe_Transaction_Header_Savings ADD COLUMN
 Program_Type_Cd   VARCHAR;
  
ALTER TABLE <<EDM_DB_NAME>>.DW_C_RETAILSALE.Epe_Transaction_Item_Savings ADD COLUMN
 Program_Type_Cd   VARCHAR;


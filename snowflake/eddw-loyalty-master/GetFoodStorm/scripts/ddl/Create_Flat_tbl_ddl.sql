--liquibase formatted sql
--changeset SYSTEM:Create_Flat_tbl_ddl runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
USE DATABASE <<EDM_DB_NAME_R>>;
USE SCHEMA <<EDM_DB_NAME_R>>.DW_R_LOYALTY;

CREATE OR REPLACE TABLE GETFOODSTORM_FLAT
(
Type         VARCHAR   NULL ,
Store        VARCHAR   NULL ,
OrderNo      VARCHAR   NULL ,
DeliveryDate VARCHAR   NULL ,
UPC          VARCHAR   NULL ,
Item         VARCHAR   NULL ,
Department   VARCHAR   NULL ,
Price        VARCHAR   NULL ,
Qty          VARCHAR   NULL ,
ItemTotal    VARCHAR   NULL ,
TaxCode      VARCHAR   NULL ,
TaxRate      VARCHAR   NULL ,
DeliveryZip  VARCHAR   NULL ,
Total        VARCHAR   NULL ,
TotalTax     VARCHAR   NULL ,
PaymentTotal VARCHAR   NULL ,
Balance      VARCHAR   NULL ,
TaxExempt    VARCHAR   NULL ,
TaxExemptId  VARCHAR   NULL ,
HouseAccount VARCHAR   NULL ,
CustomerGUID VARCHAR   NULL ,
Phone_Number VARCHAR   NULL ,
DW_CREATETS  TIMESTAMP NULL ,
FILENAME    VARCHAR   NULL
)
CHANGE_TRACKING = TRUE;

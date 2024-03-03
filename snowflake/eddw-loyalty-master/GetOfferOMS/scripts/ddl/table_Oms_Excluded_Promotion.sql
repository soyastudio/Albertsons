--liquibase formatted sql
--changeset SYSTEM:Oms_Excluded_Promotion runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database EDM_CONFIRMED_PRD;
use schema EDM_CONFIRMED_PRD.DW_C_PRODUCT;

CREATE or replace TABLE Oms_Excluded_Promotion
(
 Oms_Offer_Id          NUMBER  NOT NULL ,
 Promo_Cd              VARCHAR  NOT NULL ,
 Dw_First_Effective_Dt  DATE  NOT NULL ,
 Dw_Last_Effective_Dt  DATE  NOT NULL ,
 Dw_Create_Ts          TIMESTAMP  ,
 Dw_Last_Update_Ts     TIMESTAMP  ,
 Dw_Logical_Delete_Ind  BOOLEAN  ,
 Dw_Source_Create_Nm   VARCHAR(255)  ,
 Dw_Source_Update_Nm   VARCHAR(255)  ,
 Dw_Current_Version_Ind  BOOLEAN  
);

COMMENT ON TABLE Oms_Excluded_Promotion IS 'Entity to define Promotion excluded to combine with the main offer in OMS_OFFER table';

COMMENT ON COLUMN Oms_Excluded_Promotion.Promo_Cd IS 'Promo code cannot be used (not combinable) with other promo codes';

COMMENT ON COLUMN Oms_Excluded_Promotion.Dw_First_Effective_Dt IS 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key';

COMMENT ON COLUMN Oms_Excluded_Promotion.Dw_Last_Effective_Dt IS 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day';

COMMENT ON COLUMN Oms_Excluded_Promotion.Dw_Create_Ts IS 'The timestamp the record was inserted.';

COMMENT ON COLUMN Oms_Excluded_Promotion.Dw_Last_Update_Ts IS 'When a record is updated  this would be the current timestamp';

COMMENT ON COLUMN Oms_Excluded_Promotion.Dw_Logical_Delete_Ind IS 'Set to True when we receive a delete record for the primary key, else False';

COMMENT ON COLUMN Oms_Excluded_Promotion.Dw_Source_Create_Nm IS 'The Bod (data source) name of this insert.';

COMMENT ON COLUMN Oms_Excluded_Promotion.Dw_Source_Update_Nm IS 'The Bod (data source) name of this update or delete.';

COMMENT ON COLUMN Oms_Excluded_Promotion.Dw_Current_Version_Ind IS 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d';

ALTER TABLE Oms_Excluded_Promotion
 ADD PRIMARY KEY (Oms_Offer_Id, Promo_Cd, Dw_First_Effective_Dt, Dw_Last_Effective_Dt);

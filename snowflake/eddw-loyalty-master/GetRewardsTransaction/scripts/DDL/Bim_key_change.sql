--liquibase formatted sql
--changeset SYSTEM:Bim_key_change runOnChange:true splitStatements:false OBJECT_TYPE:TABLE

USE DATABASE <<EDM_DB_NAME>>;
USE SCHEMA <<EDM_DB_NAME>>.DW_C_LOYALTY;

ALTER TABLE Reward_Transaction
drop PRIMARY KEY;

ALTER TABLE Reward_Transaction
ALTER COLUMN STATUS_CD SET NOT NULL,
REWARD_DOLLAR_END_TS SET NOT NULL;

ALTER TABLE Reward_Transaction
ADD PRIMARY KEY (Household_Id, Transaction_Id, Loyalty_Program_Cd,
                  Transaction_Type_Cd, Status_Cd, Reward_Dollar_End_Ts, Dw_First_Effective_Dt, Dw_Last_Effective_Dt);
                  
                              
CREATE or replace TABLE REWARD_TRANSACTION_AUDIT_LOG
(
 Household_Id          NUMBER  NOT NULL ,
 Transaction_Id        VARCHAR(100)  NOT NULL ,
 Loyalty_Program_Cd    VARCHAR(300)  NOT NULL ,
 Update_Ts             TIMESTAMP  NOT NULL ,
 Transaction_Type_Cd   VARCHAR(50)  NOT NULL ,
 Status_Cd             VARCHAR  NOT NULL ,
 Reward_Dollar_End_Ts  TIMESTAMP  NOT NULL ,
 Dw_First_Effective_Dt  DATE  NOT NULL ,
 Dw_Last_Effective_Dt  DATE  NOT NULL ,
 Create_Ts             TIMESTAMP  ,
 Before_Snapshot       VARIANT  ,
 After_Snapshot        VARIANT  ,
 Dw_Create_Ts          TIMESTAMP  ,
 Dw_Last_Update_Ts     TIMESTAMP  ,
 Dw_Logical_Delete_Ind  BOOLEAN  ,
 Dw_Source_Create_Nm   VARCHAR(255)  ,
 Dw_Source_Update_Nm   VARCHAR(255)  ,
 Dw_Current_Version_Ind  BOOLEAN  
);

COMMENT ON COLUMN REWARD_TRANSACTION_AUDIT_LOG.Household_Id IS 'Unique identifier of the Household. The ''aggregateId'' in CHMS service is the HHID unlike the other services where the ''aggegatedid'' we map to the CustomerId';

COMMENT ON COLUMN REWARD_TRANSACTION_AUDIT_LOG.Dw_Create_Ts IS 'The timestamp the record was inserted.';

COMMENT ON COLUMN REWARD_TRANSACTION_AUDIT_LOG.Dw_Last_Update_Ts IS 'When a record is updated  this would be the current timestamp';

COMMENT ON COLUMN REWARD_TRANSACTION_AUDIT_LOG.Dw_Logical_Delete_Ind IS 'Set to True when we receive a delete record for the primary key, else False';

COMMENT ON COLUMN REWARD_TRANSACTION_AUDIT_LOG.Dw_Source_Create_Nm IS 'The Bod (data source) name of this insert.';

COMMENT ON COLUMN REWARD_TRANSACTION_AUDIT_LOG.Dw_Source_Update_Nm IS 'The Bod (data source) name of this update or delete.';

COMMENT ON COLUMN REWARD_TRANSACTION_AUDIT_LOG.Dw_Current_Version_Ind IS 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d';

ALTER TABLE REWARD_TRANSACTION_AUDIT_LOG
 ADD PRIMARY KEY (Household_Id, Transaction_Id,Loyalty_Program_Cd, Update_Ts,Transaction_Type_Cd, Status_Cd,Reward_Dollar_End_Ts, Dw_First_Effective_Dt, 
 Dw_Last_Effective_Dt);

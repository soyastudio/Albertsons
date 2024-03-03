--liquibase formatted sql
--changeset SYSTEM:Ocgp_Prize_Transaction runOnChange:true splitStatements:false OBJECT_TYPE:TABLE

Use database <<EDM_DB_NAME>>;
Use schema DW_C_LOYALTY;

CREATE OR REPLACE TABLE Ocgp_Prize_Transaction
(
	Prize_Id              VARCHAR  NOT NULL ,
	Program_Cd            VARCHAR  NOT NULL ,
	Retail_Customer_Uuid  VARCHAR  NOT NULL ,
	Redeem_Ts            TIMESTAMP_TZ(9) NOT NULL ,
	Dw_First_Effective_Dt  DATE  NOT NULL ,
	Dw_Last_Effective_Dt  DATE  NOT NULL ,
	Prize_Nm              VARCHAR  ,
	Transaction_Dsc       VARCHAR  ,
	Fulfillment_Dt        DATE,
	Fulfillment_Status_Cd  VARCHAR  ,
	Sweepstake_Draw_Dt    DATE  ,
	Retail_Store_Id       VARCHAR  ,
	Transaction_Status_Cd  VARCHAR  ,
	Dw_Create_Ts          TIMESTAMP_TZ(9)  ,
	Dw_Last_Update_Ts     TIMESTAMP_TZ(9) ,
	Dw_Logical_Delete_Ind  BOOLEAN  ,
	Dw_Source_Create_Nm   VARCHAR(255)  ,
	Dw_Source_Update_Nm   VARCHAR(255)  ,
	Dw_Current_Version_Ind  BOOLEAN  ,
	PRIMARY KEY (Prize_Id, Program_Cd, Retail_Customer_Uuid, Redeem_Ts, Dw_First_Effective_Dt, Dw_Last_Effective_Dt)
);

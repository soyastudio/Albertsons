--liquibase formatted sql
--changeset SYSTEM: Alter_Table runOnChange:true splitStatements:false OBJECT_TYPE:TABLE

USE DATABASE <<EDM_DB_NAME_A>>;
USE SCHEMA DW_RETAIL_EXP;

ALTER TABLE F_PARTNER_ORDER_TRANSACTION
ADD COLUMN
Alcohol_Order_Ind BOOLEAN  DEFAULT 'FALSE' NOT NULL  COMMENT 'Indicator if the order contains an alcoholic product',
Snap_Order_Ind BOOLEAN  DEFAULT 'FALSE' NOT NULL  COMMENT 'Indicator if the order is a SNAP ordrer',
Dug_Order_Ind BOOLEAN  DEFAULT 'FALSE' NOT NULL  COMMENT 'Indicator if the order is a Drive Up and Go Order',
Deli_Order_Ind BOOLEAN  DEFAULT 'FALSE' NOT NULL  COMMENT 'Indicator if the order is a Deli Order',
FFC_Order_Ind BOOLEAN  DEFAULT 'FALSE' NOT NULL  COMMENT 'Indicator if the order is a Flash Delivery Order',
Own_Brand_Item_Order_Ind BOOLEAN  DEFAULT 'FALSE' NOT NULL  COMMENT 'Indicator if the order contains atleast one Own brand product';

--liquibase formatted sql
--changeset SYSTEM:CLICK_STREAM_PRODUCT_IMPRESSIONS_OT runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_DB_NAME_VIEWS>>;
use schema <<EDM_SCHEMA_VIEWS>>;

Alter view CLICK_STREAM_PRODUCT_IMPRESSIONS rename TO CLICK_STREAM_PRODUCT_IMPRESSIONS_ADOBE;
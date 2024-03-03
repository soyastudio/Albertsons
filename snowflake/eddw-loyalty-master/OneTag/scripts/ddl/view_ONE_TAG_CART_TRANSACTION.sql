--liquibase formatted sql
--changeset SYSTEM:ONE_TAG_CART_TRANSACTION runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_DB_NAME_VIEWS>>;
use schema <<EDM_SCHEMA_VIEWS>>;

CREATE OR REPLACE VIEW  ONE_TAG_CART_TRANSACTION
(	
    event_id
, 	event_subevent
,   event_time
, 	cart_transaction_subttlmpcarts_sellerid
, 	cart_transaction_subttlmpcarts_subtotal
) COPY GRANTS comment = 'VIEW FOR ONE_TAG_CART_TRANSACTION' 
AS
SELECT
 	event_id
, 	event_subevent
,   eventtime
, 	cart_transaction_subttlmpcarts_sellerid
, 	cart_transaction_subttlmpcarts_subtotal
FROM <<EDM_DB_R_NAME>>.<<EDM_SCHEMA_R_TARGET>>."ONE_TAG_CART_TRANSACTION";

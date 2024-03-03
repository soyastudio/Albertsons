Use database EDM_CONFIRMED_PRD;
Use schema DW_APPL;



// Create Stream 


CREATE OR REPLACE STREAM OFFER_REDEMPTION_REPORTING_DETAILED_SUMMARY_Stream_HEADER_SAVINGS
ON TABLE EDM_CONFIRMED_PRD.DW_C_RETAILSALE.EPE_TRANSACTION_HEADER_SAVINGS; 

CREATE OR REPLACE STREAM OFFER_REDEMPTION_REPORTING_DETAILED_SUMMARY_Stream_ITEM_SAVINGS
ON TABLE EDM_CONFIRMED_PRD.DW_C_RETAILSALE.EPE_TRANSACTION_ITEM_SAVINGS; 






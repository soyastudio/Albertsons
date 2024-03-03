--liquibase formatted sql
--changeset SYSTEM:SP_GETFRESHPASS_SUBSCRIPTION_TO_BIM_LOAD_EVENT runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETFRESHPASS_SUBSCRIPTION_TO_BIM_LOAD_EVENT(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_LOYAL VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$


		var src_wrk_tbl = SRC_WRK_TBL;
		var cnf_schema = C_LOYAL;
		var tgt_wrk_tbl = `${CNF_DB}.${C_STAGE}.Freshpass_Subscription_Event_WRK`;
		var tgt_tbl = `${CNF_DB}.${cnf_schema}.Freshpass_Subscription_Event`;

    // **************        Truncate and Reload the work table *****************

    var truncate_tgt_wrk_table = `DELETE from ${tgt_wrk_tbl}`;
try {
snowflake.execute ({ sqlText: truncate_tgt_wrk_table });
	}
    catch (err) {
        return `Deletion of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
				}


    // **************        Load for Freshpass_Subscription_Event table BEGIN *****************
    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.
	var create_tgt_wrk_table = `INSERT INTO ${tgt_wrk_tbl}
								SELECT DISTINCT
								 src.Retail_Customer_Uuid
								,src.Event_Id
								,src.Subscription_Plan_Integration_Id
								,src.Retail_Customer_Guid
								,src.Customer_Household_Id
								,src.Banner_Nm
								,src.Customer_Comment_Txt
								,src.Customer_Order_Cnt
								,src.Cycle_Delivery_Savings_Amt
								,src.Cycle_Perk_Savings_Amt
								,src.Cycle_Total_Savings_Amt
								,src.Event_Ts
								,src.Event_Type_Dsc
								,src.Life_Delivery_Savings_Amt
								,src.Life_Perk_Savings_Amt
								,src.Life_Total_Savings_Amt
								,src.Masked_Card_Nbr
								,src.Reenrolled_User_Ind
								,src.Subscription_Status_Dsc
								,src.Subscription_Status_Id
								,src.Source_System_Cd
								,src.Subscription_Payment_Dt
								,src.Tender_Type_Dsc
								,src.Plan_Cancellation_Reason_Dsc
								,src.Plan_Enrollment_Dt
								,src.Plan_Expiry_Dt
								,src.Plan_Renewal_Dt
								,src.Plan_Signup_Dt
								,src.Subscription_Tax_Amt
								,src.Total_Charges_Amt
								,src.filename
								,src.DW_LOGICAL_DELETE_IND								
                                ,CASE
								    WHEN (
										     tgt.Retail_Customer_Uuid IS NULL
										AND  tgt.Event_Id is NULL
										--AND  tgt.Subscription_Plan_Integration_Id is NULL
								         )
									THEN 'I'
									ELSE 'U'
								END AS DML_Type
								,CASE
									WHEN tgt.DW_First_Effective_dt = CURRENT_DATE
									THEN 1
									Else 0
								END as Sameday_chg_ind
								,src.Source_Channel_Nm,
								src.Cycle_Delivery_Order_Cnt,
								src.Cycle_Dug_Order_Cnt,
								src.Cycle_Rewards_Earned_Qty,
								src.Life_Delivery_Order_Cnt,
								src.Life_Dug_Order_Cnt,
								src.Life_Rewards_Earned_Qty,
								src.Life_Reward_Points_Qty,
								src.Cycle_Reward_Points_Qty,
								src.Payment_Transaction_Id
								FROM (
										SELECT
											Retail_Customer_Uuid
											,Event_Id
											,Subscription_Plan_Integration_Id
											,Retail_Customer_Guid
											,Customer_Household_Id
											,Banner_Nm
											,Customer_Comment_Txt
											,Customer_Order_Cnt
											,Cycle_Delivery_Savings_Amt
											,Cycle_Perk_Savings_Amt
											,Cycle_Total_Savings_Amt
											,Event_Ts
											,Event_Type_Dsc
											,Life_Delivery_Savings_Amt
											,Life_Perk_Savings_Amt
											,Life_Total_Savings_Amt
											,Masked_Card_Nbr
											,Reenrolled_User_Ind
											,Subscription_Status_Dsc
											,Subscription_Status_Id
											,Source_System_Cd
											,Subscription_Payment_Dt
											,Tender_Type_Dsc
											,Plan_Cancellation_Reason_Dsc
											,Plan_Enrollment_Dt
											,Plan_Expiry_Dt
											,Plan_Renewal_Dt
											,Plan_Signup_Dt
											,Subscription_Tax_Amt
											,Total_Charges_Amt
											,filename
											,DW_LOGICAL_DELETE_IND
											,Source_Channel_Nm,
											Cycle_Delivery_Order_Cnt,
											Cycle_Dug_Order_Cnt,
											Cycle_Rewards_Earned_Qty,
											Life_Delivery_Order_Cnt,
											Life_Dug_Order_Cnt,
											Life_Rewards_Earned_Qty,
											Life_Reward_Points_Qty,
											Cycle_Reward_Points_Qty,
											Payment_Transaction_Id
										FROM (
											   SELECT
												Retail_Customer_Uuid
												,Event_Id
												,Subscription_Plan_Integration_Id
												,Retail_Customer_Guid
												,Customer_Household_Id
												,Banner_Nm
												,Customer_Comment_Txt
												,Customer_Order_Cnt
												,Cycle_Delivery_Savings_Amt
												,Cycle_Perk_Savings_Amt
												,Cycle_Total_Savings_Amt
												,Event_Ts
												,Event_Type_Dsc
												,Life_Delivery_Savings_Amt
												,Life_Perk_Savings_Amt
												,Life_Total_Savings_Amt
												,Masked_Card_Nbr
												,Reenrolled_User_Ind
												,Subscription_Status_Dsc
												,Subscription_Status_Id
												,Source_System_Cd
												,Subscription_Payment_Dt
												,Tender_Type_Dsc
												,Plan_Cancellation_Reason_Dsc
												,Plan_Enrollment_Dt
												,Plan_Expiry_Dt
												,Plan_Renewal_Dt
												,Plan_Signup_Dt
												,Subscription_Tax_Amt
												,Total_Charges_Amt
												,filename
												,DW_CREATETS
												,Source_Channel_Nm,
												Cycle_Delivery_Order_Cnt,
												Cycle_Dug_Order_Cnt,
												Cycle_Rewards_Earned_Qty,
												Life_Delivery_Order_Cnt,
												Life_Dug_Order_Cnt,
												Life_Rewards_Earned_Qty,
												Life_Reward_Points_Qty,
												Cycle_Reward_Points_Qty,
												Payment_Transaction_Id
												,false as  DW_LOGICAL_DELETE_IND
												,Row_number() OVER (
												PARTITION BY Retail_Customer_Uuid,Event_Id,Subscription_Plan_Integration_Id
												order by(Event_Ts) DESC) as rn
											FROM(
													SELECT
													Retail_Customer_Uuid
													,Event_Id
													,E.Subscription_Plan_Integration_Id
													,Retail_Customer_Guid
													,Customer_Household_Id
													,Banner_Nm
													,Customer_Comment_Txt
													,Customer_Order_Cnt
													,Cycle_Delivery_Savings_Amt
													,Cycle_Perk_Savings_Amt
													,Cycle_Total_Savings_Amt
													,Event_Ts
													,Event_Type_Dsc
													,Life_Delivery_Savings_Amt
													,Life_Perk_Savings_Amt
													,Life_Total_Savings_Amt
													,Masked_Card_Nbr
													,Reenrolled_User_Ind
													,Subscription_Status_Dsc
													,CASE
													 WHEN upper(Subscription_Status_Dsc)='ACTIVE' THEN 1
													 WHEN upper(Subscription_Status_Dsc)='CANCELLED' THEN 2
													 WHEN upper(Subscription_Status_Dsc)='SUSPENDED' THEN 3
													 WHEN upper(Subscription_Status_Dsc)='PROCESSING' THEN 4
													 ELSE 5 END Subscription_Status_Id
													,Source_System_Cd
													,Subscription_Payment_Dt
													,Tender_Type_Dsc
													,Plan_Cancellation_Reason_Dsc
													,Plan_Enrollment_Dt
													,Plan_Expiry_Dt
													,Plan_Renewal_Dt
													,Plan_Signup_Dt
													,Subscription_Tax_Amt
													,Total_Charges_Amt
													,filename
													,DW_CREATETS
													,Source_Channel_Nm,
													Cycle_Delivery_Order_Cnt,
													Cycle_Dug_Order_Cnt,
													Cycle_Rewards_Earned_Qty,
													Life_Delivery_Order_Cnt,
													Life_Dug_Order_Cnt,
													Life_Rewards_Earned_Qty,
													Life_Reward_Points_Qty,
													Cycle_Reward_Points_Qty,
													Payment_Transaction_Id
													FROM
													 (
													  (
													  SELECT
														customer_Uuid as Retail_Customer_Uuid
														,eventId as Event_Id
														,gid as Retail_Customer_Guid
														,hhid as Customer_Household_Id
														,banner as Banner_Nm
														,usercomment as Customer_Comment_Txt
														,orderCount as Customer_Order_Cnt
														,CycleSavings_DeliverySavings as Cycle_Delivery_Savings_Amt
														,CycleSavings_PerkSavings as Cycle_Perk_Savings_Amt
														,CycleSavings_TotalSavings as Cycle_Total_Savings_Amt
														,eventTime as Event_Ts
														,eventType as Event_Type_Dsc
														,LifeSavings_DeliverySavings as Life_Delivery_Savings_Amt
														,LifeSavings_PerkSavings as Life_Perk_Savings_Amt
														,LifeSavings_TotalSavings as Life_Total_Savings_Amt
														,maskedNumber as Masked_Card_Nbr
														,TRY_TO_BOOLEAN(reenrolledUser) as Reenrolled_User_Ind
														,status as Subscription_Status_Dsc
														,source as Source_System_Cd
														,to_date(paymentDate) as Subscription_Payment_Dt
														,Type as Tender_Type_Dsc
														,cancellationReason as Plan_Cancellation_Reason_Dsc
														,to_date(enrollmentDate) as Plan_Enrollment_Dt
														,to_date(expiryDate) as Plan_Expiry_Dt
														,to_date(renewalDate) as Plan_Renewal_Dt
														,to_date(signupDate) as Plan_Signup_Dt
														,taxAmount  as Subscription_Tax_Amt
														,totalCharges as Total_Charges_Amt
														,subscriptioncode
														,subscriptiontype
														,discounttype
														,Subscription_plan_type
														,filename
														,DW_CREATETS
														,Source_Channel_Nm,
														Cycle_Delivery_Order_Cnt,
														Cycle_Dug_Order_Cnt,
														Cycle_Rewards_Earned_Qty,
														Life_Delivery_Order_Cnt,
														Life_Dug_Order_Cnt,
														Life_Rewards_Earned_Qty,
														Life_Reward_Points_Qty,
														Cycle_Reward_Points_Qty,
														paymentTransactionId as Payment_Transaction_Id
														FROM ${src_wrk_tbl}
														where customer_Uuid is not null
														      and eventId is not null
													) S
													 LEFT JOIN
													 (
													 SELECT DISTINCT Subscription_Plan_Integration_Id,Subscription_Plan_Type_Nm,Subscription_Plan_Cd,Subscription_Type_Nm,Discount_Type_Dsc
													 FROM ${CNF_DB}.${cnf_schema}.Freshpass_Subscription_Plan WHERE DW_CURRENT_VERSION_IND=TRUE AND DW_LOGICAL_DELETE_IND=FALSE
													 ) E
													 ON NVL(S.subscriptioncode,'-1')=NVL(E.Subscription_Plan_Cd,'-1')
													 AND NVL(S.subscriptiontype,'-1')=NVL(E.Subscription_Type_Nm,'-1')
													 AND NVL(S.discounttype,'-1')=NVL(E.Discount_Type_Dsc,'-1')
													 AND NVL(S.Subscription_plan_type,'-1')=NVL(E.Subscription_Plan_Type_Nm,'-1')
													)
										       )
									)  where rn=1 
								) src
									LEFT JOIN
									(
											SELECT  DISTINCT
													Retail_Customer_Uuid
													,Event_Id
													,Subscription_Plan_Integration_Id
													,Retail_Customer_Guid
													,Customer_Household_Id
													,Banner_Nm
													,Customer_Comment_Txt
													,Customer_Order_Cnt
													,Cycle_Delivery_Savings_Amt
													,Cycle_Perk_Savings_Amt
													,Cycle_Total_Savings_Amt
													,Event_Ts
													,Event_Type_Dsc
													,Life_Delivery_Savings_Amt
													,Life_Perk_Savings_Amt
													,Life_Total_Savings_Amt
													,Masked_Card_Nbr
													,Reenrolled_User_Ind
													,Subscription_Status_Dsc
													,Subscription_Status_Id
													,Source_System_Cd
													,Subscription_Payment_Dt
													,Tender_Type_Dsc
													,Plan_Cancellation_Reason_Dsc
													,Plan_Enrollment_Dt
													,Plan_Expiry_Dt
													,Plan_Renewal_Dt
													,Plan_Signup_Dt
													,Subscription_Tax_Amt
													,Total_Charges_Amt
													,DW_First_Effective_dt
													,DW_LOGICAL_DELETE_IND
													,Source_Channel_Nm,
													Cycle_Delivery_Order_Cnt,
													Cycle_Dug_Order_Cnt,
													Cycle_Rewards_Earned_Qty,
													Life_Delivery_Order_Cnt,
													Life_Dug_Order_Cnt,
													Life_Rewards_Earned_Qty,
													Life_Reward_Points_Qty,
													Cycle_Reward_Points_Qty,
													Payment_Transaction_Id
									FROM ${tgt_tbl} tgt
									WHERE DW_CURRENT_VERSION_IND = TRUE
									)as tgt
									ON
									NVL(src.Retail_Customer_Uuid ,'-1') = NVL(tgt.Retail_Customer_Uuid ,'-1')
									AND  NVL(src.Event_Id,'-1') = NVL(tgt.Event_Id ,'-1')
									--AND  NVL(src.Subscription_Plan_Integration_Id,'-1') = NVL(tgt.Subscription_Plan_Integration_Id,'-1')
									WHERE  (
									tgt.Retail_Customer_Uuid IS  NULL
									AND tgt.Event_Id is  NULL
									--AND tgt.Subscription_Plan_Integration_Id is NULL
									 )
									OR
									(
									 NVL(src.Subscription_Plan_Integration_Id,'-1') <> NVL(tgt.Subscription_Plan_Integration_Id,'-1')
									 OR NVL(src.Retail_Customer_Guid,'-1') <> NVL(tgt.Retail_Customer_Guid,'-1')
									 OR NVL(src.Customer_Household_Id,'-1') <> NVL(tgt.Customer_Household_Id,'-1')
									 OR NVL(src.Banner_Nm,'-1') <> NVL(tgt.Banner_Nm,'-1')
									 OR NVL(src.Customer_Comment_Txt,'-1') <>NVL(tgt.Customer_Comment_Txt,'-1')
									 OR NVL(src.Customer_Order_Cnt,'-1') <> NVL(tgt.Customer_Order_Cnt,'-1')
									 OR NVL(src.Cycle_Delivery_Savings_Amt,'-1') <> NVL(tgt.Cycle_Delivery_Savings_Amt,'-1')
									 OR NVL(src.Cycle_Perk_Savings_Amt,'-1') <> NVL(tgt.Cycle_Perk_Savings_Amt,'-1')
									 OR NVL(src.Cycle_Total_Savings_Amt,'-1') <> NVL(tgt.Cycle_Total_Savings_Amt,'-1')
									 OR NVL(src.Event_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Event_Ts,'9999-12-31 00:00:00.000')
									 OR NVL(src.Event_Type_Dsc,'-1') <> NVL(tgt.Event_Type_Dsc,'-1')
									 OR NVL(src.Life_Delivery_Savings_Amt,'-1') <> NVL(tgt.Life_Delivery_Savings_Amt,'-1')
									 OR NVL(src.Life_Perk_Savings_Amt,'-1') <> NVL(tgt.Life_Perk_Savings_Amt,'-1')
									 OR NVL(src.Life_Total_Savings_Amt,'-1') <> NVL(tgt.Life_Total_Savings_Amt,'-1')
									 OR NVL(src.Masked_Card_Nbr,'-1') <> NVL(tgt.Masked_Card_Nbr,'-1')
									 OR NVL(to_boolean(src.Reenrolled_User_Ind),-1) <> NVL(tgt.Reenrolled_User_Ind,-1)
									 OR NVL(src.Subscription_Status_Dsc,'-1') <> NVL(tgt.Subscription_Status_Dsc,'-1')
									 OR NVL(src.Subscription_Status_Id ,'-1') <> NVL(tgt.Subscription_Status_Id ,'-1')
									 OR NVL(src.Source_System_Cd,'-1') <> NVL(tgt.Source_System_Cd,'-1')
									 OR NVL(to_date(src.Subscription_Payment_Dt),'9999-12-31') <> NVL(tgt.Subscription_Payment_Dt,'9999-12-31')
									 OR NVL(src.Tender_Type_Dsc,'-1') <> NVL(tgt.Tender_Type_Dsc,'-1')
									 OR NVL(src.Plan_Cancellation_Reason_Dsc,'-1') <> NVL(tgt.Plan_Cancellation_Reason_Dsc,'-1')
									 OR NVL(to_date(src.Plan_Enrollment_Dt),'9999-12-31') <>NVL(tgt.Plan_Enrollment_Dt,'9999-12-31')
									 OR NVL(to_date(src.Plan_Expiry_Dt),'9999-12-31') <> NVL(tgt.Plan_Expiry_Dt,'9999-12-31')
									 OR NVL(to_date(src.Plan_Renewal_Dt),'9999-12-31') <> NVL(tgt.Plan_Renewal_Dt,'9999-12-31')
									 OR NVL(to_date(src.Plan_Signup_Dt),'9999-12-31') <> NVL(tgt.Plan_Signup_Dt,'9999-12-31')
									 OR NVL(src.Subscription_Tax_Amt,'-1') <> NVL(tgt.Subscription_Tax_Amt,'-1')
									 OR NVL(src.Total_Charges_Amt,'-1') <> NVL(tgt.Total_Charges_Amt,'-1')
									 OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
									 OR NVL(src.Source_Channel_Nm,'-1') <> NVL(tgt.Source_Channel_Nm,'-1')
									 OR NVL(src.Cycle_Delivery_Order_Cnt,'-1') <> NVL(tgt.Cycle_Delivery_Order_Cnt,'-1')
									 OR NVL(src.Cycle_Dug_Order_Cnt,'-1') <> NVL(tgt.Cycle_Dug_Order_Cnt,'-1')
									 OR NVL(src.Cycle_Rewards_Earned_Qty,'-1') <> NVL(tgt.Cycle_Rewards_Earned_Qty,'-1')
									 OR NVL(src.Life_Delivery_Order_Cnt,'-1') <> NVL(tgt.Life_Delivery_Order_Cnt,'-1')
									 OR NVL(src.Life_Dug_Order_Cnt,'-1') <> NVL(tgt.Life_Dug_Order_Cnt,'-1')
									 OR NVL(src.Life_Rewards_Earned_Qty,'-1') <> NVL(tgt.Life_Rewards_Earned_Qty,'-1')
									 OR NVL(src.Life_Reward_Points_Qty,'-1') <> NVL(tgt.Life_Reward_Points_Qty,'-1')
									 OR NVL(src.Cycle_Reward_Points_Qty,'-1') <> NVL(tgt.Cycle_Reward_Points_Qty,'-1')
									 OR NVL(src.Payment_Transaction_Id,'-1') <> NVL(tgt.Payment_Transaction_Id,'-1')
									 )`;

try {
snowflake.execute ({ sqlText: create_tgt_wrk_table });
	}
    catch (err) {
        return `Creation of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
				}


// Transaction for Updates, Insert begins
    var sql_begin = "BEGIN"
// SCD Type2 - Processing Different day updates
var sql_updates = `UPDATE ${tgt_tbl} as tgt
					SET
					DW_Last_Effective_dt = CURRENT_DATE - 1,
					DW_CURRENT_VERSION_IND = FALSE,
					//DW_LOGICAL_DELETE_IND=TRUE,
					DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
					DW_SOURCE_UPDATE_NM = filename
					FROM (
							SELECT
								 Retail_Customer_Uuid
								,Event_Id
								,Subscription_Plan_Integration_Id
								,Retail_Customer_Guid
								,Customer_Household_Id
								,Banner_Nm
								,Customer_Comment_Txt
								,Customer_Order_Cnt
								,Cycle_Delivery_Savings_Amt
								,Cycle_Perk_Savings_Amt
								,Cycle_Total_Savings_Amt
								,Event_Ts
								,Event_Type_Dsc
								,Life_Delivery_Savings_Amt
								,Life_Perk_Savings_Amt
								,Life_Total_Savings_Amt
								,Masked_Card_Nbr
								,Reenrolled_User_Ind
								,Subscription_Status_Dsc
								,Subscription_Status_Id
								,Source_System_Cd
								,Subscription_Payment_Dt
								,Tender_Type_Dsc
								,Plan_Cancellation_Reason_Dsc
								,Plan_Enrollment_Dt
								,Plan_Expiry_Dt
								,Plan_Renewal_Dt
								,Plan_Signup_Dt
								,Subscription_Tax_Amt
								,Total_Charges_Amt
								,filename
								,Source_Channel_Nm,
								Cycle_Delivery_Order_Cnt,
								Cycle_Dug_Order_Cnt,
								Cycle_Rewards_Earned_Qty,
								Life_Delivery_Order_Cnt,
								Life_Dug_Order_Cnt,
								Life_Rewards_Earned_Qty,
								Life_Reward_Points_Qty,
								Cycle_Reward_Points_Qty,
								Payment_Transaction_Id
							FROM ${tgt_wrk_tbl}
							WHERE
							DML_Type = 'U'
							--AND Sameday_chg_ind = 0
					) src
					WHERE
					NVL(src.Retail_Customer_Uuid,'-1') = NVL(tgt.Retail_Customer_Uuid,'-1')
					AND NVL(src.Event_Id,'-1') = NVL(tgt.Event_Id,'-1')
					--AND NVL(src.Subscription_Plan_Integration_Id,'-1')= NVL(tgt.Subscription_Plan_Integration_Id,'-1')
					AND tgt.DW_CURRENT_VERSION_IND = TRUE`;

 /*// SCD Type1 - Processing Sameday updates
var sql_sameday = `UPDATE ${tgt_tbl} as tgt
					SET Retail_Customer_Uuid = src.Retail_Customer_Uuid,
					Event_Id = src.Event_Id,
					Subscription_Plan_Integration_Id = src.Subscription_Plan_Integration_Id,
					Retail_Customer_Guid=src.Retail_Customer_Guid,
					Banner_Nm=src.Banner_Nm,
					Customer_Household_Id = src.Customer_Household_Id,
					Customer_Comment_Txt = src.Customer_Comment_Txt,
					Customer_Order_Cnt=src.Customer_Order_Cnt,
					Cycle_Delivery_Savings_Amt = src.Cycle_Delivery_Savings_Amt,
					Cycle_Perk_Savings_Amt = src.Cycle_Perk_Savings_Amt,
					Cycle_Total_Savings_Amt = src.Cycle_Total_Savings_Amt,
					Event_Ts = src.Event_Ts,
					Event_Type_Dsc = src.Event_Type_Dsc,
					Life_Delivery_Savings_Amt=src.Life_Delivery_Savings_Amt,
					Life_Perk_Savings_Amt = src.Life_Perk_Savings_Amt,
					Life_Total_Savings_Amt = src.Life_Total_Savings_Amt,
					Masked_Card_Nbr=src.Masked_Card_Nbr,
					Reenrolled_User_Ind = src.Reenrolled_User_Ind,
					Subscription_Status_Dsc = src.Subscription_Status_Dsc,
					Subscription_Status_Id = src.Subscription_Status_Id,
					Source_System_Cd = src.Source_System_Cd,
					Subscription_Payment_Dt = src.Subscription_Payment_Dt,
					Tender_Type_Dsc=src.Tender_Type_Dsc,
					Plan_Cancellation_Reason_Dsc = src.Plan_Cancellation_Reason_Dsc,
					Plan_Enrollment_Dt = src.Plan_Enrollment_Dt,
					Plan_Expiry_Dt=src.Plan_Expiry_Dt,
					Plan_Renewal_Dt = src.Plan_Renewal_Dt,
					Plan_Signup_Dt = src.Plan_Signup_Dt,
					Subscription_Tax_Amt=src.Subscription_Tax_Amt,
					Total_Charges_Amt=src.Total_Charges_Amt,
					DW_Logical_delete_ind = src.DW_Logical_delete_ind,
					DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
					DW_SOURCE_UPDATE_NM = filename,
					Source_Channel_Nm = Source_Channel_Nm,
					Cycle_Delivery_Order_Cnt = Cycle_Delivery_Order_Cnt,
					Cycle_Dug_Order_Cnt = Cycle_Dug_Order_Cnt,
					Cycle_Rewards_Earned_Qty = Cycle_Rewards_Earned_Qty,
					Life_Delivery_Order_Cnt = Life_Delivery_Order_Cnt,
					Life_Dug_Order_Cnt = Life_Dug_Order_Cnt,
					Life_Rewards_Earned_Qty = Life_Rewards_Earned_Qty,
					Life_Reward_Points_Qty = Life_Reward_Points_Qty,
					Cycle_Reward_Points_Qty = Cycle_Reward_Points_Qty
					FROM (
							SELECT
								Retail_Customer_Uuid
								,Event_Id
								,Subscription_Plan_Integration_Id
								,Retail_Customer_Guid
								,Customer_Household_Id
								,Banner_Nm
								,Customer_Comment_Txt
								,Customer_Order_Cnt
								,Cycle_Delivery_Savings_Amt
								,Cycle_Perk_Savings_Amt
								,Cycle_Total_Savings_Amt
								,Event_Ts
								,Event_Type_Dsc
								,Life_Delivery_Savings_Amt
								,Life_Perk_Savings_Amt
								,Life_Total_Savings_Amt
								,Masked_Card_Nbr
								,Reenrolled_User_Ind
								,Subscription_Status_Dsc
								,Subscription_Status_Id
								,Source_System_Cd
								,Subscription_Payment_Dt
								,Tender_Type_Dsc
								,Plan_Cancellation_Reason_Dsc
								,Plan_Enrollment_Dt
								,Plan_Expiry_Dt
								,Plan_Renewal_Dt
								,Plan_Signup_Dt
								,Subscription_Tax_Amt
								,Total_Charges_Amt
								,filename
								,DW_Logical_delete_ind
								,Source_Channel_Nm,
								Cycle_Delivery_Order_Cnt,
								Cycle_Dug_Order_Cnt,
								Cycle_Rewards_Earned_Qty,
								Life_Delivery_Order_Cnt,
								Life_Dug_Order_Cnt,
								Life_Rewards_Earned_Qty,
								Life_Reward_Points_Qty,
								Cycle_Reward_Points_Qty
							FROM ${tgt_wrk_tbl}
							WHERE
							DML_Type = 'U'
							AND Sameday_chg_ind = 1
						) src
						WHERE
					NVL(src.Retail_Customer_Uuid,'-1') = NVL(tgt.Retail_Customer_Uuid,'-1')
					AND NVL(src.Event_Id,'-1') = NVL(tgt.Event_Id,'-1')
					AND NVL(src.Subscription_Plan_Integration_Id,'-1')= NVL(tgt.Subscription_Plan_Integration_Id,'-1')
					AND tgt.DW_CURRENT_VERSION_IND = TRUE`;*/
// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl}
					(
					Retail_Customer_Uuid
					,Event_Id
					,Subscription_Plan_Integration_Id
					,Dw_First_Effective_Dt
					,Dw_Last_Effective_Dt
					,Retail_Customer_Guid
					,Customer_Household_Id
					,Banner_Nm
					,Customer_Comment_Txt
					,Customer_Order_Cnt
					,Cycle_Delivery_Savings_Amt
					,Cycle_Perk_Savings_Amt
					,Cycle_Total_Savings_Amt
					,Event_Ts
					,Event_Type_Dsc
					,Life_Delivery_Savings_Amt
					,Life_Perk_Savings_Amt
					,Life_Total_Savings_Amt
					,Masked_Card_Nbr
					,Reenrolled_User_Ind
					,Subscription_Status_Dsc
					,Subscription_Status_Id
					,Source_System_Cd
					,Subscription_Payment_Dt
					,Tender_Type_Dsc
					,Plan_Cancellation_Reason_Dsc
					,Plan_Enrollment_Dt
					,Plan_Expiry_Dt
					,Plan_Renewal_Dt
					,Plan_Signup_Dt
					,Subscription_Tax_Amt
					,Total_Charges_Amt
					,DW_CREATE_TS
					,DW_LOGICAL_DELETE_IND
					,DW_SOURCE_CREATE_NM
					,DW_CURRENT_VERSION_IND
					,Source_Channel_Nm
					,Cycle_Delivery_Order_Cnt,
					Cycle_Dug_Order_Cnt,
					Cycle_Rewards_Earned_Qty,
					Life_Delivery_Order_Cnt,
					Life_Dug_Order_Cnt,
					Life_Rewards_Earned_Qty,
					Life_Reward_Points_Qty,
					Cycle_Reward_Points_Qty,
					Payment_Transaction_Id
					)
					SELECT
					Retail_Customer_Uuid
					,Event_Id
					,Subscription_Plan_Integration_Id
					,CURRENT_DATE
					,'31-DEC-9999'
					,Retail_Customer_Guid
					,Customer_Household_Id
					,Banner_Nm
					,Customer_Comment_Txt
					,Customer_Order_Cnt
					,Cycle_Delivery_Savings_Amt
					,Cycle_Perk_Savings_Amt
					,Cycle_Total_Savings_Amt
					,Event_Ts
					,Event_Type_Dsc
					,Life_Delivery_Savings_Amt
					,Life_Perk_Savings_Amt
					,Life_Total_Savings_Amt
					,Masked_Card_Nbr
					,Reenrolled_User_Ind
					,Subscription_Status_Dsc
					,Subscription_Status_Id
					,Source_System_Cd
					,Subscription_Payment_Dt
					,Tender_Type_Dsc
					,Plan_Cancellation_Reason_Dsc
					,Plan_Enrollment_Dt
					,Plan_Expiry_Dt
					,Plan_Renewal_Dt
					,Plan_Signup_Dt
					,Subscription_Tax_Amt
					,Total_Charges_Amt
					,CURRENT_TIMESTAMP
					,DW_LOGICAL_DELETE_IND
					,filename
					,TRUE
					,Source_Channel_Nm
					,Cycle_Delivery_Order_Cnt,
					Cycle_Dug_Order_Cnt,
					Cycle_Rewards_Earned_Qty,
					Life_Delivery_Order_Cnt,
					Life_Dug_Order_Cnt,
					Life_Rewards_Earned_Qty,
					Life_Reward_Points_Qty,
					Cycle_Reward_Points_Qty,
					Payment_Transaction_Id
					FROM ${tgt_wrk_tbl}
					WHERE
					//Sameday_chg_ind = 0 and
					Subscription_Plan_Integration_Id is not null`;


var sql_commit = "COMMIT";
var sql_rollback = "ROLLBACK";

try {
        snowflake.execute ({ sqlText: sql_begin });
        snowflake.execute ({ sqlText: sql_updates });
        //snowflake.execute ({ sqlText: sql_sameday });
        snowflake.execute ({ sqlText: sql_inserts });
        snowflake.execute ({ sqlText: sql_commit });
	} catch (err) {
        snowflake.execute ({ sqlText: sql_rollback });
		return `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
					}

// ************** Load for SP_GetFreshpass_Subscription_To_BIM_load_Event Table ENDs *****************


$$;

--liquibase formatted sql
--changeset SYSTEM:SP_GETOFFERREQUEST_TO_ANALYTICAL_LOAD_FACT_OFFER_REQUEST runOnChange:true splitStatements:false OBJECT_TYPE:SP

use database EDM_ANALYTICS_PRD;
use schema dw_appl;

CREATE OR REPLACE PROCEDURE SP_GETOFFERREQUEST_TO_ANALYTICAL_LOAD_FACT_OFFER_REQUEST
(SRC_WRK_TBL VARCHAR, ANL_DB VARCHAR, ANL_SCHEMA VARCHAR, WRK_SCHEMA VARCHAR, CNF_DB VARCHAR, CNF_SCHEMA VARCHAR, LOC_SCHEMA VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

		// **************        Load for Fact_Offer_Request table BEGIN *****************
		var src_wrk_tbl = SRC_WRK_TBL;
		var anl_db = ANL_DB;
		var anl_schema = ANL_SCHEMA;
		var wrk_schema = WRK_SCHEMA;
		//var rfn_db = RFN_DB;
		//var rfn_schema = RFN_SCHEMA;
		var cnf_db = CNF_DB;
		var cnf_schema = CNF_SCHEMA;
		var loc_schema = LOC_SCHEMA;
		
		var tgt_wrk_tbl =  anl_db + "." + wrk_schema + ".Fact_Offer_Request_WRK";
		var tgt_tbl = anl_db + "." + anl_schema + ".Fact_Offer_Request";
		var dim_store_grp_tbl = anl_db + "." + anl_schema + ".Dim_Store_Group";
		var dim_offer_type_tbl = anl_db + "." + anl_schema + ".Dim_Offer_Type";
		var dim_rog_tbl = anl_db + "." + anl_schema + ".DIM_ROG";
		var dim_discount_tbl = anl_db + "." + anl_schema + ".DIM_DISCOUNT";
		var offer_flat_tbl = cnf_db + "." + cnf_schema + ".Offeroms_flat";
		var store_grp_flat_tbl = cnf_db + "." + cnf_schema + ".storegroup_flat";
		var rog_upc_tbl = cnf_db + "." + cnf_schema + ".RETAIL_ORDER_GROUP_UPC";
		var facility_lkp_tbl = cnf_db + "." + loc_schema + ".FACILITY";
		var rog_div_lkp_tbl = cnf_db + "." + loc_schema + ".retail_order_group_division";
		var rs_lkp_tbl = cnf_db + "." + loc_schema + ".retail_store";

		// Prepare work temp table for fact table with all the lookups
		var cr_src_tmp_wrk_tbl = `CREATE OR REPLACE TRANSIENT TABLE ` + tgt_wrk_tbl + ` AS
				with offerrequest as
				(
				select 
				OfferRequestDsc
				,BenefitValueType_Code
				,rewardQty
				,BrandInfoTxt
				,DeliveryChannelTypeCd
				,CreateTs
				,UserTypeCd
				,FirstNm
				,LastNm
				,DepartmentNm
				,Discount_DiscountId
				,discountAmt
				,case when BenefitValueType_Code = 'NO_DISCOUNT' then 'No Discount' else BenefitValueType_Description end as BenefitValueType_Description
				,GiftCardInd
				,GroupCd
				,AdvertisementType_Code
				,LimitType_LimitAmt
				,TagDsc
				,tagAmt
				,tagnbr
				,UpdateTs
				,IncludedProductgroupNm
				,MinimumPurchaseAmt
				,BilledInd
				,BillingOptionType_Code
				,NOPAEndDt
				,NOPAStartDt
				,OfferEndDt
				,ReferenceOfferId
				,RESTRICTIONTYPE_CODE
				,OfferRequestId
				,OfferStartDt
				,Status_Description
				,ProtoType_Code
				,LimitType_LimitQty
				,LimitType_LimitWt
				,TriggerId
				,LoyaltyPgmTagInd
				,PrizeItemQty
				,PromotionProgramType_Code
				,TierLevelAmt
				,OfferRequestStatus_Description
				,OFFERREQUESTSTATUSTYPECD
				,CustomerSegmentInfoTxt
				,StoreGroupId
				,VendorPromotionId
				,groupnm
				,subgroupnm
				,UOMDsc
				,DiscountUptoQty
				,StoreGroupType_Code
				,ATTACHEDOFFERTYPE_DISPLAYORDERNBR
				,AttachedOfferType_StoreGroupVersionId
				,ProductGroup_ProductGroupId
				,ProductGroup_ProductGroupNm
				,ActionTypeCd
				,IncludedProductGroupid
				,offereffectiveday_qualifier
				,StartTm
				,EndTm
				,offereffectiveday
				,OfferRequestTypeCd
				,AirmileTierNm
				,AirmilePointQty
				,DISTINCTID
				,IMAGETYPECD
				,USAGELIMITNBR
				,USAGELIMITPERIODNBR
				from
				(
				select 
				OfferRequestDsc
				,BenefitValueType_Code
				,rewardQty
				,BrandInfoTxt
				,DeliveryChannelTypeCd
				,CreateTs
				,UserTypeCd
				,FirstNm
				,LastNm
				,DepartmentNm
				,Discount_DiscountId
				,discountAmt
				,BenefitValueType_Description
				,GiftCardInd
				,GroupCd
				,AdvertisementType_Code
				,LimitType_LimitAmt
				,TagDsc
				,tagAmt
				,tagnbr
				,UpdateTs
				,IncludedProductgroupNm
				,MinimumPurchaseAmt
				,BilledInd
				,BillingOptionType_Code
				,NOPAEndDt
				,NOPAStartDt
				,OfferEndDt
				,ReferenceOfferId
				,RESTRICTIONTYPE_CODE
				,OfferRequestId
				,OfferStartDt
				,Status_Description
				,ProtoType_Code
				,LimitType_LimitQty
				,LimitType_LimitWt
				,TriggerId
				,LoyaltyPgmTagInd
				,PrizeItemQty
				,PromotionProgramType_Code
				,TierLevelAmt
				,OfferRequestStatus_Description
				,OFFERREQUESTSTATUSTYPECD
				,CustomerSegmentInfoTxt
				,StoreGroupId
				,VendorPromotionId
				,groupnm
				,subgroupnm
				,UOMDsc
				,DiscountUptoQty
				,StoreGroupType_Code
				,ATTACHEDOFFERTYPE_DISPLAYORDERNBR
				,AttachedOfferType_StoreGroupVersionId
				,ProductGroup_ProductGroupId
				,ProductGroup_ProductGroupNm
				,IncludedProductGroupid
				,ActionTypeCd
				,offereffectiveday_qualifier
				,StartTm
				,EndTm
				,offereffectiveday 
				,OfferRequestTypeCd
				,AirmileTierNm
				,AirmilePointQty
				,DISTINCTID
				,IMAGETYPECD
				,USAGELIMITNBR
				,USAGELIMITPERIODNBR
				,row_number() over ( PARTITION BY OfferRequestId, ReferenceOfferId, 
					StoreGroupId,
					ProductGroup_ProductGroupId ,
					StoreGroupType_Code,
					usertypecd,
					discountAmt,
					offereffectiveday_qualifier,
					TierLeveLAmt,
					OfferRequestStatusTypeCd,
					AirMilePointQty,
					BenefitValueType_Description
					 ORDER BY to_timestamp_ntz(updatets) desc) as rn
				from ` + src_wrk_tbl + `
				where OfferRequestId is not NULL
				AND (AttachedOfferType_StoreGroupVersionId = AttachedOffer_StoreGroupVersionId OR 
				(AttachedOfferType_StoreGroupVersionId is NULL AND Upper(actiontypecd) = 'DELETE')
				OR OfferRequestStatus_Description = 'Canceled')
				AND (OfferRequestStatus_Description <> 'NOT AVAILABLE' OR (NVL(OfferRequestStatus_Description,'-1') <> 'NOT AVAILABLE' AND Upper(actiontypecd) = 'DELETE'))
				)
				where rn = 1
				)
				,nopa_numbers as
				(
					select 
					distinct a.offerrequestid
					,regexp_replace(b.VendorPromotionId, '( ){1,}',', ') as Nopa_number 
					from offerrequest a
					JOIN ` + src_wrk_tbl + ` b ON a.offerrequestid =b.offerrequestid  
					and to_timestamp_ntz(a.updatets) = to_timestamp_ntz(b.updatets)
					 where  a.vendorPromotionId is not NULL
		
				)
				, groups as
				(
					select 
					distinct offerrequestid
					,groupnm
					,groupcd
					,subgroupnm 
					from offerrequest
				)
				,str_grp as
				(
				  select distinct 
				  offerrequestid
				  ,REFERENCEOFFERID
				  ,AttachedOfferType_DisplayOrderNbr
				  ,StoreGroupType_Code
				  ,case when REFERENCEOFFERID like '%-ND' then storegroupnd else storegroupd end as storegroupid
                   from 
				      (
					       select distinct 
				            offerrequestid
				            ,REFERENCEOFFERID
				            ,AttachedOfferType_DisplayOrderNbr
				            ,StoreGroupType_Code
				            ,storegroupid as storegroupnd
							,null as storegroupd
							from offerrequest
							where StoreGroupType_Code='NonDigital'
							union all
		                    select distinct 
				            offerrequestid
				            ,REFERENCEOFFERID
				            ,AttachedOfferType_DisplayOrderNbr
				            ,StoreGroupType_Code
							,null as storegroupnd
				            ,storegroupid as storegroupd
							from offerrequest
							where StoreGroupType_Code in ('Digital','J4U')
					    ) grps
						where storegroupid is not null
                )
				, div_rog as
				(
					select 
					distinct
					offerrequestid
					,REFERENCEOFFERID
					,rog_div.division_id as Division_Id
					,StoreGroupTypeCd
					,sg.storegroupid
					,sg.StoreGroupVersion
					,store_id
					,f.facility_integration_id
					,rs.rog_id as Rog_Id
					,ATTACHEDOFFERTYPE_DISPLAYORDERNBR as display_nbr
					from
					(
					select 
					distinct
					offerrequestid
					,REFERENCEOFFERID
					,ATTACHEDOFFERTYPE_DISPLAYORDERNBR
					,AttachedOfferType_StoreGroupVersionId as StoreGroupVersion
					,StoreGroupType_Code as StoreGroupTypeCd
					,StoreGroupId
					from offerrequest
					where PRODUCTGROUP_PRODUCTGROUPID is not null
					or (PRODUCTGROUP_PRODUCTGROUPID is null and UPPER(ProtoType_Code) in ('INSTANT_WIN','CUSTOM'))
					) sg
					INNER JOIN
					(select payload_id as sg_id
					,payload_stores as store_id
					from ` + store_grp_flat_tbl + `) sg_flat
					ON sg.storegroupid = sg_flat.sg_id
					INNER JOIN
					` + facility_lkp_tbl + ` as f
					ON  LPAD(sg_flat.store_id,4,0) = LPAD(f.facility_nbr ,4,0)
					INNER JOIN
					` + rs_lkp_tbl + ` as rs
					ON LPAD(f.facility_nbr ,4,0) = LPAD(rs.facility_nbr, 4, 0)
					INNER JOIN 
					` + rog_div_lkp_tbl + ` as rog_div
					ON rs.rog_id = rog_div.rog_id
					where f.dw_current_version_ind = TRUE 
					and f.dw_logical_Delete_ind = FALSE
					-- this condition is removed due to more reliable on corporation_id rather company_id
					-- and f.COMPANY_ID = case when LPAD(f.facility_nbr ,4,0) in ('0073') and f.corporation_id = '001' then null else 1101 end
					and f.CORPORATION_ID ='001'
					and rs.dw_current_version_ind = TRUE 
					and rs.dw_logical_Delete_ind = FALSE
					and rog_div.dw_current_version_ind = TRUE 
					and rog_div.dw_logical_Delete_ind = FALSE
					and rs.rog_id in  ('SDEN','SHGN','AIMT','AJWL','ACME','AKBA','SWMA','SNCA','SHAW','APHO','VLAS','SPRT'
					,'SSEA','SSPK','SACG','ASHA','AVMT','PSOC','VSOC','ADAL','RDAL','RHOU','SPHO','UNTD')
				)
				select distinct
				Additional_Detail_Dsc
				,Amount
				,Brand_Size
				,BUY_GET
				,Channel_Type_Cd 
				,Created_Date
				,Created_By
				,Qualification_Day_Time
				,Department_Nm
				,Digital_Builder
				,Digital_Store_Group_List
				,Discount_Id 
				,Discount_Amt
				,Discount_Type_Dsc
				,Division_Offer_Request_List 
				,Division_Store_Tag_Upc_List  
				,Dollar_Limit
				,Gift_Card_Ind
				,Group_Cd
				,Group_offer_request_list 
				,In_Ad
				,Item_Limit
				,J4U_Store_Group_List
				,J4U_Tag_Comment
				,J4U_Tag_Display_Price 
				,Last_Modified_By
				,Last_Modified_Dt
				,Min_Amount_To_Buy
				,Min_Qty_To_Buy
				,Min_Purchase
				,Non_Digital_Builder
				,Non_Digital_Store_Group_List
				,Nopa_Billed_ind
				,Nopa_Billing_Option
				,Nopa_End_Dt
				,Nopa_Number_Offer_Request_List 
				,Nopa_start_dt
				,Offer_End_Dt
				,Offer_Id
				,Offer_Limit
				,Offer_Request_Id
				,Offer_Start_Dt
				,Offer_status_Cd
				,offerreq.Offer_Type_Cd
				,Weight_Limit
				,PLU
				,Point_Group
				,Points
				,Print_J4U_Tag_Ind
				,Prizes
				,Product_Group_Id
				,Product_Group_Nm 
				,Program_Cd 
				,Quantity
				,CASE WHEN offerreq.OFFER_ID IS NULL THEN Cancelled_status.OFFERREQUESTSTATUS_DESCRIPTION
				      WHEN offerreq.OfferRequestStatusTypeCd = 'digital' THEN dig_status.OFFERREQUESTSTATUS_DESCRIPTION
					 ELSE nondig_status.OFFERREQUESTSTATUS_DESCRIPTION END AS Offer_Request_status_cd
				,Rog_Id
				,Rog_Offer_Request_List
				,Rog_Store_Tag_UPC_List
				,Segment
				,Store_Group_Id 
				,Store_Id
				,Store_Group_Category_Cd
				,Tag_Comment
				,Tag_Display_Price
				,Tag_Display_Qty
				,UOM
				,Upto
				,Offer_Version
				,DW_LOGICAL_DELETE_IND
				,Store_Tag_J4U_Ind
				,IMAGE_TYPE_CD
				,copient_id
				,OFFER_LIMIT_NBR
				,OFFER_LIMIT_PERIOD_NBR
				from
				(
				select
				replace(OfferRequestDsc, '\\n',' ') as Additional_Detail_Dsc
				,BrandInfoTxt as Brand_Size
				,DeliveryChannelTypeCd as Channel_Type_Cd 
				,to_varchar(createTs::timestamp_ntz, 'MM/DD/YYYY hh24:mi:ss') as Created_Date
				,DepartmentNm as Department_Nm
				,DiscountAmt as Discount_Amt
				,CASE WHEN (GiftCardInd = True and UPPER(ProtoType_Code) = 'REWARDS_ACCUMULATION' ) then 'Yes' 
				 WHEN (UPPER(ProtoType_Code) = 'REWARDS_ACCUMULATION' AND (GiftCardInd = False or GiftCardInd is NULL)) then 'No' 
				 ELSE '' END as Gift_Card_Ind
				,GroupCd as Group_Cd
				,CASE WHEN AdvertisementType_Code = 'IA' then 'Yes' 
					WHEN AdvertisementType_Code = 'NIA' then 'No' 
					WHEN AdvertisementType_Code = 'NA' then '' 
					WHEN AdvertisementType_Code is NULL then '' END as In_Ad
				,case when LoyaltyPgmTagInd = True then TagDsc end as J4U_Tag_Comment
				,case when LoyaltyPgmTagInd = True then tagnbr || '/$' || IFF(tagAmt LIKE '%.%', IFF(tagAmt LIKE '%.00%', to_varchar(tagAmt::int), to_varchar(to_number(tagAmt, 38,2))), to_varchar(tagAmt::int)) end as J4U_Tag_Display_Price
				,to_varchar(UpdateTs::timestamp_ntz, 'MM/DD/YYYY hh24:mi:ss') as Last_Modified_Dt
				,CASE WHEN UOMDsc = 'Dollars' then IFF(TierLevelAmt is not null, to_number(TierLevelAmt, 20, 2), NULL)
					 ELSE Null END as Min_Amount_To_Buy
				,case when BilledInd = True then 'Yes' 
					WHEN BilledInd = False then 'No' 
					WHEN BilledInd is null then 'No' END as Nopa_Billed_ind
				,BillingOptionType_Code as Nopa_Billing_Option
				,NOPAEndDt as Nopa_End_Dt
				,NOPAStartDt as Nopa_start_dt
				,OfferEndDt as Offer_End_Dt
				,ReferenceOfferId as Offer_Id
				,RESTRICTIONTYPE_CODE as Offer_Limit
				,OfferRequestId as Offer_Request_Id
				,OfferStartDt as Offer_Start_Dt
				,Status_Description as Offer_status_Cd
				,ProtoType_Code as Offer_Type_Cd
				,case when TriggerId = '' then NULL else TriggerID end as PLU
				,CASE WHEN LoyaltyPgmTagInd = True then 'Yes' 
					WHEN LoyaltyPgmTagInd = False then 'No' 
					WHEN LoyaltyPgmTagInd is NULL then 'No' END as Print_J4U_Tag_Ind
				,PrizeItemQty as Prizes
				,PromotionProgramType_Code as Program_Cd
				,OfferRequestStatusTypeCd
				,CustomerSegmentInfoTxt as Segment
				,TagDsc as Tag_Comment
				,case when LoyaltyPgmTagInd = True then tagnbr || '/$' || tagAmt end as Tag_Display_Price
				,TagNbr as Tag_Display_Qty
				,AttachedOfferType_DisplayOrderNbr as Offer_Version
				,NULL as Store_Group_Category_Cd
				,Benefitvaluetype_description as Discount_Type_Dsc
				,CASE WHEN Upper(actiontypecd) = 'DELETE' THEN true ELSE false END as DW_LOGICAL_DELETE_IND
				,case when AdvertisementType_Code = 'NIA' And DeliveryChannelTypeCd = 'DO' Then IMAGETYPECD Else '' End as IMAGE_TYPE_CD
				,USAGELIMITNBR AS OFFER_LIMIT_NBR
				,USAGELIMITPERIODNBR AS OFFER_LIMIT_PERIOD_NBR
				from offerrequest ) offerreq			 
				LEFT JOIN
				(
				   select distinct offerrequestid
				   ,REFERENCEOFFERID
				   ,AttachedOfferType_DisplayOrderNbr
				   ,CASE WHEN StoreGroupType_Code='J4U' Then 'Digital' Else StoreGroupType_Code END AS StoreGroupType_Code
				   ,storegroupid as store_group_id
				   from str_grp
				) str
				on offerreq.offer_request_Id = str.offerrequestid
				and offerreq.offer_id = str.REFERENCEOFFERID
				and offerreq.offer_version = str.AttachedOfferType_DisplayOrderNbr
				LEFT JOIN
				(
					SELECT DISTINCT 
					s.offerrequestid
				    ,s.REFERENCEOFFERID
				    ,s.AttachedOfferType_DisplayOrderNbr
					,s.storegroupid as sgid
					--,CASE WHEN LoyaltyPgmTagInd = True and o.REFERENCEOFFERID like '%-D' then 'Yes' 
					,CASE WHEN LoyaltyPgmTagInd = True then 'Yes' 
					WHEN LoyaltyPgmTagInd = False then 'No' 
					WHEN LoyaltyPgmTagInd is NULL then 'No' END as Store_Tag_J4U_Ind
					from str_grp s
					INNER JOIN offerrequest o
					on s.offerrequestid = o.offerrequestid
					and s.REFERENCEOFFERID = o.REFERENCEOFFERID
					and s.AttachedOfferType_DisplayOrderNbr = o.AttachedOfferType_DisplayOrderNbr
					where s.StoreGroupType_Code = 'J4U'
				) sg_tag
				on offerreq.offer_request_Id = sg_tag.offerrequestId
				AND offerreq.offer_id = sg_tag.ReferenceOfferId
				AND offerreq.offer_version = sg_tag.AttachedOfferType_DisplayOrderNbr
				and sg_tag.sgid  = str.store_group_id
				 LEFT JOIN
				(
				select offerrequestid
				,AttachedOfferType_DisplayOrderNbr
				,listagg(distinct Store_Group_Nm, ', ') within group (order by Store_Group_Nm ) as Non_Digital_Store_Group_List
				from 
				(
					select distinct offerrequestid, AttachedOfferType_StoreGroupVersionId,AttachedOfferType_DisplayOrderNbr, storegroupid 
					,Store_group_nm
					,STOREGROUPTYPE_CODE
					from offerrequest o
					INNER JOIN
					` + dim_store_grp_tbl + `  sg
					ON o.storegroupid = sg.Store_group_id
					and o.STOREGROUPTYPE_CODE=sg.Store_Group_Category_Cd
					where Store_Group_Category_Cd = 'NonDigital'
				)
				group by offerrequestid, AttachedOfferType_StoreGroupVersionId,AttachedOfferType_DisplayOrderNbr
				) sg_list1
				on offerreq.offer_request_Id = sg_list1.offerrequestId
				and offerreq.Offer_Version  = sg_list1.AttachedOfferType_DisplayOrderNbr			
				LEFT JOIN
				(
				select offerrequestid
				,AttachedOfferType_DisplayOrderNbr
				,listagg(distinct Store_Group_Nm, ', ') within group (order by Store_Group_Nm ) as Digital_Store_Group_List
				from 
				(
					select distinct offerrequestid, AttachedOfferType_StoreGroupVersionId,AttachedOfferType_DisplayOrderNbr, storegroupid 
					,Store_group_nm
					,STOREGROUPTYPE_CODE
					from offerrequest o
					INNER JOIN
					` + dim_store_grp_tbl + `  sg
					ON o.storegroupid = sg.Store_group_id
					and o.STOREGROUPTYPE_CODE = sg.Store_Group_Category_Cd
					where Store_Group_Category_Cd = 'Digital'
				)
				group by offerrequestid, AttachedOfferType_StoreGroupVersionId,AttachedOfferType_DisplayOrderNbr
				) sg_list2
				on offerreq.offer_request_Id = sg_list2.offerrequestId
				and offerreq.Offer_Version  = sg_list2.AttachedOfferType_DisplayOrderNbr
				LEFT JOIN
				(
				select offerrequestid
				,AttachedOfferType_DisplayOrderNbr
				,listagg(distinct Store_Group_Nm, ', ') within group (order by Store_Group_Nm ) as J4U_Store_Group_List
				from 
				(
					select distinct offerrequestid, AttachedOfferType_StoreGroupVersionId, AttachedOfferType_DisplayOrderNbr,storegroupid
					,Store_group_nm
					,STOREGROUPTYPE_CODE
					from offerrequest o
					INNER JOIN
					` + dim_store_grp_tbl + `  sg
					ON o.storegroupid = sg.Store_group_id
					and o.STOREGROUPTYPE_CODE = sg.Store_Group_Category_Cd
					where Store_Group_Category_Cd = 'J4U'
				)
				group by offerrequestid, AttachedOfferType_StoreGroupVersionId,AttachedOfferType_DisplayOrderNbr
				) sg_list3
				on offerreq.offer_request_Id = sg_list3.offerrequestId
			and offerreq.Offer_Version  = sg_list3.AttachedOfferType_DisplayOrderNbr
			LEFT JOIN 
				(select distinct OfferRequestId,
				Referenceofferid,
				OfferRequestStatusTypeCd,
				offerrequeststatus_description
				from offerrequest 
				where Referenceofferid like '%-D'
				AND Offerrequeststatustypecd = 'digital'
				) dig_status
				ON  offerreq.OFFER_REQUEST_ID = dig_status.OFFERREQUESTID
				AND offerreq.OFFER_ID = dig_status.REFERENCEOFFERID
				AND offerreq.OfferRequestStatusTypeCd = dig_status.OfferRequestStatusTypeCd
				LEFT JOIN
				(select distinct OfferRequestId,
				Referenceofferid,
				OfferRequestStatusTypeCd,
				offerrequeststatus_description
				from offerrequest
				where Referenceofferid like '%-ND'
				AND Offerrequeststatustypecd = 'nonDigital'
				) nondig_status
				ON  offerreq.OFFER_REQUEST_ID = nondig_status.OFFERREQUESTID
				AND offerreq.OFFER_ID = nondig_status.REFERENCEOFFERID
				AND offerreq.OfferRequestStatusTypeCd = nondig_status.OfferRequestStatusTypeCd
			LEFT JOIN
				(select distinct OfferRequestId,
								Referenceofferid,
						OfferRequestStatusTypeCd,
						offerrequeststatus_description
						from offerrequest
						where Referenceofferid is NULL
						and offerrequeststatus_description='Canceled'  
				) Cancelled_status
				ON offerreq.OFFER_REQUEST_ID = Cancelled_status.OfferRequestId
				AND offerreq.OfferRequestStatusTypeCd = Cancelled_status.OfferRequestStatusTypeCd
			LEFT JOIN
				(
				select distinct OfferRequestId, 
				 case when upper(UserTypeCd) ='DIGITALUSER' then FirstNm || ' ' || LastNm end as Digital_Builder
				FROM offerrequest
				where upper(usertypecd) = 'DIGITALUSER'
				AND Digital_Builder is NOT NULL
				) d_user
				ON offerreq.Offer_Request_Id = d_user.OfferRequestId 
				LEFT JOIN
				(select distinct OfferRequestId 
				  , case when upper(UserTypeCd) = 'NONDIGITALUSER' then FirstNm || ' ' || LastNm end as Non_Digital_Builder
				FROM offerrequest
				where upper(usertypecd) = 'NONDIGITALUSER'
				AND Non_Digital_Builder is NOT NULL
				) nd_user
				ON offerreq.Offer_Request_Id = nd_user.OfferRequestId
				LEFT JOIN
				(select distinct OfferRequestId ,
				 case when upper(UserTypeCd)='CREATEDUSER' then FirstNm || ' ' || LastNm end as Created_By
				FROM offerrequest
				where upper(usertypecd) = 'CREATEDUSER'
				AND Created_By is NOT NULL
				) c_user
				ON offerreq.Offer_Request_Id = c_user.OfferRequestId
				LEFT JOIN
				(select distinct OfferRequestId ,
				 case when upper(UserTypeCd)='UPDATEDUSER' then FirstNm || ' ' || LastNm end as Last_Modified_By
				FROM offerrequest
				WHERE upper(usertypecd) = 'UPDATEDUSER'
				AND Last_Modified_By is NOT NULL
				) u_user
				ON offerreq.Offer_Request_Id = u_user.OfferRequestId
				LEFT JOIN
				(select payload_id as sg_id
				,payload_stores as store_id
				from ` + store_grp_flat_tbl + `) sg
				on str.store_group_id = sg.sg_id
				LEFT JOIN
				(
			select offerRequestId
			,listagg(Day_Time, ', ') as Qualification_Day_Time
			from
				(select distinct
				offerRequestId
				,case when offereffectiveday = 'true' AND StartTm is Not Null AND EndTm is Not NUll 
							then '(' || offereffectiveday_qualifier || ', ' || StartTm  || ' TO ' || EndTm   || ')' 
					  when offereffectiveday = 'true' AND ( StartTm is Null OR EndTm is NUll )
							then '(' || offereffectiveday_qualifier || ', ' || '12:00 AM' || ' TO ' ||  '11:59 PM'  || ')' 
					  END as Day_Time
				from offerrequest
				where offereffectiveday is not NULL and Day_Time is not null
			) 
			group by offerrequestId
				) offer
				ON offerreq.offer_request_Id = offer.offerrequestId
				LEFT JOIN
				(
				select 
				offerrequestid
				,listagg(Nopa_number, ', ') as Nopa_number_offer_request_list
				from nopa_numbers
				group by offerrequestid
				) as nopa
				ON offerreq.offer_request_Id = nopa.offerrequestid
				LEFT JOIN
				(
				select 
				distinct offerrequestid
				,AttachedOfferType_DisplayOrderNbr
				,ReferenceOfferId
				,ProductGroup_ProductGroupid as product_group_id
				,ProductGroup_ProductGroupnm as product_group_nm
				,CASE WHEN ProductGroup_ProductGroupNm is NOT NULL THEN 'Buy' END as BUY_GET
				,UOMDsc as UOM
				,case when UPPER(ProtoType_Code) = 'ITEM_DISCOUNT' Then '1' Else TierLevelAmt End as Quantity
				,IFF(UPPER(ProtoType_Code) = 'REWARDS_ACCUMULATION', (IFF((minimumpurchaseamt is not null)
					,IFF(contains('E+', minimumpurchaseamt), TO_NUMBER((split_part(minimumpurchaseamt, 'E+', 1) * pow(10, split_part(minimumpurchaseamt, 'E+', 2))),2), minimumpurchaseamt)
					,NULL)), NULL) as Min_Purchase
				,CASE WHEN UOMDsc = 'Items' OR UOMDsc = 'Per Pound' then IFF(TierLevelAmt is not null, to_number(TierLevelAmt), NULL)			
					  ELSE Null END as Min_Qty_To_Buy
				,NULL as Amount
				,NULL as Discount_Id
				,NULL as Dollar_Limit
				,NULL as Weight_Limit
				,NULL as Upto
				,NULL as Item_Limit
				,NULL as Points
				,NULL as Point_Group
				from offerrequest
				WHERE ProductGroup_ProductGroupNm is not null AND DISTINCTID <> 'BURN'
				UNION ALL
				select 
				distinct o.offerrequestid
				,AttachedOfferType_DisplayOrderNbr
				,o.ReferenceOfferId
				,IncludedProductGroupid as product_group_id
				,IncludedProductgroupNm as product_group_nm
				,CASE WHEN IncludedProductgroupNm is NOT NULL 
				 OR (UPPER(ProtoType_Code) = 'REWARDS_ACCUMULATION' AND DISCOUNTAMT IS NOT NULL)
				 OR (UPPER(ProtoType_Code) = 'REWARDS_FLAT' AND RewardQty IS NOT NULL)
				 OR (UPPER(ProtoType_Code) = 'ALASKA_AIRMILES' AND AirmilePointQty IS NOT NULL) THEN 'Get' END as BUY_GET
				,NULL as UOM
				,NULL as Quantity
				,NULL as Min_Purchase
				,NULL as Min_Qty_To_Buy
				,CASE WHEN (UPPER(ProtoType_Code) = 'REWARDS_ACCUMULATION' 
				 OR UPPER(ProtoType_Code) = 'REWARDS_FLAT'
				 OR UPPER(ProtoType_Code) = 'ALASKA_AIRMILES') THEN NULL
				 WHEN Benefitvaluetype_description = 'REWARDS_POINTS' THEN Rewardqty
				ELSE Discountamt END AS Amount
				,Discount_Id as Discount_Id
				,LimitType_LimitAmt as Dollar_Limit
				,LimitType_LimitWt as Weight_Limit
				,DiscountUptoQty as Upto
				,LimitType_LimitQty as Item_Limit
				,CASE WHEN UPPER(OfferRequestTypeCd) = 'REWARDS_ACCUMULATION' Then Discountamt
				WHEN UPPER(OfferRequestTypeCd) = 'REWARDS_FLAT'  THEN RewardQty
				WHEN (OfferRequestTypeCd) = 'CONTINUITY' THEN NULL
				WHEN UPPER(OfferRequestTypeCd) = 'ALASKA_AIRMILES' THEN AirmilePointQty
				END AS Points
				,CASE WHEN (UPPER(OfferRequestTypeCd) = 'REWARDS_ACCUMULATION' 
				OR UPPER(OfferRequestTypeCd) = 'REWARDS_FLAT'  ) THEN 'Points'
				WHEN UPPER(OfferRequestTypeCd) = 'CONTINUITY'     THEN NULL 
				WHEN UPPER(OfferRequestTypeCd) = 'ALASKA_AIRMILES' THEN AirmileTierNm
				END AS Point_Group
				from offerrequest o
				INNER JOIN
				(
				   select distinct offerrequestid,ReferenceOfferId,Discount_Id
				   from offerrequest o
				   LEFT JOIN
				   ` + dim_discount_tbl + ` di
				   ON o.BenefitValueType_Description = di.discount_dsc
				   WHERE Discount_Id IS NOT NULL 
				   OR (Discount_Id is NULL AND UPPER(OfferRequestTypeCd) IN ('REWARDS_FLAT','REWARDS_ACCUMULATION','ALASKA_AIRMILES'))
				 ) disc_id
				on disc_id.offerrequestid = o.offerrequestid
		        and disc_id.ReferenceOfferId=o.ReferenceOfferId
				WHERE DISTINCTID <> 'EARN' )buy_get
				ON offerreq.Offer_Request_Id = buy_get.offerrequestid
			and offerreq.Offer_Version   = buy_get.AttachedOfferType_DisplayOrderNbr
			and  offerreq.offer_id       = buy_get.ReferenceOfferId
				LEFT JOIN
				(
				select 
				offerrequestid
				,groupCd
				,case when subgroup_list = '' then GroupNm else  GroupNM || ',' || subgroup_list end as Group_offer_request_list
				from
				(
				select distinct
				offerrequestid
				,GroupNm
				,groupCd
				,Listagg(SubGroupNm, ',') as subgroup_list
				from groups
				group by offerrequestid, groupnm, groupCd
				) grp_list
				) as group_req_list
				ON offerreq.offer_request_Id = group_req_list.offerrequestid
				AND offerreq.group_Cd  = group_req_list.groupCd
				LEFT JOIN
				(
				select offerrequestid
			    ,ReferenceOfferId
				,display_nbr
				,StoreGroupTypeCd
				,Listagg(distinct Division_nm, ',') within group (order by division_nm ) as Division_Offer_Request_List
				from
				(  select
					distinct
					div_rog.offerrequestId
					,div_rog.ReferenceOfferId
					,div_rog.display_nbr
					,StoreGroupTypeCd
					,division_nm
					from div_rog
					INNER JOIN
					` + dim_rog_tbl + ` as d
					ON div_rog.division_id = d.division_id
					where StoreGroupTypeCd in ('NonDigital','Digital')
					and  d.dw_logical_Delete_ind = FALSE
				) dnm 
				group by offerrequestid,ReferenceOfferId,display_nbr,StoreGroupTypeCd
				) as div
				ON offerreq.offer_request_Id = div.offerrequestId
				AND offerreq.offer_id = div.ReferenceOfferId
				AND offerreq.offer_version = div.display_nbr
				AND str.StoreGroupType_Code = div.StoreGroupTypeCd
				LEFT JOIN
				(
				select offerrequestid
				,ReferenceOfferId
				,display_nbr
				,Listagg(distinct Division_nm, ',') within group (order by division_nm ) as Division_Store_Tag_Upc_List
				from
				(  select 
					distinct
					offerrequestId
					,ReferenceOfferId
					,j4u.Division_Id
					,display_nbr
					,Division_nm
					from
					(
						select
						offerrequestId
						,ReferenceOfferId
						,Division_Id
						,div_rog.display_nbr
						from div_rog
						where StoreGroupTypeCd = 'J4U'
						AND ReferenceOfferId  like '%-D'
					) j4u
					INNER JOIN
					` + dim_rog_tbl + ` as d
					ON j4u.division_id = d.division_id
					where d.dw_logical_Delete_ind = FALSE
				) dnm_j4u
				group by offerrequestid,ReferenceOfferId,display_nbr
				) as div_j4u
				ON offerreq.offer_request_Id = div_j4u.offerrequestId
				AND offerreq.offer_id = div_j4u.ReferenceOfferId
				AND offerreq.offer_version = div_j4u.display_nbr
				AND offerreq.offer_id  like '%-D'
				LEFT JOIN
				(
				select
				distinct
				offerrequestId
				,ReferenceOfferId
				,store_id as str_id
				,rog_id
				from div_rog
				) rogid
				ON offerreq.offer_request_Id = rogid.offerrequestId
				AND offerreq.offer_id = rogid.ReferenceOfferId
				AND sg.store_id = rogid.str_id
				LEFT JOIN
				(
				select
				offerrequestid
				,StoreGroupTypeCd
				,display_nbr
				,ReferenceOfferId
				,listagg(distinct rog_id, ', ') within  group (order by rog_id ) as ROG_Offer_request_list
				from
				(select
				distinct
				offerrequestid
				,ReferenceOfferId
				,rog_id
				,StoreGroupTypeCd
				,div_rog.display_nbr
				from div_rog
				where StoreGroupTypeCd in ('NonDigital','Digital')
				) r
				group by offerrequestid, StoreGroupTypeCd,ReferenceOfferId,display_nbr
				) rogs
				ON offerreq.offer_request_id = rogs.offerrequestid
				AND offerreq.offer_id = rogs.ReferenceOfferId
				AND offerreq.offer_version = rogs.display_nbr
				AND str.StoreGroupType_Code = rogs.StoreGroupTypeCd
				LEFT JOIN
				(
				select
				offerrequestid
				,ReferenceOfferId
				,display_nbr
				,listagg(distinct rog_id, ', ') within  group (order by rog_id ) as Rog_Store_Tag_Upc_List
				from
				(select
				distinct
				offerrequestid
				,ReferenceOfferId
			    ,div_rog.display_nbr
				,rog_id
				from div_rog
				where storegrouptypecd = 'J4U'
				AND ReferenceOfferId  like '%-D'
				) r
				group by offerrequestid,ReferenceOfferId,display_nbr
				) rog_j4u
				ON offerreq.offer_request_id = rog_j4u.offerrequestid
				AND offerreq.offer_id = rog_j4u.ReferenceOfferId
				AND offerreq.offer_version = rog_j4u.display_nbr
				AND offerreq.offer_id  like '%-D'
				LEFT JOIN
                (
                   select distinct payload_redemptionSystemId as copient_id,
                   payload_externalOfferId
				   ,PAYLOAD_OFFERREQUESTID
         			from ` + offer_flat_tbl + ` 
                 ) offerflat
                on offerreq.offer_id = offerflat.payload_externalOfferId
				AND offerreq.offer_request_id = offerflat.PAYLOAD_OFFERREQUESTID;`;

		try {
			snowflake.execute (
				{sqlText: cr_src_tmp_wrk_tbl  }
			)
		}
		catch (err)  {
			return "Creation of Fact_Offer_Request src_tmp_wrk_tbl table "+ src_wrk_tbl +" Failed with error: " + err;   // Return a error message.
		}

		var sql_begin = "BEGIN";

		// Processing deletes for store_id, upc_ids, points and point_group
		var sql_deletes = `delete from ` + tgt_tbl + `
				where (offer_request_id)
				in (select distinct offer_request_id
				from ` + tgt_wrk_tbl + `);`;

		// Processing Inserts 
		var sql_inserts = `INSERT INTO ` + tgt_tbl + `
			(
			Additional_Detail_Dsc
			,Amount
			,Brand_Size
			,BUY_GET
			,Channel_Type_Cd 
			,Created_Date
			,Created_By
			,Qualification_Day_Time
			,Department_Nm
			,Digital_Builder
			,Digital_Store_Group_List
			,Discount_Id 
			,Discount_Amt
			,Discount_Type_Dsc
			,Division_Offer_Request_List 
			,Division_Store_Tag_Upc_List  
			,Dollar_Limit
			,Gift_Card_Ind
			,Group_Cd
			,Group_offer_request_list 
			,In_Ad
			,Item_Limit
			,J4U_Store_Group_List
			,J4U_Tag_Comment
			,J4U_Tag_Display_Price 
			,Last_Modified_By
			,Last_Modified_Dt
			,Min_Amount_To_Buy
			,Min_Qty_To_Buy
			,Min_Purchase
			,Non_Digital_Builder
			,Non_Digital_Store_Group_List
			,Nopa_Billed_ind
			,Nopa_Billing_Option
			,Nopa_End_Dt
			,Nopa_Number_Offer_Request_List 
			,Nopa_start_dt
			,Offer_End_Dt
			,Offer_Id
			,Offer_Limit
			,Offer_Request_Id
			,Offer_Start_Dt
			,Offer_status_Cd
			,Offer_Type_Cd
			,Weight_Limit
			,PLU
			,Point_Group
			,Points
			,Print_J4U_Tag_Ind
			,Prizes
			,Product_Group_Id 
			,Product_Group_Nm
			,Program_Cd 
			,Quantity
			,Offer_Request_status_cd
			,Rog_Id
			,Rog_Offer_Request_List
			,Rog_Store_Tag_UPC_List
			,Segment
			,Store_Group_Id 
			,Store_Id
			,Store_Group_Category_Cd
			,Tag_Comment
			,Tag_Display_Price
			,Tag_Display_Qty
			,UOM
			,Upto
			,Offer_Version
			,Store_Tag_J4U_Ind
			,copient_id
			,IMAGE_TYPE_CD
			,DW_CREATE_TS 
			,DW_LOGICAL_DELETE_IND
			,OFFER_LIMIT_NBR
			,OFFER_LIMIT_PERIOD_NBR
			)
			select
			Additional_Detail_Dsc
			,Amount
			,Brand_Size
			,BUY_GET
			,Channel_Type_Cd 
			,Created_Date
			,Created_By
			,Qualification_Day_Time
			,Department_Nm
			,Digital_Builder
			,CASE WHEN Offer_Id like '%-D' Then Digital_Store_Group_List Else NULL END AS Digital_Store_Group_List
			,Discount_Id 
			,Discount_Amt
			,Discount_Type_Dsc
			,Division_Offer_Request_List 
			,Division_Store_Tag_Upc_List
			,Dollar_Limit
			,Gift_Card_Ind
			,Group_Cd
			,Group_offer_request_list 
			,In_Ad
			,Item_Limit
			,CASE WHEN Offer_Id like '%-D' Then J4U_Store_Group_List Else NULL END AS J4U_Store_Group_List
			,J4U_Tag_Comment
			,J4U_Tag_Display_Price 
			,Last_Modified_By
			,Last_Modified_Dt
			,Min_Amount_To_Buy
			,case when UPPER(OFFER_TYPE_CD) = 'ITEM_DISCOUNT' AND BUY_GET = 'Buy' Then '1' Else MIN_QTY_TO_BUY END AS MIN_QTY_TO_BUY
			,Min_Purchase
			,Non_Digital_Builder
			,CASE WHEN Offer_Id like '%-ND' Then Non_Digital_Store_Group_List Else NULL END AS Non_Digital_Store_Group_List
			,Nopa_Billed_ind
			,Nopa_Billing_Option
			,Nopa_End_Dt
			,Nopa_Number_Offer_Request_List 
			,Nopa_start_dt
			,Offer_End_Dt
			,Offer_Id
			,Offer_Limit
			,Offer_Request_Id
			,Offer_Start_Dt
			,Offer_status_Cd
			,Offer_Type_Cd
			,Weight_Limit
			,PLU
			,Point_Group
			,CAST (Points AS INTEGER)
			,case when Print_J4U_Tag_Ind='Yes' and (CHANNEL_TYPE_CD='DO' or CHANNEL_TYPE_CD='CC') Then 'Yes'
             else 'No' END AS Print_J4U_Tag_Ind
			,Prizes
			,Product_Group_Id
			,Product_Group_Nm
			,Program_Cd 
			,Quantity
			,Offer_Request_status_cd
			,Rog_Id
			,Rog_Offer_Request_List
			,Rog_Store_Tag_UPC_List
			,Segment
			,Store_Group_Id 
			,Store_Id
			,Store_Group_Category_Cd
			,Tag_Comment
			,Tag_Display_Price
			,Tag_Display_Qty
			,UOM
			,Upto
			,Offer_Version
			,case when Print_J4U_Tag_Ind='Yes' and (CHANNEL_TYPE_CD='DO' or CHANNEL_TYPE_CD='CC') Then 'Yes'
             else 'No' END AS Store_Tag_J4U_Ind
			,COPIENT_ID
			,IMAGE_TYPE_CD
			,current_timestamp() AS DW_CREATE_TS 
			,DW_LOGICAL_DELETE_IND
			,OFFER_LIMIT_NBR
			,OFFER_LIMIT_PERIOD_NBR
			FROM ` + tgt_wrk_tbl + `
			WHERE Offer_Request_status_cd is not null
			AND ((BUY_GET is NOT NULL OR (BUY_GET is NULL AND UPPER(Offer_Type_Cd) in ('INSTANT_WIN','CUSTOM')))
			AND (Discount_Type_Dsc is NOT NULL OR (Discount_Type_Dsc is NULL AND UPPER(Offer_Type_Cd) in ('REWARDS_FLAT','REWARDS_ACCUMULATION','ALASKA_AIRMILES','INSTANT_WIN','CUSTOM'))) or(Offer_Request_status_cd='Canceled'))
			UNION ALL	
			select
			Additional_Detail_Dsc
			,Amount
			,Brand_Size
			,BUY_GET
			,Channel_Type_Cd 
			,Created_Date
			,Created_By
			,Qualification_Day_Time
			,Department_Nm
			,Digital_Builder
			,CASE WHEN Offer_Id like '%-D' Then Digital_Store_Group_List Else NULL END AS Digital_Store_Group_List
			,Discount_Id 
			,Discount_Amt
			,Discount_Type_Dsc
			,Division_Offer_Request_List 
			,Division_Store_Tag_Upc_List
			,Dollar_Limit
			,Gift_Card_Ind
			,Group_Cd
			,Group_offer_request_list 
			,In_Ad
			,Item_Limit
			,CASE WHEN Offer_Id like '%-D' Then J4U_Store_Group_List Else NULL END AS J4U_Store_Group_List
			,J4U_Tag_Comment
			,J4U_Tag_Display_Price 
			,Last_Modified_By
			,Last_Modified_Dt
			,Min_Amount_To_Buy
			,Min_Qty_To_Buy
			,Min_Purchase
			,Non_Digital_Builder
			,CASE WHEN Offer_Id like '%-ND' Then Non_Digital_Store_Group_List Else NULL END AS Non_Digital_Store_Group_List
			,Nopa_Billed_ind
			,Nopa_Billing_Option
			,Nopa_End_Dt
			,Nopa_Number_Offer_Request_List 
			,Nopa_start_dt
			,Offer_End_Dt
			,Offer_Id
			,Offer_Limit
			,Offer_Request_Id
			,Offer_Start_Dt
			,Offer_status_Cd
			,Offer_Type_Cd
			,Weight_Limit
			,PLU
			,Point_Group
			,CAST (Points AS INTEGER)
			,Print_J4U_Tag_Ind
			,Prizes
			,Product_Group_Id
			,Product_Group_Nm
			,Program_Cd 
			,Quantity
			,Offer_Request_status_cd
			,Rog_Id
			,Rog_Offer_Request_List
			,Rog_Store_Tag_UPC_List
			,Segment
			,Store_Group_Id 
			,Store_Id
			,Store_Group_Category_Cd
			,Tag_Comment
			,Tag_Display_Price
			,Tag_Display_Qty
			,UOM
			,Upto
			,Offer_Version
			,null as Store_Tag_J4U_Ind
			,COPIENT_ID
			,IMAGE_TYPE_CD
			,current_timestamp() AS DW_CREATE_TS 
			,DW_LOGICAL_DELETE_IND
			,OFFER_LIMIT_NBR
			,OFFER_LIMIT_PERIOD_NBR
			FROM ` + tgt_wrk_tbl + `
			WHERE DW_LOGICAL_DELETE_IND = true;`;


		var sql_commit = "COMMIT"
		var sql_rollback = "ROLLBACK"
		try {
			snowflake.execute (
				{sqlText: sql_begin}
			);
			snowflake.execute (
				{sqlText: sql_deletes}
			);
			snowflake.execute (
				{sqlText: sql_inserts}
			);
			snowflake.execute (
				{sqlText: sql_commit}
			);    
		}
		catch (err) {
			snowflake.execute (
				{sqlText: sql_rollback}
			);
			return "Loading of Fact_Offer_Request " + tgt_tbl + " Failed with error: " + err;   // Return a error message.
		}
				// **************        Load for Fact_Offer_request ENDs *****************
				
		return "Done"
		
$$;

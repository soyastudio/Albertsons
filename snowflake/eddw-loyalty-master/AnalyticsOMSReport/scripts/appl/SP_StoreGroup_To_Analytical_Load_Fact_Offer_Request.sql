--liquibase formatted sql
--changeset SYSTEM:SP_StoreGroup_To_Analytical_Load_Fact_Offer_Request runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_ANALYTICS_PRD;
use schema DW_RETAIL_EXP;

CREATE OR REPLACE PROCEDURE EDM_ANALYTICS_PRD.DW_RETAIL_EXP.SP_STOREGROUP_TO_ANALYTICAL_LOAD_FACT_OFFER_REQUEST(SRC_WRK_TBL VARCHAR, ANL_DB VARCHAR, ANL_SCHEMA VARCHAR, WRK_SCHEMA VARCHAR, CNF_DB VARCHAR, CNF_SCHEMA VARCHAR, LOC_SCHEMA VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
 
     
    // **************        Load for Fact_Offer_Request table from storegroup BEGIN *****************
    var src_wrk_tbl = SRC_WRK_TBL;
    var anl_db = ANL_DB;
    var anl_schema = ANL_SCHEMA;
    var wrk_schema = WRK_SCHEMA;
    var cnf_db = CNF_DB;
    var cnf_schema = CNF_SCHEMA;
    var loc_schema = LOC_SCHEMA;
    
    var src_tmp_wrk_tbl = anl_db + "." + wrk_schema + ".Storegroup_tmp_wrk"
    var tgt_tmp_wrk_tbl =  anl_db + "." + wrk_schema + ".Fact_Offer_Request_Storegroup_TMP_WRK";
	var tgt_tmp_wrk2_tbl =  anl_db + "." + wrk_schema + ".Fact_Offer_Request_Storegroup_TMP2_WRK";
    var tgt_wrk_tbl =  anl_db + "." + wrk_schema + ".Fact_Offer_Request_Storegroup_WRK";
    var tgt_tbl = anl_db + "." + anl_schema + ".Fact_Offer_Request";
    var dim_store_grp_tbl = anl_db + "." + anl_schema + ".Dim_Store_Group";
    var dim_rog_tbl = anl_db + "." + anl_schema + ".DIM_ROG";
    var rog_upc_tbl = cnf_db + "." + cnf_schema + ".RETAIL_ORDER_GROUP_UPC";
    var facility_lkp_tbl = cnf_db + "." + loc_schema + ".FACILITY";
    var rog_div_lkp_tbl = cnf_db + "." + loc_schema + ".retail_order_group_division";
    var rs_lkp_tbl = cnf_db + "." + loc_schema + ".retail_store";
    var offerequestlkp_tbl = cnf_db + "." + cnf_schema + ".GetOfferRequest_FLAT";
	var storegroupflat_tbl = cnf_db + "." + cnf_schema + ".Storegroup_FLAT";
	var tgt_dim_wrk_tbl =  anl_db + "." + wrk_schema + ".DIM_STORE_GROUP_UPDATE_WRK";

    var cr_src_tmp_wrk = `CREATE OR REPLACE TABLE ` + src_tmp_wrk_tbl + ` AS
            select distinct payload_id
                            ,payload_name
                            ,payload_description
							,payload_createTs
							,payload_updateTs
							,payload_createdUser_userId
							,payload_CreatedUser_firstname 
							,payload_CreatedUser_lastname 
                            ,payload_updatedUser_userid 
                            ,payload_updatedUser_firstName 
							,payload_updatedUser_lastname
							,payload_stores
							,lastUpdateTs
                from ` + src_wrk_tbl + `
                where METADATA$ACTION ='INSERT' and METADATA$ISUPDATE = False;`;
    try {
        snowflake.execute (
            {sqlText: cr_src_tmp_wrk  }
        )
    }
    catch (err)  {
        return "Creation of Fact_Offer_Request src_tmp_wrk_tbl table, Failed with error: " + err;   // Return a error message.
    }

    // Prepare DIM store group update
	
	 var cr_tgt_wrkdim_tbl = `CREATE OR REPLACE TABLE `+ tgt_dim_wrk_tbl + ` AS
        select src.storegroup_id
        ,src.storegroup_nm
        from
        (select distinct payload_id as storegroup_id
        ,payload_name as storegroup_nm
        from ` + src_tmp_wrk_tbl + `
        ) src
        inner join
        (
        select distinct store_group_id
        ,store_group_nm
        from  ` + dim_store_grp_tbl + `
        where dw_logical_delete_ind = false
        )tgt
        on src.storegroup_id = tgt.store_group_id
        where src.storegroup_nm <> tgt.store_group_nm;`;

    try {
        snowflake.execute (
            {sqlText: cr_tgt_wrkdim_tbl  }
        )
    }
    catch (err)  {
        return "Creation of DIM_Store_Group tgt_dim_wrk_tbl table "+ tgt_dim_wrk_tbl +" Failed with error: " + err;   // Return a error message.
    }
	
	// Updates to DIM_STORE_GROUP
	
	var update_sql = `update `+ dim_store_grp_tbl+ ` as tgt
        set store_group_nm = src.storegroup_nm
        ,dw_last_update_ts = current_timestamp()
        from (select storegroup_id
            ,storegroup_nm
            from ` + tgt_dim_wrk_tbl + `) src
        where tgt.store_group_id = src.storegroup_id;`;

    try {
        snowflake.execute (
            {sqlText: update_sql  }
        )
    }
    catch (err)  {
        return "Update to DIM_Store_Group tgt_tbl table "+ tgt_tbl +" Failed with error: " + err;   // Return a error message.
    }
	
    // Prepare fact temp work table
    var cr_tgt_tmp_wrk_tbl = `CREATE OR REPLACE TABLE ` + tgt_tmp_wrk_tbl + ` AS
                select distinct
                offer_request_id
				,offer_id
				,offer_version
				,store_group_id
                from `+ tgt_tbl + `
                where store_group_id in 
                (
                select distinct payload_id
                from ` + src_tmp_wrk_tbl + `
                )
                AND DW_LOGICAL_DELETE_IND = False;`;
    try {
        snowflake.execute (
            {sqlText: cr_tgt_tmp_wrk_tbl  }
        )
    }
    catch (err)  {
        return "Creation of Fact_Offer_Request tgt_tmp_wrk_tbl table, Failed with error: " + err;   // Return a error message.
    }
    
	    // Prepare fact temp work2 table
    var cr_tgt_tmp2_wrk_tbl = `CREATE OR REPLACE TABLE ` + tgt_tmp_wrk2_tbl + ` AS
                select distinct
                tgt.Additional_Detail_Dsc
                ,tgt.Amount
                ,tgt.Brand_Size
                ,tgt.BUY_GET
                ,tgt.Channel_Type_Cd 
                ,tgt.Created_Date
                ,tgt.Created_By
                ,tgt.Qualification_Day_Time
                ,tgt.Department_Nm
                ,tgt.Digital_Builder
                ,tgt.Discount_Id 
                ,tgt.Discount_Amt
                ,tgt.Discount_Type_Dsc 
                ,tgt.Dollar_Limit
                ,tgt.Gift_Card_Ind
                ,tgt.Group_Cd
                ,tgt.Group_offer_request_list 
                ,tgt.In_Ad
                ,tgt.Item_Limit
                ,tgt.J4U_Tag_Comment
                ,tgt.J4U_Tag_Display_Price 
                ,tgt.Last_Modified_By
                ,tgt.Last_Modified_Dt
                ,tgt.Min_Amount_To_Buy
                ,tgt.Min_Qty_To_Buy
                ,tgt.Min_Purchase
                ,tgt.Non_Digital_Builder
                ,tgt.Nopa_Billed_ind
                ,tgt.Nopa_Billing_Option
                ,tgt.Nopa_End_Dt
                ,tgt.Nopa_Number_Offer_Request_List 
                ,tgt.Nopa_start_dt
                ,tgt.Offer_End_Dt
                ,tgt.Offer_Id
                ,tgt.Offer_Limit
                ,tgt.Offer_Request_Id
                ,tgt.Offer_Start_Dt
                ,tgt.Offer_status_Cd
                ,tgt.Offer_Type_Cd
                ,tgt.Weight_Limit
                ,tgt.PLU
                ,tgt.Point_Group
                ,tgt.Points
                ,tgt.Print_J4U_Tag_Ind
                ,tgt.Prizes
                ,tgt.Product_Group_Id
                ,tgt.Product_Group_Nm 
                ,tgt.Program_Cd 
                ,tgt.Quantity
                ,tgt.Offer_Request_status_cd
                ,tgt.Segment
                ,tgt.Store_Group_Id 
                ,tgt.Store_Group_Category_Cd
                ,tgt.Tag_Comment
                ,tgt.Tag_Display_Price
                ,tgt.Tag_Display_Qty
                ,tgt.UOM
                ,tgt.Upto
                ,tgt.Offer_Version
                ,tgt.DW_LOGICAL_DELETE_IND
		        ,tgt.Non_Digital_Store_Group_List
		        ,tgt.Digital_Store_Group_List
		        ,tgt.J4U_Store_Group_List
				,tgt.Store_Tag_J4U_Ind
			    ,tgt.IMAGE_TYPE_CD
				,tgt.copient_id
		         from `+ tgt_tbl + ` tgt
                inner join `+ tgt_tmp_wrk_tbl + ` tgt1
		        on tgt.offer_request_id = tgt1.offer_request_id
		        and tgt.offer_id = tgt1.offer_id
                and tgt.offer_version = tgt1.offer_version;`;
	 try {
        snowflake.execute (
            {sqlText: cr_tgt_tmp2_wrk_tbl  }
        )
    }
    catch (err)  {
        return "Creation of Fact_Offer_Request tgt_tmp_wrk2_tbl table, Failed with error: " + err;   // Return a error message.
    }
	

    var cr_tgt_wrk_tbl = `create or replace table ` + tgt_wrk_tbl + ` as 
                with div_rog as
                (
                    select 
                    distinct
                    offer_request_id
		            ,offer_id
                    ,rog_div.division_id as Division_Id
                    ,Offer_Version as store_group_version
                    ,sg.Store_Group_Category_Cd
                    ,sg.storegroupid
                    ,store_id
                    ,f.facility_integration_id
                    ,rs.rog_id as Rog_Id
                    from
                    (
                    select 
                    distinct
                    tgt.offer_request_id
		            ,tgt.offer_id
                    ,tgt.Offer_Version
                    ,ofr.STOREGROUPTYPE_CODE as Store_Group_Category_Cd
                    ,tgt.Store_Group_Id as storegroupid
                    from ` + tgt_tmp_wrk2_tbl + ` tgt
		            inner join ` + offerequestlkp_tbl + ` ofr
		            on ofr.offerrequestid = tgt.offer_request_id
		            and ofr.referenceofferid = tgt.offer_id
		            and ofr.AttachedOfferType_DisplayOrderNbr = tgt.Offer_Version
		            and ofr.storegroupid  = tgt.Store_Group_Id
                    ) sg
                    INNER JOIN
                    (select payload_id as sg_id
                    ,payload_stores as store_id
                    from ` + storegroupflat_tbl + `) sg_flat
                    ON sg.storegroupid = sg_flat.sg_id
                    INNER JOIN
                    ` + facility_lkp_tbl + ` as f
                    ON sg_flat.store_id = f.facility_nbr
                    INNER JOIN
                    ` + rs_lkp_tbl + ` as rs
                    ON f.facility_integration_id = rs.facility_integration_id
                    INNER JOIN 
                    ` + rog_div_lkp_tbl + ` as rog_div
                    ON rs.rog_id = rog_div.rog_id
		         where f.dw_current_version_ind = TRUE 
		    and f.dw_logical_Delete_ind = FALSE
		     and f.COMPANY_ID = 1101
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
                ,CASE when offerreq.Digital_Store_Group_List is not null then sg_list2.Digital_Store_Group_List_d else offerreq.Digital_Store_Group_List end as Digital_Store_Group_List
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
                ,case when offerreq.J4U_Store_Group_List is not null then sg_list3.J4U_Store_Group_List_j4u else offerreq.J4U_Store_Group_List end as J4U_Store_Group_List
                ,J4U_Tag_Comment
                ,J4U_Tag_Display_Price 
                ,Last_Modified_By
                ,Last_Modified_Dt
                ,Min_Amount_To_Buy
                ,Min_Qty_To_Buy
                ,Min_Purchase
                ,Non_Digital_Builder
                ,case when offerreq.Non_Digital_Store_Group_List is not null then sg_list1.Non_Digital_Store_Group_List_nd else offerreq.Non_Digital_Store_Group_List end as Non_Digital_Store_Group_List
                ,Nopa_Billed_ind
                ,Nopa_Billing_Option
                ,Nopa_End_Dt
                ,Nopa_Number_Offer_Request_List 
                ,Nopa_start_dt
                ,Offer_End_Dt
                ,offerreq.Offer_Id
                ,Offer_Limit
                ,offerreq.Offer_Request_Id
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
                ,DW_LOGICAL_DELETE_IND
				,Store_Tag_J4U_Ind
				,IMAGE_TYPE_CD
				,copient_id
                from
                (
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
                ,Discount_Id 
                ,Discount_Amt
                ,Discount_Type_Dsc 
                ,Dollar_Limit
                ,Gift_Card_Ind
                ,Group_Cd
                ,Group_offer_request_list 
                ,In_Ad
                ,Item_Limit
                ,J4U_Tag_Comment
                ,J4U_Tag_Display_Price 
                ,Last_Modified_By
                ,Last_Modified_Dt
                ,Min_Amount_To_Buy
                ,Min_Qty_To_Buy
                ,Min_Purchase
                ,Non_Digital_Builder
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
                ,Segment
                ,Store_Group_Id
                ,Tag_Comment
                ,Tag_Display_Price
                ,Tag_Display_Qty
                ,UOM
                ,Upto
                ,Offer_Version
		        ,Store_Group_Category_Cd
                ,DW_LOGICAL_DELETE_IND
		        ,Digital_Store_Group_List
		        ,Non_Digital_Store_Group_List
		        ,J4U_Store_Group_List
				,Store_Tag_J4U_Ind
				,IMAGE_TYPE_CD
				,copient_id
                from `+ tgt_tmp_wrk2_tbl + `
                ) offerreq
                LEFT JOIN
                (
                select offer_request_id
                ,store_group_version
		         ,offer_id
		         ,listagg(distinct Store_Group_Nm, ', ') within group (order by Store_Group_Nm ) as Non_Digital_Store_Group_List_nd
                from 
                (
                select distinct offer_request_id, offer_version as store_group_version,offer_id,o.store_group_id as storegroupid
                ,sg.Store_Group_Category_Cd 
                ,Store_group_nm
                from `+ tgt_tmp_wrk2_tbl + ` o
                INNER JOIN
                `+ dim_store_grp_tbl +` sg
                ON o.store_group_id = sg.Store_group_id
		        where sg.Store_Group_Category_Cd = 'NonDigital'	
                )
                group by offer_request_id, store_group_version,offer_id
                ) sg_list1
                on offerreq.offer_request_Id = sg_list1.offer_request_Id
		        and offerreq.Offer_Version  = sg_list1.store_group_version
		        and offerreq.offer_id       = sg_list1.offer_id
		        LEFT JOIN
                (
                select offer_request_id
                ,store_group_version
		        ,offer_id
		        ,listagg(distinct Store_Group_Nm, ', ') within group (order by Store_Group_Nm ) as Digital_Store_Group_List_d
                from 
                (
                select distinct offer_request_id, offer_version as store_group_version,offer_id, o.store_group_id as storegroupid
                ,sg.Store_Group_Category_Cd 
                ,Store_group_nm
                from `+ tgt_tmp_wrk2_tbl + ` o
                INNER JOIN
                `+ dim_store_grp_tbl +` sg
                ON o.store_group_id = sg.Store_group_id
		        where sg.Store_Group_Category_Cd = 'Digital'	
                )
                group by offer_request_id, store_group_version,offer_id
                ) sg_list2
                on offerreq.offer_request_Id = sg_list2.offer_request_Id
		        and offerreq.Offer_Version  = sg_list2.store_group_version
		        and offerreq.offer_id       = sg_list2.offer_id
		        LEFT JOIN
                (
                select offer_request_id
                ,store_group_version
		        ,offer_id
		        ,listagg(distinct Store_Group_Nm, ', ') within group (order by Store_Group_Nm ) as J4U_Store_Group_List_j4u
                from 
                (
                select distinct offer_request_id, offer_version as store_group_version,offer_id, o.store_group_id as storegroupid
                ,sg.Store_Group_Category_Cd 
                ,Store_group_nm
                from `+ tgt_tmp_wrk2_tbl + ` o
                INNER JOIN
                `+ dim_store_grp_tbl +` sg
                ON o.store_group_id = sg.Store_group_id
		        where sg.Store_Group_Category_Cd = 'J4U'	
                )
                group by offer_request_id, store_group_version,offer_id
                ) sg_list3
                on offerreq.offer_request_Id = sg_list3.offer_request_Id
		       and offerreq.Offer_Version  = sg_list3.store_group_version
		       and offerreq.offer_id       = sg_list3.offer_id 
                LEFT JOIN
                (select payload_id as sg_id
                ,payload_stores as store_id
                from `+ storegroupflat_tbl +`) sg
                on offerreq.store_group_id = sg.sg_id
                LEFT JOIN
                (
                select offer_request_id
		        ,offer_id
                ,Listagg(distinct Division_nm, ', ') within group (order by division_nm ) as Division_Offer_Request_List
                from
                (  select
                    distinct
                    div_rog.offer_request_Id
		             ,offer_id
                    ,division_nm
                    from div_rog
                    INNER JOIN
                    `+ dim_rog_tbl +` as d
                    ON div_rog.division_id = d.division_id
		            WHERE  d.dw_logical_Delete_ind = FALSE
                ) dnm 
                group by offer_request_id,offer_id
                ) as div
                ON offerreq.offer_request_Id = div.offer_request_Id
		        and offerreq.offer_id  = div.offer_id
                LEFT JOIN
                (
                select offer_request_id
		        ,offer_id
                ,Listagg(distinct Division_nm, ', ') within group (order by division_nm ) as Division_Store_Tag_Upc_List
                from
                (  select 
                    distinct
                    offer_request_Id
		            ,offer_id
                    ,j4u.Division_Id
                    ,Division_nm
                    from
                    (
                        select
                        offer_request_Id
			            ,offer_id
                        ,Division_Id
                        ,Store_Group_Category_Cd
                        from div_rog
                        where Store_Group_Category_Cd = 'J4U'
			            AND offer_id  like '%-D'
                    ) j4u
                    INNER JOIN
                    `+ dim_rog_tbl +` as d
                    ON j4u.division_id = d.division_id
		           WHERE  d.dw_logical_Delete_ind = FALSE
                ) dnm_j4u
                group by offer_request_id,offer_id
                ) as div_j4u
                ON offerreq.offer_request_Id = div_j4u.offer_request_Id
		       and offerreq.offer_id = div_j4u.offer_id
	           AND offerreq.offer_id  like '%-D'
                LEFT JOIN
                (
                select
                distinct
                offer_request_Id
		        ,offer_id
		       ,store_id as str_id
                ,rog_id
                from div_rog
                ) rogid
                ON offerreq.offer_request_Id = rogid.offer_request_Id
		        and offerreq.offer_id = rogid.offer_id
		        and sg.store_id  = rogid.str_id
                LEFT JOIN
                (
                select
                offer_request_id
                ,store_group_version
		        ,offer_id
                , listagg(distinct rog_id, ', ') within  group (order by rog_id ) as ROG_Offer_request_list
                from 
				(select
				distinct
				offer_request_id
				,offer_id
				,store_group_version
				,rog_id
				from div_rog
				) r
                group by offer_request_id, store_group_version,offer_id
                ) rogs
                ON offerreq.offer_request_id = rogs.offer_request_id
		         and offerreq.offer_id = rogs.offer_id
                LEFT JOIN
                (
		         select
		         offer_request_id
		        ,offer_id
		        ,store_group_version
		        ,listagg(distinct rog_id, ', ') within  group (order by rog_id ) as Rog_Store_Tag_Upc_List
		        from
                (select
		          distinct
                offer_request_id
                ,store_group_version
		         ,offer_id
                , rog_id
                from div_rog
                where store_group_category_cd = 'J4U'
                AND offer_id  like '%-D'
		        ) r
		        group by offer_request_id,offer_id,store_group_version
                ) rog_j4u
                ON offerreq.offer_request_id = rog_j4u.offer_request_id
		        and offerreq.offer_id = rog_j4u.offer_id
		        AND offerreq.offer_id  like '%-D';`;


    try {
        snowflake.execute (
            {sqlText: cr_tgt_wrk_tbl  }
        )
    }
    catch (err)  {
        return "Creation of Fact_Offer_Request tgt_wrk_tbl table, Failed with error: " + err;   // Return a error message.
    }


    var sql_begin = "BEGIN";

    // Processing deletes for store_id, upc_ids, points and point_group
    var sql_deletes = `delete from ` + tgt_tbl + `
            where (offer_request_id,offer_id,store_group_id)
            in (select distinct offer_request_id,offer_id,store_group_id
            from ` + tgt_tmp_wrk2_tbl + `);`;

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
        ,DW_CREATE_TS 
        ,DW_LOGICAL_DELETE_IND
		,Store_Tag_J4U_Ind
		,IMAGE_TYPE_CD
		,copient_id
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
        ,current_timestamp() AS DW_CREATE_TS 
        ,DW_LOGICAL_DELETE_IND
		,Store_Tag_J4U_Ind
		,IMAGE_TYPE_CD
		,copient_id
        FROM ` + tgt_wrk_tbl + `;`;

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
CREATE OR REPLACE PROCEDURE silver.load_full_silver()
LANGUAGE plpgsql
AS $$
BEGIN
	TRUNCATE TABLE silver.crm_cust_info;
	RAISE NOTICE 'Truncating and Inserting **silver.crm_cust_info** Table';
	
	INSERT INTO silver.crm_cust_info (
	    cst_id,
	    cst_key,
	    cst_firstname,
	    cst_lastname,
	    cst_marital_status,
	    cst_gndr,
	    cst_create_date
	)
	WITH transform_data_v1 AS (
		select 
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single' 
				 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				 ELSE 'other'
			END cst_marital_status,
			CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female' 
				 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				 ELSE 'other'
			END cst_gndr,
			cst_create_date,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC NULLS LAST, cst_id) as flag_last
		from bronze.crm_cust_info
		WHERE cst_create_date IS NOT NULL
	)
	select 
		cst_id,
	    cst_key,
	    cst_firstname,
	    cst_lastname,
	    cst_marital_status,
	    cst_gndr,
	    cst_create_date
	from transform_data_v1
	where flag_last = 1;
	
	RAISE NOTICE 'SUCCESS !! Truncating and Inserting **silver.crm_cust_info** Table';
	
---------------------------------------------------------------

	RAISE NOTICE 'Truncating and Inserting **silver.crm_prd_info** Table';
	
	TRUNCATE TABLE silver.crm_prd_info;
	INSERT INTO silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)
	SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
		SUBSTRING(prd_key, 7, length(prd_key)) as prd_key,
		TRIM(prd_nm) as prd_nm,
		COALESCE(prd_cost::NUMERIC, 0)as prd_cost ,
		CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'R' THEN 'Road'
			WHEN 'T' THEN 'Touring'
			ELSE 'Unknown'
		END as prd_line,
		prd_start_dt,
		LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) -1 as prd_end_dt
	from bronze.crm_prd_info;
	
	RAISE NOTICE 'SUCCESS !! Truncating and Inserting **silver.crm_prd_info** Table';
---------------------------------------------------------------
	
	RAISE NOTICE 'Truncating and Inserting **silver.crm_sales_details** Table';
	
	TRUNCATE TABLE silver.crm_sales_details;
	INSERT INTO silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
	)
	select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt<=0 THEN NULL
			WHEN LENGTH(CAST(sls_order_dt AS TEXT))!=8 THEN NULL 
			ELSE TO_DATE(CAST(sls_order_dt AS TEXT), 'YYYYMMDD')
		END AS sls_order_dt,
		CASE WHEN sls_ship_dt <=0 THEN NULL
			WHEN LENGTH(CAST(sls_ship_dt AS TEXT)) !=8 THEN NULL
			ELSE TO_DATE(CAST(sls_ship_dt AS TEXT), 'YYYYMMDD')
		END AS sls_ship_dt,
		CASE WHEN sls_due_dt <=0 THEN NULL
			WHEN LENGTH(CAST(sls_due_dt AS TEXT)) !=8 THEN NULL
			ELSE TO_DATE(CAST(sls_due_dt AS TEXT), 'YYYYMMDD')
		END AS sls_due_dt,
		CASE WHEN sls_sales ISNULL OR sls_sales <= 0 OR sls_sales != sls_quantity* ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE WHEN sls_price <=0 OR sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity,0)
			ELSE sls_price
		END AS sls_price
	from bronze.crm_sales_details;
	
	RAISE NOTICE 'SUCCESS !! Truncating and Inserting **silver.crm_sales_details** Table';

----------------------------------------------------------------------------------
	RAISE NOTICE 'Truncating and Inserting **silver.erp_cust_az12** Table';
	
	TRUNCATE TABLE silver.erp_cust_az12;
	INSERT INTO silver.erp_cust_az12 (
		cid,
		bdate,
		gen
	)
	SELECT 
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
			ELSE cid
		END AS cid,
		CASE WHEN bdate > now() THEN NULL
			ELSE bdate
		END AS bdate,
		CASE WHEN UPPER(TRIM(gen)) in ('F', 'FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(gen)) in ('M', 'MALE') THEN 'Male'
			ELSE 'Unknown'
		END AS gen
	FROM bronze.erp_cust_az12;
	
	RAISE NOTICE 'SUCCESS !! Truncating and Inserting **silver.erp_cust_az12** Table';
	
------------------------------------------------------------------
	RAISE NOTICE 'Truncating and Inserting **silver.erp_loc_a101** Table';
	
	TRUNCATE TABLE silver.erp_loc_a101;
	INSERT INTO silver.erp_loc_a101(
		cid,
		cntry
	)
	select 
		REPLACE(cid, '-', '') cid,
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) in ('US','USA') THEN 'United States'
		WHEN TRIM(cntry) IS NULL OR TRIM(cntry) = '' THEN 'N/A'
	ELSE cntry
		END AS cntry
	from bronze.erp_loc_a101;
	
	RAISE NOTICE 'SUCCESS !! Truncating and Inserting **silver.erp_loc_a101** Table';
	
------------------------------------------------------------------

	RAISE NOTICE 'Truncating and Inserting **silver.erp_px_cat_g1v2** Table';
	
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	INSERT INTO silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance
	)
	select	
		id,
		TRIM(cat) AS cat,
		TRIM(subcat) AS subcat,
		TRIM(maintenance) AS maintenance
	from bronze.erp_px_cat_g1v2;
	
	RAISE NOTICE 'SUCCESS !! Truncating and Inserting **silver.erp_px_cat_g1v2** Table';
END;
$$;




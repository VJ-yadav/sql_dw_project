CREATE OR REPLACE PROCEDURE silver.load_crm_sales_details(
	)
LANGUAGE plpgsql
AS $$
BEGIN
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

END;
$$;
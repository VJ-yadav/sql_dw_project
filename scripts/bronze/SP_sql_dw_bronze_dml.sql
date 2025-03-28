CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE 
	rows_affected int;
BEGIN

	TRUNCATE TABLE bronze.crm_cust_info;
	COPY bronze.crm_cust_info 
	FROM '/Users/vjsnapp/DATA_LAB/sql_dw_project/datasets/source_crm/cust_info.csv'
	WITH (
		FORMAT csv,
		HEADER true, 
		DELIMITER ','
	);
	
	
	TRUNCATE TABLE bronze.crm_prd_info;
	COPY bronze.crm_prd_info 
	FROM '/Users/vjsnapp/DATA_LAB/sql_dw_project/datasets/source_crm/prd_info.csv'
	WITH (
		FORMAT csv,
		HEADER true, 
		DELIMITER ','
	);
	
	
	TRUNCATE TABLE bronze.crm_sales_details;
	COPY bronze.crm_sales_details 
	FROM '/Users/vjsnapp/DATA_LAB/sql_dw_project/datasets/source_crm/sales_details.csv'
	WITH (
		FORMAT csv,
		HEADER true, 
		DELIMITER ','
	);
	
	TRUNCATE TABLE bronze.erp_cust_az12;
	COPY bronze.erp_cust_az12 
	FROM '/Users/vjsnapp/DATA_LAB/sql_dw_project/datasets/source_erp/CUST_AZ12.csv'
	WITH (
		FORMAT csv,
		HEADER true, 
		DELIMITER ','
	);
	
	TRUNCATE TABLE bronze.erp_loc_a101;
	COPY bronze.erp_loc_a101 
	FROM '/Users/vjsnapp/DATA_LAB/sql_dw_project/datasets/source_erp/LOC_A101.csv'
	WITH (
		FORMAT csv,
		HEADER true, 
		DELIMITER ','
	);
	BEGIN
		RAISE NOTICE '=======================================';
		RAISE NOTICE '>> Truncating table erp_px_cat_g1v2: %', clock_timestamp();
		RAISE NOTICE '=======================================';
		
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		RAISE NOTICE 'Truncated bronze.erp_px_cat_g1v2: %', clock_timestamp();
		
		COPY bronze.erp_px_cat_g1v2
		FROM '/Users/vjsnapp/DATA_LAB/sql_dw_project/datasets/source_erp/PX_CAT_G1V2.csv'
		WITH (
			FORMAT csv,
			HEADER true, 
			DELIMITER ','
		);
		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE NOTICE '=======================================';
        RAISE NOTICE '>> Loaded % rows into erp_px_cat_g1v2 at %', rows_affected, clock_timestamp();
		
		RAISE NOTICE '=======================================';
	EXCEPTION WHEN OTHERS THEN
		RAISE EXCEPTION 'Failed Loading erp_px_cat_g1v2: %', SQLERRM;
	END;
END;
$$;

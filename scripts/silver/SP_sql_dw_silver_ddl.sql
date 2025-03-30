-- Description: This script creates the tables in the silver layer of the data warehouse.

CREATE OR REPLACE PROCEDURE silver.create_tables(
	)
LANGUAGE 'plpgsql'
AS $BODY$
BEGIN

	BEGIN
		DROP TABLE IF EXISTS silver.crm_cust_info;
		CREATE TABLE silver.crm_cust_info(
			cst_id INTEGER,
			cst_key VARCHAR(50),
			cst_firstname VARCHAR(50),
			cst_lastname VARCHAR(50),
			cst_marital_status VARCHAR(50),
			cst_gndr VARCHAR(10),
			cst_create_date DATE
		);
		RAISE  NOTICE 'Created Table silver.crm_cust_info';
	EXCEPTION WHEN OTHERS THEN
		RAISE NOTICE 'Failed to create table silver.crm_cust_info: %', SQLERRM;
	END;

	BEGIN
		DROP TABLE IF EXISTS silver.crm_prd_info;
		CREATE TABLE silver.crm_prd_info(
			prd_id INTEGER,
			prd_key VARCHAR(50),
			prd_nm VARCHAR(50),
			prd_cost VARCHAR(50),
			prd_line VARCHAR(50),
			prd_start_dt DATE,
			prd_end_dt DATE
		);
		RAISE  NOTICE 'Created Table silver.crm_prd_info';
	EXCEPTION WHEN OTHERS THEN
		RAISE NOTICE 'Failed to create table silver.crm_prd_info: %', SQLERRM;
	END;

	BEGIN
		DROP TABLE IF EXISTS silver.crm_sales_details;
		CREATE TABLE silver.crm_sales_details(
			sls_ord_num VARCHAR(50),
			sls_prd_key VARCHAR(50),
			sls_cust_id INTEGER,
			sls_order_dt INTEGER ,
			sls_ship_dt INTEGER,
			sls_due_dt INTEGER,
			sls_sales INTEGER,
			sls_quantity INTEGER,
			sls_price INTEGER
		);
		RAISE  NOTICE 'Created Table silver.crm_sales_details';
	EXCEPTION WHEN OTHERS THEN
		RAISE NOTICE 'Failed to create table silver.crm_sales_details: %', SQLERRM;
	END;
		
	BEGIN
		DROP TABLE IF EXISTS silver.erp_cust_az12;
		CREATE TABLE silver.erp_cust_az12(
			cid VARCHAR(50),
			bdate DATE,
			gen VARCHAR(50)
		);
	RAISE  NOTICE 'Created Table silver.erp_cust_az12';
	EXCEPTION WHEN OTHERS THEN
		RAISE NOTICE 'Failed to create table silver.erp_cust_az12: %', SQLERRM;
	END;

	BEGIN
		DROP TABLE IF EXISTS silver.erp_loc_a101;
		CREATE TABLE silver.erp_loc_a101(
			cid VARCHAR(50),
			cntry VARCHAR(50)
		);
		RAISE  NOTICE 'Created Table silver.erp_loc_a101';
	EXCEPTION WHEN OTHERS THEN
		RAISE NOTICE 'Failed to create table silver.erp_loc_a101: %', SQLERRM;
	END;

	BEGIN
		DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
		CREATE TABLE silver.erp_px_cat_g1v2(
			id VARCHAR(50),
			cat VARCHAR(50),
			subcat VARCHAR(50),
			maintenance VARCHAR(50)
		);
		RAISE  NOTICE 'Created Table silver.erp_px_cat_g1v2';
	EXCEPTION WHEN OTHERS THEN
		RAISE NOTICE 'Failed to create table silver.erp_px_cat_g1v2: %', SQLERRM;
	END;

END;
$BODY$;
ALTER PROCEDURE silver.create_tables()
    OWNER TO postgres;

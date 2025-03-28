CREATE TABLE IF NOT EXISTS bronze.crm_cust_info(
	cst_id INTEGER,
	cst_key VARCHAR(50),
	cst_firstname VARCHAR(50),
	cst_marital_status VARCHAR(50),
	cst_gndr VARCHAR(10),
	cst_create_date DATE
);

CREATE TABLE IF NOT EXISTS bronze.crm_prd_info(
	prd_id INTEGER,
	prd_key VARCHAR(50),
	prd_nm VARCHAR(50),
	prd_cost VARCHAR(50),
	prd_line VARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE
);

CREATE TABLE IF NOT EXISTS bronze.crm_sales_details(
	sls_ord_num INTEGER,
	sls_prd_key VARCHAR(50),
	sls_cust_id INTEGER,
	sls_order_dt DATE ,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INTEGER,
	sls_quantity INTEGER,
	sls_price INTEGER
);

CREATE TABLE IF NOT EXISTS bronze.erp_cust_az12(
	cid VARCHAR(50),
	bdate DATE,
	gen VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS bronze.erp_loc_a101(
	cid VARCHAR(50),
	cntry VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS bronze.erp_px_cat_g1v2(
	id VARCHAR(50),
	cat VARCHAR(50),
	subcat VARCHAR(50),
	maintenance VARCHAR(50)
);


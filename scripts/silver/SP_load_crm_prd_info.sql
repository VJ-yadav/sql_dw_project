-- PROCEDURE: silver.load_crm_prd_info()

-- DROP PROCEDURE IF EXISTS silver.load_crm_prd_info();

CREATE OR REPLACE PROCEDURE silver.load_crm_prd_info(
	)
LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
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
END;
$BODY$;
ALTER PROCEDURE silver.load_crm_prd_info()
    OWNER TO postgres;
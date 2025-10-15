-----------------------------------------------------------------------------------
------------------------------Silver layer-----------------------------------------
----------------------------------------------------------------------------------
---purpose of the script
-- helps to delete the table if it already exist in the db and recreates the table
----- create the schema and  ddl from bronze schema
-- U - user defined
-----------------------------------------------------------------------------------
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
GO
create table silver.crm_cust_info(
cst_id int,
cst_key nvarchar(100),
cst_firstname nvarchar( 75),
cst_lastname nvarchar(75),
cst_material_Status nvarchar (25),
cst_gndr nvarchar(25),
cst_create_date date);

gO

IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO


--- creatiing product table
create table silver.crm_prd_info(
prd_id int,
prd_key nvarchar(100),
prd_nm nvarchar( 100),
prd_cost int,
prd_line nvarchar (100),
prd_start_dt date,
prd_end_dt date);

go

IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO



CREATE TABLE silver.crm_sales_details (
    sls_ord_num  NVARCHAR(100),
    sls_prd_key  NVARCHAR(100),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);

go

IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
GO


CREATE TABLE silver.erp_loc_a101 (
    cid    NVARCHAR(100),
    cntry  NVARCHAR(100)
);

gO

IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
GO


CREATE TABLE silver.erp_cust_az12 (
    cid    NVARCHAR(100),
    bdate  DATE,
    gen    NVARCHAR(100)
);
gO

IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
GO


CREATE TABLE silver.erp_px_cat_g1v2 (
    id           NVARCHAR(100),
    cat          NVARCHAR(100),
    subcat       NVARCHAR(100),
    maintenance  NVARCHAR(100)
);


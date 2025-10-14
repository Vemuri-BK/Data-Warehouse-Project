=====================================
  -- this scripts helps in loading the data in the csv file into the tables defined in ddl_script.sql
  ---procedures are defined in the script to handel the batch processing


-- creating store procedure
create or alter procedure bronze.load_bronze as

begin
     DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; --- new thing learned

	begin try
		print '**********************************************'
		print 'loading the bronze layer in the data warehouse'
		print'***********************************************'



		--- loading the data into the bronze layer
		-- i am using bulk insert
		--- truncate the table first and insert the data to avoid dupilcation of the data
		  print '**********************************************'
		print 'loading the crm data in to the  respective tables'
		print'***********************************************'
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';

		truncate table bronze.crm_cust_info;

		-- Bulk insert
		PRINT '>> inserting Table: bronze.crm_cust_info';

		bulk insert bronze.crm_cust_info
		-- need to provide the exact location of the source file in your local system
		from 'C:\Users\bhara\OneDrive\Desktop\DE projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with(
		 firstrow = 2,
		 fieldterminator = ',',
		 tablock
		 );
		 SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';




		 -- check the data from the table

		--select * from bronze.crm_cust_info;

		 -- counting the rows in the data -- one row is missing due to firstrow =2
		-- select count(*) from bronze.crm_cust_info;

		 --- product info 
		 
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		 truncate table bronze.crm_prd_info;

		-- Bulk insert
		bulk insert bronze.crm_prd_info
		-- need to provide the exact location of the source file in your local system
		from 'C:\Users\bhara\OneDrive\Desktop\DE projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with(
		 firstrow = 2,
		 fieldterminator = ',',
		 tablock
		 );
		  SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';



		 -- check the data from the table

		-- select * from bronze.crm_prd_info;

		 -- counting the rows in the data -- one row is missing due to firstrow =2
		 --select count(*) from bronze.crm_prd_info;


		 --- sales details
		 
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';

		  truncate table bronze.crm_sales_details;

		-- Bulk insert
		PRINT '>> Inserting Table: bronze.crm_sales_details';

		bulk insert bronze.crm_sales_details
		-- need to provide the exact location of the source file in your local system
		from 'C:\Users\bhara\OneDrive\Desktop\DE projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with(
		 firstrow = 2,
		 fieldterminator = ',',
		 tablock
		 );
		  SET @end_time = GETDATE();
		  PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	      PRINT '>> -------------';



		 -- check the data from the table

		 --select * from bronze.crm_sales_details;

		 -- counting the rows in the data -- one row is missing due to firstrow =2
		 --select count(*) from bronze.crm_sales_details;


 
		 --- erp cust_az12 details
		print '**********************************************'
		print 'loading the erp data in to respective tables 🫡☺️'
		print'***********************************************'

		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		  truncate table bronze.erp_cust_az12;

		-- Bulk insert
		PRINT '>> INserting Table: bronze.erp_cust_az12';

		bulk insert bronze.erp_cust_az12
		-- need to provide the exact location of the source file in your local system
		from 'C:\Users\bhara\OneDrive\Desktop\DE projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with(
		 firstrow = 2,
		 fieldterminator = ',',
		 tablock
		 );
		  SET @end_time = GETDATE();
          PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
          PRINT '>> -------------';



		 -- check the data from the table

		-- select * from bronze.erp_cust_az12;

		 -- counting the rows in the data -- one row is missing due to firstrow =2
		 --select count(*) from bronze.erp_cust_az12;



 
 
		 --- erp loc_a101 details
		 
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';

		  truncate table bronze.erp_loc_a101;

		  PRINT '>> Truncating Table: bronze.erp_loc_a101';

		-- Bulk insert
		bulk insert bronze.erp_loc_a101
		-- need to provide the exact location of the source file in your local system
		from 'C:\Users\bhara\OneDrive\Desktop\DE projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with(
		 firstrow = 2,
		 fieldterminator = ',',
		 tablock
		 );
		  SET @end_time = GETDATE();
          PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
          PRINT '>> -------------';



		 -- check the data from the table

		-- select * from bronze.erp_loc_a101;

		 -- counting the rows in the data -- one row is missing due to firstrow =2
		 --select count(*) from bronze.erp_loc_a101;


 
 
		 --- erp px_cat_g1v2 details
		 
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';

		  truncate table bronze.erp_px_cat_g1v2;

		-- Bulk insert
		PRINT '>> Inserting Table: bronze.erp_px_cat_g1v2';

		bulk insert bronze.erp_px_cat_g1v2
		-- need to provide the exact location of the source file in your local system
		from 'C:\Users\bhara\OneDrive\Desktop\DE projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with(
		 firstrow = 2,
		 fieldterminator = ',',
		 tablock
		 );
		  SET @end_time = GETDATE();
          PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
          PRINT '>> -------------';



		 -- check the data from the table

		 --select * from bronze.erp_px_cat_g1v2;

		 -- counting the rows in the data -- one row is missing due to firstrow =2
		 --select count(*) from bronze.erp_px_cat_g1v2;
	end try
	Begin catch
		print ' 🥲🥲🥲🥲🥲🥲🥲🥲🥲🥲🥲🥲🥲🥲🥲🥲🥲🥲'
		print' error occured while loading the data'
	end catch
 end

# Database-Management-SQL-Project
I have designed SQL Database through internal Servers
Database management SQL Project-
1.Create database-

--Create Database Datawarehouse

use master;

create database DataWarehouse;

use DataWarehouse

create schema bronze;
go
create schema silver;
go
create schema gold;
go

2.Creating Bronze data set

--Creating bronze dataset
if OBJECT_ID ('bronze.crm_cust_info', 'U') is not null
    drop table bronze.crm_cust_info;
create table bronze.crm_cust_info(
cst_id int,
cst_key nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_marital_status nvarchar(50),
cst_gndr nvarchar(50),
cst_create_date date
);

if OBJECT_ID ('bronze.crm_prd_info', 'U') is not null
    drop table bronze.crm_prd_info;
create table bronze.crm_prd_info(
prd_id int,
prd_key nvarchar(50),
prd_nm nvarchar(50),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt datetime,
prd_end_dt datetime
);

if OBJECT_ID ('bronze.crm_sales_details', 'U') is not null
    drop table bronze.crm_sales_details;
create table bronze.crm_sales_details(
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt int,
sla_sales int,
sls_quantity int,
sls_price int
);

if OBJECT_ID ('bronze.erp_loc_a101', 'U') is not null
    drop table bronze.erp_loc_a101;
create table bronze.erp_loc_a101(
cid nvarchar(50),
cntry nvarchar(50)
);

if OBJECT_ID ('bronze.erp_cust_az12', 'U') is not null
    drop table bronze.erp_cust_az12;
create table bronze.erp_cust_az12(
cid nvarchar(50),
bdate date,
gen nvarchar(50)
);

if OBJECT_ID ('bronze.erp_px_g1v2', 'U') is not null
    drop table bronze.erp_px_g1v2;
create table bronze.erp_px_g1v2(
id nvarchar(50),
cat nvarchar(50),
subcat nvarchar(50),
maintenance nvarchar(50)
);

3.Inserting Data with time calculation for bronze data set
--Inserting data
use DataWarehouse

execute bronze.load_bronze

create or alter procedure bronze.load_bronze as 
begin
    declare @start_time datetime,@end_time datetime

	begin try
		print '=============================================================';
		print 'Loading the Bronze layer';
		print '=============================================================';

		print '-------------------------------------------------------------';
		print 'Loading CRM Tables';
		print '-------------------------------------------------------------';
		set @start_time = getdate();
		print '>> Truncating Table: bronze.crm_cust_info';
		truncate table bronze.crm_cust_info;

		print '>> Inserting data into : bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'C:\Program Files\Microsoft SQL Server\MSSQL16.COPPER\MSSQL\Backup\cust_info.csv'
		with(
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
		set @end_time = getdate();
		print'>> Load Duration: ' +cast(datediff(second,@start_time,@end_time) as nvarchar) +'seconds';
		print'---------------------------';

		set @start_time = getdate();
		print '>> Truncating Table: bronze.crm_prd_info';
		truncate table bronze.crm_prd_info;

		print '>> Inserting data into : bronze.crm_prd_info';
		bulk insert bronze.crm_prd_info
		from 'C:\Program Files\Microsoft SQL Server\MSSQL16.COPPER\MSSQL\Backup\prd_info.csv'
		with(
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
		set @end_time = getdate();
		print'>> Load Duration: ' +cast(datediff(second,@start_time,@end_time) as nvarchar) +'seconds';
		print'---------------------------';

	    
		set @start_time = getdate();
		print '>> Truncating Table: bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;

		print '>> Inserting data into : bronze.crm_sales_details';
		bulk insert bronze.crm_sales_details
		from 'C:\Program Files\Microsoft SQL Server\MSSQL16.COPPER\MSSQL\Backup\sales_details.csv'
		with(
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
		set @end_time = getdate();
		print'>> Load Duration: ' +cast(datediff(second,@start_time,@end_time) as nvarchar) +'seconds';
		print'---------------------------';
	
		print '=============================================================';
		print 'Loading ERP Tables';
		print '=============================================================';

		set @start_time = getdate();
		print '>> Truncating Table: bronze.erp_cust_az12';
		truncate table  bronze.erp_cust_az12;

		print '>> Inserting data into : bronze.erp_cust_az12';
		bulk insert bronze.erp_cust_az12
		from 'C:\Program Files\Microsoft SQL Server\MSSQL16.COPPER\MSSQL\Backup\CUST_AZ12.csv'
		with(
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
		set @end_time = getdate();
		print'>> Load Duration: ' +cast(datediff(second,@start_time,@end_time) as nvarchar) +'seconds';
		print'---------------------------';
	    
		set @start_time = getdate();
		print '>> Truncating Table: bronze.erp_loc_a101';
		truncate table bronze.erp_loc_a101;

		print '>> Inserting data into : bronze.erp_loc_a101';
		bulk insert bronze.erp_loc_a101
		from 'C:\Program Files\Microsoft SQL Server\MSSQL16.COPPER\MSSQL\Backup\LOC_A101.csv'
		with(
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
		set @end_time = getdate();
		print'>> Load Duration: ' +cast(datediff(second,@start_time,@end_time) as nvarchar) +'seconds';
		print'---------------------------';
	    
		set @start_time = getdate();
		print '>> Truncating Table: bronze.erp_px_g1v2';
		truncate table bronze.erp_px_g1v2;

		print '>> Inserting data into : bronze.erp_px_g1v2';
		bulk insert bronze.erp_px_g1v2
		from 'C:\Program Files\Microsoft SQL Server\MSSQL16.COPPER\MSSQL\Backup\PX_CAT_G1V2.csv'
		with(
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
		set @end_time = getdate();
		print'>> Load Duration: ' +cast(datediff(second,@start_time,@end_time) as nvarchar) +'seconds';
		print'---------------------------';
		end try
		begin catch
			print '=================================================='
			print'Error Occured during loading Bronze Layer'
			print'Error Messege' + error_message();
			print'Error Messege' + cast (error_number() as nvarchar);
			print'Error Messege' + cast (error_state() as nvarchar);
			print '=================================================='


		end catch
end

3.Bronze  data sec checks and cleaning
--Check nulls and duplicates from Primary key
--Expectation no results

select cst_id,COUNT(*)
from bronze.crm_cust_info
group by cst_id
having COUNT(*) > 1 or cst_id is null

--check for unwanted spaces
--Expectations no results


--check for unwanted spaces
select cst_firstname 
from bronze.crm_cust_info
where cst_firstname != trim(cst_firstname)


--Data Standardization and consistency
select distinct cst_marital_status
from bronze.crm_cust_info

-------------------------------Bronze data set is done------------------------------
1.Creating Silver dataset
--Creating silver dataset
if OBJECT_ID ('silver.crm_cust_info', 'U') is not null
    drop table silver.crm_cust_info;
create table silver.crm_cust_info(
cst_id int,
cst_key nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_marital_status nvarchar(50),
cst_gndr nvarchar(50),
cst_create_date date,
dwh_create_date datetime2 default getdate()
);

if OBJECT_ID ('silver.crm_prd_info', 'U') is not null
    drop table silver.crm_prd_info;
create table silver.crm_prd_info(
prd_id int,
cat_id nvarchar(100),
prd_key nvarchar(50),
prd_nm nvarchar(50),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt date,
prd_end_dt date,
dwh_create_date datetime2 default getdate()
);

if OBJECT_ID ('silver.crm_sales_details', 'U') is not null
    drop table silver.crm_sales_details;
create table silver.crm_sales_details(
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id int,
sls_order_dt date,
sls_ship_dt date,
sls_due_dt date,
sla_sales int,
sls_quantity int,
sls_price int,
dwh_create_date datetime2 default getdate()
);

if OBJECT_ID ('silver.erp_loc_a101', 'U') is not null
    drop table silver.erp_loc_a101;
create table silver.erp_loc_a101(
cid nvarchar(50),
cntry nvarchar(50),
dwh_create_date datetime2 default getdate()
);

if OBJECT_ID ('silver.erp_cust_az12', 'U') is not null
    drop table silver.erp_cust_az12;
create table silver.erp_cust_az12(
cid nvarchar(50),
bdate date,
gen nvarchar(50),
dwh_create_date datetime2 default getdate()
);

if OBJECT_ID ('silver.erp_px_g1v2', 'U') is not null
    drop table silver.erp_px_g1v2;
create table silver.erp_px_g1v2(
id nvarchar(50),
cat nvarchar(50),
subcat nvarchar(50),
maintenance nvarchar(50),
dwh_create_date datetime2 default getdate()
);






2.Data cleaning and std-sec check

exec silver.load_silver

create or alter procedure silver.load_silver as
begin

	truncate table silver.crm_cust_info;
	Print '>>>>Inserting data into :silver.crm_cust_info '

	insert into silver.crm_cust_info(
	cst_id ,
	cst_key ,
	cst_firstname ,
	cst_lastname ,
	cst_marital_status ,
	cst_gndr ,
	cst_create_date)
	select
	cst_id,cst_key,
	trim(cst_firstname) as cst_firstname ,
	trim(cst_lastname) as cst_lastname,


	case when upper(trim(cst_marital_status)) = 'S' then 'Single'
		 when upper(trim(cst_marital_status)) = 'M' then 'Married'
		 else 'n/a'
	end cst_marital_status,

	case when upper(trim(cst_gndr)) = 'F' then 'Female'
		 when upper(trim(cst_gndr)) = 'M' then 'Male'
		 else 'n/a'
	end cst_gndr,
	cst_create_date
	from
	(
	select
	*,
	ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last
	from bronze.crm_cust_info) t where flag_last= 1

	truncate table silver.crm_prd_info;
	Print '>>>>Inserting data into :silver.crm_prd_info'

	insert into silver.crm_prd_info(
	prd_id ,
	cat_id ,
	prd_key ,
	prd_nm ,
	prd_cost ,
	prd_line ,
	prd_start_dt,
	prd_end_dt 
	)

	select 
	prd_id ,
	replace(SUBSTRING(prd_key,1,5), '-' ,'_') as cat_id,----Extract Category id
	SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key,----Extract product key
	prd_nm ,
	isnull(prd_cost,0) as prd_cost ,

	case upper(trim(prd_line)) 
		 when 'M' then 'Mountain'
		 when 'R' then 'Road'
		 when 'S' then 'Other_sales'
		 when 'T' then 'Touring'
		 else 'n/a'
	end as prd_line,
	prd_start_dt,
	prd_end_dt 
	from bronze.crm_prd_info
	--we want to join tables hence we are doing above data manupulation so that cat_id will be matching
	--where replace(SUBSTRING(prd_key,1,5), '-' ,'_') not in (select distinct id from bronze.erp_px_g1v2)
	--checking join parameter with sales table
	--where SUBSTRING(prd_key,7,LEN(prd_key)) in (
	--select sls_prd_key from bronze.crm_sales_details)
	--cat_id was not getting inserted in table after incraesing varchar size it's resolved
	--select * from silver.crm_prd_info

	--Sales Silver clean and load
	truncate table silver.crm_sales_details;
	Print '>>>>Inserting data into :silver.crm_sales_details '
	insert into silver.crm_sales_details(
	sls_ord_num ,
	sls_prd_key ,
	sls_cust_id ,
	sls_order_dt ,
	sls_ship_dt ,
	sls_due_dt ,
	sla_sales ,
	sls_quantity ,
	sls_price )

	select
	sls_ord_num ,
	sls_prd_key ,
	sls_cust_id ,
	case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
		 else cast(CAST(sls_order_dt as varchar) as date)
	end as sls_order_dt,
	case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
		 else cast(CAST(sls_ship_dt as varchar) as date)
	end as sls_ship_dt,
	case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
		 else cast(CAST(sls_due_dt as varchar) as date)
	end as sls_due_dt,
	case when sla_sales is null or sla_sales <=0 or sla_sales != sls_quantity*abs(sls_price)
		 then  sls_quantity*abs(sls_price)
		 else sla_sales
	end as sla_sales,
	sls_quantity ,
	case when sls_price is null or sls_price < 0
		 then sla_sales/nullif(sls_quantity,0)
		 else sls_price
	end as sls_price
	from bronze.crm_sales_details
	--where sls_ord_num != TRIM(sls_ord_num)

	--select * from silver.crm_sales_details




	truncate table silver.erp_px_g1v2;
	Print '>>>>Inserting data into :silver.erp_px_g1v2'

	insert into silver.erp_px_g1v2(
	id ,
	cat ,
	subcat ,
	maintenance 

	)
	select id,cat,subcat,maintenance from bronze.erp_px_g1v2

	--select * from silver.erp_px_g1v2




	--select * from bronze.erp_px_g1v2

	--select * from silver.crm_prd_info

	--check for unwanted spaces
	--select
	--distinct(cat) as cat 
	--from bronze.erp_px_g1v2
	--where TRIM(cat) != cat 

	truncate table silver.erp_cust_az12;
	Print '>>>>Inserting data into :silver.erp_cust_az12'

	insert into silver.erp_cust_az12
	(
	cid ,
	bdate ,
	gen 
	)
	select 
	case when cid like 'NAS%' then SUBSTRING(cid,4,LEN(cid))
		 else cid
	end cid,
	case when bdate > GETDATE() then null
		 else bdate
	end bdate,
	case when upper(TRIM(gen)) in ( 'F','Female') then 'Female'
		 when upper(TRIM(gen)) in ( 'M','Male') then 'Male'
		 else 'n/a'
	end as gen
	from bronze.erp_cust_az12
	--where cid like '%AW00011000%'
	--where case when cid like 'NAS%' then SUBSTRING(cid,4,LEN(cid))
	--     else cid
	--end not in (select distinct cst_key from silver.crm_cust_info)

	--Identify out of range dates
	--select * from silver.crm_cust_info
	--select distinct bdate
	--from silver.erp_cust_az12
	--where bdate < '1924-01-01' or bdate > GETDATE()
	--Data std and consistency
	--select 
	--distinct gen,
	--case when upper(TRIM(gen)) in ( 'F','Female') then 'Female'
	--     when upper(TRIM(gen)) in ( 'M','Male') then 'Male'
	--	 else 'n/a'
	--end as  gen
	--from silver.erp_cust_az12
 
	truncate table silver.erp_loc_a101;
	Print '>>>>Inserting data into :silver.erp_loc_a101'

	insert into silver.erp_loc_a101
	(
	cid ,
	cntry
	)

	select
	replace(cid,'-','') as cid,
	case when trim(cntry) = 'DE' then 'Germany'
			when trim(cntry) in ('US','USA') then 'United States'
			else trim(cntry)
	end as cntry
	from bronze.erp_loc_a101
	--where replace(cid,'-','') not in (select cst_key from bronze.crm_cust_info)

	--Data std and consistency
	--select distinct(cntry) from bronze.erp_loc_a101
	--order by cntry
End
-----------------------------------Silver data is done-----------------------------
Gold Layer
1.Gold layer Integration and table coalition
create view gold.dim_customers as 
select
ROW_NUMBER() over (order by cst_id) as customer_key,
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
la.cntry as country,
ci.cst_marital_status as marital_status,
case when ci.cst_gndr != 'n/a' then ci.cst_gndr    ---cust_info is the master table here
     else coalesce(ca.gen,'n/a')
end as gender,
ca.bdate as birth_date,
ci.cst_create_date as create_date
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key=ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid

2.

create view gold.dim_products as
select
ROW_NUMBER() over (order by pn.prd_start_dt,pn.prd_key) as product_key,
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.cat_id as category_id,
pc.cat as category,
pc.subcat as subcategory,
pn.prd_cost as cost,
pn.prd_line as product_line,
pn.prd_start_dt as start_date,
pc.maintenance as maintenance
from silver.crm_prd_info pn
left join silver.erp_px_g1v2 pc
on pn.cat_id = pc.id
where prd_end_dt is null --filter out all historical data
3.
select distinct
ci.cst_gndr,
ca.gen,
case when ci.cst_gndr != 'n/a' then ci.cst_gndr    ---cust_info is the master table here
     else coalesce(ca.gen,'n/a')
end as new_gen
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key=ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid
order by 1,2

select distinct gender from gold.dim_customers

4.
create view gold.fact_sales as 
select
sd.sls_ord_num as order_number,
pr.product_key ,
cu.customer_key,
sd.sls_cust_id as customer_id,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as ship_date,
sd.sls_due_dt as due_date,
sd.sla_sales as sales,
sd.sls_quantity as quantity,
sd.sls_price as price
from silver.crm_sales_details sd
left join gold.dim_products pr
on sd.sls_prd_key = pr.product_number
left join gold.dim_customers cu
on sd.sls_cust_id = cu.customer_id
5.
select * from gold.fact_sales
--Foreign key Integrity(dimensions)
select * from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key = f.customer_key
left join gold.dim_products g
on g.product_key = f.product_key
where c.customer_key is null


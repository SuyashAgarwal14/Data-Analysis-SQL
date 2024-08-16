-- Data Cleaning
-- 1. Remove Duplicates
-- 2. Standerize the data
-- 3. Handling Null values or blank values (either fill them or remove)
-- 4. Remove rows and columns that are unnecessary but should not remove data from raw table
-- hence we create a stagging table that is copy of the raw table

select * from layoffs;

create table layoff_stagging like layoffs;  		-- creates structure of table

insert layoff_stagging select * from layoffs;  		-- insert values to stagging table from raw table

select * from layoff_stagging;


-- Removing Duplicates
	-- if there is some unique id present we could check from it else 
	-- we assign row number based on matching every column of data and if there are some duplicates we remove them

with duplicate_cte as 
(
select *, row_number() over(partition by company, location, industry, 
total_laid_off, percentage_laid_off, `date`, stage, funds_raised_millions)
as row_num from layoff_stagging
)
select * from duplicate_cte where row_num > 1;

-- In Microsoft SQL server we can directly delete duplicate by using delete on cte Eg: Delete from duplicate_cte where row_num > 1;

CREATE TABLE `layoff_stagging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into layoff_stagging2 
select *, row_number() over(partition by company, location, industry, 
total_laid_off, percentage_laid_off, `date`, stage, funds_raised_millions)
as row_num from layoff_stagging;

select * from layoff_stagging2 where row_num > 1;

delete from layoff_stagging2 where row_num > 1;

select * from layoff_stagging2;

-- Standarizing data  Act of finding issues in data and fixing it

update layoff_stagging2 set company = trim(company);
select * from layoff_stagging2;

update layoff_stagging2 set industry = 'Crypto' where industry like 'Crypto%';

update layoff_stagging2 set country = trim(trailing '.' from country) where country like 'United States%';
  
    -- changing date from text to date datatype
update layoff_stagging2 set `date` = str_to_date(`date`, '%m/%d/%Y');
alter table layoff_stagging2 modify column 	`date` date;


-- Handling NULL / blank values

select * from layoff_stagging2 where industry is null or industry = '';

update layoff_stagging2 set industry = NULL where industry = '';

select t1.industry, t2.industry 
from layoff_stagging2 t1
join layoff_stagging2 t2
    on t1.company = t2.company
where (t1.industry is null) and t2.industry is not null;

update layoff_stagging2 t1
join layoff_stagging2 t2 
    on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null 
and t2.industry is not null;


select * from layoff_stagging2 where total_laid_off is null and percentage_laid_off is null;

delete from layoff_stagging2 where total_laid_off is null and percentage_laid_off is null;


-- deleting unnecessary columns
alter table layoff_stagging2 drop column row_num;

select * from layoff_stagging2;



-- Exploratory Data Analysis

select max(total_laid_off), max(percentage_laid_off) from layoff_stagging2;

select * from layoff_stagging2 where percentage_laid_off = 1 order by total_laid_off desc;

select * from layoff_stagging2 where percentage_laid_off = 1 order by funds_raised_millions desc;

select company, sum(total_laid_off) from layoff_stagging2 group by company order by 2 desc;

select min(`date`), max(`date`) from layoff_stagging2;

select industry, sum(total_laid_off) from layoff_stagging2 group by industry order by 2 desc;

select country, sum(total_laid_off) from layoff_stagging2 group by country order by 2 desc;

select year(`date`), sum(total_laid_off) from layoff_stagging2 group by year(`date`) order by 1 desc;

select substring(`date`, 1, 7) as `month`, sum(total_laid_off) from layoff_stagging2 where substring(`date`, 1, 7) is not null group by `month` order by 1 asc;

with rolling_total as
(
select substring(`date`, 1, 7) as `month`, sum(total_laid_off) as total_layoff
from layoff_stagging2 
where substring(`date`, 1, 7) is not null 
group by `month` 
order by 1 asc
)
select `month`, total_layoff, sum(total_layoff) over (order by `month`) as total
from rolling_total;


select company, year(`date`), sum(total_laid_off) from layoff_stagging2 group by company, year(`date`) order by 3 desc;

with company_year (company, years, total_laid_off) as
(
select company, year(`date`), sum(total_laid_off) 
from layoff_stagging2 
group by company, year(`date`) 
order by 3 desc
), company_year_rank as 
(select *, dense_rank() over (partition by years order by total_laid_off desc) as ranking
from company_year where years is not null
)
select * from company_year_rank where ranking <= 5;
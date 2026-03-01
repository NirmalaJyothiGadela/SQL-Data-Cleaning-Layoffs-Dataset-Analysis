-- data cleaning

select * from layoffs;
desc layoffs;

-- first thing need to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens.
create table layoffs_staging like layoffs;
select * from layoffs_staging;

insert layoffs_staging select * from layoffs;

-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways

-- 1. check for duplicates and remove any

-- let's just look at oda to confirm
SELECT *
FROM layoffs_staging
WHERE company = 'Oda';


-- it looks like these are all legitimate entries and shouldn't be deleted. We need to really look at every single row to be accurate

-- these are our real duplicates

-- using window function
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		layoffs_staging
) duplicates
WHERE 
	row_num > 1;


-- usind aggrigate function
create view duplicate_view as
select *,count(*) from layoffs_staging group by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions having count(*)>1;

-- these are the ones we want to delete where the row number is > 1 or 2or greater essentially

-- now you may want to write it like this:

with duplicate_cte as(
select *, 
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) as row_num 
from layoffs_staging
)
select * from duplicate_cte where row_num > 1;

-- deleting duplicates
delete from duplicate_view where row_num > 1;

-- one solution, which I think is a good one. Is to create a new column and add those row numbers in. Then delete where row numbers are over 2, then delete that column
-- so let's do it!!

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  row_num int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoffs_staging2 where row_num=2;

insert layoffs_staging2 select *, 
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) as row_num 
from layoffs_staging;

-- now that we have this we can delete rows were row_num is greater than 1
delete from layoffs_staging2 where row_num > 1 ;

select * from layoffs_staging2;


-- 2. Standardizing data

-- if we look at industry it looks like we have some null and empty rows, let's take a look at these
select distinct(industry) from layoffs_staging2 order by industry;

select * from layoffs_staging2 where industry is null or industry='' order by industry;

select * from layoffs_staging2 where company='Airbnb';

select * from layoffs_staging2 where company like "Bally%";

-- it looks like airbnb is a travel, but this one just isn't populated.
-- I'm sure it's the same for the others. What we can do is
-- write a query that if there is another row with the same company name, it will update it to the non-null industry values
-- makes it easy so if there were thousands we wouldn't have to manually check them all

-- we should set the blanks to nulls since those are typically easier to work with
update layoffs_staging2 set industry = null where industry = '';

select * from layoffs_staging2 where industry is null or industry='' order by industry;

-- now we need to populate those nulls if possible

-- I used a self-join to populate missing industry values by referencing other rows from the same company.
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- and if we check it looks like Bally's was the only one without a populated row to populate this null values
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- I also noticed the Crypto has multiple different variations. We need to standardize that - let's say all to Crypto

select distinct(company) from layoffs_staging2;

select * from layoffs_staging2 where industry like 'Crypto%';
update layoffs_staging2 set industry = 'Crypto' where industry like 'Crypto%';

set sql_safe_updates = 0;

-- we also need to look at 
select * from layoffs_staging2;

-- everything looks good except apparently we have some "United States" and some "United States." with a period at the end. Let's standardize this.
select distinct country from layoffs_staging2 order by 1;
update layoffs_staging2 set country = 'United States' where country like 'United States.';
update layoffs_staging2 set country = trim(country);

-- removing spaces before and after string if there are any
update layoffs_staging2 set company = trim(company);

-- look at remaining
select distinct location from layoffs_staging2 order by 1;

-- Let's also fix the date columns:
update layoffs_staging2 set date = date_format(str_to_date(date,'%m/%d/%Y'),'%Y-%m-%d');
-- so now we can easily change the data type
alter table layoffs_staging2 modify column date date;

desc layoffs_staging;

-- 3. Look at Null Values

-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase

-- so there isn't anything I want to change with the null values


-- 4. remove any columns and rows we need to

select * from layoffs_staging2;

select * 
from layoffs_staging2 
where total_laid_off is null 
and percentage_laid_off is null;


-- delete useless data we really can't use
delete 
from layoffs_staging2 
where total_laid_off is null 
and percentage_laid_off is null;

select * 
from layoffs_staging2 
where total_laid_off is null 
and percentage_laid_off is null;

select * 
from layoffs_staging2;

-- delete row_num, we really don't need
alter table layoffs_staging2 drop column row_num;


-- final cleaned data 
select * 
from layoffs_staging2;





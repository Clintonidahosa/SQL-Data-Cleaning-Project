# DATA CLEANING 
```
--              DUPLICATING TABLE 
SELECT*
FROM layoffs;

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT INTO layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging;

--             TO REMOVE DUPLICATE USING UNIQUE ROW NUMBERS 
SELECT*,
ROW_NUMBER() OVER (
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT*,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

--  We cannot delete a row from a cte hence we have to create a new table just like the cte and delete row_num  >1

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised` double DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT*,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

SET SQL_SAFE_UPDATES = 0;

DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;


--    STANDARDIZING DATA - Checking through each DISTINCT column for spaces, spelling errors, similarities and rectifying them
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT `date`, 
STR_TO_DATE(`date`, '%Y-%m-%d')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%Y-%m-%d');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2;


--        REMOVE NULL/ BLANK CELLS BY POPULATING THEM VIA MERGING (Could not be done with this data)

--        DELETE UNNECESSARY ROWS/COLUMN

DELETE
FROM layoffs_staging2
WHERE total_laid_off = ''
AND percentage_laid_off = '' ;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2;
```


# DATA EXPLORATION
```
SELECT *
FROM layoffs_staging2;

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY SUM(total_laid_off) DESC;

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY YEAR(`date`);

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY SUM(total_laid_off) DESC ;

SELECT country, SUM(total_laid_off), SUM(funds_raised)
FROM layoffs_staging2
GROUP BY country 
ORDER BY SUM(total_laid_off) DESC;

SELECT stage, SUM(total_laid_off), SUM(funds_raised)
FROM layoffs_staging2
GROUP BY stage
ORDER BY stage;

SELECT YEAR(`date`), MAX(total_laid_off) 
FROM layoffs_staging2
GROUP BY YEAR (`date`)
ORDER BY YEAR (`date`);

SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
GROUP BY `MONTH`
ORDER BY `MONTH` ASC;

WITH Rolling_total_table AS
(
SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
GROUP BY `MONTH`
ORDER BY `MONTH` ASC
)
SELECT `MONTH`, total_off, SUM(total_off) OVER(ORDER BY `MONTH`) AS Rolling_Total
FROM Rolling_total_table;

--  USING CTE TO GET TOP 5 COMPANIES THAT LAID OFF WORKERS EACH YEAR

WITH Company_Rank AS
(
SELECT company, YEAR(`date`) AS `Year`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
GROUP BY company, `Year`
ORDER BY `Year` ASC
), 
TOP_5_COMPANIES AS
(
SELECT *,
DENSE_RANK() OVER (PARTITION BY `Year`ORDER BY total_off DESC) AS Ranking
FROM Company_Rank
)
SELECT *
FROM TOP_5_COMPANIES
WHERE Ranking < 6;

-- ANOTHER METHOD OF USING CTEs FOR THE SAME PURPOSE

WITH Top_5_companies AS
(
SELECT company, 
YEAR(`date`) AS `Year`, 
SUM(total_laid_off) AS total_off,
DENSE_RANK() OVER(PARTITION BY YEAR(`date`) ORDER BY SUM(total_laid_off) DESC ) AS Ranking
FROM layoffs_staging2
GROUP BY company, `Year`
)
SELECT *
FROM Top_5_companies
WHERE Ranking < 6

```










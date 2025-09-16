-- Data Cleaning

SELECT *
FROM layoffs;

-- Remove duplicates
-- Standardize the data
-- Null values or blank values
-- Remove any columns

CREATE TABLE layoffs2
LIKE layoffs;

SELECT *
FROM layoffs2;

INSERT layoffs2
SELECT *
FROM layoffs;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`,
stage, country, funds_raised_millions) AS row_num
FROM layoffs2;

WITH duplicates_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`,
stage, country, funds_raised_millions) AS row_num
FROM layoffs2
)
SELECT *
FROM duplicates_cte
WHERE row_num > 1;

SELECT *
FROM layoffs2
WHERE company = 'Casper';

CREATE TABLE `layoffs3` (
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

SELECT *
FROM layoffs3;

INSERT INTO layoffs3
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`,
stage, country, funds_raised_millions) AS row_num
FROM layoffs2;

DELETE 
FROM layoffs3
WHERE row_num > 1;

-- Standardizing data

SELECT company, TRIM(company)
FROM layoffs3;

UPDATE layoffs3
SET company = TRIM(company);

SELECT distinct industry
FROM layoffs3
ORDER BY 1;

SELECT *
FROM layoffs3
WHERE industry LIKE 'Crypto%';

UPDATE layoffs3
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT industry
FROM layoffs3;

SELECT DISTINCT location
FROM layoffs3
ORDER BY 1;

SELECT DISTINCT country
FROM layoffs3
ORDER BY 1;

SELECT *
FROM layoffs3
WHERE country LIKE 'United States%'
ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs3
ORDER BY 1;

UPDATE layoffs3
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United State%';

SELECT `date`,
str_to_date(`date` , '%m/%d/%Y')
FROM layoffs3;

UPDATE layoffs3
SET `date` = str_to_date(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs3;

ALTER TABLE layoffs3
MODIFY COLUMN `date` DATE;

-- Null values or blank values

SELECT *
FROM layoffs3
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs3
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs3
WHERE company = 'Airbnb';

SELECT t1.industry, t2.industry
FROM layoffs3 t1
JOIN layoffs3 t2
    ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs3
SET industry = null
WHERE industry = '';

UPDATE layoffs3 t1
JOIN layoffs3 t2
    ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs3
WHERE company LIKE 'Bally%';

SELECT *
FROM layoffs3
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- 	Remove columns or rows

ALTER TABLE layoffs3
DROP COLUMN row_num;


--                      DATE CLEANING                   --

/* Initially opened file with excel to remove unnecessary columns. */
SELECT column_name, data_type
FROM INFORMATION_SCHEMA.columns
WHERE table_name = 'covid_data'

--Removed rows with continent totals as it would lead to repeated data.
SELECT * INTO covid_data_cleaned
FROM covid_data
WHERE continent is NOT NULL

/* There are some NULL values in the people_vaccinated, people_fully_vaccinated, and total_vaccinations column. I use the MAX aggregate over a window to replace NULLs with last previous known value into a new table. */
SELECT date, location,
       MAX(people_vaccinated) OVER (PARTITION BY location ORDER BY date) as people_vaccinated_clean,
       MAX(people_fully_vaccinated) OVER (PARTITION BY location ORDER BY date) as people_fully_vaccinated_clean,
       MAX(total_vaccinations) OVER (PARTITION BY location ORDER BY DATE) as total_vaccinations_clean
INTO vaccination_count
FROM covid_data_cleaned

/* Do the same for death count */
SELECT date, location, 
       MAX(total_deaths) OVER (PARTITION BY location ORDER BY date) as total_deaths_clean,
       MAX(new_deaths) OVER (PARTITION BY location ORDER BY date) as new_deaths_clean
INTO death_count
FROM covid_data_cleaned

--            Exploratory Analysis               --

--Global Stats Total
SELECT SUM(cd.total_cases) as [Total Cases], 
       SUM(dc.total_deaths_clean) as [Total Deaths], 
       SUM(vc.total_vaccinations_clean) as [Doses Administered]
FROM covid_data_cleaned cd 
LEFT JOIN vaccination_count vc
    ON cd.date = vc.date AND cd.location = vc.location
LEFT JOIN death_count dc 
   ON cd.date = dc.date AND cd.location = dc.location
WHERE cd.date = (SELECT MAX(date) from covid_data_cleaned)

--Global Stats by Country
SELECT cd.location, cd.continent,
       MAX(cd.total_cases) as [Total Cases], 
       MAX(dc.total_deaths_clean) as [Total Deaths], 
       MAX(vc.total_vaccinations_clean) as [Doses Administered]
FROM covid_data_cleaned cd 
LEFT JOIN vaccination_count vc
    ON cd.date = vc.date AND cd.location = vc.location
LEFT JOIN death_count dc 
   ON cd.date = dc.date AND cd.location = dc.location
GROUP BY cd.location, cd.continent
ORDER BY cd.continent, location

--Weekly Cases
SELECT MIN(weekstart) as [Week], 
       SUM(new_cases) as [Total Cases],
       SUM(new_cases) - LAG(SUM(new_cases), 1) OVER (ORDER BY MIN(weekstart)) as [Weekly Change #],
       ROUND((SUM(new_cases) - LAG(SUM(new_cases), 1) OVER (ORDER BY MIN(weekstart)))/
              LAG(SUM(new_cases), 1) OVER (ORDER BY MIN(weekstart))* 100, 2) as [Weekly Change %]
FROM
(
       SELECT MIN(date) as weekstart,
       DATEPART(iso_week, date) as iso_week,
       CASE
          WHEN MONTH(MIN(date)) = '1' and DATEPART(iso_week, date) >= '52'
              THEN YEAR(date) - 1 
          WHEN MONTH(MIN(date)) = '12' and DATEPART(iso_week, date) = '1'
              THEN YEAR(date) + 1
          ELSE YEAR(date)
       END AS iso_year,
       SUM(new_cases) as new_cases
       FROM covid_data_cleaned
       GROUP BY DATEPART(iso_week, date), YEAR(date)
) q
GROUP BY iso_week, iso_year
ORDER BY [Week]

--Weekly Deaths
SELECT MIN(weekstart) as [Week], 
       SUM(new_deaths) as [Total Deaths],
       SUM(new_deaths) - LAG (SUM(new_deaths), 1) OVER (ORDER BY MIN(weekstart)) as [Weekly Change #],
       ROUND((SUM(new_deaths) - LAG (SUM(new_deaths), 1) OVER (ORDER BY MIN(weekstart)))/
       LAG (SUM(new_deaths), 1) OVER (ORDER BY MIN(weekstart)) * 100, 2) as [Weekly Change %]
FROM
(
       SELECT MIN(date) as weekstart,
       DATEPART(iso_week, date) as iso_week,
       CASE
          WHEN MONTH(MIN(date)) = '1' and DATEPART(iso_week, date) >= '52'
              THEN YEAR(date) - 1
          WHEN MONTH(MIN(date)) = '12' and DATEPART(iso_week, date) = '1'
              THEN YEAR(date) + 1 
          ELSE YEAR(date)
       END AS iso_year,
       SUM(new_deaths) as new_deaths
       FROM covid_data_cleaned 
       GROUP BY DATEPART(iso_week, date), YEAR(date)
) q
GROUP BY iso_week, iso_year
ORDER BY [Week]
       
--Doses Administered Globally
SELECT date, sum(total_vaccinations_clean) as [Doses Administered]
FROM vaccination_count
GROUP BY date
ORDER BY date

--WHO Coronavirus (COVID-19) Dashboard
WITH seven_days_data AS
(
       SELECT DISTINCT location,
              SUM(new_cases) OVER (PARTITION BY location) as cases_past_week,
              SUM(new_deaths) OVER (PARTITION BY location) as deaths_past_week
       FROM covid_data_cleaned
       WHERE date IN (SELECT DISTINCT TOP 7 date FROM covid_data_cleaned ORDER BY date DESC)
)
SELECT cd.location, cd.continent,
       MAX(cd.total_cases) as [Total Cases], 
       MAX(sd.cases_past_week) as [Cases Past 7 Days], 
       MAX(cd.total_deaths) as [Total Deaths], 
       MAX(sd.deaths_past_week) as [Deaths Past 7 Days],
       ROUND(MAX(vc.total_vaccinations_clean)/MAX(cd.population) * 100, 2) as [Vaccinations Administered Per 100],
       ROUND(MAX(vc.people_fully_vaccinated_clean)/MAX(cd.population) * 100, 2) as [People Fully Vaccinated Per 100]
FROM covid_data_cleaned cd 
       LEFT JOIN seven_days_data sd ON cd.location = sd.location 
       LEFT JOIN vaccination_count vc ON cd.location = vc.location AND cd.date = vc.date
GROUP BY cd.location, cd.continent
ORDER BY [Total Cases] DESC

--United States Stats
SELECT location, date, new_cases, total_cases, total_deaths, 
	ROUND((total_deaths/total_cases)*100, 2) as death_rate,
       ROUND((total_cases/population)*100, 2) as infection_rate
FROM covid_data_cleaned
WHERE location = 'United States'
ORDER BY date

--Vaccination Rate Total
SELECT cd.date, 
       SUM(vc.people_vaccinated_clean) as [People Vaccinated],
       ROUND(SUM(vc.people_vaccinated_clean)/SUM(cd.population) * 100, 2) as [Percent of Population Vaccinated]
FROM covid_data_cleaned cd
       LEFT JOIN vaccination_count vc ON cd.location = vc.location AND cd.date = vc.[date]
GROUP BY cd.date
ORDER BY cd.date

--Infection rate by country
SELECT location, MAX(ROUND((total_cases/population)*100, 2)) as infection_rate
FROM covid_data_cleaned
GROUP BY location
ORDER BY infection_rate DESC

--Which countries have the highest booster count?
SELECT date, location, total_boosters
FROM covid_data_cleaned
WHERE total_boosters IS NOT NULL
ORDER BY location, date, total_boosters DESC

--Does vaccination rate affect infection rate?
SELECT location, 
       ROUND(MAX(people_vaccinated)/MAX(population)*100, 2) as percent_vaccinated,
       ROUND(MAX(total_cases)/MAX(population)*100, 2) as infection_rate
FROM covid_data_cleaned
GROUP BY location 
ORDER BY percent_vaccinated DESC

--Does the vaccination rate affect the death rate?
SELECT location,
       ROUND(SUM(people_vaccinated)/SUM(population)*100, 2) as percent_vaccinated, 
       ROUND(MAX(total_deaths)/MAX(total_cases)*100, 2) as death_rate
FROM covid_data_cleaned
GROUP BY location
ORDER BY death_rate DESC
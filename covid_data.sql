create database covid;
use covid;
CREATE TABLE covid_data1 (
	iso_code VARCHAR(8) NOT NULL, 
	continent VARCHAR(13), 
	location VARCHAR(32) NOT NULL, 
	`date` DATE NOT NULL, 
	total_cases DECIMAL(38, 0), 
	new_cases DECIMAL(38, 0), 
	new_cases_smoothed DECIMAL(38, 3), 
	total_deaths DECIMAL(38, 0), 
	new_deaths DECIMAL(38, 0), 
	new_deaths_smoothed DECIMAL(38, 3), 
	total_cases_per_million DECIMAL(38, 3), 
	new_cases_per_million DECIMAL(38, 3), 
	new_cases_smoothed_per_million DECIMAL(38, 3), 
	total_deaths_per_million DECIMAL(38, 3), 
	new_deaths_per_million DECIMAL(38, 3), 
	new_deaths_smoothed_per_million DECIMAL(38, 3), 
	reproduction_rate DECIMAL(38, 2), 
	icu_patients DECIMAL(38, 0), 
	icu_patients_per_million DECIMAL(38, 3), 
	hosp_patients DECIMAL(38, 0), 
	hosp_patients_per_million DECIMAL(38, 3), 
	weekly_icu_admissions DECIMAL(38, 0), 
	weekly_icu_admissions_per_million DECIMAL(38, 3), 
	weekly_hosp_admissions DECIMAL(38, 0), 
	weekly_hosp_admissions_per_million DECIMAL(38, 3), 
	total_tests DECIMAL(38, 0), 
	new_tests DECIMAL(38, 0), 
	total_tests_per_thousand DECIMAL(38, 3), 
	new_tests_per_thousand DECIMAL(38, 3), 
	new_tests_smoothed DECIMAL(38, 0), 
	new_tests_smoothed_per_thousand DECIMAL(38, 3), 
	positive_rate DECIMAL(38, 4), 
	tests_per_case DECIMAL(38, 1), 
	tests_units VARCHAR(15), 
	total_vaccinations DECIMAL(38, 0), 
	people_vaccinated DECIMAL(38, 0), 
	people_fully_vaccinated DECIMAL(38, 0), 
	total_boosters DECIMAL(38, 0), 
	new_vaccinations DECIMAL(38, 0), 
	new_vaccinations_smoothed DECIMAL(38, 0), 
	total_vaccinations_per_hundred DECIMAL(38, 2), 
	people_vaccinated_per_hundred DECIMAL(38, 2), 
	people_fully_vaccinated_per_hundred DECIMAL(38, 2), 
	total_boosters_per_hundred DECIMAL(38, 2), 
	new_vaccinations_smoothed_per_million DECIMAL(38, 0), 
	new_people_vaccinated_smoothed DECIMAL(38, 0), 
	new_people_vaccinated_smoothed_per_hundred DECIMAL(38, 3), 
	stringency_index DECIMAL(38, 2), 
	population DECIMAL(38, 0), 
	population_density DECIMAL(38, 3), 
	median_age DECIMAL(38, 1), 
	aged_65_older DECIMAL(38, 3), 
	aged_70_older DECIMAL(38, 3), 
	gdp_per_capita DECIMAL(38, 3), 
	extreme_poverty DECIMAL(38, 1), 
	cardiovasc_death_rate DECIMAL(38, 3), 
	diabetes_prevalence DECIMAL(38, 2), 
	female_smokers DECIMAL(38, 3), 
	male_smokers DECIMAL(38, 3), 
	handwashing_facilities DECIMAL(38, 3), 
	hospital_beds_per_thousand DECIMAL(38, 3), 
	life_expectancy DECIMAL(38, 2), 
	human_development_index DECIMAL(38, 3), 
	excess_mortality_cumulative_absolute DECIMAL(38, 1), 
	excess_mortality_cumulative DECIMAL(38, 2), 
	excess_mortality DECIMAL(38, 2), 
	excess_mortality_cumulative_per_million DECIMAL(38, 9)
);
SET SESSION sql_mode = '';
load data infile 'D:/covid-data-xl.csv'
into table covid_data1
fields terminated by ','
enclosed by '"' 
lines terminated by '\n'
ignore 1 rows;

SELECT * FROM covid_data;

-- add mysql data format for date column
alter table covid_data
add column new_date date after date;

update covid_data
set new_date = str_to_date(date, '%m/%d/%Y');

alter table covid_data
drop column date ;

select location,  new_date, total_cases, new_cases, total_deaths, population
from covid_data;

delimiter &&
create procedure select_all()
begin
select * from covid_data;
end &&

call select_all;

-- creating view 
create view covid_deaths as
select iso_code,continent,location,new_date,total_cases,new_cases,total_deaths,new_deaths,population,
icu_patients,hosp_patients,weekly_icu_admissions,weekly_hosp_admissions 
from covid_data;

delimiter &&
create procedure coviddeaths()
begin
select * from covid_deaths;
end &&

call coviddeaths;

-- covid death percentage in india
select iso_code,date(new_date), continent, location, total_deaths/total_cases*100
from covid_deaths
where location = 'India'
group by 2
-- yearly death rate in india
select year(new_date)year,location, sum(total_deaths)/sum(total_cases)*100 'death%'
from covid_deaths
where location = 'India'
group by 1

call coviddeaths;

-- cases by population in india
select continent,location,new_date, total_cases/population*100 
from covid_deaths
where location = 'India'

-- death by population in india
select continent,location,new_date,total_deaths,population, total_deaths/population*100 
from covid_deaths
where location = 'India'

-- leading covid death country in each continent 
with highest_deaths as (
select iso_code,new_date,continent,location,total_deaths, row_number() over (partition by continent order by total_deaths desc) ranking
from covid_deaths 
)
select iso_code,new_date,continent,location,total_deaths from highest_deaths where ranking = 1;

call coviddeaths;
-- leading covid cases country in each continent
with highest_cases as (
select iso_code,new_date,continent,location,total_cases, row_number() over (partition by continent order by total_cases desc) ranking
from covid_deaths 
)
select iso_code,continent,location,total_cases from highest_cases where ranking = 1 order by total_cases desc;


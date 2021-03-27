'''
* @Author: Uthsavi KP
* @Date: 2020-02-13 11:43:19 
* @Last Modified by: Uthsavi KP
* @Last Modified time: 2020-02-13 11:43:19 
* @Title : To Write HQL queries on covid19 data using beeline hive
'''

-- Creating table
DROP TABLE country_wise;
CREATE EXTERNAL TABLE country_wise
(
        Country string,
        Confirmed int,
        Deaths int,
        Recovered int,
        Active int,
        New_recovered int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "," LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION "wasbs://covid19container@covidstorage19.blob.core.windows.net/country_wise_output.csv" TBLPROPERTIES("skip.header.line.count"="1");

--Loading data into Hive tables
LOAD DATA INPATH "wasbs://covid19container@covidstorage19.blob.core.windows.net/country_wise_output.csv" INTO TABLE country_wise;

--Finding country with highest confirmed cases
SELECT Country, Confirmed
FROM(SELECT *, ROW_NUMBER()OVER(ORDER BY Confirmed DESC) cases
FROM covid19_analysis.country_wise)X
WHERE cases = 1;

--Finding country with lowest confirmed cases
SELECT Country, Confirmed
FROM(SELECT *, ROW_NUMBER()OVER(ORDER BY Confirmed ASC) cases
FROM covid19_analysis.country_wise)X
WHERE cases = 1;

--Finding country with highest deaths
SELECT Country, Deaths
FROM(SELECT *, DENSE_RANK()OVER(ORDER BY Deaths DESC) highest_deaths
FROM covid19_analysis.country_wise)X
WHERE highest_deaths = 1;

--Finding country with lowest deaths
SELECT Country, Deaths
FROM(SELECT *, DENSE_RANK()OVER(ORDER BY Deaths ASC) lowest_deaths
FROM covid19_analysis.country_wise)X
WHERE lowest_deaths = 1;

--Finding country with highest recovery
SELECT Country, Recovered
FROM(SELECT *, DENSE_RANK()OVER(ORDER BY Recovered DESC) recovery
FROM covid19_analysis.country_wise)X
WHERE recovery = 1;

--Finding country with lowest recovery
SELECT Country, Recovered
FROM(SELECT *, DENSE_RANK()OVER(ORDER BY Recovered ASC) recovery
FROM covid19_analysis.country_wise)X
WHERE recovery = 1;

--Finding country with highest new records
SELECT Country, New_recovered
FROM(SELECT *, DENSE_RANK()OVER(ORDER BY New_recovered DESC) new_recovery
FROM covid19_analysis.country_wise)X
WHERE new_recovery = 1;
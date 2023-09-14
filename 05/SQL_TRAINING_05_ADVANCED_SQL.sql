-- Databricks notebook source


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Introduction
-- MAGIC In this Databricks notebook, we will explore advanced SQL concepts to enhance your SQL skills. We'll cover topics such as window functions, common table expressions (CTEs), and subqueries.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Setting Up a Training Environment
-- MAGIC
-- MAGIC Setup will create a widget, build a database using the name you specify in the widget, and then create a table named `NYCTAXI_YELLOW` in that database using the `nyctaxi` dataset from the databricks-datasets.
-- MAGIC
-- MAGIC We then create a narrow version with basic data cleansing stored in a table named `CLEANED_NYCTAXI_YELLOW`

-- COMMAND ----------

-- DBTITLE 1,Create Widget
CREATE WIDGET TEXT TRAINING_DATABASE DEFAULT '';

-- COMMAND ----------

-- DBTITLE 1,Create Database
SELECT
  CASE "$TRAINING_DATABASE"
    WHEN "" THEN RAISE_ERROR("TRAINING_DATABASE widget cannot be left empty")
  END AS Validation;

CREATE DATABASE IF NOT EXISTS $TRAINING_DATABASE;

-- COMMAND ----------

-- DBTITLE 1,Check
-- MAGIC %python
-- MAGIC def check_solution(exercise_number, expected_hash):
-- MAGIC   solution_hash = spark.sql(
-- MAGIC     f"SELECT XXHASH64(COLLECT_SET(XXHASH64(*))) FROM EXERCISE{exercise_number}_SOLUTION"
-- MAGIC   ).first()[0]
-- MAGIC   if solution_hash == expected_hash:
-- MAGIC     print("\033[32mCorrect Solution\033[0m")
-- MAGIC   else:
-- MAGIC     raise Exception("Incorrect Solution")
-- MAGIC
-- MAGIC def check_exercise1_solution():
-- MAGIC   check_solution(1, 1493914765122705214)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We will be using the `nyctaxi` dataset from the databricks-datasets for or training. Databricks provides a variety of sample datasets that you can use in your workspace. You can learn more about how to import them here:
-- MAGIC https://docs.databricks.com/en/dbfs/databricks-datasets.html

-- COMMAND ----------

-- MAGIC %python
-- MAGIC f = open('/dbfs/databricks-datasets/nyctaxi/readme_nyctaxi.txt', 'r')
-- MAGIC print(f.read())
-- MAGIC display(dbutils.fs.ls('/databricks-datasets/nyctaxi/'))

-- COMMAND ----------

-- DBTITLE 1,Create a Clone Table
CREATE OR REPLACE TABLE $TRAINING_DATABASE.NYCTAXI_YELLOW
DEEP CLONE delta.`dbfs:/databricks-datasets/nyctaxi/tables/nyctaxi_yellow`;

-- COMMAND ----------

-- DBTITLE 1,Display Sample Data
SELECT
  *
FROM SQL_TRAINING_05_ADVANCED_SQL.NYCTAXI_YELLOW

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The data is spread out across multiple years, with multiple outliers. We will focus on a single year, narrow it down to the columns we wish to use, and clean up the data a bit. For simplicity, we will simply get rid of the records that have invalid data.

-- COMMAND ----------

-- DBTITLE 1,Record Count by Year
SELECT
  YEAR(PICKUP_DATETIME) AS PICKUP_YEAR,
  COUNT(*) AS RECORD_COUNT
FROM $TRAINING_DATABASE.NYCTAXI_YELLOW
GROUP BY
  PICKUP_YEAR
ORDER BY
  PICKUP_YEAR

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Data for the year 2009 (with 17,08,96,987 records) appears to have sufficient data for our analysis. We will create a narraow dataset and remove invalid records as mentioned above.

-- COMMAND ----------

-- DBTITLE 1,Create Filtered and Cleaned Table for 2009 Data
-- Leaving the columns we are leaving behind commented out
-- to know what's being dropped as well as for ease of
-- integrating back in
CREATE OR REPLACE TABLE $TRAINING_DATABASE.CLEANED_NYCTAXI_YELLOW AS
SELECT
  VENDOR_ID,
  PICKUP_DATETIME,
  DROPOFF_DATETIME,
  PASSENGER_COUNT,
  TRIP_DISTANCE,
  -- PICKUP_LONGITUDE,
  -- PICKUP_LATITUDE,
  -- RATE_CODE_ID,
  -- STORE_AND_FWD_FLAG,
  -- DROPOFF_LONGITUDE,
  -- DROPOFF_LATITUDE,
  PAYMENT_TYPE,
  FARE_AMOUNT,
  -- EXTRA,
  -- MTA_TAX,
  TIP_AMOUNT,
  TOLLS_AMOUNT
  -- TOTAL_AMOUNT -- Commenting this out as this had a lot of invalid data for January 2019. We can calculate this from the other fare/tips/tolls columns
FROM $TRAINING_DATABASE.NYCTAXI_YELLOW
-- 1611611035 un-filtered
WHERE
  -- Selecting records from 2009 only
  PICKUP_DATETIME IS NOT NULL AND YEAR(PICKUP_DATETIME) = 2009 -- 170896987 records
  AND
  -- Non-zero passengers only
  PASSENGER_COUNT IS NOT NULL AND PASSENGER_COUNT > 0 -- 170895861 records
  AND
  -- Positive net distance only
  TRIP_DISTANCE IS NOT NULL AND TRIP_DISTANCE > 0 -- 169594246 records
  AND
  -- Get non-numeric payment types
  PAYMENT_TYPE NOT RLIKE '[0-9]' -- 169593351 records
  AND
  -- Get non-zero fare records only
  FARE_AMOUNT IS NOT NULL AND FARE_AMOUNT > 0 -- 169593351 records
  AND
  -- Get non-negative tip records only
  TIP_AMOUNT IS NOT NULL AND TIP_AMOUNT >= 0 -- 169593351 records
  AND
  -- Get non-negative toll records only
  TOLLS_AMOUNT IS NOT NULL AND TOLLS_AMOUNT >= 0; -- 169593351 records

SELECT * FROM $TRAINING_DATABASE.CLEANED_NYCTAXI_YELLOW LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Here's some quick visualisation of the data we'll be working with

-- COMMAND ----------

SELECT
  DAYOFMONTH(PICKUP_DATETIME) AS DAY,
  MONTH(PICKUP_DATETIME) AS MONTH,
  FIRST(DATE_FORMAT(PICKUP_DATETIME, 'MMM')) AS MONTH_NAME,
  SUM(FARE_AMOUNT) AS TOTAL_FARE_AMOUNT
FROM $TRAINING_DATABASE.CLEANED_NYCTAXI_YELLOW
GROUP BY
    MONTH, DAY
ORDER BY
    MONTH, DAY

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Advanced SQL Concepts

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Window Functions
-- MAGIC Window functions allow you to perform calculations across a set of rows related to the current row. They are particularly useful for tasks like ranking, aggregation, and moving averages.
-- MAGIC
-- MAGIC You can refer to the official documentation for window functions in SQL:<br>
-- MAGIC [Window Functions Documentation](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-window-functions)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Ranking Window Functions
-- MAGIC
-- MAGIC Ranking window functions assign a unique rank or position to each row within a result set based on some criteria. They are primarily used to determine the rank or position of rows relative to others.
-- MAGIC <br>
-- MAGIC https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-functions-builtin#ranking-window-functions
-- MAGIC
-- MAGIC How are ranking window functions useful in terms of our dataset? Here are some obvious things we can do:
-- MAGIC - Top-N Highest Fare Trips Every Quarter:
-- MAGIC   Use a row ranking window function to assign a unique rank to each trip based on the fare amount.
-- MAGIC   Retrieve the top N trips with the highest fares every quarter, including scenarios where multiple trips have the same fare but receive different ranks.
-- MAGIC   In case or a draw/tie select the trip that started first
-- MAGIC
-- MAGIC - Most Popular Payment Types:
-- MAGIC   Rank payment types (payment_type) based on their frequency of use.
-- MAGIC   Identify the most popular payment methods among passengers.
-- MAGIC
-- MAGIC - Passenger Count Analysis:
-- MAGIC   Rank the number of passengers (passenger_count) based on the total number of trips each count has taken.
-- MAGIC   Identify the most common passenger counts and their rankings.

-- COMMAND ----------

-- DBTITLE 1,Simple Example
SELECT
  COL1,
  COL2,
  ROW_NUMBER() OVER(PARTITION BY COL1 ORDER BY COL2) AS ROW_NUMBER,
  DENSE_RANK() OVER(PARTITION BY COL1 ORDER BY COL2) AS DENSE_RANK,
  RANK() OVER(PARTITION BY COL1 ORDER BY COL2) AS RANK,
  PERCENT_RANK(COL2) OVER (PARTITION BY COL1 ORDER BY COL2) AS PERCENT_RANK,
  NTILE(3) OVER(ORDER BY COL2) AS NTILE
FROM VALUES
  ('A', 1), ('A', 2), ('A', 3), ('A', 3), ('A', 5),
  ('B', 1), ('B', 2), ('B', 2), ('B', 5), ('B', 5), ('B', 6), ('B', 7),
  ('C', 1), ('C', 2), ('C', 3)
TAB(COL1, COL2)

-- COMMAND ----------

-- DBTITLE 1,Bottom 3 Shortest Distance Trips
SELECT
  DATE_FORMAT(PICKUP_DATETIME, 'MMM') AS TRIP_MONTH,
  TRIP_DISTANCE,
  ROW_NUMBER() OVER(
    PARTITION BY MONTH(PICKUP_DATETIME)
    ORDER BY
      TRIP_DISTANCE,
      PICKUP_DATETIME DESC
  ) AS TRIP_DISTANCE_RANK,

  '=========' AS SEPARATOR,  
  * EXCEPT(TRIP_DISTANCE)
FROM $TRAINING_DATABASE.CLEANED_NYCTAXI_YELLOW
QUALIFY TRIP_DISTANCE_RANK <= 3
ORDER BY MONTH(PICKUP_DATETIME), TRIP_DISTANCE_RANK

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Exercise-1

-- COMMAND ----------

-- DBTITLE 1,Problem
-- MAGIC %md
-- MAGIC ***Busiest Hours for Taxi Trips***<br>
-- MAGIC - Rank the hours of the day (pickup_datetime) based on the number of taxi trips that occur during each hour.
-- MAGIC - Determine the 2nd and the 5th most busiest hours for taxi rides.
-- MAGIC - In case or a draw/tie select the hour in chronological order
-- MAGIC - Your solution view should have three columns:
-- MAGIC   - HOUR_OF_DAY: Self-explanatory, the hour of the day
-- MAGIC   - TRIPS: Number of trips that occur at that hour
-- MAGIC   - RANK: Rank of how busy taxis are at that hour, the higher the rank the less busy it is
-- MAGIC - Your solution should have two records, one for the 2nd bussiest and one for the 5th
-- MAGIC - Sample EXERCISE1_SOLUTION view:
-- MAGIC   | HOUR_OF_DAY | TRIPS | RANK |
-- MAGIC   |-------------|-------|------|
-- MAGIC   | 7           | 12372 | 2    |
-- MAGIC   | 23          | 34634 | 5    |
-- MAGIC - Expected schema:<br>
-- MAGIC   root<br>
-- MAGIC   &nbsp;&nbsp;|-- HOUR_OF_DAY: integer (nullable = true)<br>
-- MAGIC   &nbsp;&nbsp;|-- TRIPS: long (nullable = false)<br>
-- MAGIC   &nbsp;&nbsp;|-- RANK: integer (nullable = false)<br>

-- COMMAND ----------

-- DBTITLE 1,Solution
CREATE OR REPLACE TEMP VIEW EXERCISE1_SOLUTION AS
SELECT
  /* AND ADD YOUR CODE HERE */

-- COMMAND ----------

-- DBTITLE 1,Check Solution
-- MAGIC %python
-- MAGIC check_exercise1_solution()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Extras

-- COMMAND ----------

-- DBTITLE 1,Databricks-Datasets README
-- MAGIC %python
-- MAGIC f = open('/dbfs/databricks-datasets/README.md', 'r')
-- MAGIC print(f.read())

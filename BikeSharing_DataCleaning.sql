-- DATA CLEANING AND PREPARATION
-- PROJECT: BIKE-SHARING
-- BY: KHANH NGUYEN
-- DATE: MARCH 2022


---------------------------------------------------------------------------------------------
-- Take first look at table

select * from information_schema.columns

-- There are 23 tables from 04/2020 - 02/2022. This analyse uses the data in 12 months from 03/2021 - 02/2022
-- Merge all those tables 
-- USING CTE

WITH tripdata  AS (
	SELECT * 
	FROM [202202_tripdata]
	UNION ALL
	SELECT * 
	FROM [202201_tripdata]
	UNION ALL
	SELECT * 
	FROM [202112_tripdata]
	UNION ALL
	SELECT * 
	FROM [202111_tripdata]
	UNION ALL
	SELECT * 
	FROM [202110_tripdata]
	UNION ALL
	SELECT * 
	FROM [202109_tripdata]
	UNION ALL
	SELECT * 
	FROM [202108_tripdata]
	UNION ALL
	SELECT * 
	FROM [202107_tripdata]
	UNION ALL
	SELECT * 
	FROM [202106_tripdata]
	UNION ALL
	SELECT * 
	FROM [202105_tripdata]
	UNION ALL
	SELECT * 
	FROM [202104_tripdata]
	UNION ALL
	SELECT * 
	FROM [202103_tripdata]
	UNION ALL
	SELECT * 
	FROM [202102_tripdata]
)
SELECT *
INTO tripdata_merge
FROM tripdata

---------------------------------------------------------------------------------------------

-- Check this table


SELECT * FROM information_schema.columns
WHERE table_name = 'tripdata_merge'

---------------------------------------------------------------------------------------------
-- Now cleaning the tripdata_merge
-- CHECK DUPLICATE

SELECT *
FROM tripdata_merge

WITH RowNumCTE AS (
	SELECT *, ROW_NUMBER() OVER(
	PARTITION BY ride_id, started_at, start_station_id
	ORDER BY ride_id) AS row_num 
	FROM tripdata_merge)
SELECT *
FROM RowNumCTE
ORDER BY started_at

-- There is no row_num > 1 => There is no duplicate

---------------------------------------------------------------------------------------------
-- Populate Property Address Data (using start_lat and start_lng)
SELECT COUNT(*)
FROM tripdata_merge
WHERE start_station_name IS NULL

-- There are 717024 null value for start_station_name
-- Update start_station_name if there are available name with the same lattitude and longtitude

SELECT t1.ride_id, t1.start_station_name, t2.start_station_name, t1.start_lat, t1.start_lng, ISNULL(t1.start_station_name,t2.start_station_name)
FROM tripdata_merge t1
INNER JOIN tripdata_merge t2
	ON t1.start_lat = t2.start_lat AND t1.start_lng = t2.start_lng
	AND t1.ride_id <> t2.ride_id
WHERE t1.start_station_name IS NULL AND t2.start_station_name IS NOT NULL

SELECT DISTINCT t1.ride_id, t1.start_station_name, t2.start_station_name, t1.start_lat, t1.start_lng, ISNULL(t1.start_station_name,t2.start_station_name)
FROM tripdata_merge t1
INNER JOIN tripdata_merge t2
	ON t1.start_lat = t2.start_lat AND t1.start_lng = t2.start_lng
	AND t1.ride_id <> t2.ride_id
WHERE t1.start_station_name IS NULL AND t2.start_station_name IS NOT NULL

UPDATE t1
SET t1.start_station_name = ISNULL(t1.start_station_name,t2.start_station_name)
FROM tripdata_merge t1
INNER JOIN tripdata_merge t2
	ON t1.start_lat = t2.start_lat AND t1.start_lng = t2.start_lng
	AND t1.ride_id <> t2.ride_id
WHERE t1.start_station_name IS NULL AND t2.start_station_name IS NOT NULL

-- After populate 
SELECT COUNT(*)
FROM tripdata_merge
WHERE start_station_name IS NULL

-- Do the same thing for end station name
SELECT DISTINCT t1.ride_id, t1.end_station_name, t2.end_station_name, t1.end_lat, t1.end_lng, ISNULL(t1.end_station_name,t2.end_station_name)
FROM tripdata_merge t1
INNER JOIN tripdata_merge t2
	ON t1.end_lat = t2.end_lat AND t1.end_lng = t2.end_lng
	AND t1.ride_id <> t2.ride_id
WHERE t1.end_station_name IS NULL AND t2.end_station_name IS NOT NULL

UPDATE t1
SET t1.end_station_name = ISNULL(t1.end_station_name,t2.end_station_name)
FROM tripdata_merge t1
INNER JOIN tripdata_merge t2
	ON t1.end_lat = t2.end_lat AND t1.end_lng = t2.end_lng
	AND t1.ride_id <> t2.ride_id
WHERE t1.end_station_name IS NULL AND t2.end_station_name IS NOT NULL


-- Exclude the missing value rows (start_station_name or end_station_name IS NULL)

SELECT * FROM tripdata_merge
WHERE start_station_name IS NOT NULL AND end_station_name IS NOT NULL;

-- There are 4902546 rows

-- Check rideable_type (to make sure there are 3 types only) -- DONE

SELECT distinct rideable_type
FROM tripdata_merge

-- Check the len(ride_id) -- DONE

SELECT DISTINCT len(ride_id), COUNT(*)
FROM tripdata_merge
GROUP BY len(ride_id)

-- Check the time of the ride - make sure started_at < ended_at

SELECT * FROM tripdata_merge
WHERE start_station_name IS NOT NULL AND end_station_name IS NOT NULL AND started_at > ended_at;

-- Delete 122 rows which there are errors in the time
DELETE FROM tripdata_merge
	WHERE start_station_name IS NOT NULL AND end_station_name IS NOT NULL AND started_at > ended_at;
---------------------------------------------------------------------------------------------
-- After make sure the data is cleaned. I continue to explore the data
-- Data Exploration

-- Querry the top 10 start_station

SELECT DISTINCT TOP 10 (start_station_name), COUNT(*) numride
FROM tripdata_merge
WHERE start_station_name IS NOT NULL AND end_station_name IS NOT NULL
GROUP BY start_station_name
ORDER BY numride DESC;

-- Querry the top 10 end_station

SELECT DISTINCT TOP 10 (end_station_name), COUNT(*) numride
FROM tripdata_merge
WHERE start_station_name IS NOT NULL AND end_station_name IS NOT NULL
GROUP BY end_station_name
ORDER BY numride DESC;

-- Get the number of ride by month and cumulative percentage (can filter the member or casual riders)
SELECT month(started_at) as month, COUNT(*) numride, 
		SUM(COUNT(*)) OVER (ORDER BY COUNT(*) DESC) AS runningtotal, 
		(SUM(COUNT(*)) OVER (ORDER BY COUNT(*) DESC)*100/(SELECT COUNT(*) FROM tripdata_merge WHERE start_station_name IS NOT NULL AND end_station_name IS NOT NULL /* AND member_casual = 'member' */ )) AS percentage
FROM tripdata_merge
WHERE start_station_name IS NOT NULL AND end_station_name IS NOT NULL -- AND member_casual = 'member'
GROUP BY MONTH(started_at)
ORDER BY COUNT(*) DESC;

-- Get the percentage of ride by month using CTE
/* WITH monthride AS (
SELECT month(started_at) as month, COUNT(*) numride
FROM tripdata_merge
WHERE start_station_name IS NOT NULL AND end_station_name IS NOT NULL
GROUP BY MONTH(started_at))

SELECT *,(SUM(numride) OVER (ORDER BY month)),
(SUM(numride) OVER ()) AS totalride, 
CONVERT(numeric(18,2),100*(SUM(numride) OVER (ORDER BY month)))/CONVERT(numeric(18,2),(SUM(numride) OVER ()))
FROM monthride m
ORDER BY month */

-- Get the number of ride by weekday and cumulative percentage (can filter the member or casual riders)
SELECT DATENAME(dw,started_at) as weekday, COUNT(*) numride, 
		SUM(COUNT(*)) OVER (ORDER BY COUNT(*) DESC) AS runningtotal, 
		(SUM(COUNT(*)) OVER (ORDER BY COUNT(*) DESC)*100/(SELECT COUNT(*) FROM tripdata_merge WHERE start_station_name IS NOT NULL AND end_station_name IS NOT NULL )) AS percentage
FROM tripdata_merge
WHERE start_station_name IS NOT NULL AND end_station_name IS NOT NULL
GROUP BY DATENAME(dw,started_at)
ORDER BY COUNT(*) DESC;


-- Get the number of ride by hourday and cumulative percentage (can filter the member or casual riders)
SELECT DATENAME(HH,started_at) as hourofday, COUNT(*) numride, 
		SUM(COUNT(*)) OVER (ORDER BY COUNT(*) DESC) AS runningtotal, 
		(SUM(COUNT(*)) OVER (ORDER BY COUNT(*) DESC)*100/(SELECT COUNT(*) FROM tripdata_merge WHERE start_station_name IS NOT NULL AND end_station_name IS NOT NULL )) AS percentage
FROM tripdata_merge
WHERE start_station_name IS NOT NULL AND end_station_name IS NOT NULL
GROUP BY DATENAME(HH,started_at)
ORDER BY COUNT(*) DESC;


-- Get the number of ride by rideable type and cumulative percentage (can filter the member or casual riders)
SELECT rideable_type, COUNT(*) numride, 
		SUM(COUNT(*)) OVER (ORDER BY COUNT(*) DESC) AS runningtotal, 
		(SUM(COUNT(*)) OVER (ORDER BY COUNT(*) DESC)*100/(SELECT COUNT(*) FROM tripdata_merge WHERE start_station_name IS NOT NULL AND end_station_name IS NOT NULL )) AS percentage
FROM tripdata_merge
WHERE start_station_name IS NOT NULL AND end_station_name IS NOT NULL
GROUP BY rideable_type
ORDER BY COUNT(*) DESC;

--  Get the number of ride by casual_member and cumulative percentage

SELECT member_casual, COUNT(*) numride, 
		SUM(COUNT(*)) OVER (ORDER BY COUNT(*) DESC) AS runningtotal, 
		(SUM(COUNT(*)) OVER (ORDER BY COUNT(*) DESC)*100/(SELECT COUNT(*) FROM tripdata_merge WHERE start_station_name IS NOT NULL AND end_station_name IS NOT NULL )) AS percentage
FROM tripdata_merge
WHERE start_station_name IS NOT NULL AND end_station_name IS NOT NULL
GROUP BY member_casual
ORDER BY COUNT(*) DESC;

-- Add ride_length column

SELECT  *, CAST(DATEDIFF(minute, started_at, ended_at) AS decimal(38,2)) AS ride_length
FROM tripdata_merge
WHERE start_station_name IS NOT NULL AND end_station_name IS NOT NULL
ORDER BY ride_length DESC
;

ALTER TABLE tripdata_merge
ADD  ride_length decimal(38,2);

UPDATE tripdata_merge
SET ride_length = CAST(DATEDIFF(minute, started_at, ended_at) AS decimal(38,2))

SELECT * FROM tripdata_merge
WHERE start_station_name IS NOT NULL AND end_station_name IS NOT NULL

-- Calculate average ride_length by rideable_type
SELECT rideable_type, AVG(ride_length) avg_ridelength
FROM tripdata_merge
WHERE start_station_name IS NOT NULL AND end_station_name IS NOT NULL
GROUP BY rideable_type
ORDER BY avg_ridelength


-- Calculate average ride_length by member type
SELECT member_casual, AVG(ride_length) avg_ridelength
FROM tripdata_merge
WHERE start_station_name IS NOT NULL AND end_station_name IS NOT NULL
GROUP BY member_casual
ORDER BY avg_ridelength

-- Calculate average ride_length by member type and day of week
SELECT member_casual,DATENAME(dw,started_at) weekday, AVG(ride_length) avg_ridelength
FROM tripdata_merge
WHERE start_station_name IS NOT NULL AND end_station_name IS NOT NULL
GROUP BY member_casual, DATENAME(dw,started_at)
ORDER BY avg_ridelength

-- Calculate average ride_length by member type and month
SELECT member_casual,DATENAME(month,started_at) month, AVG(ride_length) avg_ridelength
FROM tripdata_merge
WHERE start_station_name IS NOT NULL AND end_station_name IS NOT NULL
GROUP BY member_casual, DATENAME(MONTH,started_at)
ORDER BY avg_ridelength

-- -- Calculate average ride_length by member type and hour of day
SELECT member_casual,DATEPART(hour,started_at) hour, AVG(ride_length) avg_ridelength
FROM tripdata_merge
WHERE start_station_name IS NOT NULL AND end_station_name IS NOT NULL
GROUP BY member_casual, DATEPART(hour,started_at)
ORDER BY avg_ridelength


-- With those information, it is possible to design a picture of annual member type of the company
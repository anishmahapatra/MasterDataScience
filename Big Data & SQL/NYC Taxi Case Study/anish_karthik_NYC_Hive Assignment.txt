-- NYC Hive Case Study
-- Author: Anish Mahapatra, Karthik Premanand
-- Email: anishmahapatra01@gmail.com, karthikprem26@gmail.com


-- ############################ Loading and Importing the Data ############################ 


-- NYC Hive Case Study
-- Author: Anish Mahapatra, Karthik Premanand
-- Email: anishmahapatra01@gmail.com, karthikprem26@gmail.com
 -- Adding the required files to process in HIVE (hue)
ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-hcatalog-core-1.1.0-cdh5.11.2.jar;

SET hive.exec.max.dynamic.partitions=100000;

SET hive.exec.max.dynamic.partitions.pernode=100000;

-- Creating a workspace to perform required tasks on

CREATE DATABASE IF NOT EXISTS anish_assignment;

USE anish_assignment;


-- Dropping table if it exists

DROP TABLE if exists anish_assignment.anish_data;

-- Creating a new External Table in HIVE (So that multiple people can work on it)

-- Dropping table if it exists

DROP TABLE IF EXISTS anish_assignment.anish_data;

-- Creating a new External Table in HIVE (So that multiple people can work on it)

CREATE EXTERNAL TABLE IF NOT EXISTS anish_assignment.anish_data
(
VendorID int, tpep_pickup_datetime TIMESTAMP,
tpep_dropoff_datetime TIMESTAMP,
passenger_count int, trip_distance decimal(10,2),
RatecodeID int, store_and_fwd_flag string,
PULocationID int, DOLocationID int, payment_type int, 
fare_amount decimal(10,2),
extra decimal(10,2),
mta_tax decimal(10,2),
tip_amount decimal(10,2),
tolls_amount decimal(10,2),
improvement_surcharge decimal(10,2),
total_amount decimal(10,2) 
) -- Using the NYC data from nyc_taxi_data into

 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
 STORED AS TEXTFILE LOCATION '/common_folder/nyc_taxi_data/' 
 tblproperties ("skip.header.line.count"="2");


-- Sanity check of the new dataset

SELECT *
FROM anish_assignment.anish_data ANI;


-- Viewing the count of the number of rows from anish_data column

SELECT count(*)
FROM anish_assignment.anish_data ANI;
-- 1174568 records



-- ############################ Sanity and Quality Check on the data ############################ 

-- 1.How many records has each TPEP provider provided? 
--   Write a query that summarizes the number of records of each provider.

SELECT vendorid,
       count(*)
FROM anish_assignment.anish_data ANI
GROUP BY vendorid;

-- 1	527385
-- 2 	647183

-- Vendor 1 has provided 527385 records and Vendor 2 has provided 647183 number fo records
-- 55% of the data belongs to Vendor 2



-- 2. The data provided is for months November and December only. 
--    Check whether the data is consistent, and if not, identify the data quality issues. 
--    Mention all data quality issues in comments.

-- Let us proceed to check and understand if the Data is within the defined range of November and December
-- tpep_pickup_datetime signifies the The date and time when the meter was engaged, so we shall choose it accordingly

-- Selecting the number of records that are out of range

SELECT count(*)
FROM anish_assignment.anish_data ANI
WHERE ANI.tpep_pickup_datetime < '2017-11-1 00:00:00.0'
  OR ANI.tpep_pickup_datetime>='2018-01-01 00:00:00.0';

-- Here, we have 14 records that are out of the defined range of November and December

SELECT vendorid,
       count(*)
FROM anish_assignment.anish_data ANI
WHERE ANI.tpep_pickup_datetime < '2017-11-1 00:00:00.0'
  OR tpep_pickup_datetime>='2018-01-01 00:00:00.0'
GROUP BY vendorid;

-- tpep_pickup_datetime signifies the The date and time when the meter was engaged
-- All 14 records are from Vendor 2

-- We know that drop-off time (tpep_dropoff_datetime) cannot be greater or equal too pick-up time (tpep_pickup_datetime)

SELECT count(*)
FROM anish_assignment.anish_data ANI
WHERE ANI.tpep_dropoff_datetime<=ANI.tpep_pickup_datetime;

-- 6555 records are such where drop off time is greater than pick up time - these records are faulty

-- Let us know understand the patterns of the customers and how they take cabs #passenger count

SELECT passenger_count,
       count(*)
FROM anish_assignment.anish_data ANI
GROUP BY passenger_count;

-- 0	6824
-- 1	827498
-- 2	176872
-- 3	50693
-- 4	24951
-- 5	54568
-- 6	33146
-- 7	12
-- 8	3

-- We notice that a majority of trips have been taken with 1 person in the cab, followed by 2 - after a sharp decline in numbers

-- Max trip and min trip distance
SELECT max(ANI.trip_distance),
       min(ANI.trip_distance)
FROM anish_assignment.anish_data ANI;

-- We know that trip_distance cannot be negative, Let us have a look at the stats for the given dataset
-- trip_distance: The elapsed trip distance in miles reported by the taximeter.
-- max_trip_distance: 126.41
-- min_trip_distance: 0

-- The number of trips where distance is <=0
SELECT count(*)
FROM anish_assignment.anish_data ANI
WHERE trip_distance<=0;

-- 7402

-- #store_and_fwd_flag: This flag indicates whether the trip record was held in vehicle memory before 
-- sending to the vendor, aka “store and forward,” because the vehicle did not have a connection to the server.
-- Y= store and forward trip
-- N= not a store and forward trip

SELECT store_and_fwd_flag,
       count(*)
FROM anish_assignment.anish_data ANI
GROUP BY store_and_fwd_flag;

-- N	1170617
-- Y	3951
-- This distribution of Y and N looks alright

-- fare_amount: The time-and-distance fare calculated by the meter
SELECT max(ANI.fare_amount),
       min(ANI.fare_amount)
FROM anish_assignment.anish_data ANI;

-- Max Fare: 650
-- Min Fare: -200

-- It does not make sense how a fare can be negative, this must be an outlier

-- Let us understand how many such negative fare_amount are present in the data

SELECT count(*)
FROM anish_assignment.anish_data ANI
WHERE ANI.fare_amount < 0;

-- 558 negative values (small number, can be ignored)
-- The max fare_amount 650 seems sensible, so we will not investigate further there

-- Let us understand the vendors responsible for outliers
SELECT vendorid,
       count(*)
FROM anish_assignment.anish_data ANI
WHERE fare_amount>600
  OR fare_amount<0
GROUP BY vendorid;

-- 	Vendor 1		1
-- 	Vendor 2		558
-- Vendor 2 is the one here that has the most discrepancy here

-- #extra: Miscellaneous extras and surcharges. Currently, this only includes the $0.50 and $1 rush hour and overnight charges
SELECT max(ANI.extra),
       min(ANI.extra)
FROM anish_assignment.anish_data ANI;

-- Max(extra):4.8
-- Min(extra):-10.6
-- Currently, per understanding from the data dictionary, the values should only include ($0, $0.5 and overnight charges (not mentioned clearly how much this is))
-- This surcharge may be a one-time fee or per km, again this is not explicitly mentioned
-- But, the negative extra charges seem to be faulty, let us understand the trends there by vendorid

SELECT count(*)
FROM anish_assignment.anish_data ANI
WHERE ANI.extra<0
GROUP BY vendorid;

-- Vendor 1 has 285 negative values of extra
-- Vendor 2 has 1 negative value of extra
-- Very small % of the total number of records, but Vendor 1 seems to have discrepancy here

-- mta_tax: $0.50 MTA tax that is automatically triggered based on the metered rate in use.
SELECT max(ANI.mta_tax),
       min(ANI.mta_tax)
FROM anish_assignment.anish_data ANI;
-- Max(mta_tax): 11.4
-- Min(mta_tax): -0.5

-- Let us look at the negative values of MTA and understand
SELECT vendorid, count(*)
FROM anish_assignment.anish_data ANI
where ANI.mta_tax<0
group by vendorid;

-- We have 544 values where mta_tax is negative (Vendor 2 is at fault for all of this)


-- tip_amount: This field is automatically populated for credit card tips. Cash tips are not included.
SELECT max(ANI.tip_amount),
       min(ANI.tip_amount)
FROM anish_assignment.anish_data ANI;

-- Max(tip_amount): 450
-- Min(tip_amount): -1.16

-- Negative tip_amount does not make sense, let us analyze what the case is and by whom
SELECT vendorid, count(*)
FROM anish_assignment.anish_data ANI
where ANI.tip_amount < 0
group by vendorid;

-- 4 tip_amount values are negative and all of them are from vendor 2

-- Let us now check if they are not credit-card based
SELECT vendorid, count(*)
FROM anish_assignment.anish_data ANI
WHERE Payment_type!=1
  AND tip_amount>0
GROUP BY vendorid;

-- 17 records are non-credit based, where the tip amount is legitimate

-- tolls_amount: Total amount of all tolls paid in trip
-- Let us understand the max and min tolls paid
SELECT max(ANI.tolls_amount),
       min(ANI.tolls_amount)
FROM anish_assignment.anish_data ANI;

-- Max(tolls_amount): 895.89
-- Min(tolls_amount): -5.76

-- Toll value cannot be negative
SELECT vendorid, count(*)
FROM anish_assignment.tolls_amount ANI
where ANI.tolls_amount < 0
group by vendorid;

-- Vendor 2 has 3 values where the toll_amount is negative. Vendor 2 is at fault.




-- improvement_surcharge: $0.30 improvement surcharge assessed trips at the flag drop. 
-- The improvement surcharge began being levied in 2015.
SELECT max(ANI.improvement_surcharge),
       min(ANI.improvement_surcharge)
FROM anish_assignment.anish_data ANI;

-- Max(improvement_surcharge): 1
-- Min(improvement_surcharge): -0.3

-- Improvement surcharge should either be 0 or 0.3 per the data dictionary
SELECT count(*)
FROM anish_assignment.anish_data ANI
WHERE improvement_surcharge NOT IN (0, 0.3);

-- We have 562 values that are not within the defined range
SELECT vendorid,
       count(*)
FROM anish_assignment.anish_data ANI
WHERE improvement_surcharge NOT IN (0, 0.3)
GROUP BY vendorid;
-- All the 562 records belong to Vendor 2



-- total_amount: The total amount charged to passengers. Does not include cash tips.
SELECT max(ANI.total_amount),
       min(ANI.total_amount)
FROM anish_assignment.anish_data ANI;

-- Max (total_amount): 928.19
-- Min (total_amount): -200.8

SELECT vendorid, count(*)
FROM anish_assignment.anish_data ANI
WHERE total_amount<0
group by vendorid;

-- All the 558 records are from Vendor 2

-- ##Basic Data Quality Checks
-- Q3. You might have encountered unusual or erroneous rows in the dataset. 
-- Can you conclude which vendor is doing a bad job in providing the records using different columns of the dataset? 
-- Summarize your conclusions based on every column where these errors are present.

-- Post the above analysis, we observe that VENDOR 2 is the one that is mostly at fault.
-- Vendor 2 is doing a bad job and is providing erroneous data.
-- They provided invalid values for total_amount, improvement_surcharge, tolls_amount, tip_amount, mta_tax, 
-- fare_amount, passenger_count, pickup and drop off time
-- The feature "extra", both Vendor 1 and Vendor 2 are at fault. Vendor 1 has a couple of tip amounts, where the method of 
-- payment is not credit card.
-- However, as we can see in the Analysis above, Vendor 2 is not providing accurate data  (per the data dictionary)


-- With this, Basic Data Quality Checks have been completed

-- Cleaning the data

SELECT sum(CASE
               WHEN VendorID IS NULL THEN 1
               ELSE 0
           END) VendorID,
       sum(CASE
               WHEN tpep_pickup_datetime IS NULL THEN 1
               ELSE 0
           END) tpep_pickup_datetime,
       sum(CASE
               WHEN tpep_dropoff_datetime IS NULL THEN 1
               ELSE 0
           END) tpep_dropoff_datetime,
       sum(CASE
               WHEN passenger_count IS NULL THEN 1
               ELSE 0
           END) passenger_count,
       sum(CASE
               WHEN trip_distance IS NULL THEN 1
               ELSE 0
           END) trip_distance,
       sum(CASE
               WHEN RatecodeID IS NULL THEN 1
               ELSE 0
           END) RatecodeID,
       sum(CASE
               WHEN store_and_fwd_flag IS NULL THEN 1
               ELSE 0
           END) store_and_fwd_flag,
       sum(CASE
               WHEN PULocationID IS NULL THEN 1
               ELSE 0
           END) PULocationID,
       sum(CASE
               WHEN DOLocationID IS NULL THEN 1
               ELSE 0
           END) DOLocationID,
       sum(CASE
               WHEN payment_type IS NULL THEN 1
               ELSE 0
           END) payment_type,
       sum(CASE
               WHEN fare_amount IS NULL THEN 1
               ELSE 0
           END) fare_amount,
       sum(CASE
               WHEN extra IS NULL THEN 1
               ELSE 0
           END) extra,
       sum(CASE
               WHEN mta_tax IS NULL THEN 1
               ELSE 0
           END) mta_tax,
       sum(CASE
               WHEN tip_amount IS NULL THEN 1
               ELSE 0
           END) tip_amount,
       sum(CASE
               WHEN tolls_amount IS NULL THEN 1
               ELSE 0
           END) tolls_amount,
       sum(CASE
               WHEN improvement_surcharge IS NULL THEN 1
               ELSE 0
           END) improvement_surcharge,
       sum(CASE
               WHEN total_amount IS NULL THEN 1
               ELSE 0
           END) total_amount
FROM anish_assignment.anish_data ANI;

-- All the values are 0s

-- Let us now look at the range of all the columns

SELECT max(VendorID) max_VendorID,
       min(VendorID) min_VendorID,
       max(tpep_pickup_datetime) max_tpep_pickup_datetime,
       min(tpep_pickup_datetime) min_tpep_pickup_datetime,
       max(tpep_dropoff_datetime) max_tpep_dropoff_datetime,
       min(tpep_dropoff_datetime) min_tpep_dropoff_datetime,
       max(passenger_count) max_passenger_count,
       min(passenger_count) min_passenger_count,
       max(trip_distance) max_trip_distance,
       min(trip_distance) min_trip_distance,
       max(RatecodeID) max_RatecodeID,
       min(RatecodeID) min_RatecodeID,
       max(store_and_fwd_flag) max_store_and_fwd_flag,
       min(store_and_fwd_flag) min_store_and_fwd_flag,
       max(PULocationID) max_PULocationID,
       min(PULocationID) min_PULocationID,
       max(DOLocationID) max_DOLocationID,
       min(DOLocationID) min_DOLocationID,
       max(payment_type) max_payment_type,
       min(payment_type) min_payment_type,
       max(fare_amount) max_fare_amount,
       min(fare_amount) min_fare_amount,
       max(extra) max_extra,
       min(extra) min_extra,
       max(mta_tax) max_mta_tax,
       min(mta_tax) min_mta_tax,
       max(tip_amount) max_tip_amount,
       min(tip_amount) min_tip_amount,
       max(tolls_amount) max_tolls_amount,
       min(tolls_amount) min_tolls_amount,
       max(improvement_surcharge) max_improvement_surcharge,
       min(improvement_surcharge) min_improvement_surcharge,
       max(total_amount) max_total_amount,
       min(total_amount) min_total_amount
FROM anish_assignment.anish_data ANI;

-- max_vendorid	min_vendorid	max_tpep_pickup_datetime	min_tpep_pickup_datetime	max_tpep_dropoff_datetime	min_tpep_dropoff_datetime	max_passenger_count	min_passenger_count	max_trip_distance	min_trip_distance	max_ratecodeid	min_ratecodeid	max_store_and_fwd_flag	min_store_and_fwd_flag	max_pulocationid	min_pulocationid	max_dolocationid	min_dolocationid	max_payment_type	min_payment_type	max_fare_amount	min_fare_amount	max_extra	min_extra	max_mta_tax	min_mta_tax	max_tip_amount	min_tip_amount	max_tolls_amount	min_tolls_amount	max_improvement_surcharge	min_improvement_surcharge	max_total_amount	min_total_amount
-- 2			1				2018-01-01 00:04:00.0		2003-01-01 00:58:00.0		2019-04-24 19:21:00.0		2003-01-01 01:28:00.0		9					0					126.41				0					99				1				Y						N						265					1					265					1					4					1					650				-200			4.8			-10.6		11.4		-0.5		450				-1.16			895.89				-5.76				1							-0.3						928.19				-200.8

-- The columns that match the data dictionary are as follows:
-- pulocationid, dolocationid, pickup and drop location that range from 1 to 265
-- Another thing to note is that payment_type in data is spread between 1-4 



use anish_assignment;

-- Before answering the below questions, you need to create a clean, ORC partitioned table for analysis.
-- Remove all the erroneous rows.

-- We shall partition the given data on the month column. Post answering the questions pertinent to that, we shall perform a second partition.
-- The second partition is based on the vendor.at the table does not already exist

-- Dropping the table to ensure th

DROP TABLE anish_assignment.ParOrc_Data_anish;

-- Creating the required table

CREATE EXTERNAL TABLE IF NOT EXISTS anish_assignment.ParOrc_Data_anish
(
	tpep_pickup_datetime TIMESTAMP,
	tpep_dropoff_datetime TIMESTAMP,
	passenger_count int, 
	trip_distance decimal(10,2),
	RatecodeID int, 
	store_and_fwd_flag string,
	PULocationID int, 
	DOLocationID int, 
	payment_type int, 
	fare_amount decimal(10,2),
	extra decimal(10,2),
	mta_tax decimal(10,2),
	tip_amount decimal(10,2),
	tolls_amount decimal(10,2),
	improvement_surcharge decimal(10,2),
	total_amount decimal(10,2)) 
partitioned BY (Mnth int,VendorID int) stored AS orc 
LOCATION '/user/anishmahapatra01_gmail/Assignment_NYC_2' 
tblproperties ("orc.compress"="SNAPPY");

-- Posting data
INSERT overwrite TABLE anish_assignment.ParOrc_Data_anish partition(Mnth,VendorID)
SELECT tpep_pickup_datetime,
       tpep_dropoff_datetime,
       passenger_count,
       trip_distance,
       RatecodeID,
       store_and_fwd_flag,
       PULocationID,
       DOLocationID,
       payment_type,
       fare_amount,
       extra,
       mta_tax,
       tip_amount,
       tolls_amount,
       improvement_surcharge,
       total_amount,
       month(tpep_pickup_datetime) Mnth,
       VendorID
FROM anish_assignment.anish_data ANI
WHERE (ANI.tpep_pickup_datetime >='2017-11-1 00:00:00.0' AND tpep_pickup_datetime<'2018-01-01 00:00:00.0')
  AND (ANI.tpep_dropoff_datetime >= '2017-11-1 00:00:00.0' AND tpep_dropoff_datetime<'2018-01-02 00:00:00.0')
  AND (ANI.tpep_dropoff_datetime>ANI.tpep_pickup_datetime)
  AND (passenger_count NOT IN (0,192))
  AND (trip_distance>0)
  AND (ratecodeid!=99)
  AND (fare_amount>0)
  AND (extra IN (0, 0.5, 1))
  AND (mta_tax IN (0, 0.5))
  AND ((tip_amount >=0 AND Payment_type=1) OR (Payment_type!=1 AND tip_amount=0))
  AND (tolls_amount >=0)
  AND (improvement_surcharge IN (0, 0.3))
  AND (total_amount>0);



SELECT count(*)
FROM anish_assignment.ParOrc_Data_anish;
-- 1153586

SELECT 1174568 - 1153586;
-- 20982 were removed

SELECT (20982/ 1174568)*100;
-- amounting to 1.786% of data


-- #End of creation of the table

-- Let us now begin the Analysis as required by the data

-- #Analysis-I:

-- 1. Compare the overall average fare per trip for November and December.
SELECT mnth,
       round(avg(total_amount),2),
       round(avg(fare_amount),2)
FROM anish_assignment.ParOrc_Data_anish
GROUP BY mnth;

-- Month 	Avg(Total_Amount)	Avg(fare_amount)
-- 11		16.19				12.91
-- 12		15.89				12.7

SELECT 16.19-15.89,
            12.91-12.7;
-- 0.3, 0.2

-- On Average, the month of November seems to be better with regards to the total amount.
-- November has a higher Average fare amount too (by $0.2)
-- This could be sure to the extra taxes and charges are effective from the month of November

-- 2. Explore the ‘number of passengers per trip’ - how many trips are made by each level of ‘Passenger_count’? 
--   	Do most people travel solo or with other people?

-- Let us look at the count of passengers
SELECT passenger_count,
       (count(*)) cnt
FROM anish_assignment.ParOrc_Data_anish
GROUP BY passenger_count
ORDER BY cnt DESC;

-- Let us get the percentages as well

SELECT passenger_count,
       round((count(*)*100/1120704),2) cnt
FROM anish_assignment.ParOrc_Data_anish
GROUP BY passenger_count
ORDER BY cnt DESC;

-- 1	72.90
-- 2	15.59
-- 5	4.82
-- 3	4.48
-- 6	2.93
-- 4	2.20
-- 7	0.00

-- Rides with a single person are the most common. They account for about 73% of all rides.
-- The next significant category is rides with two people, that accounts for about 15.5% of all rides.
-- It is interesting to note that rides with 5 people is higher than rides with 3 people by about 0.4%, where both account for about 4.5% each
-- The rest of the rides are with 4, 6 and 7 folks, accounting for under 5% f the rest of the occupancy

-- Most people prefer traveling solo


-- 3. Which is the most preferred mode of payment?

SELECT payment_type,
       round((count(*)*100/1120704),2) cnt
FROM anish_assignment.ParOrc_Data_anish
GROUP BY payment_type
ORDER BY cnt DESC;

1	69.52 	(Credit Card)
2	32.9	(Cash)
3	0.4		(No Charge)
4	0.12	(Dispute)

-- Here, we notice that payments Credit cards is the most dominant with 69.5% and cash payments are the second highest with 32.9%
-- The rest of the modes of payment are insignificant.
-- As mentioned previously, methods of payment of 5 and 6 are non-existent

-- 4. What is the average tip paid per trip? 
--		Compare the average tip with the 25th, 50th and 75th percentiles and comment whether the ‘average tip’ is a representative statistic (of the central tendency) of ‘tip amount paid’. Hint: You may use percentile_approx(DOUBLE col, p): Returns an approximate pth percentile of a numeric column (including floating point types) in the group.
SELECT round(avg(tip_amount),2)
FROM anish_assignment.ParOrc_Data_anish;
-- 1.83
SELECT percentile_approx(tip_amount,array(0.25,0.40,0.45,0.50,0.60,0.65,0.75))
FROM anish_assignment.ParOrc_Data_anish;

--  25%, 	40%, 	45%, 	50%,  	60%,  	65%,  	75%
-- 	0.0,	1.0,	1.15,	1.36,	1.76,	1.997,	2.45

-- If we notice the tips and the distribution of it, we notice that the data is skewed more the higher side (as compared to $1.83)
-- 25% or more values paying zero tip do play a high part in this behavior

-- The median $1.36 is much lower then the avg 1.82 due to the skewness towards higher values
-- Thus, the mean is not an accurate representation of the central tendency here.
-- It would make more sense to use median, instead of mean for this analysis


-- 5. Explore the ‘Extra’ (charge) variable - what fraction of total trips have an extra charge is levied?
SELECT extra,
       round((count(*)*100/1120704),2) cnt
FROM
  (SELECT CASE
              WHEN extra>0 THEN 1
              ELSE 0
          END extra
   FROM anish_assignment.ParOrc_Data_anish) T
GROUP BY extra
ORDER BY cnt DESC;

-- Extra applied    Percentage records
--      0	        55.43 %
--      1	        47.5%

-- We have a fairly even with 47.5% records having extra charges applied, and 55.43% have no extra charges applied

-- # Analysis - II

-- 1. What is the correlation between the number of passengers on any given trip, and the tip paid per trip? 
-- 		Do multiple travelers tip more compared to solo travelers? Hint: Use CORR(Col_1, Col_2)

SELECT round(corr(passenger_count, tip_amount),4)
FROM anish_assignment.ParOrc_Data_anish;

-- -0.0053

-- There is a small negative correlation, but the correlation is negligible
-- Passenger count is unrelated to the tip amount paid.

SELECT round(corr(is_solo, tip_amount),4)
FROM
  (SELECT CASE
              WHEN passenger_count=1 THEN 1
              ELSE 0
          END is_solo,
          tip_amount
   FROM anish_assignment.ParOrc_Data_anish) T;

-- 0.0062, even if we compare single vs multiple riders, there is still very low correlation

SELECT is_solo,
       round(avg(tip_amount),2)
FROM
  (SELECT CASE
              WHEN passenger_count=1 THEN 1
              ELSE 0
          END is_solo,
          tip_amount
   FROM anish_assignment.ParOrc_Data_anish) T
GROUP BY is_solo;

--  0	1.80
--	1	1.84
-- The values are almost same


-- 2. Segregate the data into five segments of ‘tip paid’: [0-5), [5-10), [10-15) , [15-20) and >=20. 
-- 		Calculate the percentage share of each bucket (i.e. the fraction of trips falling in each bucket).

SELECT Tip_range,
       round((count(*)*100/1120704),2) cnt
FROM
  (SELECT CASE
              WHEN (tip_amount>=0
                    AND tip_amount<5) THEN '[0-5)'
              WHEN (tip_amount>=5
                    AND tip_amount<10) THEN '[5-10)'
              WHEN (tip_amount>=10
                    AND tip_amount<15) THEN '[10-15)'
              WHEN (tip_amount>=15
                    AND tip_amount<20) THEN '[15-20)'
              WHEN (tip_amount>=20) THEN '>=20'
          END Tip_range
   FROM anish_assignment.ParOrc_Data_anish) T
GROUP BY Tip_range
ORDER BY cnt DESC;

-- [0-5)	95.11
-- [5-10)	5.8
-- [10-15)	1.73
-- [15-20)	0.19
-- >=20	0.09

-- Tips within the [0-5) range are the most prominent Group with about 95% of all records, where we already know that over 25% of the values are 0.
-- The [5-10) bracket represents a small fraction of 5.8%, and the remainder of the groups are almost negligible and amount to about 2% of data


-- 3. Which month has a greater average ‘speed’ - November or December? 
-- 		Note that the variable ‘speed’ will have to be derived from other metrics. 
-- 		Hint: You have columns for distance and time.


-- We shall calculate the duration by subtracting the pick up time and the drop time. 
-- We will use the unix_timestamp function for the same
-- The values will be returned in seconds, so, we will divide it by 3600 to get the desired values in hour
-- The distance is specified in miles, so the final value will be in miles/hour

SELECT mnth,
       round(avg(trip_distance/((unix_timestamp(tpep_dropoff_datetime)-unix_timestamp(tpep_pickup_datetime))/3600)),2) avg_speed
FROM anish_assignment.ParOrc_Data_anish
GROUP BY mnth
ORDER BY avg_speed DESC;

--  11	10.97
--	12	11.07

-- The month of December is marginally faster by 0.1 miles/hour
-- This is unexpected as the data suggests that taxis are running faster during the holiday season. 
-- It might be due to the fact that people are in a hurry and the office crowd is lesser.
-- However, the minimal difference suggests that New York works even through its holiday season

-- 4. Analyze the average speed of the most happening days of the year, i.e. 31st December (New year’s eve) 
-- and 25th December (Christmas) and compare it with the overall average. 

-- A trip that starts on 25th or 31st shall be considered for the average calculation


SELECT IsHoliday,
       round(avg(speed),2) avg_speed
FROM
  (SELECT CASE
              WHEN ((tpep_pickup_datetime>='2017-12-25 00:00:00.0'
                     AND tpep_pickup_datetime<'2017-12-26 00:00:00.0')
                    OR (tpep_pickup_datetime>='2017-12-31 00:00:00.0'
                        AND tpep_pickup_datetime<'2018-01-01 00:00:00.0')) THEN 1
              ELSE 0
          END IsHoliday,
          trip_distance/((unix_timestamp(tpep_dropoff_datetime)-unix_timestamp(tpep_pickup_datetime))/3600) speed
   FROM anish_assignment.ParOrc_Data_anish) T
GROUP BY IsHoliday
ORDER BY avg_speed DESC;

-- 1	14.01
-- 0	10.95

SELECT 14.01 - 10.95;
-- 3.06

-- We would like to compare speeds between holiday versus non-holiday. We notice that the streets of New York are clearer during the holiday season
-- This conclusion can be drawn as the average speed of the cab is higher by 3.06 miles/hour

-- The non-festive day's average speed is in sync with November and December
-- Let us now confirm the overall average.
SELECT round(avg(trip_distance/((unix_timestamp(tpep_dropoff_datetime)-unix_timestamp(tpep_pickup_datetime))/3600)),2) avg_speed
FROM anish_assignment.ParOrc_Data_anish;
-- 11.02 is the overall average speed as expected so the faster speed on 25th and 31 December

-- Let us compare individual days as well

SELECT Day_type,
       round(avg(trip_distance/((unix_timestamp(tpep_dropoff_datetime)-unix_timestamp(tpep_pickup_datetime))/3600)),2) avg_speed
FROM
  (SELECT trip_distance,
          tpep_dropoff_datetime,
          tpep_pickup_datetime,
          CASE
              WHEN ((tpep_pickup_datetime>='2017-12-25 00:00:00.0'
                     AND tpep_pickup_datetime<'2017-12-26 00:00:00.0')) THEN 1
              WHEN ((tpep_pickup_datetime>='2017-12-31 00:00:00.0'
                     AND tpep_pickup_datetime<'2018-01-01 00:00:00.0')) THEN 2
              ELSE 0
          END Day_type
   FROM anish_assignment.ParOrc_Data_anish) T
GROUP BY Day_type;

-- 0	10.95 (Rest of the days)
-- 1	15.27 (Christmas)
-- 2	13.24 (New Year's Eve)

-- The highest average speed is observed on Christmas day at 15.3 miles/hour. 
-- The holidays have much higher speeds as compared to the rest of the days.
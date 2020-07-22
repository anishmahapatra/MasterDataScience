
# ASSIGNMENT - Stock Market Analysis
# Author: Anish Mahapatra
# Date: 21st June 2020

-- The dataset provided here has been extracted from the NSE website. 
-- The Stock price data provided is from 1-Jan-2015 to 31-July-2018 for six stocks 
-- Eicher Motors, Hero, Bajaj Auto, TVS Motors, Infosys and TCS.

-- Please note that for the days where it is not possible to calculate the required Moving Averages, 
-- it is better to ignore these rows rather than trying to deal with NULL by filling it with average 
-- value as that would make no practical sense.


## 1 Create a new table named 'bajaj1' containing the date, close price, 20 Day MA and 50 Day MA. 
# (This has to be done for all 6 stocks)

# Bajaj Auto - Q1

# Adding a new date column
alter table `bajaj auto`
add format_date date;

# Making safe updates to 0 to bypass MySQL Default
SET SQL_SAFE_UPDATES = 0;

# Updating the format of the date column to dd-mm-yyyy format
update `bajaj auto` set format_date = str_to_date(date, '%d-%M-%Y');

# Precautionary method
drop table if exists `bajaj1`;

# Creating required table
create table bajaj1
  as (select format_date as `Date`, `Close Price` as  `Close Price` , 
  avg(`Close Price`) over (order by format_date asc rows 19 preceding) as `20 Day MA`,
  avg(`Close Price`) over (order by format_date asc rows 49 preceding) as `50 Day MA`,
  row_number() over (order by `format_date` ) as `row_num`
  from `bajaj auto`);
  
# Removing columns that will not be used for calculation
delete from bajaj1 
where  row_num < 50;

# Drpping the row number columns - row_num
alter table bajaj1
drop column row_num;

# Selecting all columns from table ordered by Date
select * from bajaj1 order by `Date`;
 
#--------------------------------------------------------------------------------------------------------------------------
# Eicher Motors - Q1

# Adding a new date column
alter table `eicher motors`
add format_date date;

# Updating the format of the date column to dd-mm-yyyy format
update `eicher motors` set format_date = str_to_date(date, '%d-%M-%Y');

# Precautionary method - just in case (best practices)
drop table if exists `eicher1`;

# Creating required table
create table eicher1
  as (select `format_date` as `Date`, `Close Price` as  `Close Price` , 
  avg(`Close Price`) over (order by format_date asc rows 19 preceding) as `20 Day MA`,
  avg(`Close Price`) over (order by format_date asc rows 49 preceding) as `50 Day MA`,
  row_number() over (order by `format_date` ) as `row_num`
  from `eicher motors`);
  
# Dropping the row number columns - row_num
delete from eicher1 
where  row_num < 50;

alter table eicher1
drop column row_num;

select * from eicher1 order by `Date`;

#--------------------------------------------------------------------------------------------------------------------------
# Hero Motocorp - Q1

alter table `hero motocorp`
add format_date date;

update `hero motocorp` set format_date = str_to_date(date, '%d-%M-%Y');

drop table if exists `hero1`;

create table hero1
  as (select `format_date` as `Date`, `Close Price` as  `Close Price` , 
  avg(`Close Price`) over (order by format_date asc rows 19 preceding) as `20 Day MA`,
  avg(`Close Price`) over (order by format_date asc rows 49 preceding) as `50 Day MA`,
   row_number()     over (order by `format_date` ) as `row_num`
  from `hero motocorp`);

# ignoring values as calculation is inappropriate 
delete from hero1 
where  row_num < 50;

alter table hero1
drop column row_num;

select * from hero1 order by `Date`;

#--------------------------------------------------------------------------------------------------------------------------
# Infosys - Q1

alter table `infosys`
add format_date date;

update `infosys` set format_date = str_to_date(date, '%d-%M-%Y');

drop table if exists `infosys1`;

create table infosys1
  as (select `format_date` as `Date`, `Close Price` as  `Close Price` , 
  avg(`Close Price`) over (order by format_date asc rows 19 preceding) as `20 Day MA`,
  avg(`Close Price`) over (order by format_date asc rows 49 preceding) as `50 Day MA`,
   row_number()     over (order by `format_date` ) as `row_num`
  from `infosys`);

# ignoring values as calculation is inappropriate 
delete from infosys1 
where  row_num < 50;

alter table infosys1
drop column row_num;

select * from infosys1 order by `Date`;

#--------------------------------------------------------------------------------------------------------------------------
# TCS

alter table `tcs`
add format_date date;

update `tcs` set format_date = str_to_date(date, '%d-%M-%Y');

drop table if exists `tcs1`;

create table tcs1
  as (select `format_date` as `Date`, `Close Price` as  `Close Price` , 
  avg(`Close Price`) over (order by format_date asc rows 19 preceding) as `20 Day MA`,
  avg(`Close Price`) over (order by format_date asc rows 49 preceding) as `50 Day MA`,
   row_number()     over (order by `format_date` ) as `row_num`
  from `tcs`);

# ignoring values as calculation is inappropriate 
delete from tcs1 
where  row_num < 50;

alter table tcs1
drop column row_num;

select * from tcs1 order by `Date`;

#--------------------------------------------------------------------------------------------------------------------------
# TVS Motors

alter table `tvs motors`
add format_date date;

update `tvs motors` set format_date = str_to_date(date, '%d-%M-%Y');

drop table if exists `tvs1`;

create table tvs1
  as (select `format_date` as `Date`, `Close Price` as  `Close Price` , 
  avg(`Close Price`) over (order by format_date asc rows 19 preceding) as `20 Day MA`,
  avg(`Close Price`) over (order by format_date asc rows 49 preceding) as `50 Day MA`,
  row_number()     over (order by `format_date` ) as `row_num`
  from `tvs motors`);

# ignoring values as calculation is inappropriate 
delete from tvs1 
where  row_num < 50;

alter table tvs1
drop column row_num;

select * from tvs1 order by `Date`;

#--------------------------------------------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------------------------------------------

# 2 Create a master table containing the date and close price of all the six stocks. 
# (Column header for the price is the name of the stock)

drop table if exists `master`;

create table master
select baj.format_date as Date , baj.`Close Price` as Bajaj , tcs.`Close Price` as TCS , 
tvs.`Close Price` as TVS , inf.`Close Price` as Infosys , eic.`Close Price` as Eicher , her.`Close Price` as Hero
from `bajaj auto` baj
inner join `tcs` tcs on tcs.format_date = baj.format_date
inner join `tvs motors` tvs on tvs.format_date = baj.format_date
inner join `infosys` inf on inf.format_date = baj.format_date
inner join `eicher motors` eic on eic.format_date = baj.format_date
inner join `hero motocorp` her on her.format_date = baj.format_date ;

select * from master order by `Date`;

# 3 Use the table created in Part(1) to generate buy and sell signal. 
# Store this in another table named 'bajaj2'. Perform this operation for all stocks.

#--------------------------------------------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------------------------------------------
# Q3  Use the table created in Part(1) to generate buy and sell signal. Store this in another table named 'bajaj2'. 
# Perform this operation for all stocks.

# bajaj2

drop table if exists `bajaj2`;

create table bajaj2 as
select 
		date_value AS "Date",
		close_price AS "Close Price",
		case when first_value(short_term_greater) over w = nth_value(short_term_greater,2) over w then  'Hold'
				when NTH_VALUE(short_term_greater,2) over w = 'Y' then 'Buy'
				when NTH_VALUE(short_term_greater,2) over w = 'N' then 'Sell'
                else 'Hold'
                end
			
		 AS "Signal" 
	FROM
(
select
		`Date` as date_value,
		`Close Price` AS close_price,
		if(`20 Day MA`>`50 Day MA`,'Y','N') short_term_greater
	from
		bajaj1 
) temp_table
window w as (order by date_value rows between 1 preceding and 0 following);

select * from bajaj2 order by `Date`;

#--------------------------------------------------------------------------------------------------------------------------
# eicher2

drop table if exists `eicher2`;

create table eicher2 as
select 
		date_value AS "Date",
		close_price AS "Close Price",
		case when first_value(short_term_greater) over w = nth_value(short_term_greater,2) over w then  'Hold'
				when NTH_VALUE(short_term_greater,2) over w = 'Y' then 'Buy'
				when NTH_VALUE(short_term_greater,2) over w = 'N' then 'Sell'
                else 'Hold'
                end
			
		 AS "Signal" 
	FROM
(
select
		`Date` as date_value,
		`Close Price` AS close_price,
		if(`20 Day MA`>`50 Day MA`,'Y','N') short_term_greater
	from
		eicher1 
) temp_table
window w as (order by date_value rows between 1 preceding and 0 following);

select * from eicher2 order by `Date`;

#--------------------------------------------------------------------------------------------------------------------------
# hero2

drop table if exists `hero2`;

create table hero2 as
select 
		date_value AS "Date",
		close_price AS "Close Price",
		case when first_value(short_term_greater) over w = nth_value(short_term_greater,2) over w then  'Hold'
				when NTH_VALUE(short_term_greater,2) over w = 'Y' then 'Buy'
				when NTH_VALUE(short_term_greater,2) over w = 'N' then 'Sell'
                else 'Hold'
                end
			
		 AS "Signal" 
	FROM
(
select
		`Date` as date_value,
		`Close Price` AS close_price,
		if(`20 Day MA`>`50 Day MA`,'Y','N') short_term_greater
	from
		hero1 
) temp_table
window w as (order by date_value rows between 1 preceding and 0 following);
  
select * from hero2 order by `Date`;

#--------------------------------------------------------------------------------------------------------------------------
# infosys2

drop table if exists `infosys2`;

create table infosys2 as
select 
		date_value AS "Date",
		close_price AS "Close Price",
		case when first_value(short_term_greater) over w = nth_value(short_term_greater,2) over w then  'Hold'
				when NTH_VALUE(short_term_greater,2) over w = 'Y' then 'Buy'
				when NTH_VALUE(short_term_greater,2) over w = 'N' then 'Sell'
                else 'Hold'
                end
			
		 AS "Signal" 
	FROM
(
select
		`Date` as date_value,
		`Close Price` AS close_price,
		if(`20 Day MA`>`50 Day MA`,'Y','N') short_term_greater
	from
		infosys1 
) temp_table
window w as (order by date_value rows between 1 preceding and 0 following);
  
select * from infosys2 order by `Date`;

#--------------------------------------------------------------------------------------------------------------------------
# tcs2

drop table if exists `tcs2`;

create table tcs2 as
select 
		date_value AS "Date",
		close_price AS "Close Price",
		case when first_value(short_term_greater) over w = nth_value(short_term_greater,2) over w then  'Hold'
				when NTH_VALUE(short_term_greater,2) over w = 'Y' then 'Buy'
				when NTH_VALUE(short_term_greater,2) over w = 'N' then 'Sell'
                else 'Hold'
                end
			
		 AS "Signal" 
	FROM
(
select
		`Date` as date_value,
		`Close Price` AS close_price,
		if(`20 Day MA`>`50 Day MA`,'Y','N') short_term_greater
	from
		tcs1 
) temp_table
window w as (order by date_value rows between 1 preceding and 0 following);
  
select * from tcs2 order by `Date`;

#--------------------------------------------------------------------------------------------------------------------------
# tvs2

drop table if exists `tvs2`;

create table tvs2 as
select 
		date_value AS "Date",
		close_price AS "Close Price",
		case when first_value(short_term_greater) over w = nth_value(short_term_greater,2) over w then  'Hold'
				when NTH_VALUE(short_term_greater,2) over w = 'Y' then 'Buy'
				when NTH_VALUE(short_term_greater,2) over w = 'N' then 'Sell'
                else 'Hold'
                end
			
		 AS "Signal" 
	FROM
(
select
		`Date` as date_value,
		`Close Price` AS close_price,
		if(`20 Day MA`>`50 Day MA`,'Y','N') short_term_greater
	from
		tvs1 
) temp_table
window w as (order by date_value rows between 1 preceding and 0 following);
  
select * from tvs2 order by `Date`;
  
#--------------------------------------------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------------------------------------------

## Q4 Create a User defined function, that takes the date as input and returns the signal for that particular day 
# (Buy/Sell/Hold) for the Bajaj stock.

delimiter $$
create function get_signal_for_date( input_date date)
returns varchar(10)
deterministic
begin
declare signal_value varchar(10);
select `Signal` into signal_value 
from bajaj2
where `Date` = input_date;
return signal_value;
end $$
delimiter ;

# Date format used - YYYY-MM-DD  
# Test function for all three signals
select get_signal_for_date('2015-09-29') as day_signal; # result - hold
select get_signal_for_date('2015-08-24') as day_signal; # result - sell
select get_signal_for_date('2015-05-18') as day_signal; # result - buy


#--------------------------------------------------------------------------------------------------------------------------
## Remove format_date from all tables to have tables as original record

alter table `bajaj auto`
drop column format_date;
alter table `eicher motors`
drop column format_date;
alter table `hero motocorp`
drop column format_date;
alter table `infosys`
drop column format_date;
alter table `tcs`
drop column format_date;
alter table `tvs motors`
drop column format_date;

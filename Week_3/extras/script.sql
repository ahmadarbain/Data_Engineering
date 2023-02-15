-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `dezoomcamp.external_fhv_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://prefect-ar-de-zoomcamp/Week_3/fhv/fhv_tripdata_2019-*.parquet']
);

-- Question 1
--count for fhv vehicle records for year 2019
SELECT COUNT(1) FROM `dezoomcamp.external_fhv_tripdata`;


-- create native table from external table without partitioning and clustering
CREATE OR REPLACE TABLE dezoomcamp.fhv_tripdata_non_partitoned AS
SELECT * FROM `dezoomcamp.external_fhv_tripdata`;

-- Question 2
select count(distinct Affiliated_base_number) 
from `dezoomcamp.external_fhv_tripdata`;

select count(distinct Affiliated_base_number) 
from `dezoomcamp.fhv_tripdata_non_partitoned`;

-- Question 3
select count(1)
from `dezoomcamp.external_fhv_tripdata`
where PUlocationID is null and DOlocationID is null;

-- Question 5

select count(distinct Affiliated_base_number),  
from `dezoomcamp.fhv_tripdata_non_partitoned` 
where pickup_datetime between '2019-03-01' and '2019-03-31 23:59:59';


create or replace table dezoomcamp.fhv_tripdata_partitoned
partition by DATE(pickup_datetime)
cluster by Affiliated_base_number
as
select * from `dezoomcamp.fhv_tripdata_non_partitoned`;


select count(distinct Affiliated_base_number)
from `dezoomcamp.fhv_tripdata_non_partitoned`
where pickup_datetime between '2019-03-01' and '2019-03-31 23:59:59';
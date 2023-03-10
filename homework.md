## Week 1 Homework

In this homework we'll prepare the environment 
and practice with Docker and SQL


## Question 1. Knowing docker tags

Run the command to get information on Docker 

```docker --help```

Now run the command to get help on the "docker build" command

Which tag has the following text? - *Write the image ID to the file* 

- `--imageid string`
- `--iidfile string` **[+]**
- `--idimage string`
- `--idfile string`

</br>

**Answer**

syntax
```
$ docker build --help
```

</br>

output:

```
--iidfile string          Write the image ID to the file
```

</br>

## Question 2. Understanding docker first run 

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash.
Now check the python modules that are installed ( use pip list). 
How many python packages/modules are installed?

- 1
- 6
- 3 **[+]** 
- 7
  
</br>

**Answer**
</br>

```
(base) PS D:\Data_Engineering> docker run -it --entrypoint=bash python:3.9
root@d774f5c2688c:/# pip list
Package    Version
---------- -------
pip        22.0.4
setuptools 58.1.0
wheel      0.38.4
WARNING: You are using pip version 22.0.4; however, version 22.3.1 is available.
You should consider upgrading via the '/usr/local/bin/python -m pip install --upgrade pip' command.
```

# Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from January 2019:

```wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz```

You will also need the dataset with zones:

```wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv```

Download this data and put it into Postgres (with jupyter notebooks or with a pipeline)


## Question 3. Count records 

How many taxi trips were totally made on January 15?

Tip: started and finished on 2019-01-15. 

Remember that `lpep_pickup_datetime` and `lpep_dropoff_datetime` columns are in the format timestamp (date and hour+min+sec) and not in date.

- 20689
- 20530 **[+]**
- 17630
- 21090

</br>

**Answer**

```
SELECT 
	COUNT(1) 
FROM 
	public.green_taxi_trips
WHERE 
	lpep_pickup_datetime between '2019-01-15' and '2019-01-15 23:59:59' AND 
	lpep_dropoff_datetime between '2019-01-15' and '2019-01-15 23:59:59'
```

</br>

## Question 4. Largest trip for each day

Which was the day with the largest trip distance
Use the pick up time for your calculations.

- 2019-01-18
- 2019-01-28
- 2019-01-15 **[+]**
- 2019-01-10
  
</br>

**Answer**

```
SELECT 
	lpep_pickup_datetime
FROM 
		public.green_taxi_trips
WHERE 
	trip_distance = (
		SELECT 
			MAX(trip_distance) 
		FROM public.green_taxi_trips
	)
```

## Question 5. The number of passengers

In 2019-01-01 how many trips had 2 and 3 passengers?
 
- 2: 1282 ; 3: 266
- 2: 1532 ; 3: 126
- 2: 1282 ; 3: 254 **[+]**
- 2: 1282 ; 3: 274

</br>

**Answer**

```
SELECT 
	passenger_count, 
	COUNT(1) 
FROM 
	public.green_taxi_trips
WHERE 
	CAST(lpep_pickup_datetime AS DATE) = '2019-01-01' and passenger_count in (2,3)
group by 1
```

</br>

## Question 6. Largest tip

For the passengers picked up in the Astoria Zone which was the drop off zone that had the largest tip?
We want the name of the zone, not the id.

Note: it's not a typo, it's `tip` , not `trip`

- Central Park
- Jamaica
- South Ozone Park
- Long Island City/Queens Plaza [+]

</br>

**Answer**

```
SELECT 
	zdo."Zone"
FROM 
	public.green_taxi_trips t
	left join public.zones zpu on zpu."LocationID" = t."PULocationID"
	left join public.zones zdo on zdo."LocationID" = t."DOLocationID"
WHERE 
	zpu."Zone" = 'Astoria'AND tip_amount = (
		SELECT MAX(tip_amount)
FROM 
	public.green_taxi_trips t
	left join public.zones zpu on zpu."LocationID" = t."PULocationID"
WHERE zpu."Zone" = 'Astoria')
```

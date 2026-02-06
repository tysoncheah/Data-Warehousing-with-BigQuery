-- Wildcard to load multiple files
kestra-bucket-module3/yellow_tripdata_2024-*.parquet
  
-- Count of records for the 2024 Yellow Taxi Data
SELECT COUNT(*) FROM `zoomcamp-module3-486307.zoomcamp.external_yellow_tripdata` 

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE zoomcamp-module3-486307.zoomcamp.yellow_tripdata_non_partitioned AS
SELECT * FROM zoomcamp-module3-486307.zoomcamp.external_yellow_tripdata;

-- Estimated amount of data to read external table
SELECT DISTINCT(PULocationID)
FROM zoomcamp-module3-486307.zoomcamp.external_yellow_tripdata;

-- Estimated amount of data to read materialized table
SELECT DISTINCT(PULocationID)
FROM zoomcamp-module3-486307.zoomcamp.yellow_tripdata_non_partitioned;

-- Count records have a fare_amount of 0
SELECT COUNT(*)
FROM zoomcamp-module3-486307.zoomcamp.yellow_tripdata_non_partitioned
WHERE fare_amount=0; 

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE zoomcamp-module3-486307.zoomcamp.yellow_tripdata_partitioned
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM zoomcamp-module3-486307.zoomcamp.yellow_tripdata_non_partitioned;

-- Scan partitioned data
SELECT count(*) as trips
FROM zoomcamp-module3-486307.zoomcamp.yellow_tripdata_partitioned
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15'
  AND VendorID=1;

-- Scan non-partitioned data 
SELECT count(*) as trips
FROM zoomcamp-module3-486307.zoomcamp.yellow_tripdata_non_partitioned
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15'
  AND VendorID=1;

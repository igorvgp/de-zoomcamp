-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `data-warehouse-450112.de_zoomcamp.external_yellow_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://yellow-taxi-data-450112/yellow_tripdata_2024-*.parquet']
);

-- Check yellow trip data
SELECT * FROM data-warehouse-450112.de_zoomcamp.external_yellow_tripdata limit 10;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE data-warehouse-450112.de_zoomcamp.yellow_tripdata_non_partitoned AS
SELECT * FROM data-warehouse-450112.de_zoomcamp.external_yellow_tripdata;

-- Question 1
SELECT COUNT(*) FROM data-warehouse-450112.de_zoomcamp.yellow_tripdata_non_partitoned;

-- Question 2
SELECT COUNT(DISTINCT(PULocationID)) FROM data-warehouse-450112.de_zoomcamp.external_yellow_tripdata;

-- Question 3
SELECT PULocationID FROM data-warehouse-450112.de_zoomcamp.yellow_tripdata_non_partitoned;
SELECT PULocationID, DOLocationID FROM data-warehouse-450112.de_zoomcamp.yellow_tripdata_non_partitoned;

-- Question 4
SELECT COUNT(*) FROM data-warehouse-450112.de_zoomcamp.yellow_tripdata_non_partitoned WHERE fare_amount = 0;

-- Question 5
CREATE OR REPLACE TABLE data-warehouse-450112.de_zoomcamp.yellow_tripdata_partitoned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM data-warehouse-450112.de_zoomcamp.external_yellow_tripdata;

-- Question 6
SELECT DISTINCT(VendorID) FROM data-warehouse-450112.de_zoomcamp.yellow_tripdata_partitoned_clustered
WHERE tpep_dropoff_datetime > '2024-03-01' AND tpep_dropoff_datetime <= '2024-03-15';

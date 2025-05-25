-- This is auto-generated code

--This query retrieves the first 1000 rows from a CSV file stored in Azure Data Lake Storage Gen2 using the OPENROWSET function.
SELECT
    TOP 1000 *
FROM
    OPENROWSET(
        BULK 'https://gen2data0storage0work.dfs.core.windows.net/file0gen2/GreenTaxiTripData_201812 - sample.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0'
    ) AS [result]

    -- Counts the total number of rows (trips) in the CSV file.
  SELECT COUNT(*) AS TripCount
FROM OPENROWSET(
    BULK 'https://gen2data0storage0work.dfs.core.windows.net/file0gen2/GreenTaxiTripData_201812 - sample.csv',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE
) AS result;

-- converts the datetime strings right after reading the CSV:
-- for Calculate time differences or filter by dates

SELECT
  VendorID,
  CAST(lpep_pickup_datetime AS DATETIME) AS pickup_datetime,
  CAST(lpep_dropoff_datetime AS DATETIME) AS dropoff_datetime,
  trip_distance
FROM OPENROWSET(
  BULK 'https://gen2data0storage0work.dfs.core.windows.net/file0gen2/GreenTaxiTripData_201812 - sample.csv',
  FORMAT = 'CSV',
  PARSER_VERSION = '2.0',
  HEADER_ROW = TRUE
)
WITH (
  VendorID INT,
  lpep_pickup_datetime VARCHAR(30),
  lpep_dropoff_datetime VARCHAR(30),
  trip_distance FLOAT
) AS result
WHERE trip_distance > 1;
 
 --difference between pickup time and dropoff time basically, the trip duration.

SELECT
  VendorID,
  CAST(lpep_pickup_datetime AS DATETIME) AS pickup_dt,
  CAST(lpep_dropoff_datetime AS DATETIME) AS dropoff_dt,
  DATEDIFF(MINUTE, 
           CAST(lpep_pickup_datetime AS DATETIME), 
           CAST(lpep_dropoff_datetime AS DATETIME)
  ) AS trip_duration_minutes,
  trip_distance
FROM OPENROWSET(
    BULK 'https://gen2data0storage0work.dfs.core.windows.net/file0gen2/GreenTaxiTripData_201812 - sample.csv',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE
)
WITH (
    VendorID INT,
    lpep_pickup_datetime VARCHAR(30),
    lpep_dropoff_datetime VARCHAR(30),
    trip_distance FLOAT
) AS result
WHERE trip_distance > 30;

--filter date and time using casting date from text to date.
SELECT
    *
FROM
    OPENROWSET(
        BULK 'https://gen2data0storage0work.dfs.core.windows.net/file0gen2/GreenTaxiTripData_201812 - sample.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0A',
        HEADER_ROW = TRUE
    )
WITH (
    VendorID INT,
    lpep_pickup_datetime VARCHAR(30),
    lpep_dropoff_datetime VARCHAR(30),
    store_and_fwd_flag VARCHAR(5),
    RatecodeID INT,
    PULocationID INT,
    DOLocationID INT,
    passenger_count INT,
    trip_distance FLOAT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    ehail_fee VARCHAR(10),
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    payment_type INT,
    trip_type INT
) AS [result]
WHERE
    CAST(lpep_pickup_datetime AS DATE) = '2018-12-01';

--5 busiest pickup locations on a given day
SELECT TOP 5
    PULocationID,
    COUNT(*) AS TripCount,
    AVG(trip_distance) AS AvgTripDistance
FROM OPENROWSET(
    BULK 'https://gen2data0storage0work.dfs.core.windows.net/file0gen2/GreenTaxiTripData_201812 - sample.csv',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE
)
WITH (
    PULocationID INT,
    lpep_pickup_datetime VARCHAR(30) COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    trip_distance FLOAT
) AS result
WHERE
    CAST(lpep_pickup_datetime AS DATE) = '2018-12-01'
GROUP BY
    PULocationID
ORDER BY
    TripCount DESC;

    --Total revenue by day
    SELECT
    CAST(lpep_pickup_datetime AS DATE) AS TripDate,
    SUM(fare_amount + extra + mta_tax + tip_amount + tolls_amount + improvement_surcharge) AS TotalRevenue
FROM OPENROWSET(
    BULK 'https://gen2data0storage0work.dfs.core.windows.net/file0gen2/GreenTaxiTripData_201812 - sample.csv',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE
)
WITH (
    lpep_pickup_datetime VARCHAR(30) COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT
) AS result
GROUP BY CAST(lpep_pickup_datetime AS DATE)
ORDER BY TripDate;




    


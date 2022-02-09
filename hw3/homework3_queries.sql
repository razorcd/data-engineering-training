-- SELECT * FROM `razor-project-339321.trips_data_all.fhv10` LIMIT 10 
-- SELECT * FROM `razor-project-339321.trips_data_all.yellow_tripdata4`

-- SELECT COUNT(DISTINCT dispatching_base_num) FROM `razor-project-339321.trips_data_all.fhv10`

-- SELECT FORMAT_DATE("%b %Y", tpep_pickup_datetime) AS ym, count(*) AS cnt FROM `razor-project-339321.trips_data_all.yellow_tripdata4` GROUP BY ym;
-- SELECT FORMAT_DATE("%b %Y", pickup_datetime) AS ym, count(*) AS cnt FROM `razor-project-339321.trips_data_all.fhv10` GROUP BY ym;

-- SELECT COUNT(*) FROM `razor-project-339321.trips_data_all.fhv10`
-- WHERE EXTRACT(YEAR FROM pickup_datetime) = 2019


-- -- # Q3
-- SELECT *
-- FROM `razor-project-339321.trips_data_all.fhv10_p_c`
-- WHERE DATE(pickup_datetime) > '2019-05-01'
-- ORDER BY dispatching_base_num

-- -- Partition and cluster:
-- CREATE OR REPLACE TABLE `razor-project-339321.trips_data_all.fhv10_p_c`
-- PARTITION BY DATE(pickup_datetime)
-- CLUSTER BY dispatching_base_num AS
-- SELECT * FROM `razor-project-339321.trips_data_all.fhv10`

-- -- Partition only:
-- CREATE OR REPLACE TABLE `razor-project-339321.trips_data_all.fhv10_p`
-- PARTITION BY DATE(pickup_datetime)
-- AS
-- SELECT * FROM `razor-project-339321.trips_data_all.fhv10`


-- -- # Q4
-- counts trip between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279

-- SELECT COUNT(*)
-- FROM `razor-project-339321.trips_data_all.fhv10_p_c`
-- WHERE DATE(pickup_datetime) BETWEEN '2019-01-01' AND '2019-03-31'
-- AND (dispatching_base_num = "B00987" OR  dispatching_base_num = "B02060" OR  dispatching_base_num = "B02279")

-- -- Partition and cluster:
-- CREATE OR REPLACE TABLE `razor-project-339321.trips_data_all.fhv10_p_c`
-- PARTITION BY DATE(pickup_datetime)
-- CLUSTER BY dispatching_base_num AS
-- SELECT * FROM `razor-project-339321.trips_data_all.fhv10`

-- -- Partition only:
-- CREATE OR REPLACE TABLE `razor-project-339321.trips_data_all.fhv10_p`
-- PARTITION BY DATE(pickup_datetime)
-- AS
-- SELECT * FROM `razor-project-339321.trips_data_all.fhv10`



-- -- # Q5
-- filtering on dispatching_base_num and SR_Flag
-- RANGE_BUCKET(customer_id, GENERATE_ARRAY(0, 43, 5))

-- SELECT * FROM `razor-project-339321.trips_data_all.fhv10` 
-- WHERE SR_FLAG IS NOT NULL
-- ORDER BY SR_FLAG DESC

-- The test query:
SELECT *
FROM `razor-project-339321.trips_data_all.fhv10_Q5_c_c`
WHERE (dispatching_base_num != "B02864" AND  dispatching_base_num != "B02877" AND  dispatching_base_num != "B02880")
AND (SR_Flag > 25 OR SR_Flag IS NULL)


-- -- Option2: Partition and cluster:
-- CREATE OR REPLACE TABLE `razor-project-339321.trips_data_all.fhv10_Q5_p_c`
-- PARTITION BY RANGE_BUCKET(SR_Flag, GENERATE_ARRAY(0, 43, 5))
-- CLUSTER BY dispatching_base_num
-- AS
-- SELECT * FROM `razor-project-339321.trips_data_all.fhv10`

-- -- Option:3 Cluster and cluster
-- CREATE OR REPLACE TABLE `razor-project-339321.trips_data_all.fhv10_Q5_c_c`
-- CLUSTER BY dispatching_base_num, SR_Flag
-- AS
-- SELECT * FROM `razor-project-339321.trips_data_all.fhv10`


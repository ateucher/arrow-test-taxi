library(arrow)
library(duckdb)
library(dplyr)
library(tictoc)

# arrow::copy_files("s3://ursa-labs-taxi-data", "nyc-taxi")
# beepr::beep()


ds <- open_dataset("~/dev/personal/taxi/nyc-taxi/", partitioning = c("year", "month"))

dat_16 <- filter(ds, year ==  2016)
dat_19 <- filter(ds, year ==  2019)

con <- DBI::dbConnect(duckdb::duckdb(), "db_temp")

duckdb_register_arrow(con, "dat_16", dat_16)
duckdb_register_arrow(con, "dat_19", dat_19)

duckdb_list_arrow(con)

tic("send query")
res <- dbSendQuery(con, 
"
SELECT
  d16.vendor_id,
  d16.month,
  d16.passenger_count AS p_count_16,
  d19.passenger_count AS p_count_19,
  d16.fare_amount AS amt_16,
  d19.fare_amount AS amt_19,
  d16.trip_distance AS dist_16,
  d19.trip_distance AS dist_19
FROM 
  dat_16 d16
LEFT JOIN 
  dat_19 d19
ON
  d16.vendor_id = d19.vendor_id AND
  d16.month = d19.month AND
  d16.pickup_longitude = d19.pickup_longitude AND 
  d16.pickup_latitude = d19.pickup_latitude AND 
  d16.fare_amount >= d19.fare_amount AND
  d16.trip_distance <= d19.trip_distance;
", arrow = TRUE)
toc()

tic("fetch")
res_batch <- duckdb_fetch_record_batch(res)
toc()

tic("open and see head")
to_arrow(res_batch) |> head() |> collect()
toc()

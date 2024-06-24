library(sparklyr)
library(dplyr)
library(lubridate)

# Spark configuration
config <- spark_config()
config$spark.security.credentials.hiveserver2.enabled <- "false"
config$spark.datasource.hive.warehouse.read.via.llap <- "false"
config$spark.sql.hive.hwc.execution.mode <- "spark"
config$spark.sql.extensions <- "com.qubole.spark.hiveacid.HiveAcidAutoConvertExtension"
config$spark.kryo.registrator <- "com.qubole.spark.hiveacid.util.HiveAcidKyroRegistrator"
config$sparklyr.jars.default <- "/opt/spark/optional-lib/hive-warehouse-connector-assembly.jar"
config$spark.yarn.access.hadoopFileSystems <- "s3a://go01-demo/datalake/warehouse/tablespace/managed/hive"

# Connect to Spark
sc <- spark_connect(config = config)

# Specify S3 bucket and file path
s3_path <- "s3a://go01-demo/data/iowa-liquor-sales/Iowa_Liquor_Sales.csv"

# Read the dataset from S3 into Spark
sales_tbl <- spark_read_csv(sc, name = "sales_data", path = s3_path, header = TRUE, infer_schema = TRUE)

# Rename columns to lowercase
sales_data <- sales_tbl %>%
  rename_with(tolower)

# EDA - Get summary stats
distinct_stores <- sales_data %>%
  select(store_number) %>%
  distinct() %>%
  collect()

# Aggregate by store and date, summing sale_dollars
aggregated_df <- sales_data %>%
  group_by(store_number, date) %>%
  summarize(total_sales = sum(sale_dollars), .groups = 'drop')

print(class(aggregated_df))  # Check class of aggregated_df

# Collect the results back to R
aggregated_result <- collect(aggregated_df)

# Convert date column to Date format
aggregated_result$date <- as.Date(aggregated_result$date, format = "%m/%d/%Y")

# Create a new column for the week
aggregated_result <- aggregated_result %>%
  mutate(week = floor_date(date, unit = "week"))

# Aggregate the data by store and week
weekly_aggregated_result <- aggregated_result %>%
  group_by(store_number, week) %>%
  summarize(
    total_sales = sum(total_sales, na.rm = TRUE),
    .groups = 'drop'
  )

# Define a custom function to add new features
new_features <- function(df) {
  df %>%
    mutate(
      date_new = as.Date(week, format = "%m/%d/%Y"),
      day = day(date_new),
      month = month(date_new),
      year = year(date_new),
      random_number = runif(n())
    )
}

# Apply new features function
native_df <- new_features(weekly_aggregated_result)

# Rename date_new to date
native_df <- native_df %>%
  rename(date = date_new)

# Remove week column
native_df <- native_df %>% select(-week)

# Check that all dates for each store are valid and exclude stores with invalid dates
native_df <- native_df %>%
  filter(!is.na(date))

# Check the max date in the entire dataset
max_date <- max(native_df$date)

# Include only stores that contain this last date
stores_with_max_date <- native_df %>%
  filter(date == max_date) %>%
  pull(store_number) %>%
  unique()

native_df <- native_df %>%
  filter(store_number %in% stores_with_max_date)

# Check for date completeness and fill missing weeks with 0 sales
complete_weeks <- seq.Date(min(native_df$date), max_date, by = "week")
store_numbers <- unique(native_df$store_number)

complete_data <- expand.grid(
  store_number = store_numbers,
  date = complete_weeks
)

native_df <- complete_data %>%
  left_join(native_df, by = c("store_number", "date")) %>%
  mutate(
    total_sales = ifelse(is.na(total_sales), 0, total_sales),
    day = ifelse(is.na(day), day(date), day),
    month = ifelse(is.na(month), month(date), month),
    year = ifelse(is.na(year), year(date), year),
    random_number = ifelse(is.na(random_number), runif(1, min = 0, max = 1), random_number)
  )

# Print number of rows in native_df
print(nrow(native_df))

# Write to CSV without row names
write.csv(native_df, "local_df.csv", row.names = FALSE)

# Disconnect from Spark
spark_disconnect(sc)

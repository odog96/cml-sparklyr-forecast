##########################################################
# Spark Data Processing and Forecasting Script
##########################################################

# Description:
# This script performs data processing and forecasting using Sparklyr in R. 
# It reads data from an S3 bucket, applies various data preparation steps, 
# and finally saves a processed version of the data locally.

# Key Steps:
# 1. Install and load necessary R packages for Spark interaction, data manipulation, and forecasting.
# 2. Set up Spark configuration for connecting to a Spark cluster and accessing data from an S3 bucket.
# 3. Connect to Spark and read data from a specified path in the S3 bucket into a Spark DataFrame.
# 4. Perform data preparation steps, including filtering data based on certain conditions and transforming date fields.
# 5. Add new features to the data such as day, month, year, and random numbers, and generate a unique identifier for each record.
# 6. Arrange the data and collect the final processed DataFrame into the local R environment.
# 7. Save the processed data as a CSV file locally for further use or analysis.

# Note: The script is designed to run in an environment where Sparklyr is configured to access a Spark cluster and an S3 bucket.
##########################################################


install.packages("sparklyr")
install.packages("lubridate")
install.packages("forecast")

install.packages("arrow")
library(sparklyr)
library(dplyr)
library(lubridate)
library(arrow)
library(forecast)

#####################################################################
# Configurations
######################################################################

config <- spark_config()
config$spark.security.credentials.hiveserver2.enabled="false"
config$spark.datasource.hive.warehouse.read.via.llap="false"
config$spark.r.libpaths=c("/home/cdsw/.local/lib/R/4.3/library")
config$spark.sql.hive.hwc.execution.mode="spark"
config$spark.sql.extensions="com.qubole.spark.hiveacid.HiveAcidAutoConvertExtension"
config$spark.kryo.registrator="com.qubole.spark.hiveacid.util.HiveAcidKyroRegistrator"
config$sparklyr.jars.default <- "/opt/spark/optional-lib/hive-warehouse-connector-assembly.jar"
#config$spark.yarn.access.hadoopFileSystems="s3a://jmsmucker"
config$spark.kerberos.access.hadoopFileSystems="s3a://jmsmucker"

hdfs_path <- "s3a://jmsmucker/data/landing/anon_base_shipment_data.csv"

sc <- spark_connect(config = config, packages = c())

cpg_data <- spark_read_csv(sc, path = hdfs_path)


###### Data Prep Steps #########################
# Filter for combinations that only have all available dates
# Change date to date format
# add 4 new features - day, month, year, and random
# add a unique id
###############################################

# distinct date count
date_count = cpg_data %>% 
    select(Date) %>% 
    distinct() %>% 
    arrange(desc(Date))%>% sparklyr::sdf_nrow()

# Group, summarize, filter for count = date_count
cpg_data_small <- cpg_data %>% 
                  group_by(PPG, Planning_Account) %>% 
                  summarise(count = n()) %>%
                  filter(count == date_count) %>%
                  arrange(desc(count)) 

# Create a smaller DataFrame with only specific combinations of PPG and Planning_Account
filtered_data <- cpg_data %>%
                  semi_join(cpg_data_small, by = c("PPG", "Planning_Account"))

#Extract unique dates
new_date <- filtered_data %>%
            select(Date) %>%
            distinct() %>%
            collect()

## converting date time format
# Define a custom function to convert the date string to a date type

new_features <- function(df) {
  df$Date_new <- as.Date(df$Date, format = "%m/%d/%Y")
  df$Day <- day(df$Date_new)
  df$Month <- month(df$Date_new)
  df$Year <- year(df$Date_new)
  df$Random_Number <- runif(nrow(df), min = 0, max = 1)
  return(df)
}

# Apply conversion function
new_feat_df <- new_features(new_date)

# Convert new_date back to a Spark DataFrame if it's not already
new_date_spark <- copy_to(sc, new_feat_df, "new_date", overwrite = TRUE)

filtered_data_with_new_date <- filtered_data %>%
                               left_join(new_date_spark, by = c("Date" = "Date"))

# Remove the old Date field
filtered_new <- filtered_data_with_new_date %>%
              select(-Date) %>%
              rename(Date = Date_new)

# Clean up unused large dfs
rm(filtered_data_with_new_date)
rm(new_date_spark)
rm(new_feat_df)

#  add an id for each unique set to be forecast, then sort
filtered_new <- filtered_new %>%
                  mutate(unique_id = paste(PPG, Planning_Account, sep = "_")) 

# View the result
#print(colnames(filtered_new))

# sort dataframe by date
filtered_new <- filtered_new %>%
                         arrange(unique_id, Date)

# Assuming 'filtered_data' is your Spark DataFrame collected into R
local_data <- filtered_new %>% collect()

########################################################

#######################################################

############################

write.csv(local_data, "local_df.csv")

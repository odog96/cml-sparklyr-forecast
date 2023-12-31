##########################################################
# Local Mode Time Series Forecasting with Sparklyr in R
##########################################################

# Description:
# This script executes time series forecasting in Spark's local mode using Sparklyr in R. It showcases 
# the application of a custom ARIMA-based forecasting function to a dataset grouped by unique identifiers. 
# The script is designed to measure the impact of processing varying numbers of data subsets on execution time, 
# providing insights into scalability and performance.

# Key Steps:
# 1. Install and load essential R packages (Sparklyr, Lubridate, Forecast, Arrow, Purrr).
# 2. Configure Sparklyr for local mode execution.
# 3. Read a pre-processed CSV file into a Spark DataFrame for forecasting.
# 4. Define custom forecasting functions for ARIMA modeling with external regressors.
# 5. Apply forecasting to different subsets of data, tracking execution times.
# 6. Store and analyze execution time metrics to understand performance scalability.
# 7. Save processed results as a local CSV file for external use.

# Objectives:
# - To utilize Sparklyr for efficient time series forecasting in a local Spark environment.
# - To analyze the effect of data subset size on the computational performance of forecasting routines.
# - To demonstrate integration of Sparklyr with time series forecasting techniques in R.

# Usage Notes:
# - This script is intended for local execution and testing purposes.
# - Performance results are specific to the machine's specifications and may vary across different setups.

##########################################################
# Begin Script
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

library(purrr)

#######################
# Configurations
########################

config <- spark_config()
config$spark.security.credentials.hiveserver2.enabled="false"
config$spark.datasource.hive.warehouse.read.via.llap="false"
config$spark.r.libpaths=c("/home/cdsw/.local/lib/R/4.3/library")
config$spark.sql.hive.hwc.execution.mode="spark"
config$spark.sql.extensions="com.qubole.spark.hiveacid.HiveAcidAutoConvertExtension"
config$spark.kryo.registrator="com.qubole.spark.hiveacid.util.HiveAcidKyroRegistrator"
config$sparklyr.jars.default <- "/opt/spark/optional-lib/hive-warehouse-connector-assembly.jar"

sc <- spark_connect(config = config)

local_path = "/home/cdsw/local_df.csv"

# read from local directory
filtered_new <- spark_read_csv(sc, "cpg_t_series", 
                          path =  local_path, 
                          memory = TRUE, 
                         infer_schema = TRUE)

####################### Forecasting Functions ###################
# function takes in df and horizon, creates a forecast, returns 
# as df with new dates and forecasted values

mvar_fcast_fx <- function(df, horizon = 18) {
    target_series <- ts(df$Base_Units, frequency = 52)
    predictors <- cbind(df$Day, df$Month, df$Year, df$Random_Number)
  
    # Fit an ARIMA model with external regressors
    fit <- auto.arima(target_series, xreg = predictors)

    # Forecast future values
    results <- make_future_predictors(df$Date, horizon)
    future_predictors <- results$predictors_matrix
    future_dates <- results$dates
  
    forecast_result <- forecast(fit, xreg = future_predictors, h = horizon)
    forecast_df <- data.frame ('Date'  = future_dates,
                  'FC_Base_Units' = forecast_result$mean
                  )
    return(forecast_df)
}

# since we're using exogenous variables, need to generate forward,
# contemporaneous variables
make_future_predictors <- function(series, horizon) {
    # Last date in the series
    last_date <- as.Date(tail(series, 1))
    
    # Generate future dates (weekly)
    future_dates <- seq(last_date + 7, by = "week", length.out = horizon)
    
    # Extract Day, Month, and Year from future dates
    Day <- as.integer(format(future_dates, "%d"))
    Month <- as.integer(format(future_dates, "%m"))
    Year <- as.integer(format(future_dates, "%Y"))
    
    # Generate random numbers
    Random_Number <- runif(horizon, min = 0, max = 1)
    
    # Combine into a dataframe
    future_predictors <- data.frame(Day, Month, Year, Random_Number)

    # Convert to a matrix and return along with future dates
    return(list(predictors_matrix = as.matrix(future_predictors), 
                dates = future_dates))
}

# Assuming 'filtered_data' is your Spark DataFrame collected into R
local_data <- filtered_new %>% collect()

#######################################################
# Experiments
# run with x unique_ids

select_10 = unique(local_data$unique_id)[1:10]

smaller_data <- local_data %>%
                filter(unique_id %in% select_10)

############using PURR

# Split the dataframe into a list of dataframes, each containing data for one unique_id

list_of_dfs <- smaller_data %>% 
               group_by(unique_id) %>% 
               group_split()


# Apply the forecasting function to each time series
execution_time <- system.time(
forecast_results <- map(list_of_dfs, ~ mvar_fcast_fx(.x, horizon = 18))
)
print(execution_time)


############## Test execute times #########################
# Define iteration values
iteration_values <- c(10, 50, 100, 250, 500)

# Initialize a dataframe to store results
results <- data.frame(NumberOfIDs = integer(), ExecutionTime = numeric())

# Loop over iteration values
for (n in iteration_values) {
    # Select n unique IDs
    selected_ids <- unique(local_data$unique_id)[1:n]

    # Filter the data for these IDs
    smaller_data <- local_data %>%
                    filter(unique_id %in% selected_ids) %>%
                    group_by(unique_id) %>%
                    group_split()

    # Measure execution time
    execution_time <- system.time(
        forecast_results <- map(smaller_data, ~ mvar_fcast_fx(.x, horizon = 18))
    )["elapsed"]

    # Store the results
    results <- rbind(results, data.frame(NumberOfIDs = n, ExecutionTime = execution_time))
}

# View the results
print(results)


write.csv(results, "local_results.csv")


########### Spark Apply approach with n unique_id##########################
#spark_df <- copy_to(sc, smaller_data, "spark_df", overwrite = TRUE)
#
## Partition the DataFrame by unique_id
#spark_df_partitioned <- spark_df %>% sdf_repartition(partition_by = "unique_id")
#
## Apply the function to each partition
##forecast_results <- spark_df_partitioned %>% 
##                    spark_apply(mvar_fcast_fx, group_by = "unique_id")
#
test_results <- spark_df_partitioned %>% 
                    spark_apply(test_fx, group_by = "unique_id")
#
#local_test_results <- test_results %>% collect()
##############################################################################
#
#spark_df <- copy_to(sc, local_data, "spark_df", overwrite = TRUE)
#
## Partition the DataFrame by unique_id
#spark_df_partitioned <- spark_df %>% sdf_repartition(partition_by = "unique_id")
#
## Apply the function to each partition
#forecast_results <- spark_df_partitioned %>% 
#                    spark_apply(mvar_fcast_fx, group_by = "unique_id")
#

              
spark_disconnect(sc)

#install.packages("sparklyr")
#install.packages("lubridate")
#install.packages("dplyr")
#install.packages("forecast")
#install.packages("arrow")
#install.packages("vctrs")

library(lubridate)
library(sparklyr)
library(dplyr)
library(forecast)
library(arrow)
library(vctrs)


# Modify Spark executor template
# Define the file path
file_path <- "/tmp/spark-executor.json"

# Read the contents of the file
file_contents <- readLines(file_path)

# Replace the string
file_contents <- gsub('"workingDir":"/tmp"', '"workingDir":"/usr/local"', file_contents)

# Write the modified contents back to the file
writeLines(file_contents, file_path)

#######################
# Configurations
########################

config <- spark_config()
config$spark.security.credentials.hiveserver2.enabled="false"
config$spark.datasource.hive.warehouse.read.via.llap="false"

config$spark.sql.hive.hwc.execution.mode="spark"
config$spark.sql.extensions="com.qubole.spark.hiveacid.HiveAcidAutoConvertExtension"
config$spark.kryo.registrator="com.qubole.spark.hiveacid.util.HiveAcidKyroRegistrator"
config$sparklyr.jars.default <- "/opt/spark/optional-lib/hive-warehouse-connector-assembly.jar"

config$spark.dynamicAllocation.minExecutors = 1
config$spark.dynamicAllocation.maxExecutors = 8
config$spark.executor.cores = 4
config$spark.executor.memory = "8g"

config$spark.executorEnv.R_LIBS="/home/cdsw/.local/lib/R/4.3/library"
config$spark.executorEnv.R_LIBS_USER="/home/cdsw/.local/lib/R/4.3/library"
config$spark.executorEnv.R_LIBS_SITE="/opt/cmladdons/r/libs"

sc <- spark_connect(config = config)

local_path = "/home/cdsw/local_df.csv"

# read from local directory
filtered_new <- spark_read_csv(sc, "sales_data", 
                          path =  local_path, 
                          memory = TRUE, 
                         infer_schema = TRUE)

# DataFrame collected into R
local_data <- filtered_new %>% collect()

local_data$date <- as.Date(local_data$date, format = "%Y-%m-%d")


mvar_fcast_fx <- function(df, horizon = 18) {
  library(sparklyr)
  library(dplyr)
  library(forecast)
  library(lubridate)
  library(arrow)
  
  horizon = 18
  
  print("Starting function...")
  print(paste("Number of rows in partition:", nrow(df)))
  
  # Create time series object
  target_series <- ts(df$total_sales, frequency = 52)
  
  # Ensure column names are consistent and in the correct order
  predictors <- df %>%
    select(day, month, year, random_number) %>%
    as.matrix()
  
  print("Predictors matrix created.")
  print(paste("Number of rows in predictors matrix:", nrow(predictors)))
  
  # Fit the ARIMA model with external regressors
  fit <- auto.arima(target_series, xreg = predictors)
  print("ARIMA model fitted.")
  
  # Extract the last date from the series
  series <- df$date
  last_date <- as.Date(tail(series, 1), format = "%Y-%m-%d")
  print(paste("Last date in series:", last_date))
  
  if (is.na(last_date)) {
    stop("The last date is not valid.")
  }
  
  # Generate future dates (weekly)
  
  print('horizon is')
  print(horizon)
  
  #future_dates <- seq.Date(last_date + 7, by = "week", length.out = horizon)
  future_dates <-seq(last_date, by = "week", length.out = horizon)
  print("Future dates generated:")
  print(future_dates)
  
  # Extract Day, Month, and Year from future dates
  day <- as.integer(format(future_dates, "%d"))
  month <- as.integer(format(future_dates, "%m"))
  year <- as.integer(format(future_dates, "%Y"))
  # Generate random numbers
  random_number <- runif(horizon, min = 0, max = 1)
  
  
  # Ensure future predictors have consistent column names
  future_predictors <- data.frame(day, month, year, random_number)
  
  print('future_preds created')
  
  # Convert to a matrix
  future_predictors <- as.matrix(future_predictors)
  print("Future predictors matrix created.")
  
  forecast_result <- forecast(fit, xreg = future_predictors, h = horizon)
  print("Forecast created.")
  
  forecast_df <- data.frame(
    Date = future_dates,
    #FC_Base_Units = forecast_result$mean
    FC_Base_Units = as.numeric(forecast_result$mean)
  )
  
  print("Forecast dataframe created.")
  
  return(forecast_df)
}


#######################################################
# run with x store_number


select_10 = unique(local_data$store_number)[1:10]

smaller_data <- local_data %>%
                filter(store_number %in% select_10)


########### Spark Apply approach with n unique_id##########################
spark_df <- copy_to(sc, smaller_data, "spark_df", overwrite = TRUE)

# Partition the DataFrame by unique_id
spark_df_partitioned <- spark_df %>% 
                        sdf_repartition(partition_by = "store_number")


# Apply the function to each partition
forecast_results <- spark_df_partitioned %>% 
                    spark_apply(mvar_fcast_fx, group_by = "store_number", packages = FALSE)


local_fc_results <- forecast_results %>% collect()



# save the forecast results 
write.csv(local_fc_results, "forecast_results.csv")



spark_disconnect(sc)
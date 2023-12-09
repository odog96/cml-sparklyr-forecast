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
filtered_new <- spark_read_csv(sc, "cpg_t_series", 
                          path =  local_path, 
                          memory = TRUE, 
                         infer_schema = TRUE)

# DataFrame collected into R
local_data <- filtered_new %>% collect()

####################### Forecasting Functions ###################
# function takes in df and horizon, creates a forecast, returns 
# as df with new dates and forecasted values

test_fx = function() {
library(sparklyr)
library(dplyr)
library(forecast)
library(lubridate)
library(arrow)
library(vctrs)
  return(1)
}

mvar_fcast_fx <- function(df, context) {
#mvar_fcast_fx <- function(df, horizon) {
    library(sparklyr)
    library(dplyr)
    library(forecast)
    library(lubridate)
    library(arrow)
    library(vctrs)

    print('context class')
    print(class(context))
    print('printing class')
    print(context)
  
  # Assign context items to the local environment
  for (name in names(context)) assign(name, context[[name]], envir = .GlobalEnv)
  for (name in names(context)) {
    if (is.function(context[[name]])) {
      assign(name, context[[name]], envir = .GlobalEnv)
    } else {
      assign(name, context[[name]], envir = environment())
    }
  }

    # Access horizon directly
    print(horizon)

    # Use test_fx function as needed
    test_fx_result <- test_fx()

  
    horizon = 18
    target_series <- ts(df$Base_Units, frequency = 52)
    predictors <- cbind(df$Day, df$Month, df$Year, df$Random_Number)
    fit <- auto.arima(target_series, xreg = predictors)
  
    series <- df$Date
    last_date <- as.Date(tail(series, 1))

    # Generate future dates (weekly)
    future_dates <- seq(last_date + 7, by = "week", length.out = horizon)

    # Combine into a dataframe
    # Extract Day, Month, and Year from future dates
    Day <- as.integer(format(future_dates, "%d"))
    Month <- as.integer(format(future_dates, "%m"))
    Year <- as.integer(format(future_dates, "%Y"))
    # Generate random numbers
    Random_Number <- runif(horizon, min = 0, max = 1)
  
    future_predictors <- data.frame(Day, Month, Year, Random_Number)
  
    # Convert to a matrix and return along with future dates
    results <- list(predictors_matrix = as.matrix(future_predictors), 
                dates = future_dates)
  
  future_predictors <- results$predictors_matrix
  future_dates <- results$dates   
  
  forecast_result <- forecast(fit, xreg = future_predictors, h = horizon)
  
    forecast_df <- data.frame ('Date'  = future_dates,
                  'FC_Base_Units' = forecast_result$mean
                  )
    print('done w reg code')
#    print(class(context))  
    #test_dummy <- context$test_fx()
#    print(test_dummy)
  
#    return(forecast_df) # DOESN'T WORK
  
    return(head(forecast_df,n=horizon))
}



#######################################################
# run with x unique_ids

select_2 = unique(local_data$unique_id)[1:2]

smaller_data <- local_data %>%
                filter(unique_id %in% select_2)

########### Spark Apply approach with n unique_id##########################
spark_df <- copy_to(sc, smaller_data, "spark_df", overwrite = TRUE)

# Partition the DataFrame by unique_id
spark_df_partitioned <- spark_df %>% 
                        sdf_repartition(partition_by = "unique_id")



#forecast_results <- spark_df_partitioned %>% 
#                    spark_apply(mvar_fcast_fx, group_by = "unique_id", packages = FALSE)
#
#forecast_results <- spark_df_partitioned %>% 
#                    spark_apply(mvar_fcast_fx(df, horizon),
#                                group_by = "unique_id", packages = FALSE)
#
#  
#forecast_results <- spark_df_partitioned %>%
#                    spark_apply(
#                        function(df) mvar_fcast_fx(df, horizon), 
#                        group_by = "unique_id",
#                        context = list(test_fx = test_fx)
#                    )

#forecast_results <- spark_df_partitioned %>%
#                    spark_apply(
#                        function(df) mvar_fcast_fx(df, horizon, context), 
#                        group_by = "unique_id",, packages = FALSE,
#                        context = list(test_fx = test_fx)
#                    )

# Define the horizon value and the context with the function
horizon_value <- 18
context_list <- list(horizon = horizon_value, test_fx = test_fx)

# Apply the function to each partition with the context, without distributing packages
test_results <- spark_df_partitioned %>% 
                spark_apply(mvar_fcast_fx, context = list(horizon = horizon_value, test_fx = test_fx),
                             group_by = "unique_id", packages = FALSE)



forecast_results <- spark_df_partitioned %>% 
                    spark_apply(mvar_fcast_fx(df, context_list),
                                group_by = "unique_id", packages = FALSE)



#forecast_results <- spark_df_partitioned %>%
#                    spark_apply(
#                        function(df, context) {
#                          # If you need to use test_fx from context
#                          context$test_fx()
#
#                          # Call your forecasting function
#                          mvar_fcast_fx(df, context$horizon, context)
#                        }, 
#                        group_by = "unique_id",
#                        context = list(
#                          horizon = 18,
#                          test_fx = test_fx
#                        ),
#                        packages = FALSE
#                    )


local_fc_results <- forecast_results %>% collect()



# save the forecast results 
write.csv(local_fc_results, "forecast_results.csv")



spark_disconnect(sc)
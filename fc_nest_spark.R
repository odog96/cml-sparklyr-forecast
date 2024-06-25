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
test_fx = function(df) {
  return(colnames(df))
}


mvar_fcast_fx <- function(df,context) {
  library(sparklyr)
  library(dplyr)
  library(forecast)
  library(lubridate)
  library(arrow)

  horizon = context$horizon
  
  target_series <- ts(df$Base_Units, frequency = 52)
  predictors <- cbind(df$Day, df$Month, df$Year, df$Random_Number)

  # Fit an ARIMA model with external regressors
  fit <- auto.arima(target_series, xreg = predictors)
  
  print('number of rows in input df')
  print(nrow(df))
  
  
  # Access context contents
  #test_fx = context$test_fx
  #make_future_predictors = context$make_future_predictors
  #horizon = context$horizon
  
  
  for (name in names(context)) assign(name, context[[name]], envir = .GlobalEnv)

  print(df$unique_id[1])
    
  # call function 
  test_fx_result <- test_fx(df)
  
  print('printing test results')
  print(test_fx_result)
  
  # Forecast future values
  results <- make_future_predictors(df$Date, horizon)
  future_predictors <- results$predictors_matrix
  future_dates <- results$dates
  
  forecast_result <- forecast(fit, xreg = future_predictors, h = horizon)
  
  
  future_predictors <- results$predictors_matrix
  future_dates <- results$dates   
  
  forecast_result <- forecast(fit, xreg = future_predictors, h = horizon)
  
  forecast_df <- data.frame ('Date'  = future_dates,
                             'FC_Base_Units' = as.numeric(forecast_result$mean)
  )
  return(forecast_df)
  #return(df)
}

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

## function to create a context list of all available functions, except itself
## can inlcude other variables to pass 

create_context_list <- function(types = c("function"), var_names = NULL, env = parent.frame()) {
  if (!is.null(types) && !is.character(types)) {
    stop("'types' should be a character vector or NULL.")
  }
  
  if (!is.null(var_names) && !is.character(var_names)) {
    stop("'var_names' should be a character vector or NULL.")
  }
  
  this_function_name <- deparse(substitute(create_context_list))
  context_list <- list()
  objects_in_env <- ls(env)
  
  for (obj_name in objects_in_env) {
    if (obj_name == this_function_name) {
      next
    }
    
    obj <- get(obj_name, envir = env)
    
    if ((!is.null(var_names) && (obj_name %in% var_names)) || 
        (!is.null(types) && any(sapply(types, function(t) inherits(obj, t))))) {
      context_list[[obj_name]] <- obj
    }
  }
  
  return(context_list)
}

#######################################################
# run with x unique_ids

select_4 = unique(local_data$unique_id)[1:4]

smaller_data <- local_data %>%
  filter(unique_id %in% select_4)

########### Spark Apply approach with n unique_id##########################
spark_df <- copy_to(sc, smaller_data, "spark_df", overwrite = TRUE)

# Partition the DataFrame by unique_id
spark_df_partitioned <- spark_df %>% 
  sdf_repartition(partition_by = "unique_id")

# Apply the function to each partition
#forecast_results <- spark_df_partitioned %>% 
#                    spark_apply(mvar_fcast_fx, group_by = "unique_id", packages = FALSE)

# works 
horizon <- 18

# context_list_specific_vars <- create_context_list(var_names = c("var1", "var2"))
context_list <- create_context_list(var_names = "horizon")

#context_list <- list(horizon = horizon_value, test_fx = test_fx, make_future_predictors = make_future_predictors)

test_results =  spark_df_partitioned %>% 
  spark_apply(mvar_fcast_fx,context = context_list,group_by = "unique_id", packages = FALSE)

local_fc_results <- test_results %>% collect()

# save the forecast results 
write.csv(local_fc_results, "forecast_results.csv")



spark_disconnect(sc)
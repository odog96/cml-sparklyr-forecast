## CPG Forecasting in SparklyR with Cloudera Machine Learning
This project shows an example of use SparklyR with CML to access large datasets from S3 (can be other object store)
and then 


**next steps**

### This repository contains scripts for processing and forecasting sales data using Spark. The pipeline includes data preparation and forecasting steps, leveraging Sparklyr for scalable data processing and ARIMA models for forecasting future sales.

execute install_packages as a job. include in yaml

make forecast_prep into a job. One that can later be scheduled weekly


- forecast_prep.R - This script sets up the Spark configuration and connects to a specified S3 bucket to read sales data. It processes the data by renaming columns, aggregating sales by store and week, and ensuring data completeness before saving the processed data to a local CSV file.

- forecast_spark.R - This script loads the processed sales data from a local CSV file into Spark for forecasting. 
It defines a function to forecast future sales using an ARIMA model with external regressors, applies this function to the data, 
and saves the forecast results to a CSV file. Larger workloads by further spark tuning.
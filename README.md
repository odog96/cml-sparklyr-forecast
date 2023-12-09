## CPG Forecasting in SparklyR with Cloudera Machine Learning
This project shows an example of use SparklyR with CML to access large datasets from S3 (can be other object store)
and then 

- forecast_prep.R - reads from s3 location main file ~ 40MB dataset, does some data preparation. 
  Creates a filtered dataset that contains products that have all 156 weeks of data.
- forecast_local.R - reads the filtered dataset. Creates forecasting functions, runs forecast for 
  n products in local mode.
- forecast_spark.R - reads the filtered dataset. Creates forecasting functions, runs forecast for 
  n products in spark distributed mode. Note existing code configurations can be modified to run 
  larger workloads by further spark tuining
- local_df.csv - represents the filtered file (post s3 read and transformations)
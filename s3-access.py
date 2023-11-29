

# Access data from Cloud Storage or the Hive metastore
#
# Accessing data from [the Hive metastore](https://docs.cloudera.com/machine-learning/cloud/import-data/topics/ml-accessing-data-from-apache-hive.html)
# that comes with CML only takes a few more steps.
# But first we need to fetch the data from Cloud Storage and save it as a Hive table.
#
!pip3 install -r requirements.txt
import os
import sys
import subprocess
import xml.etree.ElementTree as ET

#from cmlbootstrap import CMLBootstrap
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import *

path = "s3a://jmsmucker/data/landing/anon_base_shipment_data.csv"

spark = SparkSession.builder.appName("PythonSQL").master("local[*]").getOrCreate()

smucker_data = spark.read.csv(path, header=True,  sep=",", nullValue="NA")

smucker_data.printSchema()

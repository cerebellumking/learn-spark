from flask import Flask, request,jsonify
from flask_cors import CORS
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
import pandas as pd
import json
app = Flask(__name__)
CORS(app)

def getSpark():
    spark = SparkSession.builder.appName("SparkHive")\
        .config("hive.metastore.uris","thrift://100.81.9.75:9083")\
        .config("spark.driver.memory", '2G')\
        .config("spark.executor.memory", '2G')\
        .enableHiveSupport()\
        .master("local[*]")\
        .getOrCreate()
    return spark

getSpark().sql("show databases").show()
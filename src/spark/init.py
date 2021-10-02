from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from subprocess import check_output
from pyspark import SparkFiles
import collections
import random

spark_conf = SparkConf()
spark_conf.setAll([
    ('spark.master', "spark://127.0.0.1:7077"),
    ('spark.app.name', 'Test'),
    ('spark.submit.deployMode', 'client'),
    ('spark.ui.showConsoleProgress', 'true'),
    ('spark.eventLog.enabled', 'false'),
    ('spark.logConf', 'false'),
    ('spark.driver.bindAddress', '0.0.0.0'),
    ('spark.driver.host', "127.0.0.1"),
])

conf = spark_conf.getAll()

for i in conf:
    print(i)

spark_sess = SparkSession.builder.config(conf=spark_conf).getOrCreate()
sc = spark_sess.sparkContext

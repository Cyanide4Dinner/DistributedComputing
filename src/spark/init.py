from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from subprocess import check_output
from pyspark import SparkFiles
import collections
import random
import csv

HOST_IP = "192.168.1.19"

spark_conf = SparkConf()
spark_conf.setAll([
    ('spark.master', "spark://127.0.0.1:7077"),
    ('spark.app.name', 'init'),
    ('spark.submit.deployMode', 'client'),
    ('spark.ui.showConsoleProgress', 'true'),
    ('spark.eventLog.enabled', 'false'),
    ('spark.logConf', 'false'),
    ('spark.driver.bindAddress', '0.0.0.0'),
    ('spark.driver.host', HOST_IP),
])

conf = spark_conf.getAll()
spark_sess = SparkSession.builder.config(conf=spark_conf).getOrCreate()
sc = spark_sess.sparkContext

def f(x):
    print(x)

def loadIntoRDD( filename ):
    with open('../../data/' + filename + '.csv', 'r') as csv_file:
        reader = csv.reader(csv_file)
        return sc.parallelize(list(reader))

moviesRDD = loadIntoRDD("movies")
ratingRDD = loadIntoRDD("rating")
usersRDD = loadIntoRDD("users")
zipcodesRDD = loadIntoRDD("zipcodes")

from logic import parseAndExecute

EXAMPLE_SQL = "SELECT age, gender FROM Users WHERE age = 20"
result = parseAndExecute(EXAMPLE_SQL, [moviesRDD, ratingRDD, usersRDD, zipcodesRDD])
result = result.collect()
for row in result:
    #print(row[0] + ',' + row[1]+ ',' + row[2]+ ',' + row[3]+ ',' + row[4])
    print(row[0] + ',' + row[1])

print("Hello")

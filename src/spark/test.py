from pyspark import SparkContext
logFile = "/opt/spark/data/src/spark/log.txt"
sc = SparkContext("local", "first app")
logData = sc.textFile(logFile).cache()
numKs = logData.filter(lambda s: 'INFO' in s).count()
numTs = logData.filter(lambda s: 'trial' in s).count()
print("Lines with K: %i, lines with trial: %i" % (numKs, numTs))
print("Hello")
open('/opt/spark/data/src/bubu.txt', 'x')

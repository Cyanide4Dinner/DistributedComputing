#!/bin/bash

docker exec namenode hdfs dfs -rm -R output
docker exec namenode hadoop jar /tmp/SQL.jar SQL "$1" output
#docker exec namenode hadoop jar /tmp/SQL.jar SQL "SELECT userid, age, gender FROM Users WHERE age > 20" output
docker exec namenode hadoop fs -getmerge output /tmp/output.txt
docker cp namenode:/tmp/output.txt ../tmp/output.txt

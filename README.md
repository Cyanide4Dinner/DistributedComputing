## Project
### MapReduce
#### Setup
- Intall hadoop (preferably in `system/mapreduce` folder)
- The compilation must happen only in Java 8. Please set the environment to it accordingly.
- Export the hadoop libraries present in `/system` by changing directory to `/system/mapreduce/hadoop`
- Then execute the following, `export CLASSPATH="$PWD/share/hadoop/mapreduce/hadoop-mapreduce-client-core-3.3.1.jar:$PWD/share/hadoop/mapreduce/hadoop-mapreduce-client-common-3.3.1.jar:$PWD/share/hadoop/common/hadoop-common-3.3.1.jar:$PWD/lib/*"`

#### Compiling
- Compiling can be done by `javac -d classes <JAVA_FILES>` in `/src/mapreduce`
- Compress to JAR using `jar -cvf SQL.jar -C classes/ .`

#### Running
- Run the docker setup by `docker-compose up -d`.
- Create a user input folder with `hdfs dfs -mkdir /user/root/input`
- Copy the JAR file to Namenode's tmp folder using `docker cp <PATH-TO-JAR> namenode:/tmp`.
- Similarly copy the input files `docker cp <PATH-TO-CSV-FILES> namenode:/tmp`.
- Execute the following in namenode's shell. Switch to it using `docker exec -it namenode bash`
- Put the input file in the HDFS as "input" using `hdfs -put tmp/<FILE-NAME-CSV> input`.
- Execute `hadoop jar tmp/SQL.jar SQL output`
- Output will be written to `output` directory in hdfs. It then needs to be pulled back to docker fs and then to our local fs. Finally then it can be converted to our required format (JSON).

### Spark
#### Setup
- PySpark currently works with versions till Python-3.6. Hence, Python 3.6 must be installed on the host system.
- An environment must be created for Python3.6. This can be done with `python3.6 -m venv <ENV-NAME>` preferably in /tmp folder.
- Then `source <ENV-NAME>/bin/activate.fish` (for fish shell users).
- Install PySpark. `pip install pyspark`.
- Find your host IP by `hostname -i`. Any one IP will suffice.
- Add this IP to HOST_IP variable in the init.py.

#### Start
- Use the docker-compose.yml in docker/spark by `docker-compose up -d`.

#### Run
- Any script in src/spark can be run by `python <SCRIPT>.py`.

#### Other
- Check the status of jobs by going to `http://localhost:8080`.

## SQL
Following syntax of SQL are supported:
- SELECT C1, C2 ... FROM Table WHERE CX Operator Value
- SELECT C1, C2, ... CN FROM Table WHERE CX Operator Value GROUP BY CP, CP+1, ... CQ HAVING CY Operator Value
- SELECT C1, C2, ... CN, AG FROM Table WHERE CX Operator Value GROUP BY C1, C2, ... CN HAVING AG Operator Value
- SELECT C1, C2, ... CN, AG FROM Table WHERE CX Operator Value GROUP BY C1, C2, ... CN HAVING CY Operator Value
- SELECT C1, C2, ... CN, D1, D2 ... DM FROM Table1 WHERE CX Operator Value NATURAL JOIN Table2 ON Column 
- SELECT C1, C2, ... CN, D1, D2, ... DM FROM Table1 WHERE CX Operator Value INNER JOIN Table2 ON Table1.Column1 = Table2.Column2
- SELECT C1, C2, ... CN, D1, D2, ... DM FROM Table1 WHERE CX Operator Value LEFT OUTER JOIN Table2 ON Table1.Column1 = Table2.Column2 

where:
- AG : Aggregator (COUNT, MAX, MIN, AVG, SUM)

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

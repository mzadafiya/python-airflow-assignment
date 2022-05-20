# python-airflow-assignment


## Start airflow
"sh restart.sh"

Open hhttp://localhost:8080/connection/list/ in browser

username = airflow
passwrod = airflow

## Create below connectction

* Conn Id: rapid_api_connection
* Conn Type: HTTP
* Host: community-open-weather-map.p.rapidapi.com
* Schema: https
* Extra: {"X-RapidAPI-Key": "**your secret**"}

## Update below connection

* Conn Id: postgres_default
* Conn Type: Postgres
* Host: postgres
* Schema: airflow_db
* Login: airflow
* Password: airflow
* Port: 5432

## Enable weather dag

## Check psql table

* docker ps
* docker exec -it <docker_id> /bin/bash
* psql -U airflow  -d airflow_db
* SELECT * FROM weather_data;

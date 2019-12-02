GridU: Airflow
===

1) `docker-compose up webserver scheduler worker flower` 
2) open `http://localhost:8080`
3) trigger `trigger_dag` dag
4) go to `<project_dir>/dags/triggers` directory and create run file:
   `touch ./dags/triggers/run`
5) wait dag to complete, watch `<project_dir>/dags/finished` directory to find a file,
   indicating dag is finished 
6) `docker-compose down`
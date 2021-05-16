mkdir ./dags ./logs ./plugins ./data

echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env

docker-compose up airflow-init

docker-compose up # to run application 

docker-compose down #to terminate application
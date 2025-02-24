#!/bin/bash

rm -f /usr/local/airflow/airflow.db

airflow db init

airflow scheduler &
airflow webserver --port 8080

sleep 10

airflow users create --username admin --firstname admin --lastname admin  --role Admin --email admin@example.org -p 12345

airflow connections add 'ch_1'  --conn-type 'HTTP'  --conn-login 'data_engineer' --conn-password 'njvfc135' --conn-host 'clickhouse1' --conn-port '9000' 

airflow connections add 'ch_2' --conn-type 'HTTP'   --conn-login 'data_engineer'  -conn-password 'njvfc135' --conn-host 'clickhouse2' --conn-port '9000' 

airflow scheduler & airflow webserver

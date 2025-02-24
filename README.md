Аналитика в реальном времени для новых токенов на Solana: влияние социальных сетей на цену и ликвидность.

Репозиторий для проектной работы по курсу "ClickHouse для инженеров и архитекторов БД".

Для запуска проекта нужно последовательно выполнить команды:
./docker-compose up
docker exec -it click1 clickhouse-client --queries-file /tmp/scripts/databases_and_tables.sql
docker exec -it superset superset fab create-admin --username admin --firstname Superset  --lastname Admin --email admin@superset.com --password admin
docker exec -it superset superset db upgrade
docker exec -it superset superset init
docker exec -it superset python -m pip install clickhouse-connect
docker restart superset
docker exec -it kafka /bin/bash
kafka-topics --create --bootstrap-server localhost:19092 --replication-factor 1 --partitions 1 --topic social_token
kafka-topics --create --bootstrap-server localhost:19092 --replication-factor 1 --partitions 1 --topic token_prices_topic
kafka-topics --create --bootstrap-server localhost:19092 --topic new_tokens_topic --partitions 1 --replication-factor 1
После чего запустить скрипты Python:
new_token.py
token_price.py
И затем все социальные сети которые представлены в папке python

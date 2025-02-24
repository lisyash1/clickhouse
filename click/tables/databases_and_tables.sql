DROP DATABASE IF EXISTS crypto ON CLUSTER crypto;

CREATE DATABASE IF NOT EXISTS crypto ON CLUSTER crypto COMMENT 'База данных с данными токенов';


CREATE TABLE crypto.kafka_price_tokens ON cluster crypto(
    tokenaddress String,
    symbol String,
    priceUsd Float64,
    marketCap Float64,
    date DateTime
) ENGINE = Kafka(
    'kafka:9092',
    'token_prices_topic',
    'clickhouse_consumer_group',
    'JSONEachRow'
);


CREATE MATERIALIZED VIEW crypto.price_tokens_consumer ON cluster crypto TO crypto.price_tokens AS
SELECT *
 FROM crypto.kafka_price_tokens;

CREATE TABLE crypto.price_tokens ON cluster crypto (
    tokenaddress String,
    symbol String,
    priceUsd Float64,
    marketCap Float64,
    date DateTime
) ENGINE = MergeTree()
ORDER BY date;

CREATE TABLE crypto.kafka_new_tokens ON cluster crypto (
    mint String,
    name String,
    symbol String,
    marketCapSol Float64,
    date DateTime
) ENGINE = Kafka(
    'kafka:9092',
    'new_tokens_topic',
    'clickhouse_consumer_group',
    'JSONEachRow'
);

CREATE MATERIALIZED VIEW crypto.new_tokens_consumer ON cluster crypto TO crypto.new_tokens AS
SELECT mint as tokenaddress,
    name,
    symbol,
    marketCapSol as marketcap,
    date
 FROM crypto.kafka_new_tokens;

CREATE TABLE crypto.new_tokens ON cluster crypto(
    tokenaddress String,
    name String,
    symbol String,
    marketcap Float64,
    date DateTime
) ENGINE = MergeTree()
ORDER BY date;

CREATE TABLE crypto.kafka_social_tokens ON cluster crypto(
    token String,
    contract_address String,
    timestamp DateTime,
    social String
) ENGINE = Kafka(
    'kafka:9092',
    'social_token',
    'clickhouse_consumer_group',
    'JSONEachRow'
);

CREATE MATERIALIZED VIEW crypto.kafka_social_consumer ON cluster crypto TO crypto.social_tokens AS
SELECT *
 FROM crypto.kafka_social_tokens;

CREATE TABLE crypto.social_tokens ON cluster crypto(
    token String,
    contract_address String,
    timestamp DateTime,
    social String
) ENGINE = MergeTree()
ORDER BY timestamp;


CREATE USER data_engineer IDENTIFIED WITH plaintext_password BY 'njvfc135';
GRANT ALL ON crypto.* TO data_engineer;
import asyncio
import aiohttp
from clickhouse_driver import Client
from kafka import KafkaProducer
import json
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 9101
CLICKHOUSE_USER = "data_engineer"
CLICKHOUSE_PASSWORD = "njvfc135"
CLICKHOUSE_DATABASE = "crypto"

KAFKA_BROKER = "localhost:19092"
KAFKA_TOPIC = "token_prices_topic"

try:
    clickhouse_client = Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE
    )
    # Проверка подключения
    clickhouse_client.execute("SELECT 1")
    logger.info("Успешное подключение к ClickHouse")
except Exception as e:
    logger.error(f"Ошибка подключения к ClickHouse: {e}")
    exit(1)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

async def get_token_data(token_address):
    url = f"https://api.dexscreener.com/latest/dex/tokens/{token_address}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                return data
            else:
                logger.error(f"Ошибка при запросе данных токена {token_address}: {response.status} - {await response.text()}")
                return None

async def update_token_prices():
    try:
        # Получаем список токенов из ClickHouse
        query = f"SELECT tokenaddress FROM {CLICKHOUSE_DATABASE}.new_tokens"
        tokens = clickhouse_client.execute(query)
    except Exception as e:
        logger.error(f"Ошибка при получении токенов из ClickHouse: {e}")
        return

    for token in tokens:
        token_address = token[0]
        token_data = await get_token_data(token_address)
        if token_data is None:
            logger.warning(f"Не удалось получить данные для токена {token_address}")
            continue
        if token_data["pairs"] is None:
            continue
        if "pairs" in token_data and len(token_data["pairs"]) > 0:
            pair_data = token_data["pairs"][0]
            price_usd_str = pair_data.get("priceUsd")
            market_cap = pair_data.get("marketCap")
            symbol = pair_data.get("baseToken", {}).get("symbol")

            if price_usd_str is not None and market_cap is not None and symbol is not None:
                try:
                    price_usd = float(price_usd_str)
                except ValueError:
                    logger.error(f"Не удалось преобразовать priceUsd в число для токена {token_address}")
                    continue

                data = {
                    "tokenaddress": token_address,
                    "symbol": symbol,
                    "priceUsd": price_usd,
                    "marketCap": market_cap,
                    "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }

                try:
                    producer.send(KAFKA_TOPIC, value=data)
                    producer.flush()
                    logger.info(f"Данные отправлены в Kafka: {data}")
                except Exception as e:
                    logger.error(f"Ошибка при отправке данных в Kafka: {e}")
            else:
                logger.warning(f"Не удалось извлечь данные для токена {token_address}")
        else:
            logger.warning(f"Нет данных о парах для токена {token_address}")

async def main():
    while True:
        await update_token_prices()
        await asyncio.sleep(60)  # Опрашивать каждую минуту

asyncio.get_event_loop().run_until_complete(main())
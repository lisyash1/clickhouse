import asyncio
import websockets
import json
from kafka import KafkaProducer
import logging
from datetime import datetime
import aiohttp


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


KAFKA_BROKER = "localhost:19092"
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


async def get_sol_price():
    url = "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                return data["solana"]["usd"]
            else:
                logger.error(f"Ошибка при запросе цены SOL: {response.status}")
                return None

async def subscribe():
    uri = "wss://pumpportal.fun/api/data"
    async with websockets.connect(uri) as websocket:
        # Подписка на новые токены
        payload = {
            "method": "subscribeNewToken",
        }
        await websocket.send(json.dumps(payload))

        # Подписка на торговые операции
        payload = {
            "method": "subscribeAccountTrade",
            "keys": ["AArPXm8JatJiuyEffuC1un2Sc835SULa4uQqDcaGpAjV"]
        }
        await websocket.send(json.dumps(payload))


        payload = {
            "method": "subscribeTokenTrade",
            "keys": ["91WNez8D22NwBssQbkzjy4s2ipFrzpmn5hfvWVe2aY5p"]
        }
        await websocket.send(json.dumps(payload))

        async for message in websocket:
            data = json.loads(message)
            logger.info(f"Получены данные: {data}")


            data["date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


            if "marketCapSol" in data:
                sol_price = await get_sol_price()
                if sol_price is not None:
                    data["marketCapSol"] = data["marketCapSol"] * sol_price
                    logger.info(f"Обновленные данные с учетом цены SOL: {data}")
                else:
                    logger.warning("Не удалось получить цену SOL, оставляем marketCapSol без изменений")
            else:
                logger.warning("Поле marketCapSol отсутствует в данных, пропускаем обновление")

            if data.get("txType") == "create":
                try:
                    producer.send("new_tokens_topic", value=data)
                    producer.flush()
                    logger.info("Данные о новом токене отправлены в Kafka")
                except Exception as e:
                    logger.error(f"Ошибка при отправке данных в Kafka: {e}")



asyncio.get_event_loop().run_until_complete(subscribe())
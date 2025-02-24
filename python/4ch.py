import asyncio
import aiohttp
from bs4 import BeautifulSoup
import re
from datetime import datetime
import json
from aiokafka import AIOKafkaProducer

token_pattern = r'\b[A-Z]{3,5}\b'  # Токены вроде BONK, WIF, SHIB
address_pattern = r'\b[a-zA-Z0-9]{32,44}\b'  # Адреса Solana
output_file = 'crypto_mentions_4chan_2ch.jsonl'

chan4_boards = ['biz']
chan2_base_url = 'https://2ch.hk/biz/'


processed_posts_4chan = set()  # Для 4chan используем ID постов
processed_texts_2ch = set()    # Для 2ch.hk используем текст поста

kafka_producer = None

async def start_kafka_producer():
    global kafka_producer
    kafka_producer = AIOKafkaProducer(
        bootstrap_servers='localhost:19092',  # Адрес вашего Kafka-брокера
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    await kafka_producer.start()

async def stop_kafka_producer():
    if kafka_producer:
        await kafka_producer.stop()

async def send_to_kafka(result):
    if kafka_producer:
        try:
            await kafka_producer.send_and_wait('social_token', result)
            print(f"Отправлено в Kafka: {json.dumps(result)}")
        except Exception as e:
            print(f"Ошибка при отправке в Kafka: {e}")

async def get_token_info(address):
    url = f"https://api.dexscreener.com/latest/dex/tokens/{address}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("pairs") and len(data["pairs"]) > 0:
                        return data["pairs"][0].get("baseToken", {}).get("name")
                    return None
                return None
    except Exception as e:
        print(f"Ошибка при запросе к DexScreener для {address}: {e}")
        return None

def format_result(token=None, address=None, timestamp=None, social=None):
    result = {
        "token": token if token else None,
        "contract_address": address if address else "Not found",
        "timestamp": timestamp.strftime('%Y-%m-%d %H:%M:%S') if timestamp else datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "social": social.split()[0]  # Только "4chan" или "2ch.hk"
    }
    return result

async def monitor_4chan(session):
    while True:
        try:
            for board in chan4_boards:
                url = f'https://a.4cdn.org/{board}/threads.json'
                async with session.get(url) as response:
                    if response.status == 200:
                        threads = await response.json()
                        for thread in threads[0]['threads'][:10]:
                            thread_id = thread['no']
                            thread_url = f'https://a.4cdn.org/{board}/thread/{thread_id}.json'
                            async with session.get(thread_url) as thread_response:
                                if thread_response.status == 200:
                                    posts = await thread_response.json()
                                    for post in posts['posts']:
                                        post_id = post.get('no')
                                        if post_id in processed_posts_4chan:
                                            continue
                                        processed_posts_4chan.add(post_id)

                                        text = post.get('com', '').lower()
                                        timestamp = datetime.fromtimestamp(post.get('time', int(datetime.now().timestamp())))

                                        tokens = re.findall(token_pattern, text)
                                        addresses = re.findall(address_pattern, text)

                                        with open(output_file, 'a', encoding='utf-8') as f:
                                            if tokens:
                                                for token in tokens:
                                                    result = format_result(
                                                        token=token,
                                                        timestamp=timestamp,
                                                        social=f"4chan /{board}"
                                                    )
                                                    f.write(json.dumps(result) + '\n')
                                                    print(f"Найден токен: {json.dumps(result)}")
                                                    await send_to_kafka(result)
                                            if addresses:
                                                for address in addresses:
                                                    token_name = await get_token_info(address)
                                                    result = format_result(
                                                        token=token_name,
                                                        address=address,
                                                        timestamp=timestamp,
                                                        social=f"4chan /{board}"
                                                    )
                                                    f.write(json.dumps(result) + '\n')
                                                    print(f"Найден адрес: {json.dumps(result)}")
                                                    await send_to_kafka(result)
        except Exception as e:
            print(f"Ошибка в 4chan: {e}")
        await asyncio.sleep(60)  # Проверка каждые 60 секунд

async def monitor_2ch(session):
    while True:
        try:
            async with session.get(chan2_base_url) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    posts = soup.select('.post__message')

                    for post in posts[:10]:
                        text = post.get_text().lower()
                        if text in processed_texts_2ch:
                            continue
                        processed_texts_2ch.add(text)

                        timestamp = datetime.now()

                        tokens = re.findall(token_pattern, text)
                        addresses = re.findall(address_pattern, text)

                        with open(output_file, 'a', encoding='utf-8') as f:
                            if tokens:
                                for token in tokens:
                                    result = format_result(
                                        token=token,
                                        timestamp=timestamp,
                                        social="2ch.hk /biz/"
                                    )
                                    f.write(json.dumps(result) + '\n')
                                    print(f"Найден токен: {json.dumps(result)}")
                                    await send_to_kafka(result)
                            if addresses:
                                for address in addresses:
                                    token_name = await get_token_info(address)
                                    result = format_result(
                                        token=token_name,
                                        address=address,
                                        timestamp=timestamp,
                                        social="2ch.hk /biz/"
                                    )
                                    f.write(json.dumps(result) + '\n')
                                    print(f"Найден адрес: {json.dumps(result)}")
                                    await send_to_kafka(result)
        except Exception as e:
            print(f"Ошибка в 2ch.hk: {e}")
        await asyncio.sleep(60)

async def main():
    print("Начинаем мониторинг 4chan и 2ch.hk с проверкой DexScreener и отправкой в Kafka...")
    await start_kafka_producer()
    try:
        async with aiohttp.ClientSession() as session:
            await asyncio.gather(
                monitor_4chan(session),
                monitor_2ch(session)
            )
    finally:
        await stop_kafka_producer()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Остановлено пользователем")
    except Exception as e:
        print(f"Ошибка: {e}")
import praw
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import re
import json
from kafka import KafkaProducer
import aiohttp


reddit = praw.Reddit(
    client_id='-Iei10kRdr-Elo_BQUGzkw',
    client_secret='LyumO7mnC-Zqeb18WWNuEabG_L91Ng',
    user_agent='MyRedditBot v1.0 by',
)


KAFKA_BROKER = "localhost:19092"
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


subreddits_list = [
    'solana', 'CryptoCurrency', 'CryptoMoonShots', 'SatoshiStreetBets',
    'Crypto_General', 'Defi', 'CryptoTrade', 'altcoin', 'CryptoMarkets',
    'CryptoTechnology', 'Daytrading', 'algotrading', 'StockMarket',
    'WallStreetBets', 'Trading', 'memecoins', 'shitcoin', 'dogecoin',
    'CryptoMoon', 'SafeMoon', 'BonkCoin', 'WifCoin', 'PumpAndDump',
    'CryptoMemes', 'ShibaInu', 'Raydium', 'JupiterAggregator', 'Orca_so',
    'SerumDEX', 'StarAtlas', 'Aurory', 'KinFoundation', 'Helium',
    'RenderNetwork', 'STEPN', 'Bitcoin', 'Ethereum', 'Cardano', 'Polkadot',
    'Algorand', 'XRP', 'Stellar', 'Hedera', 'Tezos', 'NEO', 'Tronix', 'EOS',
    'Iota', 'Vechain', 'CosmosNetwork', 'Avalanche', 'Polygon', 'Binance',
    'CoinBase', 'Kraken', 'Crypto_Giveaways', 'CryptoAirdrops', 'NFT',
    'OpenSea', 'NFTmarket', 'CryptoArt', 'Web3', 'Metaverse', 'Decentraland',
    'Sandbox', 'GameFi', 'PlayToEarn', 'CryptoGaming', 'BlockchainGaming',
    'CryptoDevelopers', 'Solidity', 'Rust', 'Programming', 'Tech',
    'Futurology', 'DataIsBeautiful', 'Statistics', 'Economics', 'Finance',
    'PersonalFinance', 'Money', 'Entrepreneur', 'Startups', 'Business',
    'CryptoSecurity', 'Scams', 'CryptoScams', 'Privacy', 'Cybersecurity',
    'CryptoEducation', 'SolanaMemeCoins', 'SolanaNFTs', 'SolanaTech',
    'SolanaMarkets', 'investing', 'options', 'Forex', 'Blockchain'
]


token_pattern = r'\b[A-Z]{3,5}\b'
address_pattern = r'\b[a-zA-Z0-9]{32,44}\b'



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


async def get_address_by_name(token_name):
    url = f"https://api.solscan.io/token/meta?token={token_name}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("address")
                return None
    except Exception as e:
        print(f"Ошибка при запросе к Solscan для {token_name}: {e}")
        return None


def process_subreddit(subreddit_name):
    subreddit = reddit.subreddit(subreddit_name)
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        for submission in subreddit.stream.submissions(skip_existing=True):
            process_content(subreddit_name,
                            submission.title + ' ' + submission.selftext,
                            datetime.fromtimestamp(submission.created_utc),
                            loop)

        for comment in subreddit.stream.comments(skip_existing=True):
            process_content(subreddit_name,
                            comment.body,
                            datetime.fromtimestamp(comment.created_utc),
                            loop)

    except Exception as e:
        print(f"Ошибка в r/{subreddit_name}: {e}")
    finally:
        loop.close()


def process_content(subreddit_name, text, timestamp, loop):
    tokens = re.findall(token_pattern, text)
    addresses = re.findall(address_pattern, text)

    for token in tokens:
        contract_address = loop.run_until_complete(get_address_by_name(token))

        data = {
            "token": token,
            "contract_address": contract_address if contract_address else "Not found",
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "social": "reddit"
        }
        try:
            producer.send("social_token", value=data)
            producer.flush()
            print(f"Отправлено в Kafka (token): {data}")
        except Exception as e:
            print(f"Ошибка отправки токена в Kafka: {e}")

    for address in addresses:
        token_name = loop.run_until_complete(get_token_info(address))

        data = {
            "address": address,
            "token_name": token_name if token_name else "Unknown",
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "social": "reddit"
        }
        try:
            producer.send("social_token", value=data)
            producer.flush()
            print(f"Отправлено в Kafka (address): {data}")
        except Exception as e:
            print(f"Ошибка отправки адреса в Kafka: {e}")


async def monitor_subreddits():
    print("Запуск мониторинга в реальном времени...")
    loop = asyncio.get_running_loop()

    with ThreadPoolExecutor(max_workers=10) as executor:
        tasks = [
            loop.run_in_executor(executor, process_subreddit, subreddit_name)
            for subreddit_name in subreddits_list
        ]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    print("Начинаем асинхронный поиск токенов и адресов в реальном времени...")
    try:
        asyncio.run(monitor_subreddits())
    except KeyboardInterrupt:
        print("Остановлено пользователем")
    except Exception as e:
        print(f"Ошибка: {e}")
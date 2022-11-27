# stream websocket connection to pubsub_v1
import asyncio
import websockets
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1  # type: ignore
import argparse
import os

BINANCE_URL = "wss://stream.binance.com:9443/ws/"
PROJECT_ID = os.popen("gcloud config get-value project").read().strip()
TOPIC_NAME = "binance"


# connect to binance websocket
async def stream_to_pubsub(data):

    # create publisher client
    publisher = pubsub_v1.PublisherClient()
    # get topic path
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)
    # publish data
    publisher.publish(topic_path, data=data.encode("utf-8"))


async def candle_stick_data(symbol="BTCUSDT", interval="1m"):
    url = BINANCE_URL + symbol.lower() + "@kline_" + interval  # stream address
    print(url)

    async with websockets.connect(url) as sock:
        await sock.pong()

        while True:
            resp = await sock.recv()
            await stream_to_pubsub(resp)


async def main(loop):
    """Connect to binance websocket"""
    # parser
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols", nargs="+", help="list of symbols")
    parser.add_argument("--intervals", nargs="+", help="list of intervals")
    args = parser.parse_args()
    tasks = []
    # loop
    for symbol in args.symbols:
        for interval in args.intervals:
            tasks.append(
                loop.create_task(candle_stick_data(symbol=symbol, interval=interval))
            )

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main(loop))

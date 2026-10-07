"""
Step 09: side experiment with the official BitMEX client.

Same structure as step 08, but orders go through the `bitmex` client. Its
calls are blocking, so each one runs in the default thread pool via
run_in_executor, which lets them still overlap under asyncio.gather.
Unlike step 08, it only waits for the first price and has no freshness
check.

Needs the `bitmex` package. Needs the BITMEX_API_KEY and BITMEX_API_SECRET environment variables.
See experiments/README.md for how this step fits into the project's history.
"""

import aiohttp
import asyncio
import time
import datetime
import hashlib
import hmac
import json
import urllib.parse
import threading
import websocket
import signal
import sys
import logging
import os
import bitmex

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

class APIKeyAuthenticator:
    """
    Signs BitMEX REST API requests with an API key and secret.

    Adapted from BitMEX's reference authenticator:
    https://github.com/BitMEX/api-connectors/blob/master/official-http/python-swaggerpy/BitMEXAPIKeyAuthenticator.py

    Attributes:
        host (str): The base URL for the API.
        api_key (str): The API key.
        api_secret (str): The API secret.
    """
    def __init__(self, host, api_key, api_secret):
        """
        Store the API host and credentials. Arguments are described in the class docstring.
        """
        self.host = host
        self.api_key = api_key
        self.api_secret = api_secret

    def generate_signature(self, secret, verb, url, expires, data):
        """
        Generate the api-signature header value for a BitMEX request.

        BitMEX expects a hex-encoded HMAC-SHA256, keyed with the API secret, of
        verb + path (including any query string) + expires + body.

        Args:
            secret (str): The API secret.
            verb (str): HTTP method in upper case (GET, POST, DELETE).
            url (str): Full request URL. Only the path and query are signed.
            expires (int): Unix time in seconds after which BitMEX rejects the request.
            data (str or bytes): Request body, or '' if there is none.

        Returns:
            str: The hex-encoded signature.
        """
        parsedURL = urllib.parse.urlparse(url)
        path = parsedURL.path
        if parsedURL.query:
            path = path + '?' + parsedURL.query

        if isinstance(data, (bytes, bytearray)):
            data = data.decode('utf8')

        message = verb + path + str(expires) + data
        signature = hmac.new(bytes(secret, 'utf-8'), message.encode('utf-8'), digestmod=hashlib.sha256).hexdigest()
        return signature

class BackstopMarketMaker:
    """
    Quotes a bid and an ask on BitMEX around a price streamed from Binance's
    WebSocket, sending orders through the official bitmex client in a thread
    pool so they can overlap.

    Attributes:
        reference_exchange (str): Name of the reference exchange. Not used: the Binance stream URL is hard-coded.
        target_exchange (str): Name of the exchange orders go to. Not used by the code.
        symbol (str): BitMEX instrument to quote.
        buy_cost (float): How far below the reference price to bid, as a fraction (0.005 = 50 bps).
        sell_cost (float): How far above the reference price to ask, as a fraction (0.0075 = 75 bps).
        interval (int): Seconds to wait between cycles.
        buy_qty (int): Bid size in contracts.
        sell_qty (int): Ask size in contracts.
        latest_price (float or None): Last price received from the WebSocket.
        latest_price_time (datetime or None): When latest_price arrived.
        ws (WebSocketApp or None): The Binance WebSocket connection.
        stop_event (Event): Set to stop the WebSocket thread and the main loop.
        client: The official bitmex Swagger client, pointed at the testnet.
        ws_thread (Thread): Background thread running fetch_reference_price().
    """
    def __init__(self, reference_exchange, target_exchange, symbol, buy_cost, sell_cost, interval, bitmex_api_key, bitmex_api_secret, buy_qty, sell_qty):
        """
        Store the configuration, create the bitmex testnet client, start the
        WebSocket price thread and install SIGINT/SIGTERM handlers. Arguments are described in the class docstring.
        """
        self.reference_exchange = reference_exchange
        self.target_exchange = target_exchange
        self.symbol = symbol
        self.buy_cost = buy_cost
        self.sell_cost = sell_cost
        self.interval = interval
        self.latest_price = None
        self.latest_price_time = None  # Timestamp for the latest price
        self.ws = None
        self.stop_event = threading.Event()

        self.buy_qty = buy_qty
        self.sell_qty = sell_qty

        # Initialize BitMEX client
        self.client = bitmex.bitmex(test=True, api_key=bitmex_api_key, api_secret=bitmex_api_secret)

        self.ws_thread = threading.Thread(target=self.fetch_reference_price, name="WS-Thread")
        self.ws_thread.start()

        # Set up signal handling
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def fetch_reference_price(self):
        """
        Stream the BTC/USDT last price from Binance into self.latest_price and
        record when it arrived.

        Runs in the WebSocket thread until stop_event is set. Because on_error
        and on_close set stop_event, a dropped connection ends the loop instead
        of reconnecting.
        """
        def on_message(ws, message):
            """
            Cache the last price (the 'c' field) from a ticker message, record when
            it arrived and log it.

            Args:
                ws (WebSocketApp): The WebSocket connection.
                message (str): Raw JSON ticker message.
            """
            data = json.loads(message)
            self.latest_price = float(data['c'])  # 'c' is the current price in the ticker stream
            self.latest_price_time = datetime.datetime.now()  # Record the time when price was updated
            logger.info(f"Fetched reference price: {self.latest_price}")

        def on_error(ws, error):
            """
            Log a WebSocket error and set stop_event, which stops the bot.

            Args:
                ws (WebSocketApp): The WebSocket connection.
                error (Exception): The error raised by the connection.
            """
            logger.error(f"WebSocket error: {error}")
            self.stop_event.set()

        def on_close(ws, close_status_code, close_msg):
            """
            Log the close code and message and set stop_event, which stops the bot.

            Args:
                ws (WebSocketApp): The WebSocket connection.
                close_status_code (int or None): WebSocket close code, if any.
                close_msg (str or None): Close reason, if any.
            """
            logger.info(f"WebSocket closed with status code: {close_status_code} and message: {close_msg}")
            self.stop_event.set()

        def on_open(ws):
            """
            Log that the connection is open.

            Args:
                ws (WebSocketApp): The WebSocket connection.
            """
            logger.info("WebSocket connection opened")

        self.ws = websocket.WebSocketApp(
            "wss://stream.binance.com:9443/ws/btcusdt@ticker",
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open
        )
        
        while not self.stop_event.is_set():
            try:
                self.ws.run_forever()
            except Exception as e:
                logger.error(f"Error in WebSocket connection: {str(e)}")
                time.sleep(5)  # Reconnect after a short delay

    def signal_handler(self, signum, frame):
        """
        Close the WebSocket, set stop_event and exit on SIGINT or SIGTERM. Open orders are left on the book.

        Args:
            signum (int): The signal number.
            frame (FrameType): The current stack frame.
        """
        logger.info("Signal received, closing WebSocket connection...")
        if self.ws:
            self.ws.close()
        self.stop_event.set()
        sys.exit(0)

    async def run(self):
        """
        Run the market-making loop until stop_event is set.

        Waits for the first WebSocket price, then each cycle computes the quotes
        and runs the cancel, buy and sell calls concurrently with asyncio.gather,
        each in the thread pool. Sleeps `interval` seconds between cycles.
        """
        while not self.stop_event.is_set():
            start_time = time.time()

            try:
                logger.info("Getting reference price...")
                ref_start = time.time()
                if self.latest_price is None:
                    logger.info("Waiting for price update...")
                    await asyncio.sleep(1)
                    continue
                
                reference_price = self.latest_price
                ref_elapsed = time.time() - ref_start
                logger.info(f"Reference price: {reference_price:.2f} (Fetched at: {self.latest_price_time})")
                
                logger.info("Calculating target prices...")
                calc_start = time.time()
                buy_price, sell_price = self.calculate_target_prices(reference_price)
                calc_elapsed = time.time() - calc_start
                logger.info(f"Calculated buy price: {buy_price:.2f}, sell price: {sell_price:.2f}")

                logger.info("Cancelling existing orders and placing new orders...")
                cancel_and_place_start = time.time()
                await asyncio.gather(
                    self.cancel_existing_orders(),
                    self.place_buy_order(buy_price),
                    self.place_sell_order(sell_price)
                )
                cancel_and_place_elapsed = time.time() - cancel_and_place_start
                logger.info(f"Orders cancelled and placed. Elapsed time: {cancel_and_place_elapsed:.4f} seconds")

                end_time = time.time()
                latency = end_time - start_time
                logger.info(f"CYCLE COMPLETED. Total latency: {latency:.4f} seconds \n")

            except Exception as e:
                logger.error(f"Error occurred: {str(e)}")

            await asyncio.sleep(self.interval)

    def calculate_target_prices(self, reference_price):
        """
        Turn the reference price into bid and ask prices.

        The bid is buy_cost below the reference and the ask is sell_cost above
        it, both rounded to the nearest 0.5, the instrument's tick size.

        Args:
            reference_price (float): The current reference price.

        Returns:
            tuple: (buy_price, sell_price).
        """
        tick_size = 0.5
        buy_price = round(reference_price * (1 - self.buy_cost) / tick_size) * tick_size
        sell_price = round(reference_price * (1 + self.sell_cost) / tick_size) * tick_size
        return buy_price, sell_price

    async def place_buy_order(self, buy_price):
        """
        Place a limit buy for buy_qty contracts at buy_price through the bitmex client.

        The blocking call runs in the default thread pool so it doesn't block
        the event loop, and the response is logged.

        Args:
            buy_price (float): Limit price for the buy order.
        """
        order = {
            "symbol": self.symbol,
            "price": buy_price,
            "orderQty": self.buy_qty,
            "side": "Buy",
            "ordType": "Limit"
        }
        response = await asyncio.get_event_loop().run_in_executor(None, lambda: self.client.Order.Order_new(**order).result())
        logger.info(f"Buy order response: {response}")

    async def place_sell_order(self, sell_price):
        """
        Place a limit sell for sell_qty contracts at sell_price through the bitmex client.

        The blocking call runs in the default thread pool so it doesn't block
        the event loop, and the response is logged.

        Args:
            sell_price (float): Limit price for the sell order.
        """
        order = {
            "symbol": self.symbol,
            "price": sell_price,
            "orderQty": self.sell_qty,
            "side": "Sell",
            "ordType": "Limit"
        }
        response = await asyncio.get_event_loop().run_in_executor(None, lambda: self.client.Order.Order_new(**order).result())
        logger.info(f"Sell order response: {response}")

    async def cancel_existing_orders(self):
        """
        Cancel every open order on the account through the bitmex client.

        The blocking call runs in the default thread pool so it doesn't block
        the event loop, and the response is logged.
        """
        response = await asyncio.get_event_loop().run_in_executor(None, lambda: self.client.Order.Order_cancelAll().result())
        logger.info(f"Cancel orders response: {response}")

if __name__ == "__main__":
    reference_exchange = "binance"
    target_exchange = "bitmex_testnet"
    symbol = "XBTUSDT"
    buy_cost = 0.0050  # 50 basis points = 0.5% = 0.005
    sell_cost = 0.0075  # 75 basis points = 0.75% = 0.0075
    buy_qty = 1000
    sell_qty = 1000
    interval = 10  # seconds

    bitmex_api_key = os.environ["BITMEX_API_KEY"]
    bitmex_api_secret = os.environ["BITMEX_API_SECRET"]

    market_maker = BackstopMarketMaker(
        reference_exchange=reference_exchange,
        target_exchange=target_exchange,
        symbol=symbol,
        buy_cost=buy_cost,
        sell_cost=sell_cost,
        buy_qty=buy_qty,
        sell_qty=sell_qty,
        interval=interval,
        bitmex_api_key=bitmex_api_key,
        bitmex_api_secret=bitmex_api_secret
    )
    asyncio.run(market_maker.run())

    # no significant gain in performance

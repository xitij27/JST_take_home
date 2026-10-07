"""
Step 12: wait for the WebSocket before the first cycle.

Adds a connection_ready event that the WebSocket thread sets once the
connection is open, and the main loop waits on it before quoting. Apart
from docstrings and the interval, this is the code that became the final
script.

Needs the BITMEX_API_KEY and BITMEX_API_SECRET environment variables.
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
    WebSocket, sending the cancel and new orders concurrently.

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
        connection_ready (Event): Set once the WebSocket connection is open.
        base_url (str): BitMEX testnet REST API base URL.
        api_key (str): BitMEX API key.
        api_secret (str): BitMEX API secret.
        auth (APIKeyAuthenticator): Signs each request.
        ws_thread (Thread): Background thread running fetch_reference_price().
    """
    def __init__(self, reference_exchange, target_exchange, symbol, buy_cost, sell_cost, interval, bitmex_api_key, bitmex_api_secret, buy_qty, sell_qty):
        """
        Store the configuration, create the request signer, start the WebSocket
        price thread and install SIGINT/SIGTERM handlers. Arguments are described in the class docstring.
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
        self.connection_ready = threading.Event()  # Event to indicate WebSocket connection is ready

        self.buy_qty = buy_qty
        self.sell_qty = sell_qty

        self.base_url = "https://testnet.bitmex.com/api/v1"
        self.api_key = bitmex_api_key
        self.api_secret = bitmex_api_secret
        self.auth = APIKeyAuthenticator(self.base_url, self.api_key, self.api_secret)

        # Start the WebSocket thread to fetch reference prices
        self.ws_thread = threading.Thread(target=self.fetch_reference_price, name="WS-Thread")
        self.ws_thread.start()

        # Set up signal handling for graceful termination
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def fetch_reference_price(self):
        """
        Stream the BTC/USDT last price from Binance into self.latest_price and
        record when it arrived.

        Runs in the WebSocket thread until stop_event is set, and sets
        connection_ready once the connection opens. Because on_error and on_close
        set stop_event, a dropped connection ends the loop instead of
        reconnecting.
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
            Log that the connection is open and set connection_ready, so run() can
            start quoting.

            Args:
                ws (WebSocketApp): The WebSocket connection.
            """
            logger.info("WebSocket connection opened")
            self.connection_ready.set()  # Indicate that the connection is ready

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

    async def send_request(self, session, verb, endpoint, data=None):
        """
        Send a signed request to the BitMEX REST API.

        Args:
            session (aiohttp.ClientSession): The shared HTTP session.
            verb (str): HTTP method.
            endpoint (str): Path relative to base_url, for example "/order".
            data (dict, optional): JSON request body.

        Returns:
            dict or list: The parsed JSON response.

        Raises:
            aiohttp.ClientResponseError: If BitMEX returns a non-200 status. The
            response body is logged first.
        """
        url = self.base_url + endpoint
        expires = int(round(time.time()) + 5)
        data_str = json.dumps(data, separators=(',', ':')) if data else ''

        signature = self.auth.generate_signature(self.api_secret, verb.upper(), url, expires, data_str)

        headers = {
            'api-expires': str(expires),
            'api-key': self.api_key,
            'api-signature': signature,
            'Content-Type': 'application/json'
        }

        async with session.request(verb.upper(), url, headers=headers, data=data_str) as response:
            response_text = await response.text()
            if response.status != 200:
                logger.error(f"HTTP request error: {response.status} {response_text}")
                response.raise_for_status()
            return json.loads(response_text)

    async def run(self):
        """
        Run the market-making loop until stop_event is set.

        Waits for the WebSocket to connect. Each cycle then skips quoting while
        the cached price is more than 0.6 s old, retrying every 0.3 s. Otherwise
        it computes the quotes, sends the cancel, buy and sell requests
        concurrently with asyncio.gather and logs the order IDs. Sleeps
        `interval` seconds between cycles.
        """
        async with aiohttp.ClientSession() as session:
            self.connection_ready.wait()  # Wait for WebSocket connection to be ready
            while not self.stop_event.is_set():
                start_time = time.time()

                try:
                    ref_start = time.time()
                    if self.latest_price is None or (time.time() - self.latest_price_time.timestamp()) > 0.6:
                        logger.info("Waiting for price update...")
                        await asyncio.sleep(0.3)
                        continue
                    
                    reference_price = self.latest_price
                    ref_elapsed = time.time() - ref_start
                    price_age = (time.time() - self.latest_price_time.timestamp()) # how much old is the last fetched price
                    logger.info(f"Reference price: {self.latest_price:.2f}, Age: {price_age:.3f}s")
                    
                    logger.info("Calculating target prices...")
                    calc_start = time.time()
                    buy_price, sell_price = self.calculate_target_prices(self.latest_price)
                    calc_elapsed = time.time() - calc_start
                    logger.info(f"Calculated buy price: {buy_price:.2f}, sell price: {sell_price:.2f}")

                    logger.info("Cancelling existing orders and placing new orders...")
                    cancel_and_place_start = time.time()
                    cancel_order_ids, buy_order_id, sell_order_id = await asyncio.gather(
                        self.cancel_existing_orders(session),
                        self.place_buy_order(session, buy_price),
                        self.place_sell_order(session, sell_price)
                    )
                    cancel_and_place_elapsed = time.time() - cancel_and_place_start
                    logger.info(f"Orders cancelled: {cancel_order_ids}, buy order placed: {buy_order_id}, sell order placed: {sell_order_id}. Elapsed time: {cancel_and_place_elapsed:.4f} seconds")

                    end_time = time.time()
                    latency = end_time - start_time
                    logger.info(f"CYCLE COMPLETED. Total time: {latency:.4f} seconds \n")

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

    async def place_buy_order(self, session, buy_price):
        """
        Place a limit buy for buy_qty contracts at buy_price.

        Args:
            session (aiohttp.ClientSession): The shared HTTP session.
            buy_price (float): Limit price for the buy order.

        Returns:
            str: The order ID of the new buy order.
        """
        response = await self.send_request(
            session,
            "POST",
            "/order",
            {
                "symbol": self.symbol,
                "price": buy_price,
                "orderQty": self.buy_qty,
                "side": "Buy",
                "ordType": "Limit"
            }
        )
        return response['orderID']

    async def place_sell_order(self, session, sell_price):
        """
        Place a limit sell for sell_qty contracts at sell_price.

        Args:
            session (aiohttp.ClientSession): The shared HTTP session.
            sell_price (float): Limit price for the sell order.

        Returns:
            str: The order ID of the new sell order.
        """
        response = await self.send_request(
            session,
            "POST",
            "/order",
            {
                "symbol": self.symbol,
                "price": sell_price,
                "orderQty": self.sell_qty,
                "side": "Sell",
                "ordType": "Limit"
            }
        )
        return response['orderID']

    async def cancel_existing_orders(self, session):
        """
        Cancel every open order on the account (DELETE /order/all).

        Args:
            session (aiohttp.ClientSession): The shared HTTP session.

        Returns:
            list: IDs of the cancelled orders.
        """
        response = await self.send_request(session, "DELETE", "/order/all")
        return [order['orderID'] for order in response]

if __name__ == "__main__":
    reference_exchange = "binance"
    target_exchange = "bitmex_testnet"
    symbol = "XBTUSDT"
    buy_cost = 0.0050
    sell_cost = 0.0075
    buy_qty = 1000
    sell_qty = 1000
    interval = 10

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

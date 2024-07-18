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
import KEYS

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

class APIKeyAuthenticator:
    def __init__(self, host, api_key, api_secret):
        """
        Initializes the APIKeyAuthenticator with the necessary credentials.

        Args:
            host (str): The base URL of the API.
            api_key (str): The API key.
            api_secret (str): The API secret.
        """
        self.host = host
        self.api_key = api_key
        self.api_secret = api_secret

    def generate_signature(self, secret, verb, url, expires, data):
        """
        Generates an HMAC SHA256 signature for API requests.

        Args:
            secret (str): The API secret.
            verb (str): HTTP method (GET, POST, etc.).
            url (str): The request URL.
            expires (int): The expiration timestamp.
            data (str): The request payload.

        Returns:
            str: The generated signature.
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
    def __init__(self, reference_exchange, target_exchange, symbol, buy_cost, sell_cost, interval, bitmex_api_key, bitmex_api_secret, buy_qty, sell_qty):
        """
        Initializes the BackstopMarketMaker with the necessary parameters.

        Args:
            reference_exchange (str): The reference exchange name.
            target_exchange (str): The target exchange name.
            symbol (str): The trading symbol.
            buy_cost (float): The buy cost as a percentage.
            sell_cost (float): The sell cost as a percentage.
            interval (int): The interval between price checks and order placements.
            bitmex_api_key (str): The BitMEX API key.
            bitmex_api_secret (str): The BitMEX API secret.
            buy_qty (int): The quantity to buy.
            sell_qty (int): The quantity to sell.
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
        Fetches the reference price from the WebSocket and updates the latest price.
        """
        def on_message(ws, message):
            """
            Handles incoming messages from the WebSocket.

            Args:
                ws (WebSocketApp): The WebSocketApp instance.
                message (str): The incoming message.
            """
            data = json.loads(message)
            self.latest_price = float(data['c'])  # 'c' is the current price in the ticker stream
            self.latest_price_time = datetime.datetime.now()  # Record the time when price was updated
            logger.info(f"Fetched reference price: {self.latest_price}")

        def on_error(ws, error):
            """
            Handles errors from the WebSocket.

            Args:
                ws (WebSocketApp): The WebSocketApp instance.
                error (str): The error message.
            """
            logger.error(f"WebSocket error: {error}")
            self.stop_event.set()

        def on_close(ws, close_status_code, close_msg):
            """
            Handles WebSocket closure.

            Args:
                ws (WebSocketApp): The WebSocketApp instance.
                close_status_code (int): The close status code.
                close_msg (str): The close message.
            """
            logger.info(f"WebSocket closed with status code: {close_status_code} and message: {close_msg}")
            self.stop_event.set()

        def on_open(ws):
            """
            Handles WebSocket opening.

            Args:
                ws (WebSocketApp): The WebSocketApp instance.
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
        Handles system signals for graceful termination.

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
        Sends an HTTP request to the BitMEX API.

        Args:
            session (ClientSession): The aiohttp ClientSession.
            verb (str): The HTTP method (GET, POST, etc.).
            endpoint (str): The API endpoint.
            data (dict, optional): The request payload.

        Returns:
            dict: The JSON response from the API.
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
        Main loop for the market maker. Periodically checks prices and places orders.
        """
        async with aiohttp.ClientSession() as session:
            while not self.stop_event.is_set():
                start_time = time.time()

                try:
                    ref_start = time.time()
                    if self.latest_price is None or (time.time() - self.latest_price_time.timestamp())>0.6:
                        logger.info("Waiting for price update...")
                        await asyncio.sleep(0.3)
                        continue
                    
                    reference_price = self.latest_price
                    ref_elapsed = time.time() - ref_start
                    price_age = (time.time() - self.latest_price_time.timestamp()) # how much old is the last fetched price
                    logger.info(f"Reference price:{self.latest_price:.2f}, Age:{price_age:.3f}s")
                    
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
                    logger.info(f"CYCLE COMPLETED. Total latency: {latency:.4f} seconds \n")

                except Exception as e:
                    logger.error(f"Error occurred: {str(e)}")

                await asyncio.sleep(self.interval)

    def calculate_target_prices(self, reference_price):
        """
        Calculates the target buy and sell prices based on the reference price.

        Args:
            reference_price (float): The current reference price.

        Returns:
            tuple: The target buy price and sell price.
        """
        tick_size = 0.5
        buy_price = round(reference_price * (1 - self.buy_cost) / tick_size) * tick_size
        sell_price = round(reference_price * (1 + self.sell_cost) / tick_size) * tick_size
        return buy_price, sell_price

    async def place_buy_order(self, session, buy_price):
        """
        Places a buy order on the target exchange.

        Args:
            session (ClientSession): The aiohttp ClientSession.
            buy_price (float): The price at which to place the buy order.

        Returns:
            str: The order ID of the placed buy order.
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
        Places a sell order on the target exchange.

        Args:
            session (ClientSession): The aiohttp ClientSession.
            sell_price (float): The price at which to place the sell order.

        Returns:
            str: The order ID of the placed sell order.
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
        Cancels all existing orders on the target exchange.

        Args:
            session (ClientSession): The aiohttp ClientSession.

        Returns:
            list: A list of canceled order IDs.
        """
        response = await self.send_request(session, "DELETE", "/order/all")
        return [order['orderID'] for order in response]

if __name__ == "__main__":
    # Parameters for the market maker
    reference_exchange = "binance"
    target_exchange = "bitmex_testnet"
    symbol = "XBTUSDT"
    buy_cost = 0.0050
    sell_cost = 0.0075
    buy_qty = 1000
    sell_qty = 1000
    interval = 10

    # API credentials
    bitmex_api_key = KEYS.API_ID
    bitmex_api_secret = KEYS.API_SECRET

    # Instantiate and run the market maker
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
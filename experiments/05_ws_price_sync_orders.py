"""
Step 05: reference price from a WebSocket thread.

A background thread subscribes to Binance's btcusdt@ticker stream, which
pushes the last price about once a second, and caches it, so the main loop
no longer requests the price. Orders are still sent synchronously with
`requests`. After each cycle the cached price is reset to None, so the next
cycle waits for a new tick.

Needs the BITMEX_API_KEY and BITMEX_API_SECRET environment variables.
See experiments/README.md for how this step fits into the project's history.
"""

import os
import requests
import time
import hashlib
import hmac
import json
import urllib.parse
import threading
import websocket

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
    WebSocket, sending orders synchronously.

    Attributes:
        reference_exchange (str): Name of the reference exchange. Not used: the Binance stream URL is hard-coded.
        target_exchange (str): Name of the exchange orders go to. Not used by the code.
        symbol (str): BitMEX instrument to quote.
        buy_cost (float): How far below the reference price to bid, as a fraction (0.005 = 50 bps).
        sell_cost (float): How far above the reference price to ask, as a fraction (0.0075 = 75 bps).
        buy_qty (int): Bid size in contracts.
        sell_qty (int): Ask size in contracts.
        interval (int): Seconds to wait between cycles.
        latest_price (float or None): Last price received from the WebSocket.
        base_url (str): BitMEX testnet REST API base URL.
        api_key (str): BitMEX API key.
        api_secret (str): BitMEX API secret.
        auth (APIKeyAuthenticator): Signs each request.
        ws_thread (Thread): Background thread running fetch_reference_price().
    """
    def __init__(self, reference_exchange, target_exchange, symbol, buy_cost, sell_cost, buy_qty, sell_qty, interval, bitmex_api_key, bitmex_api_secret):
        """
        Store the configuration, create the request signer and start the
        WebSocket price thread. Arguments are described in the class docstring.
        """
        self.reference_exchange = reference_exchange
        self.target_exchange = target_exchange
        self.symbol = symbol
        self.buy_cost = buy_cost
        self.sell_cost = sell_cost
        self.buy_qty = buy_qty
        self.sell_qty = sell_qty
        self.interval = interval
        self.latest_price = None

        self.base_url = "https://testnet.bitmex.com/api/v1"
        self.api_key = bitmex_api_key
        self.api_secret = bitmex_api_secret
        self.auth = APIKeyAuthenticator(self.base_url, self.api_key, self.api_secret)

        self.ws_thread = threading.Thread(target=self.fetch_reference_price)
        self.ws_thread.start()

    def fetch_reference_price(self):
        """
        Stream the BTC/USDT last price from Binance into self.latest_price.

        Runs in the WebSocket thread. Subscribes to the btcusdt@ticker stream and
        calls run_forever() in a loop, so a closed connection is reopened. Nothing
        stops this thread, so the process has to be killed to exit.
        """
        def on_message(ws, message):
            """
            Cache the last price (the 'c' field) from a ticker message.

            Args:
                ws (WebSocketApp): The WebSocket connection.
                message (str): Raw JSON ticker message.
            """
            data = json.loads(message)
            self.latest_price = float(data['c'])  # 'c' is the current price in the ticker stream

        def on_error(ws, error):
            """
            Print a WebSocket error.

            Args:
                ws (WebSocketApp): The WebSocket connection.
                error (Exception): The error raised by the connection.
            """
            print(f"WebSocket error: {error}")

        def on_close(ws):
            """
            Print that the connection closed.

            Note: websocket-client 1.x calls on_close with (ws, close_status_code,
            close_msg), so with current versions this one-argument callback fails
            with a TypeError. Step 08 fixes the signature.

            Args:
                ws (WebSocketApp): The WebSocket connection.
            """
            print("WebSocket closed")

        ws = websocket.WebSocketApp("wss://stream.binance.com:9443/ws/btcusdt@ticker",
                                    on_message=on_message,
                                    on_error=on_error,
                                    on_close=on_close)
        while True:
            try:
                ws.run_forever()
            except Exception as e:
                print(f"Error in WebSocket connection: {str(e)}")
                time.sleep(5)  # Reconnect after a short delay

    def send_request(self, verb, endpoint, data=None):
        """
        Send a signed request to the BitMEX REST API and print how long it took.

        Args:
            verb (str): HTTP method.
            endpoint (str): Path relative to base_url, for example "/order".
            data (dict, optional): JSON request body.

        Returns:
            dict or list: The parsed JSON response.

        Raises:
            ValueError: If BitMEX returns a non-200 status.
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

        start_time = time.time()
        response = requests.request(verb.upper(), url, headers=headers, data=data_str)
        elapsed_time = time.time() - start_time
        print(f"send_request() elapsed time: {elapsed_time:.4f} seconds")

        if response.status_code != 200:
            raise ValueError(f"Error: {response.status_code} {response.text}")
        return response.json()

    def run(self):
        """
        Run the market-making loop forever.

        Each cycle waits until the WebSocket thread has a price, checking once a
        second, then computes the quotes, cancels all orders and places new ones
        synchronously. Afterwards the cached price is reset to None, so the next
        cycle waits for a fresh tick. Sleeps `interval` seconds between cycles.
        """
        while True:
            start_time = time.time()

            try:
                if self.latest_price is None:
                    print("Waiting for the reference price update...")
                    time.sleep(1)
                    continue

                reference_price_start = time.time()
                reference_price = self.latest_price
                reference_price_end = time.time()
                print(f"Reference price: {reference_price}")
                print(f"get_reference_price() elapsed time: {reference_price_end - reference_price_start:.4f} seconds")

                calculate_prices_start = time.time()
                buy_price, sell_price = self.calculate_target_prices(reference_price)
                calculate_prices_end = time.time()
                print(f"calculate_target_prices() elapsed time: {calculate_prices_end - calculate_prices_start:.4f} seconds")

                cancel_orders_start = time.time()
                self.cancel_existing_orders()
                cancel_orders_end = time.time()
                print(f"cancel_existing_orders() elapsed time: {cancel_orders_end - cancel_orders_start:.4f} seconds")

                place_orders_start = time.time()
                self.place_orders(buy_price, sell_price, buy_qty, sell_qty)
                place_orders_end = time.time()
                print(f"place_orders() elapsed time: {place_orders_end - place_orders_start:.4f} seconds")

                end_time = time.time()
                latency = end_time - start_time
                print(f"Cycle completed. Total latency: {latency:.4f} seconds")
                self.latest_price = None

            except Exception as e:
                print(f"Error occurred: {str(e)}")

            time.sleep(self.interval)

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

    def place_orders(self, buy_price, sell_price, buy_qty, sell_qty):
        """
        Place a limit buy and a limit sell, one after the other.

        Args:
            buy_price (float): Limit price for the buy order.
            sell_price (float): Limit price for the sell order.
            buy_qty (int): Bid size in contracts.
            sell_qty (int): Ask size in contracts.
        """

        self.send_request(
            "POST",
            "/order",
            {
                "symbol": self.symbol,
                "price": buy_price,
                "orderQty": buy_qty,
                "side": "Buy",
                "ordType": "Limit"
            }
        )

        self.send_request(
            "POST",
            "/order",
            {
                "symbol": self.symbol,
                "price": sell_price,
                "orderQty": sell_qty,
                "side": "Sell",
                "ordType": "Limit"
            }
        )

    def cancel_existing_orders(self):
        """
        Cancel every open order on the account (DELETE /order/all).
        """
        self.send_request("DELETE", "/order/all")

if __name__ == "__main__":
    reference_exchange = "binance"
    target_exchange = "bitmex_testnet"
    symbol = "XBTUSD"
    buy_cost = 0.0050 # 50 basis points = 0.5% points = 0.005
    sell_cost = 0.0075 # 75 basis points = 0.75% points = 0.0075
    buy_qty = 100
    sell_qty = 100
    interval = 5

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
    market_maker.run()

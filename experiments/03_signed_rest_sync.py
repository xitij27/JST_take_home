"""
Step 03: synchronous baseline with hand-signed REST requests.

Replaces the `bitmex` client with direct calls to the BitMEX REST API made
with `requests` and signed with HMAC-SHA256 (see APIKeyAuthenticator). Each
step of the cycle is timed separately. This is the ~1.8-2.5 s per cycle
baseline quoted in the main README.

Needs the BITMEX_API_KEY and BITMEX_API_SECRET environment variables.
See experiments/README.md for how this step fits into the project's history.
"""

import time
import requests
import hashlib
import hmac
import json
import urllib.parse
import os


class APIKeyAuthenticator:
    """?api_key authenticator.
    This authenticator adds BitMEX API key support via header.
    :param host: Host to authenticate for.
    :param api_key: API key.
    :param api_secret: API secret.
    """

    def __init__(self, host, api_key, api_secret):
        """
        Store the API host and credentials. Arguments are described in the class docstring.
        """
        self.host = host
        self.api_key = api_key
        self.api_secret = api_secret

    # Forces this to apply to all requests.
    def matches(self, url):
        """
        Tell a Swagger client whether to authenticate a request.

        Left over from BitMEX's Swagger authenticator. Not used in this script,
        which signs requests in send_request().

        Args:
            url (str): The request URL.

        Returns:
            bool: False for the swagger.json spec itself, True otherwise.
        """
        if "swagger.json" in url:
            return False
        return True

    # Add the proper headers via the `expires` scheme.
    def apply(self, r):
        """
        Add the api-expires, api-key and api-signature headers to a request.

        Left over from BitMEX's Swagger authenticator. Not used in this script,
        which signs requests in send_request().

        Args:
            r (requests.Request): The request to sign.

        Returns:
            requests.Request: The same request with the auth headers added.
        """
        # 5s grace period in case of clock skew
        expires = int(round(time.time()) + 5)
        r.headers['api-expires'] = str(expires)
        r.headers['api-key'] = self.api_key
        prepared = r.prepare()
        body = prepared.body or ''
        url = prepared.path_url
        r.headers['api-signature'] = self.generate_signature(self.api_secret, r.method, url, expires, body)
        return r

    # First attempt at generate_signature(), kept for reference. Unlike the working
    # version below, it doesn't decode a bytes body before signing, which caused
    # the invalid-signature errors mentioned in the main README.
    # def generate_signature(self, secret, verb, url, expires, data):
    #     """Generate a request signature compatible with BitMEX."""
    #     # Parse the url so we can remove the base and extract just the path.
    #     parsedURL = urllib.parse.urlparse(url)
    #     path = parsedURL.path
    #     if parsedURL.query:
    #         path = path + '?' + parsedURL.query

    #     message = verb + path + str(expires) + data

    #     signature = hmac.new(bytes(secret, 'utf-8'), bytes(message, 'utf-8'), digestmod=hashlib.sha256).hexdigest()
    #     print("Signature: %s" % signature)
    #     return signature

    def generate_signature(self, secret, verb, url, expires, data):
        """Generate a request signature compatible with BitMEX."""
        # Parse the url so we can remove the base and extract just the path.
        parsedURL = urllib.parse.urlparse(url)
        path = parsedURL.path
        if parsedURL.query:
            path = path + '?' + parsedURL.query

        if isinstance(data, (bytes, bytearray)):
            data = data.decode('utf8')

        message = verb + path + str(expires) + data
        # print(f"Message to sign: {message}")

        signature = hmac.new(bytes(secret, 'utf-8'), message.encode('utf-8'), digestmod=hashlib.sha256).hexdigest()
        return signature

class BackstopMarketMaker:
    """
    Quotes a bid and an ask on BitMEX around the Binance price, using
    hand-signed REST requests sent one at a time.

    Attributes:
        reference_exchange (str): Where the reference price comes from. Only "binance" is supported.
        target_exchange (str): Name of the exchange orders go to. Not used by the code.
        symbol (str): BitMEX instrument to quote.
        buy_cost (float): How far below the reference price to bid, as a fraction (0.005 = 50 bps).
        sell_cost (float): How far above the reference price to ask, as a fraction (0.0075 = 75 bps).
        interval (int): Seconds to wait between cycles.
        base_url (str): BitMEX testnet REST API base URL.
        api_key (str): BitMEX API key.
        api_secret (str): BitMEX API secret.
        auth (APIKeyAuthenticator): Signs each request.
    """
    def __init__(self, reference_exchange, target_exchange, symbol, buy_cost, sell_cost, interval, bitmex_api_key, bitmex_api_secret):
        """
        Store the configuration and create the request signer. Arguments are described in the class docstring.
        """
        self.reference_exchange = reference_exchange
        self.target_exchange = target_exchange
        self.symbol = symbol
        self.buy_cost = buy_cost
        self.sell_cost = sell_cost
        self.interval = interval

        # Initialize BitMEX client
        self.base_url = "https://testnet.bitmex.com/api/v1"
        self.api_key = bitmex_api_key
        self.api_secret = bitmex_api_secret
        self.auth = APIKeyAuthenticator(self.base_url, self.api_key, self.api_secret)

    def get_reference_price(self):
        """
        Fetch the latest BTC/USDT price from Binance's REST ticker.

        The Binance symbol is hard-coded to BTCUSDT.

        Returns:
            float: The last traded price.

        Raises:
            ValueError: If reference_exchange is not "binance".
        """
        if self.reference_exchange == "binance":
            url = f'https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT'  # {self.symbol}
            response = requests.get(url)
            data = response.json()
            return float(data['price'])
        else:
            raise ValueError("Unsupported reference exchange")

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

    def send_request(self, verb, endpoint, data=None):
        """
        Send a signed request to the BitMEX REST API.

        Only POST (with a JSON body) and DELETE (without one) are supported.

        Args:
            verb (str): HTTP method.
            endpoint (str): Path relative to base_url, for example "/order".
            data (dict, optional): JSON body for POST requests.

        Returns:
            dict or list: The parsed JSON response.

        Raises:
            ValueError: For an unsupported verb or a non-200 response.
        """
        url = self.base_url + endpoint
        expires = int(round(time.time()) + 5)
        data_str = json.dumps(data, separators=(',', ':')) if data else ''

        # print(f"Verb: {verb}")
        # print(f"Full URL: {url}")
        # print(f"Expires: {expires}")
        # print(f"Data: {data_str}")

        signature = self.auth.generate_signature(self.api_secret, verb.upper(), url, expires, data_str)

        headers = {
            'api-expires': str(expires),
            'api-key': self.api_key,
            'api-signature': signature,
            'Content-Type': 'application/json'
        }
        # print(f"Headers: {headers}")

        if verb.upper() == "POST":
            response = requests.post(url, headers=headers, data=data_str)
        elif verb.upper() == "DELETE":
            response = requests.delete(url, headers=headers)
        else:
            raise ValueError("Unsupported HTTP verb")

        if response.status_code != 200:
            raise ValueError(f"Error: {response.status_code} {response.text}")

        return response.json()

    def place_orders(self, buy_price, sell_price):
        """
        Place a limit buy and a limit sell, 100 contracts each, one after the other.

        Args:
            buy_price (float): Limit price for the buy order.
            sell_price (float): Limit price for the sell order.

        Returns:
            tuple: (buy_order, sell_order), the order objects returned by BitMEX.
        """
        order_qty = 100  # Must be a multiple of 100

        # Place buy order
        buy_order = self.send_request(
            "POST",
            "/order",
            {
                "symbol": self.symbol,
                "price": buy_price,
                "orderQty": order_qty,
                "side": "Buy",
                "ordType": "Limit"
            }
        )

        # Place sell order
        sell_order = self.send_request(
            "POST",
            "/order",
            {
                "symbol": self.symbol,
                "price": sell_price,
                "orderQty": order_qty,
                "side": "Sell",
                "ordType": "Limit"
            }
        )

        return buy_order, sell_order

    def cancel_existing_orders(self):
        """
        Cancel every open order on the account (DELETE /order/all).

        Returns:
            list: The cancelled orders, as returned by BitMEX.
        """
        return self.send_request("DELETE", "/order/all")

    def run(self):
        """
        Run the market-making loop forever.

        Each cycle fetches the reference price, computes the quotes, cancels all
        open orders and places new ones, one request at a time, and prints how
        long each step took. Errors are printed and the loop carries on. Sleeps
        `interval` seconds between cycles.
        """
        while True:
            print("\n--- Starting new cycle ---")
            start_time = time.time()

            try:
                # Measure latency for getting the reference price
                print("1. Getting reference price...")
                ref_start_time = time.time()
                reference_price = self.get_reference_price()
                ref_end_time = time.time()
                ref_latency = ref_end_time - ref_start_time
                print(f"   Reference price: {reference_price}")
                print(f"   Latency for getting reference price: {ref_latency:.4f} seconds")

                # Measure latency for calculating target prices
                print("2. Calculating target prices...")
                calc_start_time = time.time()
                buy_price, sell_price = self.calculate_target_prices(reference_price)
                calc_end_time = time.time()
                calc_latency = calc_end_time - calc_start_time
                print(f"   Buy price: {buy_price:.2f}")
                print(f"   Sell price: {sell_price:.2f}")
                print(f"   Latency for calculating target prices: {calc_latency:.4f} seconds")

                # Measure latency for cancelling existing orders
                print("3. Cancelling existing orders...")
                cancel_start_time = time.time()
                cancel_result = self.cancel_existing_orders()
                cancel_end_time = time.time()
                cancel_latency = cancel_end_time - cancel_start_time
                print(f"   Cancel result: {[order['orderID'] for order in cancel_result]}")
                print(f"   Latency for cancelling existing orders: {cancel_latency:.4f} seconds")

                # Measure latency for placing new orders
                print("4. Placing new orders...")
                place_start_time = time.time()
                buy_order, sell_order = self.place_orders(buy_price, sell_price)
                place_end_time = time.time()
                place_latency = place_end_time - place_start_time
                print(f"   Buy order result: {buy_order['orderID']}")
                print(f"   Sell order result: {sell_order['orderID']}")
                print(f"   Latency for placing new orders: {place_latency:.4f} seconds")

                # Calculate and print total cycle latency
                end_time = time.time()
                cycle_latency = end_time - start_time
                print(f"5. Cycle completed. Total latency: {cycle_latency:.4f} seconds")

            except Exception as e:
                print(f"Error occurred: {str(e)}")

            print(f"Waiting for {self.interval} seconds before next cycle...")
            time.sleep(self.interval)


if __name__ == "__main__":
    reference_exchange = "binance"
    target_exchange = "bitmex_testnet"
    symbol = "XBTUSD"  # BitMEX symbol for BTC/USD
    buy_cost = 0.0050  # 50 basis points
    sell_cost = 0.0075  # 75 basis points
    interval = 60  # 1 minute

    # Replace with your actual BitMEX API key and secret
    bitmex_api_key = os.environ["BITMEX_API_KEY"]
    bitmex_api_secret = os.environ["BITMEX_API_SECRET"]

    market_maker = BackstopMarketMaker(
        reference_exchange=reference_exchange,
        target_exchange=target_exchange,
        symbol=symbol,
        buy_cost=buy_cost,
        sell_cost=sell_cost,
        interval=interval,
        bitmex_api_key=bitmex_api_key,
        bitmex_api_secret=bitmex_api_secret
    )
    market_maker.run()

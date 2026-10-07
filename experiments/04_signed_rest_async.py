"""
Step 04: asyncio and aiohttp, still sequential.

Same flow as step 03, but every HTTP call goes through one shared aiohttp
ClientSession. Requests are still awaited one after another, so there is
no concurrency yet.

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
import os

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
    Quotes a bid and an ask on BitMEX around the Binance price, using
    hand-signed aiohttp requests awaited one at a time.

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

        self.base_url = "https://testnet.bitmex.com/api/v1"
        self.api_key = bitmex_api_key
        self.api_secret = bitmex_api_secret
        self.auth = APIKeyAuthenticator(self.base_url, self.api_key, self.api_secret)

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

        async with session.request(verb.upper(), url, headers=headers, data=data_str) as response:
            response_text = await response.text()            
            if response.status != 200:
                raise ValueError(f"Error: {response.status} {response_text}")
            return json.loads(response_text)

    async def run(self):
        """
        Run the market-making loop forever over one shared aiohttp session.

        Same steps as step 03, each awaited in turn and printed with its elapsed
        time and a timestamp. Errors are printed and the loop carries on. Sleeps
        `interval` seconds between cycles.
        """
        async with aiohttp.ClientSession() as session:
            while True:
                start_time = time.time()

                try:
                    print("Getting reference price...")
                    ref_start = time.time()
                    reference_price = await self.get_reference_price(session)
                    ref_elapsed = time.time() - ref_start
                    print(f"get_reference_price() elapsed time: {ref_elapsed:.4f} seconds", datetime.datetime.now())
                    
                    print("Calculating target prices...")
                    calc_start = time.time()
                    buy_price, sell_price = self.calculate_target_prices(reference_price)
                    calc_elapsed = time.time() - calc_start
                    print(f"calculate_target_prices() elapsed time: {calc_elapsed:.4f} seconds", datetime.datetime.now())

                    print("Cancelling existing orders...")
                    cancel_start = time.time()
                    await self.cancel_existing_orders(session)
                    cancel_elapsed = time.time() - cancel_start
                    print(f"cancel_existing_orders() elapsed time: {cancel_elapsed:.4f} seconds", datetime.datetime.now())
                    
                    print("Placing new orders...")
                    place_start = time.time()
                    await self.place_orders(session, buy_price, sell_price)
                    place_elapsed = time.time() - place_start
                    print(f"place_orders() elapsed time: {place_elapsed:.4f} seconds", datetime.datetime.now())

                    end_time = time.time()
                    latency = end_time - start_time
                    print(f"Cycle completed. Total latency: {latency:.4f} seconds", datetime.datetime.now(), "\n\n")

                except Exception as e:
                    print(f"Error occurred: {str(e)}")

                await asyncio.sleep(self.interval)

    async def get_reference_price(self, session):
        """
        Fetch the latest BTC/USDT price from Binance's REST ticker.

        The Binance symbol is hard-coded to BTCUSDT.

        Args:
            session (aiohttp.ClientSession): The shared HTTP session.

        Returns:
            float: The last traded price.

        Raises:
            ValueError: If reference_exchange is not "binance".
        """
        if self.reference_exchange == "binance":
            url = f'https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT'
            async with session.get(url) as response:
                data = await response.json()
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

    async def place_orders(self, session, buy_price, sell_price):
        """
        Place a limit buy and a limit sell, 100 contracts each, awaiting the
        first request before sending the second.

        Args:
            session (aiohttp.ClientSession): The shared HTTP session.
            buy_price (float): Limit price for the buy order.
            sell_price (float): Limit price for the sell order.
        """
        order_qty = 100

        await self.send_request(
            session,
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

        await self.send_request(
            session,
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

    async def cancel_existing_orders(self, session):
        """
        Cancel every open order on the account (DELETE /order/all).

        Args:
            session (aiohttp.ClientSession): The shared HTTP session.
        """
        await self.send_request(session, "DELETE", "/order/all")

if __name__ == "__main__":
    reference_exchange = "binance"
    target_exchange = "bitmex_testnet"
    symbol = "XBTUSD"
    buy_cost = 0.0050 # 50 basis points = 0.5% = 0.005
    sell_cost = 0.0075 # 75 basis points = 0.75% = 0.0075
    interval = 5

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
    asyncio.run(market_maker.run())

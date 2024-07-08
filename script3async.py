import aiohttp
import asyncio
import time
import datetime
import hashlib
import hmac
import json
import urllib.parse
import KEYS

class APIKeyAuthenticator:
    def __init__(self, host, api_key, api_secret):
        self.host = host
        self.api_key = api_key
        self.api_secret = api_secret

    def generate_signature(self, secret, verb, url, expires, data):
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
    def __init__(self, reference_exchange, target_exchange, symbol, buy_cost, sell_cost, interval, bitmex_api_key, bitmex_api_secret):
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
        if self.reference_exchange == "binance":
            url = f'https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT'
            async with session.get(url) as response:
                data = await response.json()
                return float(data['price'])
        else:
            raise ValueError("Unsupported reference exchange")

    def calculate_target_prices(self, reference_price):
        tick_size = 0.5
        buy_price = round(reference_price * (1 - self.buy_cost) / tick_size) * tick_size
        sell_price = round(reference_price * (1 + self.sell_cost) / tick_size) * tick_size
        return buy_price, sell_price

    async def place_orders(self, session, buy_price, sell_price):
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
        await self.send_request(session, "DELETE", "/order/all")

if __name__ == "__main__":
    reference_exchange = "binance"
    target_exchange = "bitmex_testnet"
    symbol = "XBTUSD"
    buy_cost = 0.0050 # 50 basis points = 0.5% = 0.005
    sell_cost = 0.0075 # 75 basis points = 0.75% = 0.0075
    interval = 5

    bitmex_api_key = KEYS.API_ID
    bitmex_api_secret = KEYS.API_SECRET

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

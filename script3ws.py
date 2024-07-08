import requests
import time
import hashlib
import hmac
import json
import urllib.parse
import threading
import websocket

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
    def __init__(self, reference_exchange, target_exchange, symbol, buy_cost, sell_cost, buy_qty, sell_qty, interval, bitmex_api_key, bitmex_api_secret):
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
        def on_message(ws, message):
            data = json.loads(message)
            self.latest_price = float(data['c'])  # 'c' is the current price in the ticker stream

        def on_error(ws, error):
            print(f"WebSocket error: {error}")

        def on_close(ws):
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
        tick_size = 0.5
        buy_price = round(reference_price * (1 - self.buy_cost) / tick_size) * tick_size
        sell_price = round(reference_price * (1 + self.sell_cost) / tick_size) * tick_size
        return buy_price, sell_price

    def place_orders(self, buy_price, sell_price, buy_qty, sell_qty):

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

    bitmex_api_key = 'isZdN8ybNIMSinAk9WztI-Fr'
    bitmex_api_secret = 'Aeyzah7-8ML26kXxDGEHmCGfPey_z66alSnz8PiAJwV1zDzP'

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

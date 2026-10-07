import os
import time
import requests
from bitmex import bitmex

class BackstopMarketMaker:
    def __init__(self, reference_exchange, target_exchange, symbol, buy_cost, sell_cost, interval, bitmex_api_key, bitmex_api_secret):
        self.reference_exchange = reference_exchange
        self.target_exchange = target_exchange
        self.symbol = symbol
        self.buy_cost = buy_cost
        self.sell_cost = sell_cost
        self.interval = interval

        # Initialize Bitmex client
        self.client = bitmex(test=True, api_key=bitmex_api_key, api_secret=bitmex_api_secret)

    def get_reference_price(self):
        if self.reference_exchange == "binance":
            url = f'https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT' # {self.symbol}
            response = requests.get(url)
            data = response.json()
            return float(data['price'])
        else:
            raise ValueError("Unsupported reference exchange")

    def calculate_target_prices(self, reference_price):
        # buy_price = reference_price * (1 - self.buy_cost)
        # sell_price = reference_price * (1 + self.sell_cost)
        tick_size = 0.5
        buy_price = round(reference_price * (1 - self.buy_cost) / tick_size) * tick_size
        sell_price = round(reference_price * (1 + self.sell_cost) / tick_size) * tick_size
        return buy_price, sell_price

    def place_orders(self, buy_price, sell_price):
        # Place buy order
        self.client.Order.Order_new(
            symbol=self.symbol,
            price=buy_price,
            orderQty=100,
            side='Buy',
            ordType='Limit'
        ).result()

        # Place sell order
        self.client.Order.Order_new(
            symbol=self.symbol,
            price=sell_price,
            orderQty=100,
            side='Sell',
            ordType='Limit'
        ).result()

    def cancel_existing_orders(self):
        self.client.Order.Order_cancelAll().result()

    # def run(self):
    #     while True:
    #         start_time = time.time()

    #         # Get reference price
    #         reference_price = self.get_reference_price()

    #         print(reference_price)

    #         # Calculate target prices
    #         buy_price, sell_price = self.calculate_target_prices(reference_price)

    #         # Cancel existing orders
    #         self.cancel_existing_orders()

    #         # Place new orders
    #         self.place_orders(buy_price, sell_price)

    #         end_time = time.time()
    #         latency = end_time - start_time
    #         print(f"Latency: {latency:.4f} seconds")

    #         # Wait for the next interval
    #         time.sleep(self.interval)
    def run(self):
        while True:
            print("\n--- Starting new cycle ---")
            start_time = time.time()
            
            try:
                print("1. Getting reference price...")
                reference_price = self.get_reference_price()
                print(f"   Reference price: {reference_price}")
                
                print("2. Calculating target prices...")
                buy_price, sell_price = self.calculate_target_prices(reference_price)
                print(f"   Buy price: {buy_price:.2f}")
                print(f"   Sell price: {sell_price:.2f}")
                
                print("3. Cancelling existing orders...")
                cancel_result = self.cancel_existing_orders()
                print(f"   Cancel result: {cancel_result}")
                
                print("4. Placing new orders...")
                buy_order = self.place_orders(buy_price, sell_price)
                print(f"   Buy order result: {buy_order}")
                sell_order = self.place_orders(sell_price, buy_price)
                print(f"   Sell order result: {sell_order}")
                
                end_time = time.time()
                latency = end_time - start_time
                print(f"5. Cycle completed. Latency: {latency:.4f} seconds")
            
            except Exception as e:
                print(f"Error occurred: {str(e)}")
            
            print(f"Waiting for {self.interval} seconds before next cycle...")
            time.sleep(self.interval)

if __name__ == "__main__":
    reference_exchange = "binance"
    target_exchange = "bitmex_testnet"
    symbol = "XBTUSD"  # Bitmex symbol for BTC/USD
    buy_cost = 0.0050  # 50 basis points
    sell_cost = 0.0075  # 75 basis points
    interval = 60  # 1 minute

    # Replace with your actual Bitmex API key and secret
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

"""
Step 02: step 01 with the duplicate order placement fixed.

place_orders() is now called once per cycle and returns both orders, and
the loop prints the ID and status of every order placed or cancelled.

Needs the `bitmex` package. Needs the BITMEX_API_KEY and BITMEX_API_SECRET environment variables.
See experiments/README.md for how this step fits into the project's history.
"""

import os
import time
import requests
from bitmex import bitmex

class BackstopMarketMaker:
    """
    Quotes a bid and an ask on BitMEX around the Binance price, one request
    at a time, using the official bitmex client.

    Attributes:
        reference_exchange (str): Where the reference price comes from. Only "binance" is supported.
        target_exchange (str): Name of the exchange orders go to. Not used by the code.
        symbol (str): BitMEX instrument to quote.
        buy_cost (float): How far below the reference price to bid, as a fraction (0.005 = 50 bps).
        sell_cost (float): How far above the reference price to ask, as a fraction (0.0075 = 75 bps).
        interval (int): Seconds to wait between cycles.
        client: The official bitmex Swagger client, pointed at the testnet.
    """
    def __init__(self, reference_exchange, target_exchange, symbol, buy_cost, sell_cost, interval, bitmex_api_key, bitmex_api_secret):
        """
        Store the configuration and create the bitmex testnet client. Arguments are described in the class docstring.
        """
        self.reference_exchange = reference_exchange
        self.target_exchange = target_exchange
        self.symbol = symbol
        self.buy_cost = buy_cost
        self.sell_cost = sell_cost
        self.interval = interval

        # Initialize Bitmex client
        self.client = bitmex(test=True, api_key=bitmex_api_key, api_secret=bitmex_api_secret)

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
            url = f'https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT' # {self.symbol}
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

    def place_orders(self, buy_price, sell_price):
        """
        Place a limit buy and a limit sell, 100 contracts each, one after the other.

        Args:
            buy_price (float): Limit price for the buy order.
            sell_price (float): Limit price for the sell order.

        Returns:
            tuple: (buy_order, sell_order), each the (result, response) pair
            returned by the bitmex client.
        """
        # Place buy order
        buy_order = self.client.Order.Order_new(
            symbol=self.symbol,
            price=buy_price,
            orderQty=100,
            side='Buy',
            ordType='Limit'
        ).result()

        # Place sell order
        sell_order = self.client.Order.Order_new(
            symbol=self.symbol,
            price=sell_price,
            orderQty=100,
            side='Sell',
            ordType='Limit'
        ).result()

        return buy_order, sell_order

    def cancel_existing_orders(self):
        """
        Cancel every open order on the account with the bitmex client.

        Returns:
            tuple: The (result, response) pair from the bitmex client, where
            result is the list of cancelled orders.
        """
        cancel_result = self.client.Order.Order_cancelAll().result()
        return cancel_result

    def run(self):
        """
        Run the market-making loop forever.

        Each cycle fetches the reference price, computes the quotes, cancels all
        open orders and places new ones, printing each step, the ID and status
        of every order, and the cycle's total time. Errors are printed and the
        loop carries on. Sleeps `interval` seconds between cycles.
        """
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
                for order in cancel_result[0]:
                    print(f"   Canceled order ID: {order['orderID']} Status: {order['ordStatus']}")
                
                print("4. Placing new orders...")
                buy_order, sell_order = self.place_orders(buy_price, sell_price)
                print(f"   Buy order ID: {buy_order[0]['orderID']} Status: {buy_order[0]['ordStatus']}")
                print(f"   Sell order ID: {sell_order[0]['orderID']} Status: {sell_order[0]['ordStatus']}")
                
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

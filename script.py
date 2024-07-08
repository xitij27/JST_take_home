import time
from bitmex import bitmex
from datetime import datetime

# BitMEX production API details (Pricing Source)
BITMEX_PROD_API_KEY = "your_production_api_key"
BITMEX_PROD_API_SECRET = "your_production_api_secret"

# BitMEX Testnet API details (Target Venue)
BITMEX_TESTNET_API_KEY = "your_testnet_api_key"
BITMEX_TESTNET_API_SECRET = "your_testnet_api_secret"

# Cost constants
COST_TO_BUY = 0.005  # 50 basis points
COST_TO_SELL = 0.0075  # 75 basis points

# Repricing interval in seconds
REPRICE_INTERVAL = 60  # 1 minute

# Setup BitMEX API clients
prod_client = bitmex(test=False, api_key=BITMEX_PROD_API_KEY, api_secret=BITMEX_PROD_API_SECRET)
testnet_client = bitmex(test=True, api_key=BITMEX_TESTNET_API_KEY, api_secret=BITMEX_TESTNET_API_SECRET)

def get_bitmex_mid_price():
    """Fetch mid-price from BitMEX production."""
    orderbook = prod_client.OrderBook.OrderBook_getL2(symbol='XBTUSD', depth=1).result()
    bid_price = next(item for item in orderbook if item['side'] == 'Buy')['price']
    ask_price = next(item for item in orderbook if item['side'] == 'Sell')['price']
    mid_price = (ask_price + bid_price) / 2
    return mid_price

def calculate_prices(px_mid_1):
    """Calculate new buy and sell prices based on mid-price."""
    px1 = px_mid_1 * (1 - COST_TO_BUY)
    px2 = px_mid_1 * (1 + COST_TO_SELL)
    return px1, px2

def place_order(client, price, side):
    """Place limit order on BitMEX testnet."""
    order = client.Order.Order_new(
        symbol='XBTUSD',
        side=side,
        orderQty=1,  # Place a dummy quantity
        price=price,
        ordType='Limit'
    ).result()
    return order

# def main():
#     """Main loop to periodically reprice and replace orders."""
#     while True:
#         start_time = time.time()
        
#         # Fetch mid-price from BitMEX production
#         px_mid_1 = get_bitmex_mid_price()
        
#         # Calculate new buy and sell prices
#         px1, px2 = calculate_prices(px_mid_1)
        
#         # Cancel existing orders (if any)
#         testnet_client.Order.Order_cancelAll().result()
        
#         # Place new orders
#         place_order(testnet_client, px1, 'Buy')
#         place_order(testnet_client, px2, 'Sell')
        
#         # Measure latency
#         latency = time.time() - start_time
#         print(f"{datetime.now()} - Pricing and order placement latency: {latency:.2f} seconds")
        
#         # Wait until next repricing interval
#         time.sleep(REPRICE_INTERVAL)

def main():
    """Main loop to periodically fetch and print the mid-price."""
    while True:
        try:
            # Fetch mid-price from BitMEX production
            px_mid_1 = get_bitmex_mid_price()
            
            # Print the mid-price
            print(f"{datetime.now()} - Mid-price: {px_mid_1:.2f} USD")
            
            # Wait until next fetch interval
            time.sleep(FETCH_INTERVAL)
        except Exception as e:
            print(f"Error fetching price: {e}")
            
if __name__ == "__main__":
    main()

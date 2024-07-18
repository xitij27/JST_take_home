# Backstop Market Making Strategy
## Prerequisites 
### Conda Setup
Conda offers a very convenient way to manage different Python versions and environments. More instructions for installing the same can be found [here](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html)  

The conda version used for this project is **23.7.4**  

### Environment Setup
- **Python version**: 3.11.3  

The necessary packages have been listed in `jstenv.yml` file. To set up a new Python environment with the packages mentioned in `jstenv.yml`, the following command can be used in the directory containing this file:
```
$ conda env create -f jstenv.yml
```
This should create a new environment called `jstenv`.

## Running script.py
The Python environment we've created first needs to be activated using:
```
$ conda activate jstenv
```
To further verify if the correct environment is active, type `$ conda info --envs` into the terminal.  
Ensure that the terminal is in the directory containing `script.py`.
Now we can proceed with running the script as shown below:
```
$ python script.py
```

## Terminating the script
The code will continue running until `Ctrl`+`C` is pressed in the terminal.

## Current Approach
### Inspiration  
The current approach is inspired by this [YouTube video](https://www.youtube.com/watch?v=xKRRquqQkAo). The architecture and idea of using web socket for fetching reference prize was obtained through one of the slides shared in the video. A screenshot has also been provided with the name: `example_architecture.png`.

![Example Architecture](example_architecture.png)

### Overview
This project implements a backstop market maker that periodically fetches the reference price from a Binance WebSocket and places buy and sell orders on the BitMEX exchange based on predefined costs and quantities.

### 1. Fetching Reference Price
[Binance web socket documentation](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams) provides updates every ~1000ms. By saving the latest price received from the WebSocket and using it within a certain time frame (e.g., 600ms), we can avoid the delay typically caused by waiting for the latest value.

To achieve this, we record values received from the WebSocket in a separate thread called `WS-Thread`. This thread keeps the reference price updated regularly, allowing the `MainThread` to use the most recent price for calculating buy and sell prices. Consequently, the latency caused by fetching the reference price is reduced to zero.

We also save the time at which the latest price was recorded and if the difference between recording time and current time exceeds 600ms, we have a mechanism in place to wait for 300ms and then recheck the validity of latest recorded price.

### 2. Calculation of Target Prices
The target prices are calculated using the following formula:
```
tick_size = 0.5
buy_price = round(reference_price * (1 - self.buy_cost) / tick_size) * tick_size
sell_price = round(reference_price * (1 + self.sell_cost) / tick_size) * tick_size
```
The `tick_size` is smallest possible price increment for the trading instrument. It ensures that the prices are rounded to a multiple of this increment, i.e., 0.00 or 0.50.

Such basic arithmetic operations don't cause much latency and it's been almost always observed to give a reading of 0.000s for latency.

### 3. Cancellation and Placement of Orders

Cancellation and placement of orders are executed via BitMEX REST APIs, as detailed in the [documentation](https://testnet.bitmex.com/api/explorer/). Each REST API request is implemented through the `send_request` function, which manages the type (cancel, buy, sell) and handles the response.

#### **asyncio.gather**

`asyncio.gather` is a method provided by Python's `asyncio` module that allows multiple asynchronous functions to run concurrently and waits for all of them to complete. It takes multiple coroutine objects (functions defined with `async` and using `await` inside) as arguments and returns their results as a tuple. In our implementation, `asyncio.gather` handles the following tasks concurrently:

- Cancellation of existing orders
- Placement of new buy orders
- Placement of new sell orders

This approach ensures that these tasks are executed efficiently, reducing the overall cycle time.

#### Performance

Under stable and running conditions, the time taken to complete these tasks ranges between 200-500ms. However, during the initial run of the script, the first few cycles may show unstable results, for example, latencies as high as 1.5 seconds and then 800ms. After these initial cycles, the process gradually stabilizes to the 200-500ms range.

This step, involving the cancellation and placement of orders, is the most time-consuming part of our cycle.

### 4. Completion of Cycle
On the overall, we have managed to optimise step 1 and 3 which would've otherwise taken a lot of time if done synchronously on the same thread.  

In stable and running conditions, the cycle typically completes in 200-500ms. However, when running the script for the first time, the initial cycles exhibit unstable results, with latencies as high as 1.5 seconds. After these initial cycles, the timings stabilize within the expected range of 200-500ms.

### Sample Terminal Output for 5 Cycles
To get this output, the interval was set to 10 seconds, i.e., we are trying to run the cycle every 10 seconds. 
```
(jstenv) C:\Users\kp27d\Downloads\projects\JST_take_home>python script.py
2024-07-09 23:08:26,565 [INFO] [WS-Thread] Websocket connected
2024-07-09 23:08:26,566 [INFO] [WS-Thread] WebSocket connection opened
2024-07-09 23:08:26,566 [INFO] [MainThread] Waiting for price update...
2024-07-09 23:08:26,878 [INFO] [MainThread] Waiting for price update...
2024-07-09 23:08:27,185 [INFO] [MainThread] Waiting for price update...
2024-07-09 23:08:27,494 [INFO] [MainThread] Waiting for price update...
2024-07-09 23:08:27,625 [INFO] [WS-Thread] Fetched reference price: 57570.0
2024-07-09 23:08:27,805 [INFO] [MainThread] Reference price: 57570.00, Age: 0.180s
2024-07-09 23:08:27,805 [INFO] [MainThread] Calculating target prices...
2024-07-09 23:08:27,806 [INFO] [MainThread] Calculated buy price: 57282.00, sell price: 58002.00
2024-07-09 23:08:27,806 [INFO] [MainThread] Cancelling existing orders and placing new orders...
2024-07-09 23:08:28,635 [INFO] [WS-Thread] Fetched reference price: 57569.99
2024-07-09 23:08:29,295 [INFO] [MainThread] Orders cancelled: ['b11731f1-f3cd-434d-a6aa-d721182f5195', 'fa093aa9-ea6a-4888-b3e7-1a9d0ad26a3d'], buy order placed: b11731f1-f3cd-434d-a6aa-d721182f5195, sell order placed: fa093aa9-ea6a-4888-b3e7-1a9d0ad26a3d. Elapsed time: 1.4898 seconds
2024-07-09 23:08:29,295 [INFO] [MainThread] CYCLE COMPLETED. Total time: 1.4907 seconds

2024-07-09 23:08:29,631 [INFO] [WS-Thread] Fetched reference price: 57569.99
2024-07-09 23:08:30,633 [INFO] [WS-Thread] Fetched reference price: 57570.0
2024-07-09 23:08:31,629 [INFO] [WS-Thread] Fetched reference price: 57569.99
2024-07-09 23:08:32,632 [INFO] [WS-Thread] Fetched reference price: 57569.99
2024-07-09 23:08:33,631 [INFO] [WS-Thread] Fetched reference price: 57569.99
2024-07-09 23:08:34,631 [INFO] [WS-Thread] Fetched reference price: 57569.99
2024-07-09 23:08:35,631 [INFO] [WS-Thread] Fetched reference price: 57570.0
2024-07-09 23:08:36,633 [INFO] [WS-Thread] Fetched reference price: 57569.99
2024-07-09 23:08:37,635 [INFO] [WS-Thread] Fetched reference price: 57570.0
2024-07-09 23:08:38,634 [INFO] [WS-Thread] Fetched reference price: 57570.0
2024-07-09 23:08:39,309 [INFO] [MainThread] Waiting for price update...
2024-07-09 23:08:39,620 [INFO] [MainThread] Waiting for price update...
2024-07-09 23:08:39,634 [INFO] [WS-Thread] Fetched reference price: 57570.0
2024-07-09 23:08:39,931 [INFO] [MainThread] Reference price: 57570.00, Age: 0.296s
2024-07-09 23:08:39,931 [INFO] [MainThread] Calculating target prices...
2024-07-09 23:08:39,931 [INFO] [MainThread] Calculated buy price: 57282.00, sell price: 58002.00
2024-07-09 23:08:39,931 [INFO] [MainThread] Cancelling existing orders and placing new orders...
2024-07-09 23:08:40,637 [INFO] [WS-Thread] Fetched reference price: 57569.99
2024-07-09 23:08:40,825 [INFO] [MainThread] Orders cancelled: ['3ef32c5d-a7eb-447a-9002-869c31505ae2'], buy order placed: 3ef32c5d-a7eb-447a-9002-869c31505ae2, sell order placed: 0d0c131a-d883-4d3f-8932-3b6c5551ab70. Elapsed time: 0.8931 seconds
2024-07-09 23:08:40,826 [INFO] [MainThread] CYCLE COMPLETED. Total time: 0.8952 seconds

2024-07-09 23:08:41,634 [INFO] [WS-Thread] Fetched reference price: 57569.99
2024-07-09 23:08:42,640 [INFO] [WS-Thread] Fetched reference price: 57570.0
2024-07-09 23:08:43,635 [INFO] [WS-Thread] Fetched reference price: 57570.0
2024-07-09 23:08:44,640 [INFO] [WS-Thread] Fetched reference price: 57569.99
2024-07-09 23:08:45,635 [INFO] [WS-Thread] Fetched reference price: 57569.99
2024-07-09 23:08:46,636 [INFO] [WS-Thread] Fetched reference price: 57569.99
2024-07-09 23:08:47,641 [INFO] [WS-Thread] Fetched reference price: 57570.0
2024-07-09 23:08:48,638 [INFO] [WS-Thread] Fetched reference price: 57569.99
2024-07-09 23:08:49,641 [INFO] [WS-Thread] Fetched reference price: 57569.99
2024-07-09 23:08:50,641 [INFO] [WS-Thread] Fetched reference price: 57570.0
2024-07-09 23:08:50,831 [INFO] [MainThread] Reference price: 57570.00, Age: 0.190s
2024-07-09 23:08:50,831 [INFO] [MainThread] Calculating target prices...
2024-07-09 23:08:50,831 [INFO] [MainThread] Calculated buy price: 57282.00, sell price: 58002.00
2024-07-09 23:08:50,831 [INFO] [MainThread] Cancelling existing orders and placing new orders...
2024-07-09 23:08:51,059 [INFO] [MainThread] Orders cancelled: ['b8b4e38e-de93-486f-a7c3-ef32282e7b17', '0d0c131a-d883-4d3f-8932-3b6c5551ab70'], buy order placed: 4b1f5da1-5e9e-49de-b816-9e55f95a5284, sell order placed: b8b4e38e-de93-486f-a7c3-ef32282e7b17. Elapsed time: 0.2264 seconds
2024-07-09 23:08:51,059 [INFO] [MainThread] CYCLE COMPLETED. Total time: 0.2279 seconds

2024-07-09 23:08:51,635 [INFO] [WS-Thread] Fetched reference price: 57570.0
2024-07-09 23:08:52,642 [INFO] [WS-Thread] Fetched reference price: 57560.79
2024-07-09 23:08:53,636 [INFO] [WS-Thread] Fetched reference price: 57560.53
2024-07-09 23:08:54,640 [INFO] [WS-Thread] Fetched reference price: 57560.52
2024-07-09 23:08:55,640 [INFO] [WS-Thread] Fetched reference price: 57560.52
2024-07-09 23:08:56,643 [INFO] [WS-Thread] Fetched reference price: 57560.53
2024-07-09 23:08:57,643 [INFO] [WS-Thread] Fetched reference price: 57560.53
2024-07-09 23:08:58,642 [INFO] [WS-Thread] Fetched reference price: 57560.52
2024-07-09 23:08:59,647 [INFO] [WS-Thread] Fetched reference price: 57560.52
2024-07-09 23:09:00,645 [INFO] [WS-Thread] Fetched reference price: 57560.53
2024-07-09 23:09:01,072 [INFO] [MainThread] Reference price: 57560.53, Age: 0.427s
2024-07-09 23:09:01,073 [INFO] [MainThread] Calculating target prices...
2024-07-09 23:09:01,073 [INFO] [MainThread] Calculated buy price: 57272.50, sell price: 57992.00
2024-07-09 23:09:01,074 [INFO] [MainThread] Cancelling existing orders and placing new orders...
2024-07-09 23:09:01,316 [INFO] [MainThread] Orders cancelled: ['4b1f5da1-5e9e-49de-b816-9e55f95a5284'], buy order placed: 54719057-8391-4120-a811-0d65202de74f, sell order placed: e6ab132a-b854-4772-84f5-28393aedc12b. Elapsed time: 0.2420 seconds
2024-07-09 23:09:01,317 [INFO] [MainThread] CYCLE COMPLETED. Total time: 0.2451 seconds

2024-07-09 23:09:01,646 [INFO] [WS-Thread] Fetched reference price: 57560.53
2024-07-09 23:09:02,644 [INFO] [WS-Thread] Fetched reference price: 57560.52
2024-07-09 23:09:03,644 [INFO] [WS-Thread] Fetched reference price: 57560.53
2024-07-09 23:09:04,645 [INFO] [WS-Thread] Fetched reference price: 57560.53
2024-07-09 23:09:05,650 [INFO] [WS-Thread] Fetched reference price: 57560.53
2024-07-09 23:09:06,646 [INFO] [WS-Thread] Fetched reference price: 57560.52
2024-07-09 23:09:07,647 [INFO] [WS-Thread] Fetched reference price: 57560.52
2024-07-09 23:09:08,649 [INFO] [WS-Thread] Fetched reference price: 57560.52
2024-07-09 23:09:09,650 [INFO] [WS-Thread] Fetched reference price: 57560.52
2024-07-09 23:09:10,650 [INFO] [WS-Thread] Fetched reference price: 57560.52
2024-07-09 23:09:11,329 [INFO] [MainThread] Waiting for price update...
2024-07-09 23:09:11,642 [INFO] [MainThread] Waiting for price update...
2024-07-09 23:09:11,649 [INFO] [WS-Thread] Fetched reference price: 57560.52
2024-07-09 23:09:11,950 [INFO] [MainThread] Reference price: 57560.52, Age: 0.301s
2024-07-09 23:09:11,950 [INFO] [MainThread] Calculating target prices...
2024-07-09 23:09:11,950 [INFO] [MainThread] Calculated buy price: 57272.50, sell price: 57992.00
2024-07-09 23:09:11,950 [INFO] [MainThread] Cancelling existing orders and placing new orders...
2024-07-09 23:09:12,186 [INFO] [MainThread] Orders cancelled: ['da0249fc-d2fd-495e-9d85-cdd4556536e5', 'e6ab132a-b854-4772-84f5-28393aedc12b', '54719057-8391-4120-a811-0d65202de74f'], buy order placed: f4078ab6-79bc-429d-b9c3-ffe8e91304c2, sell order placed: da0249fc-d2fd-495e-9d85-cdd4556536e5. Elapsed time: 0.2362 seconds
2024-07-09 23:09:12,186 [INFO] [MainThread] CYCLE COMPLETED. Total time: 0.2362 seconds
```

## Extra Details
### First Approach: 
**Single thread, Synchronous Execution, All requests are HTTP**  
This example has been provided to compare the current approach with the most basic approach.
This took roughly 1.8-2.5 seconds for completion of one cycle. The latency isn't impressive!
```
--- Starting new cycle ---
1. Getting reference price...
   Reference price: 57427.13
   Latency for getting reference price: 0.2999 seconds
2. Calculating target prices...
   Buy price: 57140.00
   Sell price: 57858.00
   Latency for calculating target prices: 0.0000 seconds
3. Cancelling existing orders...
   Cancel result: []
   Latency for cancelling existing orders: 0.4285 seconds
4. Placing new orders...
   Buy order result: 02e566e4-81c0-45df-9b1e-400208c9115a
   Sell order result: cb993f70-acfc-4483-b7e8-c1048c45135d
   Latency for placing new orders: 1.3075 seconds
5. Cycle completed. Total time: 2.0401 seconds
Waiting for 60 seconds before next cycle...
```
### Brief Overview of Other Attempts
- Tried `bitmex` Python client [(Github repo)](https://github.com/BitMEX/api-connectors/tree/master/official-http/python-swaggerpy)  
Negligible improvement in latency but it can help avoid generation of signature for the BitMEX API. 
- Tried to place orders with [BitMEX REST APIs](https://testnet.bitmex.com/api/explorer/)  
Initially had trouble generating a valid signature which was probably caused by byte to utf-8 conversions but that got fixed with a few changes. Realised that [BitMEX Github code](https://github.com/BitMEX/api-connectors/blob/master/official-http/python-swaggerpy/BitMEXAPIKeyAuthenticator.py) might not work smoothly. Once this was working, I was able to focus on Multithreading to get that latest price without waiting for 600-800ms.


## Possible Improvements  
### Time taken for Cancellation and Placement of Orders  
By far, this is the only step now which significantly affects the overall time to complete one cycle. If possible, I'd like to seek guidance and explore other ways with which we can decrease the latency introduced by this step, i.e., REST API response time.

### Separation of Concerns
The code could've been split into smaller, more manageable modules. For example, new classes for each exchange, web socket handling, etc., could've been implemented. I didn't want to redesign the entire code after coming this far. But since this was my first attempt at making a trading bot, I'm sure I'll have a better idea on how to start next time.

### Calculation of Prices
The calculations performed in the code are currently simple and thus have negligible latency. However, for more complex computations, it is important to explore optimization techniques to minimize latency.

### More Configurable Variables & Coding Conventions
The code could've been made more configurable by allowing the user to specify the exchange, the symbol, API for fetching reference price, etc. While C++ has access specifiers like public, private and protected, Python does not. Therefore, in Python we use naming conventions to denote the intended access levels as shown below:
- **Public:** No underscore prefix. Example: self.symbol
- **Protected:** Single underscore prefix. Example: self._ws
- **Private:** Double underscore prefix. Example: self.__api_key

### Unit Testing and Integration Testing
Unit tests could've been written to cover various parts of the code like API interactions and WebSocket handling. Integration testing could've helped determine whether all the components work together as expected.




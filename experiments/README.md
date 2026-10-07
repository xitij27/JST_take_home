# Experiments

These are the earlier iterations, in order, that led to [`market_maker.py`](../market_maker.py). They are kept as a record of how the design evolved and are **not maintained**. Use the top-level script for anything real.

Every file now has a module docstring describing its step, plus docstrings or comments for each class and function. Most of these were added later, when the repo was tidied up, and the code itself was left unchanged.

## Running them

Like the final version, they read credentials from the `BITMEX_API_KEY` and `BITMEX_API_SECRET` environment variables and trade on the BitMEX **testnet**. Steps 01, 02 and 09 also need the official BitMEX client (`pip install bitmex`), which is not in `requirements.txt`. The C++ file is an unfinished sketch and isn't meant to be built.

## At a glance

| Step | Reference price | Order requests |
|---|---|---|
| [01](01_bitmex_client.py) | Binance REST, fetched every cycle | `bitmex` client, one after another |
| [02](02_bitmex_client_order_results.py) | Binance REST, fetched every cycle | `bitmex` client, one after another |
| [03](03_signed_rest_sync.py) | Binance REST, fetched every cycle | Hand-signed REST (`requests`), one after another |
| [03 (C++)](03_signed_rest_sync.cpp) | Placeholder value | Hand-signed REST (libcurl), one after another |
| [04](04_signed_rest_async.py) | Binance REST (`aiohttp`), fetched every cycle | Hand-signed REST (`aiohttp`), one after another |
| [05](05_ws_price_sync_orders.py) | WebSocket thread | Hand-signed REST (`requests`), one after another |
| [06](06_ws_price_async_orders.py) | WebSocket thread | Hand-signed REST (`aiohttp`), **concurrent** |
| [07](07_order_quantities.py) | WebSocket thread, timestamped | Concurrent |
| [08](08_logging.py) | WebSocket thread, 0.8 s freshness check | Concurrent |
| [09](09_logging_bitmex_client.py) | WebSocket thread, timestamped | `bitmex` client in a thread pool, concurrent |
| [10](10_docstrings.py) | WebSocket thread, 0.8 s freshness check | Concurrent |
| [11](11_order_id_logging.py) | WebSocket thread, 0.6 s freshness check | Concurrent, order IDs logged |
| [12](12_wait_for_ws_connection.py) | WebSocket thread, 0.6 s freshness check, waits for connection | Concurrent, order IDs logged |

## Step by step

### Phase 1: a synchronous baseline (01–03)

**[`01_bitmex_client.py`](01_bitmex_client.py)** is the simplest possible version. On a single thread, each cycle:
1. fetches BTC/USDT from Binance's REST ticker,
2. computes the quotes,
3. cancels all orders with the official `bitmex` Python client,
4. places a buy and a sell on the `XBTUSD` inverse perpetual, 100 contracts each.

It has a bug. `place_orders` places both sides, but the loop calls it twice, the second time with the prices swapped. Each cycle therefore sends four orders, and two of them (a buy at the ask price and a sell at the bid price) are priced through the market, so they would likely fill immediately.

**[`02_bitmex_client_order_results.py`](02_bitmex_client_order_results.py)** fixes that bug. `place_orders` is called once and returns both orders, and the loop prints the ID and status of every order placed or cancelled.

**[`03_signed_rest_sync.py`](03_signed_rest_sync.py)** drops the client library in favour of raw REST calls made with `requests`. It signs each request by hand: an HMAC-SHA256 of the verb, path, expiry and body, sent in the `api-key`, `api-expires` and `api-signature` headers. Each step is timed separately, which produced the baseline in the main README: about 1.8–2.5 s per cycle, most of it spent in the three sequential order requests.

The first, commented-out version of `generate_signature` is still in the file. The working version differs by decoding a bytes body to text before signing, which is the bytes/UTF-8 issue mentioned in the main README.

**[`03_signed_rest_sync.cpp`](03_signed_rest_sync.cpp)** is a C++ port of step 03 using libcurl and OpenSSL. It was never finished, for two reasons:
- `get_reference_price` returns a hard-coded placeholder (`50000.0`) instead of parsing Binance's response.
- The signature is base64-encoded over the full URL, but BitMEX expects a hex digest over the path, so its requests would be rejected.

### Phase 2: taking the network off the critical path (04–06)

**[`04_signed_rest_async.py`](04_signed_rest_async.py)** moves to `asyncio` and `aiohttp`, with one `ClientSession` reused across requests. Every request is still awaited one after another, so there is no concurrency yet. Each step logs its elapsed time and a timestamp.

**[`05_ws_price_sync_orders.py`](05_ws_price_sync_orders.py)** moves the reference price to a background thread subscribed to Binance's `btcusdt@ticker` WebSocket stream, which pushes the last price about once a second. Orders go back to synchronous `requests`. After each cycle the cached price is reset to `None`, so the next cycle waits for a new tick, in 1 s steps, instead of using the cached one.

**[`06_ws_price_async_orders.py`](06_ws_price_async_orders.py)** combines the two ideas and is the architecture of the final version. The WebSocket thread keeps the latest price, and the cancel, buy and sell requests go out at the same time with `asyncio.gather`. The cached price is now reused, so the loop only waits for the very first one. SIGINT and SIGTERM handlers close the WebSocket on <kbd>Ctrl</kbd>+<kbd>C</kbd>.

Two problems that lasted into the final version start here:
- Sending "cancel all" concurrently with the new orders can also cancel those new orders. This was fixed later; see [Bug fix: cancel/place race](../README.md#bug-fix-cancelplace-race).
- Any WebSocket error or close sets the stop flag, so the reconnect loop never actually reconnects. This is listed under [Future work](../README.md#future-work).

### Phase 3: hardening and polish (07–12)

**[`07_order_quantities.py`](07_order_quantities.py)** turns order sizes into parameters and switches to the linear `XBTUSDT` contract at 1000 contracts, which is 0.001 BTC, the minimum. The WebSocket thread now records when each price arrived. The interval goes from 5 s to 10 s.

**[`08_logging.py`](08_logging.py)** makes several changes:
- It replaces `print` with `logging` and names the threads (`MainThread`, `WS-Thread`), so the log shows which thread did what.
- It adds the first freshness check: if the cached price is more than 0.8 s old, the loop waits 0.5 s and checks again.
- It fixes the `on_close` callback signature for current `websocket-client` versions and raises on HTTP errors with `raise_for_status`.

**[`09_logging_bitmex_client.py`](09_logging_bitmex_client.py)** is a side experiment from the same point. The loop is the same, but orders go through the official `bitmex` client, run in a thread pool (`run_in_executor`) so the three blocking calls can still overlap. The main README notes that the client made a negligible difference to latency. Later steps go back to the hand-signed `aiohttp` requests.

**[`10_docstrings.py`](10_docstrings.py)** is where docstrings and comments were first written. Its code is identical to step 08.

**[`11_order_id_logging.py`](11_order_id_logging.py)** tightens the freshness check to 0.6 s, with 0.3 s retries. It also makes the cancel and place calls return order IDs and logs them every cycle. Those logs are what later made the cancel/place race visible.

**[`12_wait_for_ws_connection.py`](12_wait_for_ws_connection.py)** adds a `connection_ready` event, so the main loop waits for the WebSocket connection to open before the first cycle.

### From step 12 to `market_maker.py`

The original final script was step 12's code with the docstrings restored and the interval set to 60 s. Since then, [`market_maker.py`](../market_maker.py) has gained:
- credentials read from environment variables instead of a `KEYS.py` file,
- the [cancel/place race fix](../README.md#bug-fix-cancelplace-race): each cycle cancels only the previous cycle's orders, by ID,
- error handling for each request separately, so successfully placed orders stay tracked even when other requests fail.

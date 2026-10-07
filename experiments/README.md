# Experiments

These are the earlier iterations, in order, that led to [`market_maker.py`](../market_maker.py). They are kept as a record of how the design evolved and are **not maintained**. Use the top-level script for anything real.

Like the final version, they read credentials from the `BITMEX_API_KEY` and `BITMEX_API_SECRET` environment variables. A few also need the `bitmex` client library, which is not in `requirements.txt`.

| Step | What changed / what it tried |
|---|---|
| [`01_bitmex_client.py`](01_bitmex_client.py) | Official `bitmex` Python client (Swagger) for orders, Binance REST for the reference price. Single thread, synchronous. |
| [`02_bitmex_client_order_results.py`](02_bitmex_client_order_results.py) | Same as step 01, but returns the order results so they can be inspected. |
| [`03_signed_rest_sync.py`](03_signed_rest_sync.py) | Replaced the `bitmex` client with hand-rolled HMAC-SHA256 signing against the BitMEX REST API (`requests`). Prints per-step latency. This is the *first approach* baseline in the main README (~2 s per cycle). |
| [`03_signed_rest_sync.cpp`](03_signed_rest_sync.cpp) | C++ port of step 03 using libcurl and OpenSSL. |
| [`04_signed_rest_async.py`](04_signed_rest_async.py) | Moved to `aiohttp` + `asyncio`. Reference price is still polled over REST. |
| [`05_ws_price_sync_orders.py`](05_ws_price_sync_orders.py) | Reference price now comes from the Binance WebSocket in a background thread. Orders are still sent synchronously with `requests`. |
| [`06_ws_price_async_orders.py`](06_ws_price_async_orders.py) | WebSocket thread combined with `asyncio.gather` for cancel/buy/sell, which is the architecture of the final version. |
| [`07_order_quantities.py`](07_order_quantities.py) | Adds configurable buy/sell quantities. |
| [`08_logging.py`](08_logging.py) | Replaces `print` with `logging`, tagging each line with its thread name. |
| [`09_logging_bitmex_client.py`](09_logging_bitmex_client.py) | Same as step 08, but sends orders through the `bitmex` client in a thread-pool executor. |
| [`10_docstrings.py`](10_docstrings.py) | Adds docstrings. Price staleness threshold of 0.8 s. |
| [`11_order_id_logging.py`](11_order_id_logging.py) | Logs the order IDs returned by the cancel and place calls. Staleness threshold back to 0.6 s. |
| [`12_wait_for_ws_connection.py`](12_wait_for_ws_connection.py) | Adds the `connection_ready` event so the first cycle waits for the WebSocket to open. |

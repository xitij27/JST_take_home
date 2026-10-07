# Experiments

These are the earlier iterations that led to [`market_maker.py`](../market_maker.py). They are kept as a record of how the design evolved and are **not maintained**. Use the top-level script for anything real.

Like the final version, they read credentials from the `BITMEX_API_KEY` and `BITMEX_API_SECRET` environment variables. A few also need the `bitmex` client library, which is not in `requirements.txt`.

| File | What changed / what it tried |
|---|---|
| `script2.py` | Official `bitmex` Python client (Swagger) for orders, Binance REST for the reference price. Single thread, synchronous. |
| `script2_1.py` | Same as `script2.py`, but returns the order results so they can be inspected. |
| `script3.py` | Replaced the `bitmex` client with hand-rolled HMAC-SHA256 signing against the BitMEX REST API (`requests`). Prints per-step latency. This is the *first approach* baseline in the main README (~2 s per cycle). |
| `script3.cpp` | C++ port of the synchronous REST approach using libcurl and OpenSSL. |
| `script3async.py` | Moved to `aiohttp` + `asyncio`. Reference price is still polled over REST. |
| `script3ws.py` | Reference price now comes from the Binance WebSocket in a background thread. Orders are still sent synchronously with `requests`. |
| `script3wsasync.py` | WebSocket thread combined with `asyncio.gather` for cancel/buy/sell, which is the architecture of the final version. |
| `script3wsasync1.py` | Adds configurable buy/sell quantities. |
| `script3wsasynclogging.py` | Replaces `print` with `logging`, tagging each line with its thread name. |
| `script3wsasyncloggingbitmex.py` | Same as above, but sends orders through the `bitmex` client in a thread-pool executor. |
| `script4.py` | Adds docstrings. Price staleness threshold of 0.8 s. |
| `script4oID.py` | Logs the order IDs returned by the cancel and place calls. Staleness threshold back to 0.6 s. |
| `script4oID1.py` | Adds the `connection_ready` event so the first cycle waits for the WebSocket to open. |

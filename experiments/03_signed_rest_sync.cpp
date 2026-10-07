// Step 03 (C++): unfinished port of 03_signed_rest_sync.py using libcurl and OpenSSL.
//
// Intended flow, every `interval` seconds: fetch the Binance BTC/USDT price, compute
// a bid and an ask around it, cancel all BitMEX testnet orders and place the new
// ones, one request at a time.
//
// Not finished, and not meant to be built or run as is:
//   - get_reference_price() returns a hard-coded placeholder instead of parsing
//     Binance's response.
//   - generate_signature() base64-encodes an HMAC over the full URL, but BitMEX
//     expects a hex-encoded HMAC over the path, so BitMEX would reject every
//     signed request.
//
// Reads BITMEX_API_KEY and BITMEX_API_SECRET from the environment.
// See experiments/README.md for how this step fits into the project's history.

#include <cstdlib>
#include <iostream>
#include <string>
#include <chrono>
#include <thread>
#include <curl/curl.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/buffer.h>

using namespace std;

// Base64-encode a byte buffer with OpenSSL's BIO API. Used for the request
// signature, although BitMEX actually expects it hex-encoded.
string base64_encode(const unsigned char* buffer, size_t length) {
    BIO* bio;
    BIO* b64;
    BUF_MEM* buffer_ptr;
    string result;

    b64 = BIO_new(BIO_f_base64());
    bio = BIO_new(BIO_s_mem());
    bio = BIO_push(b64, bio);
    BIO_write(bio, buffer, length);
    BIO_flush(bio);
    BIO_get_mem_ptr(bio, &buffer_ptr);

    result.assign(buffer_ptr->data, buffer_ptr->length - 1);
    BIO_free_all(bio);

    return result;
}

// HMAC-SHA256 of verb + url + expires + data, keyed with the API secret and
// returned base64-encoded. BitMEX instead signs the path (not the full URL) and
// expects the digest hex-encoded, so these signatures would be rejected.
string generate_signature(const string& secret, const string& verb, const string& url, int expires, const string& data) {
    string message = verb + url + to_string(expires) + data;
    unsigned char* digest;
    digest = HMAC(EVP_sha256(), secret.c_str(), secret.length(), reinterpret_cast<const unsigned char*>(message.c_str()), message.length(), nullptr, nullptr);

    return base64_encode(digest, SHA256_DIGEST_LENGTH);
}

// Send an HTTP request with libcurl and return the response body. Supports GET,
// POST (with a JSON body) and DELETE. Always adds the BitMEX auth headers, even
// for the Binance request, which passes empty credentials. api-expires is meant
// to be now + 5 s, but dividing system_clock ticks by 1,000,000 only gives
// seconds when the clock counts microseconds. Errors are printed, and whatever
// was received (possibly nothing) is returned.
string perform_request(const string& url, const string& verb, const string& data, const string& api_key, const string& api_secret) {
    CURL* curl;
    CURLcode res;
    struct curl_slist* headers = nullptr;
    string read_buffer;
    string api_expires = to_string(chrono::system_clock::now().time_since_epoch().count() / 1000000 + 5);

    curl_global_init(CURL_GLOBAL_DEFAULT);
    curl = curl_easy_init();
    if (curl) {
        string signature = generate_signature(api_secret, verb, url, stoi(api_expires), data);

        headers = curl_slist_append(headers, ("api-expires: " + api_expires).c_str());
        headers = curl_slist_append(headers, ("api-key: " + api_key).c_str());
        headers = curl_slist_append(headers, ("api-signature: " + signature).c_str());
        headers = curl_slist_append(headers, "Content-Type: application/json");

        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        
        if (verb == "POST") {
            curl_easy_setopt(curl, CURLOPT_POSTFIELDS, data.c_str());
        } else if (verb == "DELETE") {
            curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
        }
        
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, [](void* contents, size_t size, size_t nmemb, void* userp) -> size_t {
            ((string*)userp)->append((char*)contents, size * nmemb);
            return size * nmemb;
        });
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &read_buffer);

        res = curl_easy_perform(curl);
        if (res != CURLE_OK) {
            cerr << "curl_easy_perform() failed: " << curl_easy_strerror(res) << endl;
        }

        curl_easy_cleanup(curl);
        curl_slist_free_all(headers);
    }

    curl_global_cleanup();
    return read_buffer;
}

// Quotes a bid and an ask around the reference price on the BitMEX testnet, one
// request at a time. C++ counterpart of BackstopMarketMaker in
// 03_signed_rest_sync.py.
class BackstopMarketMaker {
public:
    // Store the credentials and quoting parameters. Orders go to the BitMEX testnet.
    BackstopMarketMaker(const string& api_key, const string& api_secret, const string& symbol, double buy_cost, double sell_cost, int interval)
        : api_key(api_key), api_secret(api_secret), symbol(symbol), buy_cost(buy_cost), sell_cost(sell_cost), interval(interval) {
        base_url = "https://testnet.bitmex.com/api/v1";
    }

    // Run the market-making loop forever: fetch the reference price, compute the
    // quotes, cancel all orders and place new ones, print each step and the
    // cycle's total time, then sleep `interval` seconds.
    void run() {
        while (true) {
            cout << "\n--- Starting new cycle ---" << endl;
            auto start_time = chrono::high_resolution_clock::now();

            try {
                cout << "1. Getting reference price..." << endl;
                double reference_price = get_reference_price();
                cout << "   Reference price: " << reference_price << endl;

                cout << "2. Calculating target prices..." << endl;
                double buy_price, sell_price;
                tie(buy_price, sell_price) = calculate_target_prices(reference_price);
                cout << "   Buy price: " << buy_price << endl;
                cout << "   Sell price: " << sell_price << endl;

                cout << "3. Cancelling existing orders..." << endl;
                string cancel_result = cancel_existing_orders();
                cout << "   Cancel result: " << cancel_result << endl;

                cout << "4. Placing new orders..." << endl;
                string buy_order = place_order(buy_price, "Buy");
                string sell_order = place_order(sell_price, "Sell");
                cout << "   Buy order result: " << buy_order << endl;
                cout << "   Sell order result: " << sell_order << endl;

                auto end_time = chrono::high_resolution_clock::now();
                chrono::duration<double> latency = end_time - start_time;
                cout << "5. Cycle completed. Latency: " << latency.count() << " seconds" << endl;

            } catch (const exception& e) {
                cerr << "Error occurred: " << e.what() << endl;
            }

            cout << "Waiting for " << interval << " seconds before next cycle..." << endl;
            this_thread::sleep_for(chrono::seconds(interval));
        }
    }

private:
    // Configuration and credentials.
    string base_url;
    string api_key;
    string api_secret;
    string symbol;
    double buy_cost;
    double sell_cost;
    int interval;

    // Meant to fetch the BTC/USDT price from Binance's REST ticker. Unfinished: the
    // response is fetched but never parsed, and a placeholder price is returned.
    double get_reference_price() {
        string url = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT";
        string response = perform_request(url, "GET", "", "", "");
        // Parse the response to extract the price
        // Assuming the response is JSON and we have a method to parse it
        return 50000.0; // Placeholder value
    }

    // Bid buy_cost below and ask sell_cost above the reference price, both rounded
    // to the 0.5 tick size. Returns (buy_price, sell_price).
    pair<double, double> calculate_target_prices(double reference_price) {
        double tick_size = 0.5;
        double buy_price = round(reference_price * (1 - buy_cost) / tick_size) * tick_size;
        double sell_price = round(reference_price * (1 + sell_cost) / tick_size) * tick_size;
        return make_pair(buy_price, sell_price);
    }

    // Place a 100-contract limit order on the given side ("Buy" or "Sell") at
    // price. Returns the raw response body.
    string place_order(double price, const string& side) {
        string url = base_url + "/order";
        string data = "{\"symbol\":\"" + symbol + "\",\"price\":" + to_string(price) + ",\"orderQty\":100,\"side\":\"" + side + "\",\"ordType\":\"Limit\"}";
        return perform_request(url, "POST", data, api_key, api_secret);
    }

    // Cancel every open order (DELETE /order/all). Returns the raw response body.
    string cancel_existing_orders() {
        string url = base_url + "/order/all";
        return perform_request(url, "DELETE", "", api_key, api_secret);
    }
};

// Read the credentials from the environment, configure the market maker and run
// it. Both variables must be set: getenv returns null otherwise, and building a
// std::string from null is undefined behaviour.
int main() {
    string api_key = getenv("BITMEX_API_KEY");
    string api_secret = getenv("BITMEX_API_SECRET");
    string symbol = "XBTUSD";
    double buy_cost = 0.0050;
    double sell_cost = 0.0075;
    int interval = 60; // 1 minute

    BackstopMarketMaker market_maker(api_key, api_secret, symbol, buy_cost, sell_cost, interval);
    market_maker.run();

    return 0;
}

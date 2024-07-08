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

// Utility function to base64 encode the signature
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

// Utility function to generate HMAC SHA256 signature
string generate_signature(const string& secret, const string& verb, const string& url, int expires, const string& data) {
    string message = verb + url + to_string(expires) + data;
    unsigned char* digest;
    digest = HMAC(EVP_sha256(), secret.c_str(), secret.length(), reinterpret_cast<const unsigned char*>(message.c_str()), message.length(), nullptr, nullptr);

    return base64_encode(digest, SHA256_DIGEST_LENGTH);
}

// Utility function to perform HTTP request
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

// Class to handle market making
class BackstopMarketMaker {
public:
    BackstopMarketMaker(const string& api_key, const string& api_secret, const string& symbol, double buy_cost, double sell_cost, int interval)
        : api_key(api_key), api_secret(api_secret), symbol(symbol), buy_cost(buy_cost), sell_cost(sell_cost), interval(interval) {
        base_url = "https://testnet.bitmex.com/api/v1";
    }

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
    string base_url;
    string api_key;
    string api_secret;
    string symbol;
    double buy_cost;
    double sell_cost;
    int interval;

    double get_reference_price() {
        string url = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT";
        string response = perform_request(url, "GET", "", "", "");
        // Parse the response to extract the price
        // Assuming the response is JSON and we have a method to parse it
        return 50000.0; // Placeholder value
    }

    pair<double, double> calculate_target_prices(double reference_price) {
        double tick_size = 0.5;
        double buy_price = round(reference_price * (1 - buy_cost) / tick_size) * tick_size;
        double sell_price = round(reference_price * (1 + sell_cost) / tick_size) * tick_size;
        return make_pair(buy_price, sell_price);
    }

    string place_order(double price, const string& side) {
        string url = base_url + "/order";
        string data = "{\"symbol\":\"" + symbol + "\",\"price\":" + to_string(price) + ",\"orderQty\":100,\"side\":\"" + side + "\",\"ordType\":\"Limit\"}";
        return perform_request(url, "POST", data, api_key, api_secret);
    }

    string cancel_existing_orders() {
        string url = base_url + "/order/all";
        return perform_request(url, "DELETE", "", api_key, api_secret);
    }
};

int main() {
    string api_key = "isZdN8ybNIMSinAk9WztI-Fr";
    string api_secret = "Aeyzah7-8ML26kXxDGEHmCGfPey_z66alSnz8PiAJwV1zDzP";
    string symbol = "XBTUSD";
    double buy_cost = 0.0050;
    double sell_cost = 0.0075;
    int interval = 60; // 1 minute

    BackstopMarketMaker market_maker(api_key, api_secret, symbol, buy_cost, sell_cost, interval);
    market_maker.run();

    return 0;
}

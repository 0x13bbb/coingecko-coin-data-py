import concurrent.futures
import requests
import json
import logging
import traceback
import time
from queue import Queue, Empty
from threading import Lock, Thread, Event
import sys
from config import loadConfig

logging.basicConfig(filename='cgk_threaded.log', filemode='w', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
config = loadConfig()

backup_count = 0
BACKUP_THRESHOLD = 2500

class RateLimiter:
    def __init__(self, calls, per_second):
        self.calls = calls
        self.per_second = per_second
        self.lock = Lock()
        self.calls_made = 0
        self.start_time = time.time()

    def wait(self):
        with self.lock:
            if self.calls_made >= self.calls:
                elapsed = time.time() - self.start_time
                if elapsed < self.per_second:
                    logger.info("Within 4% of rate limit, pre-emptively sleeping")
                    time.sleep(self.per_second - elapsed)
                self.calls_made = 0
                self.start_time = time.time()
            self.calls_made += 1

def makeGetRequest(url, headers, rate_limiter):
    rate_limiter.wait()
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_error:
        if http_error.response.status_code == 429:
            logger.info("Hit rate limit, sleeping for 20s")
            time.sleep(20)
            return makeGetRequest(url, headers, rate_limiter)
        else:
            raise

def safe_get(d, *keys):
    for key in keys:
        if isinstance(d, dict):
            d = d.get(key)
        else:
            logger.info(f"No such keys found: {keys}")
            return None
    return d

def writeJSON(data, filename):
    with open(filename, 'w') as file:
        json.dump(data, file, indent=2)

def writeBackup(normalized_data, raw_data):
    logger.info(f"Backing up {len(normalized_data)} items")
    for filename, data in [('new_cgkhardlist_threaded.json', normalized_data), 
                           ('raw_cgoutput_threaded.json', raw_data)]:
        try:
            with open(filename, 'r+') as f:
                existing_data = json.load(f)
                existing_data.extend(data)
                f.seek(0)
                json.dump(existing_data, f, indent=2)
                f.truncate()
        except FileNotFoundError:
            with open(filename, 'w') as f:
                json.dump(data, f, indent=2)
        except json.decoder.JSONDecodeError:
            logger.error(f"Error decoding JSON from {filename}")
        except Exception as e:
            logger.error(f"Error writing backup: {e}")
            logger.error(traceback.format_exc())

def periodic_backup(output_queue, raw_output_queue):
    normalized_buffer = []
    raw_buffer = []
    while True:
        try:
            normalized = output_queue.get(timeout=1)
            if normalized is None:
                break
            normalized_buffer.append(normalized)

            raw = raw_output_queue.get(timeout=1)
            if raw is None:
                break
            raw_buffer.append(raw)

            if len(normalized_buffer) >= BACKUP_THRESHOLD:
                writeBackup(normalized_buffer, raw_buffer)
                normalized_buffer = []
                raw_buffer = []
        except Empty:
            if normalized_buffer:
                writeBackup(normalized_buffer, raw_buffer)
                normalized_buffer = []
                raw_buffer = []

    # Write any remaining data
    if normalized_buffer:
        writeBackup(normalized_buffer, raw_buffer)

def process_coin(coin, headers, info_url, rate_limiter):
    try:
        logger.info(f"Processing {coin['id']}")
        info_json = makeGetRequest(info_url.format(coin["id"]), headers, rate_limiter)
        normalized_json = {
            "id": safe_get(info_json,"id"),
            "symbol": safe_get(info_json,"symbol"),
            "name": safe_get(info_json,"name"),
            "image": safe_get(info_json,"image", "large"),
            "current_price": safe_get(info_json,"market_data","current_price","usd"),
            "market_cap": safe_get(info_json,"market_data","market_cap","usd"),
            "market_cap_rank": safe_get(info_json,"market_cap_rank"),
            "fully_diluted_valuation": safe_get(info_json,"market_data","fully_diluted_valuation","usd"),
            "total_volume": safe_get(info_json,"market_data","total_volume","usd"),
            "high_24h": safe_get(info_json,"market_data","high_24h","usd"),
            "low_24h": safe_get(info_json,"market_data","low_24h","usd"),
            "price_change_24h": safe_get(info_json,"market_data","price_change_24h"),
            "price_change_percentage_24h": safe_get(info_json,"market_data","price_change_percentage_24h"),
            "market_cap_change_24h": safe_get(info_json,"market_data","market_cap_change_24h"),
            "market_cap_change_percentage_24h": safe_get(info_json,"market_data","market_cap_change_percentage_24h"),
            "circulating_supply": safe_get(info_json,"market_data","circulating_supply"),
            "total_supply": safe_get(info_json,"market_data","total_supply"),
            "max_supply": safe_get(info_json,"market_data","max_supply"),
            "ath": safe_get(info_json,"market_data","ath","usd"),
            "ath_change_percentage": safe_get(info_json,"market_data","ath_change_percentage","usd"),
            "ath_date": safe_get(info_json,"market_data","ath_date","usd"),
            "atl": safe_get(info_json,"market_data","atl","usd"),
            "atl_change_percentage": safe_get(info_json,"market_data","atl_change_percentage","usd"),
            "atl_date": safe_get(info_json,"market_data","atl_date","usd"),
            "roi": safe_get(info_json,"market_data","roi"),
            "last_updated": safe_get(info_json,"last_updated"),
        }
        return normalized_json, info_json
    except Exception as e:
        logger.error(f"Error processing {coin['id']}: {e}")
        logger.error(traceback.format_exc())
        return None, None

def worker(task_queue: Queue, output_queue, raw_output_queue, headers, info_url, rate_limiter, shutdown_event: Event):
    try:
        while not shutdown_event.is_set():
            coin = task_queue.get(timeout=1)
            if coin is None:
                logger.info("Worker received shutdown signal. Exiting.")
                break
            normalized, raw = process_coin(coin, headers, info_url, rate_limiter)
            if normalized:
                output_queue.put(normalized)
                raw_output_queue.put(raw)
            task_queue.task_done()
    except Exception as e:
        logger.error(f"Worker error: {e}")
        logger.error(traceback.format_exc())

def print_progress(processed, total, start_time):
    elapsed_time = time.time() - start_time
    coins_per_second = processed / elapsed_time if elapsed_time > 0 else 0
    eta_seconds = (total - processed) / coins_per_second if coins_per_second > 0 else 0
    
    eta_minutes, eta_seconds = divmod(int(eta_seconds), 60)
    completion_percentage = (processed / total) * 100 if total > 0 else 0
    
    sys.stdout.write(f"\rProcessed {processed}/{total} coins | "
                     f"Speed: {coins_per_second:.2f} coins/s | "
                     f"ETA: {eta_minutes}m {eta_seconds}s | "
                     f"Completion: {completion_percentage:.2f}%     ")
    sys.stdout.flush()

def read_json_file(filename):
    with open(filename, 'r') as file:
        data = json.load(file)
    return data

def process_ids():
    logger.info("Procesing started")
    
    list_url = "https://pro-api.coingecko.com/api/v3/coins/list"
    info_url = "https://pro-api.coingecko.com/api/v3/coins/{0}"
    headers = {
        "accept": "application/json",
        "x-cg-pro-api-key": config['cg_api_key']
    }

    rate_limiter = RateLimiter(calls=8, per_second=1)
    interrupted = False
    shutdown_event = Event()

    try:
        list_json = makeGetRequest(list_url, headers, rate_limiter)
        # list_json = read_json_file('test.json') # use this to test it with a local file
        
        # Initialize variables for ETA calculation and progress display
        total_coins = len(list_json)
        processed_coins = 0
        start_time = time.time()
        
        task_queue = Queue()
        output_queue = Queue()
        raw_output_queue = Queue()
    
        for coin in list_json:
            task_queue.put(coin)

        backup_thread = Thread(target=periodic_backup, args=(output_queue, raw_output_queue))
        backup_thread.start()

        num_threads = 5 # Seems reasonable we're limited to 8.33 calls per second
        threads = []

        # Create worker threads
        for _ in range(num_threads):
            t = Thread(target=worker, args=(task_queue, output_queue, raw_output_queue, headers, info_url, rate_limiter, shutdown_event))
            t.start()
            threads.append(t)

        # Monitor progress every second
        while processed_coins < total_coins:
            processed_coins = total_coins - task_queue.qsize()
            print_progress(processed_coins, total_coins, start_time)
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nKeyboard interrupt detected. Gracefully shutting down...")
        logger.info("Keyboard interrupt detected. Shutting down.")
        interrupted = True
    except Exception as e:
        print(f"\nAn error occurred: {e}. Gracefully shutting down...")
        logger.error(f"Error during processing: {e}")
        logger.error(traceback.format_exc())
        interrupted = True
    finally:
        shutdown_event.set()

        for _ in range(num_threads):
            task_queue.put(None)

        # Wait for all threads to finish
        for t in threads:
            t.join(timeout=5)

        # Signal backup thread to exit
        output_queue.put(None)
        raw_output_queue.put(None)
        backup_thread.join(timeout=5)

        # Collect output from queues and write to file
        output = []
        raw_output = []
        while not output_queue.empty():
            output.append(output_queue.get())
        while not raw_output_queue.empty():
            raw_output.append(raw_output_queue.get())

        if interrupted:
            logger.info("Processing interrupted. Partial results saved.")
            print("Saved partial results.")
        else:
            # Calculate and print total time and coins per second
            end_time = time.time()
            total_time = end_time - start_time
            coins_per_second = total_coins / total_time
            logger.info("Processing completed successfully. Full results saved.")
            print(f"\nCompleted: {total_coins} coins in {total_time:.2f}s | "
            f"Avg speed: {coins_per_second:.2f} coins/s")

    logger.info("Procesing done")

if __name__ == "__main__":
    process_ids()
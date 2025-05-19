"""
Course    : CSE 351
Assignment: 04
Student   : Emma Davis

Instructions:
    - review instructions in the course

In order to retrieve a weather record from the server, Use the URL:

f'{TOP_API_URL}/record/{name}/{recno}

where:

name: name of the city
recno: record number starting from 0
"""

import time
import threading
import logging
import traceback
import sys
from queue import Queue, Empty
from common import *
from cse351 import *

# Configuration
THREADS = 50
WORKERS = 10
RECORDS_TO_RETRIEVE = 5000
QUEUE_SIZE = 10
RETRY_ATTEMPTS = 5
RETRY_BACKOFF = 0.1
VERIFICATION_TOLERANCE = 0.001
JOIN_TIMEOUT = 60

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] Thread-%(thread)d: %(message)s',
    handlers=[
        logging.FileHandler('assignment.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def fetch_with_retry(url, retries=RETRY_ATTEMPTS, backoff=RETRY_BACKOFF):
    """Fetch data from server with retries on failure."""
    for attempt in range(retries):
        try:
            data = get_data_from_server(url)
            if data is not None:
                return data
            logger.warning(f"Attempt {attempt + 1} for {url}: No data returned")
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed for {url}: {type(e).__name__}: {str(e)}")
            traceback.print_exc()
        time.sleep(backoff * (2 ** attempt))
    logger.error(f"Failed to fetch {url} after {retries} attempts")
    return None

def retrieve_weather_data(command_queue, worker_queue, total_records):
    """Retriever thread to fetch weather data from server."""
    while True:
        try:
            command = command_queue.get(timeout=1.0)
            if command == "done":
                command_queue.task_done()
                break
            city, recno = command
            url = f'{TOP_API_URL}/record/{city}/{recno}'
            print(f"Fetching {url}")
            sys.stdout.flush()
            data = fetch_with_retry(url)
            if data and 'date' in data and 'temp' in data:
                worker_queue.put((city, data['date'], data['temp']))
            else:
                logger.warning(f"Invalid data for {url}: {data}")
            command_queue.task_done()
        except Empty:
            continue
        except Exception as e:
            logger.error(f"Error in retrieve_weather_data: {type(e).__name__}: {str(e)}")
            traceback.print_exc()

class Worker(threading.Thread):
    """Worker thread to process weather data and store in NOAA."""
    def __init__(self, worker_queue, noaa):
        super().__init__()
        self.worker_queue = worker_queue
        self.noaa = noaa
        self.daemon = True

    def run(self):
        while True:
            try:
                item = self.worker_queue.get(timeout=1.0)
                if item == "done":
                    self.worker_queue.task_done()
                    break
                city, date, temp = item
                self.noaa.add_weather_record(city, date, temp)
                self.worker_queue.task_done()
            except Empty:
                continue
            except Exception as e:
                logger.error(f"Error in worker: {type(e).__name__}: {str(e)}")
                traceback.print_exc()

class NOAA:
    """Stores weather data and computes average temperatures."""
    def __init__(self):
        self.weather_data = {city: [] for city in CITIES}
        self.lock = threading.Lock()

    def add_weather_record(self, city, date, temp):
        with self.lock:
            self.weather_data[city].append((date, temp))

    def get_temp_details(self, city):
        with self.lock:
            records = self.weather_data[city]
            if not records:
                logger.warning(f"No records for {city}")
                return 0.0
            temps = [temp for _, temp in records]
            return sum(temps) / len(records)

def verify_noaa_results(noaa):
    """Verify computed average temperatures against expected values."""
    answers = {
        'sandiego': 14.5004,
        'philadelphia': 14.865,
        'san_antonio': 14.638,
        'san_jose': 14.5756,
        'new_york': 14.6472,
        'houston': 14.591,
        'dallas': 14.835,
        'chicago': 14.6584,
        'los_angeles': 15.2346,
        'phoenix': 12.4404,
    }
    print("\nNOAA Results: Verifying Results")
    sys.stdout.flush()
    logger.info("\nNOAA Results: Verifying Results")
    logger.info("=" * 35)
    for name in CITIES:
        answer = answers[name]
        avg = noaa.get_temp_details(name)
        record_count = len(noaa.weather_data[name])
        if record_count < RECORDS_TO_RETRIEVE:
            logger.warning(f"{name:>15}: Only {record_count} records retrieved, expected {RECORDS_TO_RETRIEVE}")
        if abs(avg - answer) > VERIFICATION_TOLERANCE:
            msg = f"FAILED  Expected {answer}"
        else:
            msg = "PASSED"
        logger.info(f"{name:>15}: {avg:<10.4f} {msg}")
        print(f"{name:>15}: {avg:<10.4f} {msg}")
    logger.info("=" * 35)
    sys.stdout.flush()

def initialize_server():
    """Initialize server and fetch city details."""
    print("Retrieving city details")
    sys.stdout.flush()
    logger.info("Retrieving city details")
    print(f"{'City':>15}: Records")
    print("=" * 35)
    logger.info(f"{'City':>15}: Records")
    logger.info("=" * 35)
    city_details = {}
    for name in CITIES:
        data = fetch_with_retry(f'{TOP_API_URL}/city/{name}')
        if data is None:
            raise Exception(f"Failed to get city details for {name}")
        city_details[name] = data
        logger.info(f"{name:>15}: Records = {city_details[name]['records']:,}")
        print(f"{name:>15}: Records = {city_details[name]['records']:,}")
    print("=" * 35)
    logger.info("=" * 35)
    sys.stdout.flush()
    return city_details

def start_threads(noaa, command_queue, worker_queue, total_records):
    """Start retriever and worker threads."""
    logger.info(f"Starting {THREADS} retriever threads")
    retriever_threads = []
    try:
        for _ in range(THREADS):
            t = threading.Thread(
                target=retrieve_weather_data,
                args=(command_queue, worker_queue, total_records)
            )
            t.daemon = True
            t.start()
            retriever_threads.append(t)
    except Exception as e:
        logger.error(f"Failed to start retriever threads: {type(e).__name__}: {str(e)}")
        traceback.print_exc()
        raise

    logger.info(f"Starting {WORKERS} worker threads")
    worker_threads = []
    try:
        for _ in range(WORKERS):
            w = Worker(worker_queue, noaa)
            w.start()
            worker_threads.append(w)
    except Exception as e:
        logger.error(f"Failed to start worker threads: {type(e).__name__}: {str(e)}")
        traceback.print_exc()
        raise
    return retriever_threads, worker_threads

def queue_commands(command_queue, cities, records):
    """Queue commands for retriever threads."""
    logger.info("Queueing commands")
    for city in cities:
        for recno in range(records):
            command_queue.put((city, recno))

def shutdown_threads(command_queue, worker_queue, retriever_threads, worker_threads):
    """Shutdown retriever and worker threads."""
    logger.info("Waiting for command queue to complete")
    try:
        start_time = time.time()
        while not command_queue.empty() and time.time() - start_time < JOIN_TIMEOUT:
            time.sleep(0.1)
        if not command_queue.empty():
            logger.warning("Command queue not empty after timeout")
    except Exception as e:
        logger.error(f"Error during queue join: {type(e).__name__}: {str(e)}")
        traceback.print_exc()

    logger.info("Sending 'done' to retriever threads")
    for _ in range(THREADS):
        command_queue.put("done")
    for t in retriever_threads:
        t.join()

    logger.info("Sending 'done' to worker threads")
    for _ in range(WORKERS):
        worker_queue.put("done")
    for w in worker_threads:
        w.join()

def main():
    """Main function to orchestrate weather data retrieval."""
    print("Starting program")
    sys.stdout.flush()
    try:
        log = Log(show_terminal=True, filename_log='assignment.log')
        logger.info("Starting log")
        log.start_timer()

        try:
            get_data_from_server
        except NameError:
            logger.error("get_data_from_server not defined in common or cse351")
            raise

        city_details = initialize_server()
        noaa = NOAA()
        total_records = len(CITIES) * RECORDS_TO_RETRIEVE
        command_queue = Queue(maxsize=QUEUE_SIZE)
        worker_queue = Queue(maxsize=QUEUE_SIZE)

        retriever_threads, worker_threads = start_threads(noaa, command_queue, worker_queue, total_records)
        queue_commands(command_queue, CITIES, RECORDS_TO_RETRIEVE)
        shutdown_threads(command_queue, worker_queue, retriever_threads, worker_threads)

        verify_noaa_results(noaa)

        log.stop_timer('Run time: ')
        logger.info("Program completed successfully")
        print("Program completed successfully")
        sys.stdout.flush()
    except Exception as e:
        logger.error(f"Error in main: {type(e).__name__}: {str(e)}")
        print(f"Error in main: {e}")
        traceback.print_exc()
        sys.stdout.flush()
        raise

if __name__ == '__main__':
    main()
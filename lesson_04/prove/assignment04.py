"""
Course    : CSE 351
Assignment: 04
Student   : <your name here>

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
from queue import Queue, Empty
from common import *
from cse351 import *

# Configuration
THREADS = 1000
WORKERS = 4
RECORDS_TO_RETRIEVE = 5000  
QUEUE_SIZE = 5
JOIN_TIMEOUT = 10

# Barriers for synchronization
RETRIEVER_BARRIER = threading.Barrier(THREADS + 1)  # +1 for main thread
WORKER_BARRIER = threading.Barrier(WORKERS + 1)    # +1 for main thread

# ---------------------------------------------------------------------------
def retrieve_weather_data(command_queue, worker_queue):
    """Retriever thread to fetch weather data from server."""
    while True:
        try:
            command = command_queue.get(timeout=0.5)
            if command == "done":
                command_queue.task_done()
                break
            city, recno = command
            url = f'{TOP_API_URL}/record/{city}/{recno}'
            print(f"Fetching {url}")
            data = get_data_from_server(url)
            if data and 'date' in data and 'temp' in data:
                worker_queue.put((city, data['date'], data['temp']))
            else:
                print(f"Invalid data for {url}: {data}")
            command_queue.task_done()
        except Empty:
            continue
        except Exception as e:
            print(f"Error in retrieve_weather_data: {type(e).__name__}: {str(e)}")
    print("Retriever thread reached barrier")
    RETRIEVER_BARRIER.wait()

# ---------------------------------------------------------------------------
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
                item = self.worker_queue.get(timeout=0.5)
                if item == "done":
                    self.worker_queue.task_done()
                    break
                city, date, temp = item
                self.noaa.add_weather_record(city, date, temp)
                self.worker_queue.task_done()
            except Empty:
                continue
            except Exception as e:
                print(f"Error in worker: {type(e).__name__}: {str(e)}")
        print("Worker thread reached barrier")
        WORKER_BARRIER.wait()

# ---------------------------------------------------------------------------
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
                print(f"No records for {city}")
                return 0.0
            total_temp = sum(temp for _, temp in records)
            return total_temp / len(records) if records else 0.0

# ---------------------------------------------------------------------------
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
    print()
    print('NOAA Results: Verifying Results')
    print('===================================')
    for name in CITIES:
        answer = answers[name]
        avg = noaa.get_temp_details(name)
        if abs(avg - answer) > 0.00001:
            msg = f'FAILED  Expected {answer}'
        else:
            msg = f'PASSED'
        print(f'{name:>15}: {avg:<10} {msg}')
    print('===================================')

# ---------------------------------------------------------------------------
def main():
    log = Log(show_terminal=True, filename_log='assignment.log')
    log.start_timer()

    noaa = NOAA()

    # Start server
    data = get_data_from_server(f'{TOP_API_URL}/start')
    print(f"/start response: {data}")
    if data is None or data.get('status') != 'OK':
        raise Exception("Failed to start server: invalid or no response")

    # Get all cities number of records
    print('Retrieving city details')
    city_details = {}
    name = 'City'
    print(f'{name:>15}: Records')
    print('===================================')
    for name in CITIES:
        city_details[name] = get_data_from_server(f'{TOP_API_URL}/city/{name}')
        print(f'{name:>15}: Records = {city_details[name]['records']:,}')
    print('===================================')

    records = RECORDS_TO_RETRIEVE

    # Create queues and barriers
    command_queue = Queue(maxsize=QUEUE_SIZE)
    worker_queue = Queue(maxsize=QUEUE_SIZE)

    # Start retriever threads
    print("Starting retriever threads")
    retriever_threads = []
    for _ in range(THREADS):
        t = threading.Thread(target=retrieve_weather_data, args=(command_queue, worker_queue))
        t.daemon = True
        t.start()
        retriever_threads.append(t)

    # Start worker threads
    print("Starting worker threads")
    worker_threads = []
    for _ in range(WORKERS):
        w = Worker(worker_queue, noaa)
        w.start()
        worker_threads.append(w)

    # Queue commands
    print("Queueing commands")
    for city in CITIES:
        for recno in range(records):
            command_queue.put((city, recno))

    # Shutdown threads
    print("Shutting down threads")
    print("Sending 'done' to retriever threads")
    for _ in range(THREADS):
        command_queue.put("done")
    RETRIEVER_BARRIER.wait()  # Wait for all retrievers to finish
    for t in retriever_threads:
        t.join()

    print("Sending 'done' to worker threads")
    for _ in range(WORKERS):
        worker_queue.put("done")
    WORKER_BARRIER.wait()  # Wait for all workers to finish
    for w in worker_threads:
        w.join()

    # End server
    data = get_data_from_server(f'{TOP_API_URL}/end')
    print(data)

    verify_noaa_results(noaa)

    log.stop_timer('Run time: ')

if __name__ == '__main__':
    main()
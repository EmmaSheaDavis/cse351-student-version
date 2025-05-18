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
from queue import Queue, Empty
from common import *
from cse351 import *

# Constants
THREADS = 5  # Number of retriever threads (optimized for performance)
WORKERS = 10
RECORDS_TO_RETRIEVE = 5000  # 5,000 records per city

def retrieve_weather_data(command_queue, worker_queue):
    while True:
        try:
            command = command_queue.get(timeout=1)
            if command == "done":
                command_queue.task_done()
                break
            city, recno = command
            url = f'{TOP_API_URL}/record/{city}/{recno}'
            data = get_data_from_server(url)
            if data and data.get('status') == 'OK':
                worker_queue.put((city, data['date'], data['temp']))
            command_queue.task_done()
        except Empty:
            continue
        except Exception as e:
            print(f"Error in retrieve_weather_data: {e}")

class Worker(threading.Thread):
    def __init__(self, worker_queue, noaa):
        super().__init__()
        self.worker_queue = worker_queue
        self.noaa = noaa
        self.daemon = True

    def run(self):
        while True:
            try:
                item = self.worker_queue.get(timeout=1)
                if item == "done":
                    self.worker_queue.task_done()
                    break
                city, date, temp = item
                self.noaa.add_weather_record(city, date, temp)
                self.worker_queue.task_done()
            except Empty:
                continue

class NOAA:
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
                return 0.0
            total_temp = sum(temp for _, temp in records)
            return total_temp / len(records) if records else 0.0

def verify_noaa_results(noaa):
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
    print('\nNOAA Results: Verifying Results')
    print('===================================')
    for name in CITIES:
        answer = answers[name]
        avg = noaa.get_temp_details(name)
        if abs(avg - answer) > 0.00001:
            msg = f'FAILED  Expected {answer}'
        else:
            msg = f'PASSED'
        print(f'{name:>15}: {avg:<10.4f} {msg}')
    print('===================================')

def main():
    try:
        log = Log(show_terminal=True, filename_log='assignment.log')
        print("Starting log")
        log.start_timer()

        noaa = NOAA()

        # Send /start request
        print("Sending /start request")
        data = get_data_from_server(f'{TOP_API_URL}/start')
        print(f"/start response: {data}")
        if data is None or data.get('status') != 'OK':
            raise Exception("Failed to start server: invalid or no response")

        # Retrieve city details
        print('Retrieving city details')
        print(f'{"City":>15}: Records')
        print('===================================')
        city_details = {}
        for name in CITIES:
            city_details[name] = get_data_from_server(f'{TOP_API_URL}/city/{name}')
            if city_details[name] is None:
                raise Exception(f"Failed to get city details for {name}")
            print(f'{name:>15}: Records = {city_details[name]["records"]:,}')
        print('===================================')

        # Create queues
        command_queue = Queue(maxsize=10)
        worker_queue = Queue(maxsize=10)

        # Start retriever threads
        print("Starting retriever threads")
        retriever_threads = []
        for _ in range(THREADS):
            t = threading.Thread(
                target=retrieve_weather_data,
                args=(command_queue, worker_queue)
            )
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

        # Populate command queue
        print("Queueing commands")
        for city in CITIES:
            for recno in range(RECORDS_TO_RETRIEVE):
                command_queue.put((city, recno))

        # Wait for command queue to complete
        print("Waiting for command queue to complete")
        command_queue.join()

        # Signal retriever threads to stop
        for _ in range(THREADS):
            command_queue.put("done")

        # Wait for retriever threads to finish
        for t in retriever_threads:
            t.join()

        # Signal worker threads to stop
        for _ in range(WORKERS):
            worker_queue.put("done")

        # Wait for worker threads to finish
        for w in worker_threads:
            w.join()

        # Send /end request
        print("Sending /end request")
        data = get_data_from_server(f'{TOP_API_URL}/end')
        if data is None:
            raise Exception("Failed to get /end response")
        print(data)

        # Verify results
        verify_noaa_results(noaa)

        log.stop_timer('Run time: ')

    except Exception as e:
        print(f"Error in main: {e}")
        raise

if __name__ == '__main__':
    main()
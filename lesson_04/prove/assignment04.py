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

THREADS = 50                 # TODO - set for your program
THREADS = 50
WORKERS = 10
RECORDS_TO_RETRIEVE = 100

def retrieve_weather_data(command_queue, worker_queue):
    processed = 0
    while True:
        try:
            command = command_queue.get(timeout=0.1)
            if command == "done":
                print(f"Retriever thread stopping: processed {processed} records")
                command_queue.task_done()
                break
            city, recno = command
            url = f'{TOP_API_URL}/record/{city}/{recno}'
            print(f"Fetching {url}")
            data = get_data_from_server(url)
            if data and 'date' in data and 'temp' in data:
                worker_queue.put((city, data['date'], data['temp']))
                processed += 1
                if processed % 100 == 0:
                    print(f"Processed {processed} records")
            else:
                print(f"Failed to retrieve data for {url}: {data}")
            command_queue.task_done()
        except Empty:
            continue
        except Exception as e:
            print(f"Error in retrieve_weather_data: {e}")

# ---------------------------------------------------------------------------
def retrieve_weather_data(command_queue, worker_queue):
    # TODO - fill out this thread function (and arguments)
    while True:
        try:
            command = command_queue.get(timeout = 1)
            if command == "done":
                command_queue.task_done()
                break
            city, recno = command

            url = f'{TOP_API_URL}/record/{city}/{recno}'
            data = get_data_from_server(url)
            if data and "date" in data and "temp" in data:
                worker_queue.put((city, data["date"], data["temp"]))
            command_queue.task_done()
        except Empty:
            continue
class Worker(threading.Thread):
    def __init__(self, worker_queue, noaa):
        super().__init__()
        self.worker_queue = worker_queue
        self.noaa = noaa
        self.daemon = True

    def run(self):
        while True:
            try:
                item = self.worker_queue.get(timeout=0.1)
                if item == "done":
                    self.worker_queue.task_done()
                    break
                city, date, temp = item
                self.noaa.add_weather_record(city, date, temp)
                self.worker_queue.task_done()
            except Empty:
                continue

# ---------------------------------------------------------------------------
# TODO - Create Worker threaded class
class Worker(threading.Thread):
    def __init__(self, worker_queue, noaa):
        super().__init__()
        self._worker_queue = worker_queue
        self._noaa = noaa

    def run(self):
        while True:
            try:
                item = self._worker_queue.get(timeout = 1)
                if item == "done":
                    self._worker_queue.task_done()
                    break
                city, date, temp = item
                self._noaa.add_weather_record(city, date, temp)
                self._worker_queue.task_done()
            except Empty:
                continue
        



# ---------------------------------------------------------------------------
# TODO - Complete this class
class NOAA:
    def __init__(self):
        super().__init__()
        self._weather_data = {city: [] for city in CITIES}
        self.weather_data = {city: [] for city in CITIES}
        self.lock = threading.Lock()

    def add_weather_record(self, city, date, temp):
        with self.lock:
            self._weather_data[city].append((date, temp))

    def get_temp_details(self, city):
        with self.lock:
            records = self._weather_data[city]
            if not records:
                return 0.0
            total_temp = sum(temp for date , temp in records)
            self.weather_data[city].append((date, temp))

    def get_temp_details(self, city):
        with self.lock:
            records = self.weather_data[city]
            if not records:
                return 0.0
            total_temp = sum(temp for date, temp in records)
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

def main():
    try:
        log = Log(show_terminal=True, filename_log='assignment.log')
        print("Starting log")
        log.start_timer()

        noaa = NOAA()

        print("Sending /start request")
        print(f"URL: {TOP_API_URL}/start")
        data = get_data_from_server(f'{TOP_API_URL}/start')
        print(f"/start response: {data}")
        if data is None or 'status' not in data or data['status'] != 'OK':
            raise Exception("Failed to start server: invalid or no response")

        print('Retrieving city details')
        city_details = {}
        name = 'City'
        print(f'{name:>15}: Records')
        print('===================================')
        for name in CITIES:
            print(f"Requesting city: {name}")
            city_details[name] = get_data_from_server(f'{TOP_API_URL}/city/{name}')
            if city_details[name] is None:
                raise Exception(f"Failed to get city details for {name}")
            print(f'{name:>15}: Records = {city_details[name]["records"]:,}')
        print('===================================')

        records = RECORDS_TO_RETRIEVE
        command_queue = Queue()
        worker_queue = Queue()


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

        print("Starting worker threads")
        worker_threads = []
        for _ in range(WORKERS):
            w = Worker(worker_queue, noaa)
            w.start()
            worker_threads.append(w)
    except Exception as e:
        print(f"Error in main: {e}")
        raise

    # TODO - Create any queues, pipes, locks, barriers you need

    command_queue = Queue(maxsize=10)
    worker_queue = Queue(maxsize=10)

    for city in CITIES:
        for recno in range(records):
            command_queue.put((city, recno))

    retriever_threads = []
    for _ in range(THREADS):
        t = threading.Thread(target=retrieve_weather_data, args=(command_queue, worker_queue))

        t.daemon = True
        t.start()
        retriever_threads.append(t)

    worker_threads = []
    for _ in range(WORKERS):
        w = Worker(worker_queue, noaa)
        w.start()
        worker_threads.append(w)

    command_queue.join()

    for _ in range(THREADS):
        command_queue.put("done")
    
    for t in retriever_threads:
        t.join()

    for w in worker_threads:
        w.join()

        print("Queueing commands")
        for city in CITIES:
            for recno in range(records):
                command_queue.put((city, recno))

        print("Waiting for command queue to complete")
        command_queue.join()

        for _ in range(THREADS):
            command_queue.put("done")

        for t in retriever_threads:
            t.join()

        for _ in range(WORKERS):
            worker_queue.put("done")

        for w in worker_threads:
            w.join()

        print("Sending /end request")
        data = get_data_from_server(f'{TOP_API_URL}/end')
        if data is None:
            raise Exception("Failed to get /end response")
        print(data)

        verify_noaa_results(noaa)

        log.stop_timer('Run time: ')
    

if __name__ == '__main__':
    main()
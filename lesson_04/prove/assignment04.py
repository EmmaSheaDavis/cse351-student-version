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
<<<<<<< HEAD

=======
>>>>>>> 67bc1643d38300cd3ec606606082407f78e1a555
from common import *
from cse351 import *

<<<<<<< HEAD
THREADS = 50                 # TODO - set for your program
=======
THREADS = 50
>>>>>>> 67bc1643d38300cd3ec606606082407f78e1a555
WORKERS = 10
RECORDS_TO_RETRIEVE = 5000

def retrieve_weather_data(command_queue, worker_queue):
    while True:
        try:
            command = command_queue.get(timeout=.01)
            if command == "done":
                command_queue.task_done()
                break
            city, recno = command
            url = f'{TOP_API_URL}/record/{city}/{recno}'
            data = get_data_from_server(url)
            if data and 'date' in data and 'temp' in data:
                worker_queue.put((city, data['date'], data['temp']))
            command_queue.task_done()
        except Empty:
            continue

<<<<<<< HEAD
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
=======
class Worker(threading.Thread):
    def __init__(self, worker_queue, noaa):
        super().__init__()
        self.worker_queue = worker_queue
        self.noaa = noaa
        self.daemon = True
>>>>>>> 67bc1643d38300cd3ec606606082407f78e1a555

    def run(self):
        while True:
            try:
                item = self.worker_queue.get(timeout=.01)
                if item == "done":
                    self.worker_queue.task_done()
                    break
                city, date, temp = item
                self.noaa.add_weather_record(city, date, temp)
                self.worker_queue.task_done()
            except Empty:
                continue

<<<<<<< HEAD
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
=======
>>>>>>> 67bc1643d38300cd3ec606606082407f78e1a555
class NOAA:
    def __init__(self):
        super().__init__()
<<<<<<< HEAD
        self._weather_data = {city: [] for city in CITIES}
=======
        self.weather_data = {city: [] for city in CITIES}
>>>>>>> 67bc1643d38300cd3ec606606082407f78e1a555
        self.lock = threading.Lock()

    def add_weather_record(self, city, date, temp):
        with self.lock:
<<<<<<< HEAD
            self._weather_data[city].append((date, temp))

    def get_temp_details(self, city):
        with self.lock:
            records = self._weather_data[city]
            if not records:
                return 0.0
            total_temp = sum(temp for date , temp in records)
=======
            self.weather_data[city].append((date, temp))

    def get_temp_details(self, city):
        with self.lock:
            records = self.weather_data[city]
            if not records:
                return 0.0
            total_temp = sum(temp for date, temp in records)
>>>>>>> 67bc1643d38300cd3ec606606082407f78e1a555
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
    log = Log(show_terminal=True, filename_log='assignment.log')
    log.start_timer()

    noaa = NOAA()

    data = get_data_from_server(f'{TOP_API_URL}/start')

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

<<<<<<< HEAD
    # TODO - Create any queues, pipes, locks, barriers you need
=======
>>>>>>> 67bc1643d38300cd3ec606606082407f78e1a555
    command_queue = Queue(maxsize=10)
    worker_queue = Queue(maxsize=10)

    for city in CITIES:
        for recno in range(records):
            command_queue.put((city, recno))
<<<<<<< HEAD

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
=======
>>>>>>> 67bc1643d38300cd3ec606606082407f78e1a555

    retriever_threads = []
    for _ in range(THREADS):
        t = threading.Thread(
            target=retrieve_weather_data,
            args=(command_queue, worker_queue)
        )
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

    for _ in range(WORKERS):
        worker_queue.put("done")

    for w in worker_threads:
        w.join()

    data = get_data_from_server(f'{TOP_API_URL}/end')
    print(data)

    verify_noaa_results(noaa)

    log.stop_timer('Run time: ')

if __name__ == '__main__':
    main()
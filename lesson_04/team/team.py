""" 
Course: CSE 351
Team  : Week 04
File  : team.py
Author: <Student Name>

See instructions in canvas for this team activity.

"""

import random
import threading

# Include CSE 351 common Python files. 
from cse351 import *


# Constants
MAX_QUEUE_SIZE = 10
PRIME_COUNT = 1000
FILENAME = 'primes.txt'
PRODUCERS = 3
CONSUMERS = 5

# ---------------------------------------------------------------------------
def is_prime(n: int):
    if n <= 3:
        return n > 1
    if n % 2 == 0 or n % 3 == 0:
        return False
    i = 5
    while i ** 2 <= n:
        if n % i == 0 or n % (i + 2) == 0:
            return False
        i += 6
    return True

# ---------------------------------------------------------------------------
class Queue351():
    """ This is the queue object to use for this class. Do not modify!! """

    def __init__(self):
        self.__items = []
   
    def put(self, item):
        assert len(self.__items) <= 10
        self.__items.append(item)

    def get(self):
        return self.__items.pop(0)

    def get_size(self):
        """ Return the size of the queue like queue.Queue does -> Approx size """
        extra = 1 if random.randint(1, 50) == 1 else 0
        if extra > 0:
            extra *= -1 if random.randint(1, 2) == 1 else 1
        return len(self.__items) + extra

# ---------------------------------------------------------------------------
def producer(queue, items_sem, spaces_sem, producer_id, all_done_event):
    for i in range(PRIME_COUNT):
        number = random.randint(1, 1_000_000_000_000)
        # TODO - place on queue for workers
        spaces_sem.aquire()
def producer(queue, items_sem, spaces_sem, producer_id, barrier):
    for i in range(PRIME_COUNT):
        number = random.randint(1, 1_000_000_000_000)
        # TODO - place on queue for workers
        spaces_sem.acquire()
        queue.put(number)
        items_sem.release()

    barrier.wait()

    # TODO - select one producer to send the "All Done" message
    if producer_id == 0:
        for _ in range(CONSUMERS):
            spaces_sem.acquire()
            queue.put(None)
            items_sem.release()

# ---------------------------------------------------------------------------
def consumer(queue, items_sem, spaces_sem, filename):
    # TODO - get values from the queue and check if they are prime
    # TODO - if prime, write to the file
    # TODO - if "All Done" message, exit the loop
    while True:
        items_sem.acquire()
        item = queue.get()
        spaces_sem.release()

        if item == None:
            break
        
        if is_prime(item):
            print(f"Found prime: {item}")
            with open(filename, "a") as f:
                f.write(f"{item}\n")

# ---------------------------------------------------------------------------
def main():

    random.seed(102030)

    with open(FILENAME, "w") as f:
        f.write("")

    que = Queue351()

    items_sem = threading.Semaphore(0)
    spaces_sem = threading.Semaphore(MAX_QUEUE_SIZE)
    barrier = threading.Barrier(PRODUCERS)
    

    producer_threads = []
    consumer_threads = []

    for i in range(PRODUCERS):
        p = threading.Thread(target=producer, args=(que, items_sem, spaces_sem, i, barrier))
        producer_threads.append(p)
    
    for i in range(CONSUMERS):
        c = threading.Thread(target=consumer, args=(que, items_sem, spaces_sem, FILENAME))
        consumer_threads.append(c)

    for p in producer_threads:
        p.start()
    for c in consumer_threads:
        c.start()

    for p in producer_threads:
        p.join()
    for c in consumer_threads:
        c.join()

    if os.path.exists(FILENAME):
        with open(FILENAME, 'r') as f:
            primes = len(f.readlines())
    else:
        primes = 0
    print(f"Found {primes} primes. Must be 108 found.")



if __name__ == '__main__':
    main()

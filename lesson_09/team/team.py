# Course: CSE 351
# Team  : 
# File  : Week 9 team.py
# Author: Luc Comeau

from cse351 import *
import time
import random
import multiprocessing as mp

CLEANING_STAFF = 2
HOTEL_GUESTS = 8  # Balanced for guest overlap and cleaner access
TIME = 60

STARTING_PARTY_MESSAGE = 'Turning on the lights for the party vvvvvvvvvvvvvv'
STOPPING_PARTY_MESSAGE = 'Turning off the lights  ^^^^^^^^^^^^^^^^^^^^^^^^^^'
STARTING_CLEANING_MESSAGE = 'Starting to clean the room >>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
STOPPING_CLEANING_MESSAGE = 'Finish cleaning the room <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<'

def cleaner_waiting():
    time.sleep(random.uniform(0.3, 0.8))  # Frequent cleaner attempts

def cleaner_cleaning(id):
    print(f'Cleaner: {id}')
    time.sleep(random.uniform(0.8, 1.5))

def guest_waiting():
    time.sleep(random.uniform(1, 2))  # Spaces out guest arrivals

def guest_partying(id, count):
    print(f'Guest: {id}, count = {count}')
    time.sleep(random.uniform(0.3, 0.8))  # Short for guest overlap

def cleaner(cleaner_id, room_lock, count_lock, guest_count, cleaned_count):
    """
    Cleaner process: Acquires room_lock, checks guest_count, cleans if empty.
    """
    start_time = time.time()
    while time.time() - start_time < TIME:
        cleaner_waiting()
        
        with room_lock:
            with count_lock:
                if guest_count.value > 0:
                    continue
            print(STARTING_CLEANING_MESSAGE)
            cleaner_cleaning(cleaner_id)
            print(STOPPING_CLEANING_MESSAGE)
            with count_lock:
                cleaned_count.value += 1

def guest(guest_id, room_lock, count_lock, guest_count, party_count):
    """
    Guest process: Multiple guests can party, first guest acquires room_lock,
    last guest releases it.
    """
    start_time = time.time()
    while time.time() - start_time < TIME:
        guest_waiting()
        
        with room_lock:
            with count_lock:
                if guest_count.value == 0:
                    print(STARTING_PARTY_MESSAGE)
                    party_count.value += 1
                guest_count.value += 1
                current_count = guest_count.value
        
        guest_partying(guest_id, current_count)
        
        with room_lock:
            with count_lock:
                guest_count.value -= 1
                if guest_count.value == 0:
                    print(STOPPING_PARTY_MESSAGE)

def main():
    start_time = time.time()
    
    # Two locks
    room_lock = mp.Lock()
    count_lock = mp.Lock()
    
    # Shared variables
    guest_count = mp.Value('i', 0)
    cleaned_count = mp.Value('i', 0)
    party_count = mp.Value('i', 0)
    
    # Create processes
    processes = []
    
    for i in range(CLEANING_STAFF):
        p = mp.Process(target=cleaner, args=(i+1, room_lock, count_lock, guest_count, cleaned_count))
        processes.append(p)
        p.start()
    
    for i in range(HOTEL_GUESTS):
        p = mp.Process(target=guest, args=(i+1, room_lock, count_lock, guest_count, party_count))
        processes.append(p)
        p.start()
    
    # Run for TIME seconds
    time.sleep(TIME)
    
    # Terminate processes
    for p in processes:
        p.terminate()
        p.join()
    
    # Results
    print(f'Room was cleaned {cleaned_count.value} times, there were {party_count.value} parties')

if __name__ == '__main__':
    main()
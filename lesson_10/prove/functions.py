"""
Course: CSE 351
Lesson Week: 10
File: functions.py
Author: <Your Name>
Purpose: Assignment 10 - Family Search implementation using DFS and BFS with threading

# Part 1: Speed up recursive DFS by caching family/person data to avoid redundant API calls. Use bounded ThreadPoolExecutor (max_workers=20) for parallel API requests. Batch family/person fetches, update Tree incrementally. Target: 6 gens < 8s.
# Part 2: Optimize iterative BFS with visited set to skip duplicates. Dynamically adjust ThreadPoolExecutor (max_workers=10-20). Batch API calls per level, use in-memory storage. Target: 6 gens < 7s.
# Part 3: Refine BFS with priority queue, track duplicates. Use ThreadPoolExecutor (max_workers=5), batch person ID fetches in groups of 5. Monitor server.log for compliance. Maximize speed within thread limit.
"""

from common import *
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue

def depth_fs_pedigree(family_id, tree):
    """
    Build the family tree using a depth-first search with threading.
    
    Args:
        family_id (str): Starting family ID
        tree (Tree): Tree object to store the family data
    """
    def process_family(fam_id, generation):
        if not fam_id or generation > 6:
            return
        
        data = get_data_from_server(f"{TOP_API_URL}/family/{fam_id}")
        if not data:
            return
        family = Family(data)
        print(f"Retrieved family: {family}")
        tree.add_family(family)

        person_ids = [pid for pid in [family.get_husband(), family.get_wife(), *family.get_children()] if pid]
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(get_data_from_server, f'{TOP_API_URL}/person/{pid}') for pid in person_ids]
            persons = [Person(f.result()) for f in as_completed(futures) if f.result()]

        for person in persons:
            print(f"Retrieved person: {person}")
            tree.add_person(person)

        parent_fids = []
        husband = tree.get_person(family.get_husband())
        wife = tree.get_person(family.get_wife())
        if husband and husband.get_parentid():
            parent_fids.append(husband.get_parentid())
        if wife and wife.get_parentid():
            parent_fids.append(wife.get_parentid())

        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(process_family, fid, generation + 1) for fid in parent_fids]
            for f in as_completed(futures):
                f.result()
    
    process_family(family_id, 1)

def breadth_fs_pedigree(family_id, tree):
    """
    Build the family tree using a breadth-first search with threading.
    
    Args:
        family_id (str): Starting family ID
        tree (Tree): Tree object to store the family data
    """
    q = Queue()
    q.put((family_id, 1))
    processed_families = set()

    while not q.empty():
        level_size = q.qsize()
        family_futures = []
        person_futures = []
        families = []

        with ThreadPoolExecutor() as executor:
            for _ in range(level_size):
                fam_id, generation = q.get()
                if fam_id in processed_families or generation > 6:
                    continue
                processed_families.add(fam_id)
                family_futures.append(executor.submit(get_data_from_server, f"{TOP_API_URL}/family/{fam_id}"))

            for f in as_completed(family_futures):
                data = f.result()
                if data:
                    family = Family(data)
                    print(f"Retrieved family: {family}")
                    tree.add_family(family)
                    families.append((family, generation))

            for family, generation in families:
                person_ids = [pid for pid in [family.get_husband(), family.get_wife(), *family.get_children()] if pid]
                person_futures.extend([executor.submit(get_data_from_server, f'{TOP_API_URL}/person/{pid}') for pid in person_ids])

            for f in as_completed(person_futures):
                data = f.result()
                if data:
                    person = Person(data)
                    print(f"Retrieved Person: {person}")
                    tree.add_person(person)
            
            for family, generation in families:
                husband = tree.get_person(family.get_husband())
                wife = tree.get_person(family.get_wife())
                parent_fids = []
                if husband and husband.get_parentid():
                    parent_fids.append(husband.get_parentid())
                if wife and wife.get_parentid():
                    parent_fids.append(wife.get_parentid())
                for fid in parent_fids:
                    if fid not in processed_families:
                        q.put((fid, generation + 1))

def breadth_fs_pedigree_limit5(family_id, tree):
    """
    Build the family tree using a breadth-first search with a limit of 5 concurrent threads.
    
    Args:
        family_id (str): Starting family ID
        tree (Tree): Tree object to store the family data
    """
    q = Queue()
    q.put((family_id, 1))
    processed_families = set()

    with ThreadPoolExecutor(max_workers=5) as executor:
        while not q.empty():
            level_size = min(q.qsize(), 5)
            family_futures = []
            person_futures = []
            current_families = []
            all_person_ids = []

            # Fetch families for the current level
            for _ in range(level_size):
                if q.empty():
                    break
                fam_id, generation = q.get()
                if fam_id in processed_families or generation > 6:
                    continue
                processed_families.add(fam_id)
                family_futures.append(executor.submit(get_data_from_server, f'{TOP_API_URL}/family/{fam_id}'))
        
            # Process family results
            for f in as_completed(family_futures):
                data = f.result()
                if data:
                    family = Family(data)
                    print(f"Retrieved Family: {family}")
                    tree.add_family(family)
                    current_families.append((family, generation))

            # Collect all person IDs for the current level
            for family, generation in current_families:
                person_ids = [pid for pid in [family.get_husband(), family.get_wife(), *family.get_children()] if pid]
                all_person_ids.extend(person_ids)

            # Process person IDs in batches of up to 5
            while all_person_ids:
                batch = all_person_ids[:5]  # Take up to 5 IDs
                all_person_ids = all_person_ids[5:]  # Remove processed IDs
                person_futures = [executor.submit(get_data_from_server, f'{TOP_API_URL}/person/{pid}') for pid in batch]
                
                for f in as_completed(person_futures):
                    data = f.result()
                    if data:
                        person = Person(data)
                        print(f"Retrieved Person: {person}")
                        tree.add_person(person)

            # Enqueue parent families for the next level
            for family, generation in current_families:
                husband = tree.get_person(family.get_husband())
                wife = tree.get_person(family.get_wife())
                parent_fids = []
                if husband and husband.get_parentid():
                    parent_fids.append(husband.get_parentid())
                if wife and wife.get_parentid():
                    parent_fids.append(wife.get_parentid())
                for fid in parent_fids:
                    if fid not in processed_families:
                        q.put((fid, generation + 1))
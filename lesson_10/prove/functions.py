"""
Course: CSE 351, week 10
File: functions.py
Author: <your name>
 
Instructions:

Depth First Search
https://www.youtube.com/watch?v=9RHO6jU--GU

Breadth First Search
https://www.youtube.com/watch?v=86g8jAQug04


Requesting a family from the server:
family_id = 6128784944
data = get_data_from_server('{TOP_API_URL}/family/{family_id}')

Example JSON returned from the server
{
    'id': 6128784944, 
    'husband_id': 2367673859,        # use with the Person API
    'wife_id': 2373686152,           # use with the Person API
    'children': [2380738417, 2185423094, 2192483455]    # use with the Person API
}

Requesting an individual from the server:
person_id = 2373686152
data = get_data_from_server('{TOP_API_URL}/person/{person_id}')

Example JSON returned from the server
{
    'id': 2373686152, 
    'name': 'Stella', 
    'birth': '9-3-1846', 
    'parent_id': 5428641880,   # use with the Family API
    'family_id': 6128784944    # use with the Family API
}


--------------------------------------------------------------------------------------
You will lose 10% if you don't detail your part 1 and part 2 code below

Describe how to speed up part 1

<Add your comments here>


Describe how to speed up part 2

<Add your comments here>


Extra (Optional) 10% Bonus to speed up part 3

<Add your comments here>

"""
from common import *
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue

# -----------------------------------------------------------------------------
def depth_fs_pedigree(family_id, tree):
    # KEEP this function even if you don't implement it
    # TODO - implement Depth first retrieval
    # TODO - Printing out people and families that are retrieved from the server will help debugging

    def process_family(fam_id, generation):
        if not fam_id or generation > 6:
            return
        
        data = get_data_from_server(f"{TOP_API_URL}/family/{fam_id}")
        if not data:
            return
        family = Family(data)
        print(f"Retrieved family: {family}")
        tree.add_family(family)

        person_ids = [pid for pid in [family.get_husband(), family.get_wife(), family.get_children()] if pid]
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(get_data_from_server, f'{TOP_API_URL}/person/{pid}') for pid in person_ids]
            persons = [Person(f.result())for f in as_completed(futures) if f.result ()]

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

# -----------------------------------------------------------------------------
def breadth_fs_pedigree(family_id, tree):
    # KEEP this function even if you don't implement it
    # TODO - implement breadth first retrieval
    # TODO - Printing out people and families that are retrieved from the server will help debugging

    q = Queue
    q.put((family_id, 1))
    processed_families = set()

    while not q.empty():
        level_size = q.qsize()
        family_futures = []
        person_futures = []

        with ThreadPoolExecutor as executor:
            for _ in range(level_size):
                fam_id, generation = q.get()
                if fam_id in processed_families or generation > 6:
                    continue
                processed_families.add(fam_id)
                family_futures.append(executor.submit(get_data_from_server, f"{TOP_API_URL}/family/{fam_id}"))

            families = []
            for f in as_completed(family_futures):
                date = f.result()
                if data:
                    family = Family(data)
                    print(f"Retrieved family: {family}")
                    tree.add_family(family)
                    families.append((family, generation))

            for family, generation in families:
                person_ids = [pid for pid in[family.get_husband(), family.get_wife(), family.get_children()] if pid]
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

# -----------------------------------------------------------------------------
def breadth_fs_pedigree_limit5(family_id, tree):
    # KEEP this function even if you don't implement it
    # TODO - implement breadth first retrieval
    #      - Limit number of concurrent connections to the FS server to 5
    # TODO - Printing out people and families that are retrieved from the server will help debugging

    pass
"""
Course: CSE 351
Assignment: 06
Author: [Emma Davis]

Instructions:

- see instructions in the assignment description in Canvas
CATEGORY: 4
Time to completion was lessened from 53 secones to 9 seconds with queues and workers

""" 

import multiprocessing as mp
import os
import cv2
import numpy as np

from cse351 import *

# Folders
INPUT_FOLDER = "faces"
STEP1_OUTPUT_FOLDER = "step1_smoothed"
STEP2_OUTPUT_FOLDER = "step2_grayscale"
STEP3_OUTPUT_FOLDER = "step3_edges"

# Parameters for image processing
GAUSSIAN_BLUR_KERNEL_SIZE = (5, 5)
CANNY_THRESHOLD1 = 75
CANNY_THRESHOLD2 = 155

# Allowed image extensions
ALLOWED_EXTENSIONS = ['.jpg']

# ---------------------------------------------------------------------------
def create_folder_if_not_exists(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        print(f"Created folder: {folder_path}")

# ---------------------------------------------------------------------------
def task_convert_to_grayscale(image):
    if len(image.shape) == 2 or (len(image.shape) == 3 and image.shape[2] == 1):
        return image # Already grayscale
    return cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

# ---------------------------------------------------------------------------
def task_smooth_image(image, kernel_size):
    return cv2.GaussianBlur(image, kernel_size, 0)

# ---------------------------------------------------------------------------
def task_detect_edges(image, threshold1, threshold2):
    if len(image.shape) == 3 and image.shape[2] == 3:
        print("Warning: Applying Canny to a 3-channel image. Converting to grayscale first for Canny.")
        image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    elif len(image.shape) == 3 and image.shape[2] != 1 : # Should not happen with typical images
        print(f"Warning: Input image for Canny has an unexpected number of channels: {image.shape[2]}")
        return image # Or raise error
    return cv2.Canny(image, threshold1, threshold2)

# ---------------------------------------------------------------------------
def smooth_worker(input_queue, output_queue, kernel_size):
    while True:
        item = input_queue.get()
        if item is None:  # Sentinel value to stop
            output_queue.put(None)  # Pass sentinel to next stage
            break
        input_path, output_path = item
        try:
            img = cv2.imread(input_path)
            if img is None:
                print(f"Warning: Could not read image '{input_path}'. Skipping.")
                continue
            processed_img = task_smooth_image(img, kernel_size)
            output_queue.put((processed_img, output_path))
        except Exception as e:
            print(f"Error smoothing image '{input_path}': {e}")

# ---------------------------------------------------------------------------
def grayscale_worker(input_queue, output_queue):
    while True:
        item = input_queue.get()
        if item is None:  # Sentinel value to stop
            output_queue.put(None)  # Pass sentinel to next stage
            break
        img, output_path = item
        try:
            processed_img = task_convert_to_grayscale(img)
            output_queue.put((processed_img, output_path))
        except Exception as e:
            print(f"Error converting image to grayscale for '{output_path}': {e}")

# ---------------------------------------------------------------------------
def edge_worker(input_queue, output_folder, threshold1, threshold2):
    create_folder_if_not_exists(output_folder)
    while True:
        item = input_queue.get()
        if item is None:  # Sentinel value to stop
            break
        img, output_path = item
        try:
            processed_img = task_detect_edges(img, threshold1, threshold2)
            cv2.imwrite(output_path, processed_img)
        except Exception as e:
            print(f"Error detecting edges for '{output_path}': {e}")
# ---------------------------------------------------------------------------
def run_image_processing_pipeline():
    print("Starting image processing pipeline...")

    # TODO
    # - create queues
    # - create barriers
    # - create the three processes groups
    # - you are free to change anything in the program as long as you
    #   do all requirements.
    queue1 = mp.Queue()
    queue2 = mp.Queue()
    queue3 = mp.Queue()

    num_smooth_processes = 2
    num_grayscale_processes = 2
    num_edge_processes = 2
    processes = []

    # --- Step 1: Smooth Images ---
    for _ in range(num_smooth_processes):
        p = mp.Process(target=smooth_worker, args=(queue1, queue2, GAUSSIAN_BLUR_KERNEL_SIZE))
        p.start()
        processes.append(p)

    # --- Step 2: Convert to Grayscale ---
    for _ in range(num_grayscale_processes):
        p = mp.Process(target=grayscale_worker, args=(queue2, queue3))
        p.start()
        processes.append(p)

    # --- Step 3: Detect Edges ---
    for _ in range(num_edge_processes):
        p = mp.Process(target=edge_worker, args=(queue3, STEP3_OUTPUT_FOLDER, CANNY_THRESHOLD1, CANNY_THRESHOLD2))
        p.start()
        processes.append(p)

    create_folder_if_not_exists(STEP3_OUTPUT_FOLDER)
    for filename in os.listdir(INPUT_FOLDER):
        file_ext = os.path.splitext(filename)[1].lower()
        if file_ext not in ALLOWED_EXTENSIONS:
            continue
        input_path = os.path.join(INPUT_FOLDER, filename)
        output_path = os.path.join(STEP3_OUTPUT_FOLDER, filename)
        queue1.put((input_path, output_path))

    for _ in range(num_smooth_processes):
        queue1.put(None)

    for p in processes:
        p.join()
    
    queue1.close()
    queue2.close()
    queue3.close()
    queue1.join_thread()
    queue2.join_thread()
    queue3.join_thread()

    print("\nImage processing pipeline finished!")
    print(f"Original images are in: '{INPUT_FOLDER}'")
    print(f"Grayscale images are in: '{STEP1_OUTPUT_FOLDER}'")
    print(f"Smoothed images are in: '{STEP2_OUTPUT_FOLDER}'")
    print(f"Edge images are in: '{STEP3_OUTPUT_FOLDER}'")


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    log = Log(show_terminal=True)
    log.start_timer('Processing Images')

    # check for input folder
    if not os.path.isdir(INPUT_FOLDER):
        print(f"Error: The input folder '{INPUT_FOLDER}' was not found.")
        print(f"Create it and place your face images inside it.")
        print('Link to faces.zip:')
        print('   https://drive.google.com/file/d/1eebhLE51axpLZoU6s_Shtw1QNcXqtyHM/view?usp=sharing')
    else:
        run_image_processing_pipeline()

    log.write()
    log.stop_timer('Total Time To complete')

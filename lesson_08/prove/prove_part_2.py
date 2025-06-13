"""
Course: CSE 351 
Assignment: 08 Prove Part 2
File:   prove_part_2.py
Author: Emma Davis

Purpose: Part 2 of assignment 8, finding the path to the end of a maze using recursive threading.

Instructions:
- Do not create classes for this assignment, just functions.
- Do not use any other Python modules other than the ones included.
- You MUST use recursive threading to find the end of the maze.
- Each thread MUST have a different color than the previous thread:
    - Use get_color() to get the color for each thread; you will eventually have duplicated colors.
    - Keep using the same color for each branch that a thread is exploring.
    - When you hit an intersection spin off new threads for each option and give them their own colors.

This code is not interested in tracking the path to the end position. Once you have completed this
program however, describe how you could alter the program to display the found path to the exit
position:

What would be your strategy?

I'd tweak the explore function to keep a path list for each thread, adding the current (x, y) position as it goes. When a thread hits
 the end (maze.at_end(x, y)), I'd save that path in a shared found_path variable, using a lock to keep things safe. After all threads 
 stop, I'd draw the path by looping through found_path and using maze.move(x, y, COLOR) in blue, checking maze.can_move_here(x, y) to 
 avoid errors. This would show the winning path clearly over the colorful exploration.

Why would it work?

Each thread's path list tracks its route like a trail of breadcrumbs. Saving the path when the end is found captures the right one. 
The lock prevents mix-ups between threads. Drawing the path later with maze.move makes it stand out, and checking maze.can_move_here 
skips any problem spots. It's a simple change that fits the existing setup and gets the job done.
"""

import math
import threading 
from screen import Screen
from maze import Maze
import sys
import cv2

# Include cse 351 files
from cse351 import *

SCREEN_SIZE = 700
COLOR = (0, 0, 255)
COLORS = (
    (0,0,255),
    (0,255,0),
    (255,0,0),
    (255,255,0),
    (0,255,255),
    (255,0,255),
    (128,0,0),
    (128,128,0),
    (0,128,0),
    (128,0,128),
    (0,128,128),
    (0,0,128),
    (72,61,139),
    (143,143,188),
    (226,138,43),
    (128,114,250)
)
SLOW_SPEED = 100
FAST_SPEED = 0

# Globals
current_color_index = 0
thread_count = 0
stop = False
speed = SLOW_SPEED

def get_color():
    """ Returns a different color when called """
    global current_color_index
    if current_color_index >= len(COLORS):
        current_color_index = 0
    color = COLORS[current_color_index]
    current_color_index += 1
    return color

def explore(maze, x, y, color, visited, lock):
    """ Recursively explore the maze from (x, y) with a given color in a thread. """
    global stop, thread_count
    
    if stop:
        return
    
    with lock:
        if (x, y) in visited or not maze.can_move_here(x, y):
            return
        visited.add((x, y))
    
    maze.move(x, y, color)
    
    if maze.at_end(x, y):
        with lock:
            stop = True 
        return
    
    neighbors = maze.get_possible_moves(x, y)
    
    if len(neighbors) > 1:
        for i in range(1, len(neighbors)):
            with lock:
                thread_count += 1
            new_color = get_color()
            t = threading.Thread(target=explore, args=(maze, neighbors[i][0], neighbors[i][1], new_color, visited, lock))
            t.start()
            t.join() 
    
    if neighbors and not stop:
        explore(maze, neighbors[0][0], neighbors[0][1], color, visited, lock)

def solve_find_end(maze):
    """ Finds the end position using threads. Nothing is returned. """
    global stop, thread_count
    stop = False
    thread_count = 1
    visited = set()
    lock = threading.Lock()
    
    start_x, start_y = maze.get_start_pos()
    
    color = get_color()
    t = threading.Thread(target=explore, args=(maze, start_x, start_y, color, visited, lock))
    t.start()
    t.join() 

def find_end(log, filename, delay):
    """ Do not change this function """
    global thread_count
    global speed
    screen = Screen(SCREEN_SIZE, SCREEN_SIZE)
    screen.background((255, 255, 0))
    maze = Maze(screen, SCREEN_SIZE, SCREEN_SIZE, filename, delay=delay)
    solve_find_end(maze)
    log.write(f'Number of drawing commands = {screen.get_command_count()}')
    log.write(f'Number of threads created  = {thread_count}')
    done = False
    while not done:
        if screen.play_commands(speed): 
            key = cv2.waitKey(0)
            if key == ord('1'):
                speed = SLOW_SPEED
            elif key == ord('2'):
                speed = FAST_SPEED
            elif key == ord('q'):
                exit()
            elif key != ord('p'):
                done = True
        else:
            done = True

def find_ends(log):
    """ Do not change this function """
    files = (
        ('very-small.bmp', True),
        ('very-small-loops.bmp', True),
        ('small.bmp', True),
        ('small-loops.bmp', True),
        ('small-odd.bmp', True),
        ('small-open.bmp', False),
        ('large.bmp', False),
        ('large-loops.bmp', False),
        ('large-squares.bmp', False),
        ('large-open.bmp', False)
    )
    log.write('*' * 40)
    log.write('Part 2')
    for filename, delay in files:
        filename = f'./mazes/{filename}'
        log.write()
        log.write(f'File: {filename}')
        find_end(log, filename, delay)
    log.write('*' * 40)

def main():
    """ Do not change this function """
    sys.setrecursionlimit(5000)
    log = Log(show_terminal=True)
    find_ends(log)

if __name__ == "__main__":
    main()
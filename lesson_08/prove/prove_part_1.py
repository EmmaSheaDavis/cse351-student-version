"""
Course: CSE 251 
Assignment: 08 Prove Part 1
File:   prove_part_1.py
Author: Emma Davis

Purpose: Part 1 of assignment 8, finding the path to the end of a maze using recursion.

Instructions:

- Do not create classes for this assignment, just functions.
- Do not use any other Python modules other than the ones included.
- Complete any TODO comments.
"""

import math
from screen import Screen
from maze import Maze
import cv2
import sys

# Include cse 351 files
from cse351 import *

SCREEN_SIZE = 800
COLOR = (0, 0, 255)
SLOW_SPEED = 100
FAST_SPEED = 1
speed = SLOW_SPEED

def solve_path(maze):
    """ Solve the maze and return the path found between the start and end positions.  
        The path is a list of positions, (x, y) """
    path = []
    visited = set() 
    
    def explore(x, y):
        """ Recursively explore the maze from position (x, y). Returns True if a path is found. """
        if (x, y) in visited or not maze.can_move_here(x, y):
            return False
        
        if maze.at_end(x, y):
            path.append((x, y)) 
            return True
        
        visited.add((x, y))
        maze.restore(x, y)  
        
        neighbors = maze.get_possible_moves(x, y)
        
        # Explore each neighbor
        for next_x, next_y in neighbors:
            if explore(next_x, next_y): 
                path.append((x, y)) 
                return True
        
        return False 
    
   
    start_x, start_y = maze.get_start_pos()
   
    if not maze.can_move_here(start_x, start_y):
        return path
    
    # Explore from the start
    if explore(start_x, start_y):
        path.append((start_x, start_y))
        path.reverse()  
        for x, y in path:
            if maze.can_move_here(x, y): 
                maze.move(x, y, COLOR)  
    else:
        path = []  
    
    return path

def get_path(log, filename):
    """ Do not change this function """
    global speed
    screen = Screen(SCREEN_SIZE, SCREEN_SIZE)
    screen.background((255, 255, 0))
    maze = Maze(screen, SCREEN_SIZE, SCREEN_SIZE, filename)
    path = solve_path(maze)
    log.write(f'Drawing commands to solve = {screen.get_command_count()}')
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
    return path

def find_paths(log):
    """ Do not change this function """
    files = (
        'very-small.bmp',
        'very-small-loops.bmp',
        'small.bmp',
        'small-loops.bmp',
        'small-odd.bmp',
        'small-open.bmp',
        'large.bmp',
        'large-loops.bmp',
        'large-squares.bmp',
        'large-open.bmp'
    )
    log.write('*' * 40)
    log.write('Part 1')
    for filename in files:
        filename = f'./mazes/{filename}'
        log.write()
        log.write(f'File: {filename}')
        path = get_path(log, filename)
        log.write(f'Found path has length     = {len(path)}')
    log.write('*' * 40)

def main():
    """ Do not change this function """
    sys.setrecursionlimit(5000)
    log = Log(show_terminal=True)
    find_paths(log)

if __name__ == "__main__":
    main()
import numpy as np
from multiprocessing import Process

from memory import initialize_memory
from mission_control import initialize_mission_control
from Data import inializeTickMemorySubscriptions


def run_all_tests():
    pass


def start_server():

    # Run Full Test Suite
    run_all_tests()

    # Initialize Memory
    memory = initialize_memory()

    # Start Mission Control
    initialize_mission_control(memory)


if __name__ == '__main__':
    start_server()

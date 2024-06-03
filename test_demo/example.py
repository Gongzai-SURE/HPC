from datetime import datetime
import time
import socket
import threading
import re
import subprocess
import os
import string
import random

def current_time():
    now = datetime.now()
    return now.strftime("%Y-%m-%d %H:%M:%S")

def communicate_with_barrier_server():
    barrier_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    barrier_client_socket.connect(("127.0.0.1", 6675))
    barrier_client_socket.send('Program is blocking'.encode('utf-8'))
    while True:
        response = barrier_client_socket.recv(1024).decode('utf-8')
        if response == 'GOON':
            break
    barrier_client_socket.close()


if __name__ == "__main__":
    print('First time is '+current_time())
    time.sleep(2)
    communicate_with_barrier_server()
    print('Second time is '+current_time())
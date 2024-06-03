import socket
import threading
import re
import subprocess
import os
import string
import random
import time

def start_barrier_server():
    barrier_flag = False
    host = "127.0.0.1" 
    port = 6675
    barrier_server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    barrier_server_socket.bind((host, port))
    barrier_server_socket.listen(5)
    print("Barrier server listening on", host, "port", port)
    while barrier_flag == False:
        program_barrier_socket, addr = barrier_server_socket.accept()
        print("Accepted connection from", addr)
        while barrier_flag == False:
            request = program_barrier_socket.recv(1024).decode('utf-8')
            if request == 'Program is blocking':
                barrier_flag == True
                print('Received a block!')
                break
        while True:
            message = input("Enter GOON to contiune program running: ")
            if message.upper() == "QUIT":
                program_barrier_socket.close()
                break
            elif message == 'GOON':
                program_barrier_socket.send(message.encode('utf-8'))
                time.sleep(1)
                break
        break
    barrier_server_socket.close()

def check_string_format(string):
    pattern = r"^send\s+(\w+)\.py$"
    match = re.match(pattern, string)
    if match:
        filename = match.group(1)+".py"
        return filename
    else:
        return None

def send_program_and_get_result(client_socket,filename):
    with open(filename,'rb') as f:
            # 按每一段分割文件上传
            for i in f:
                client_socket.send(i)
                # 等待接收完成标志
                data=client_socket.recv(1024)
                # 判断是否真正接收完成
                if data != b'success':
                    break
    client_socket.send('quit'.encode())
    print('file update successfully!')
    start_barrier_server()
    result = client_socket.recv(1024).decode('utf-8')
    print("Result received from server:\n", result)

def communicate_with_server(host, port):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((host, port))
    while True:
        message = input("Enter message to send (QUIT to exit): ")
        client_socket.send(message.encode('utf-8'))
        if message.upper() == "QUIT":
            break
        elif filename:=check_string_format(message):
            send_program_and_get_result(client_socket,filename)
            client_socket.send('program reslut received!'.encode('utf-8'))
        response = client_socket.recv(1024).decode('utf-8')
        print("Server response:", response)
    client_socket.close()


if __name__ == "__main__":
    communicate_with_server("127.0.0.1", 9988)
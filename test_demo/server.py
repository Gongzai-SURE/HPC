import socket
import threading
import re
import subprocess
import os
import string
import random

def check_string_format(string):
    pattern = r"^send\s+(\w+)\.py$"
    match = re.match(pattern, string)
    if match:
        filename = match.group(1)+".py"
        return filename
    else:
        return None

def generate_random_string(length=5):
    letters = string.ascii_letters + string.digits
    return (''.join(random.choice(letters) for _ in range(length)))+'.py'

def handle_client(client_socket):
    while True:
        data = client_socket.recv(1024).decode('utf-8')
        if not data:
            break
        print("Received message from client:", data)
        if data.upper() == "QUIT":
            break
        elif check_string_format(data):
            print('Receiving file from client ...')
            receive_and_run_program(client_socket)
            continue
        client_socket.send("Message received".encode('utf-8'))
    client_socket.close()
    print("Close connection with ", client_socket)

def receive_and_run_program(client_socket):
    tmp_filename = generate_random_string()
    while True:
        with open(tmp_filename,'ab') as f:
        #with open('received_program.py', "wb") as f:
            # 接收数据
            data = client_socket.recv(1024)
            if data == b'quit':
                break
            # 写入文件
            f.write(data)
            # 接受完成标志
            client_socket.send('success'.encode())
    print("File received successfully")
    # Run the received program
    result = subprocess.run(["python", tmp_filename], capture_output=True, text=True)
    print(result)
    client_socket.send(result.stdout.encode())
    os.remove(tmp_filename)
            

def start_server(host, port):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(5)
    print("Server listening on", host, "port", port)
    while True:
        client_socket, addr = server_socket.accept()
        print("Accepted connection from", addr)
        client_handler = threading.Thread(target=handle_client, args=(client_socket,))
        client_handler.start()

if __name__ == "__main__":
    start_server("127.0.0.1", 9988)
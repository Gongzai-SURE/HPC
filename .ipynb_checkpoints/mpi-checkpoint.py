import socket
import threading
import time
import json
import os
import importlib.util
import random
import string

class ComputeNode:
    def __init__(self, control_host, control_port):
        self.control_host = control_host
        self.control_port = control_port
        self.host = socket.gethostbyname(socket.gethostname())
        self.port = self.get_random_port()
        self.node_id = None
        self.reduce_addr = None
        self.control_conn = None
        self.conn_node = None

    def get_random_port(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', 0))
            return s.getsockname()[1]

    def start(self):
        self.register_with_control()
        self.listen_for_tasks()
        if self.node_id == 0:
            self.listen_for_results()

    def register_with_control(self):
        while self.node_id is None:
            try:
                self.control_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.control_conn.connect((self.control_host, self.control_port + 1))
                message = {'type': 'register', 'host': self.host, 'port': self.port}
                self.control_conn.sendall(json.dumps(message).encode())
                data = self.control_conn.recv(1024)
                if not data:
                    raise ValueError("Received empty response from control node")
                response = json.loads(data.decode())
                self.node_id = response['id']
                self.reduce_addr = tuple(response['reduce_addr']) if response['reduce_addr'] else None
                print(f"Registered with control node as node {self.node_id}")
            except (ValueError, json.JSONDecodeError) as e:
                print(f"Error registering with control node: {e}")
                time.sleep(5)
            except Exception as e:
                print(f"Unexpected error: {e}")
                time.sleep(5)

    def listen_for_tasks(self):
        threading.Thread(target=self.listen_for_tasks_thread).start()

    def listen_for_tasks_thread(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen()
            print(f"Compute node listening for tasks on {self.host}:{self.port}")
            while True:
                conn, addr = s.accept()
                threading.Thread(target=self.handle_task, args=(conn, addr)).start()

    def handle_task(self, conn, addr):
        try:
            print(f"Task connection from {addr}")
            with conn:
                data = conn.recv(1024)
                if data:
                    message = json.loads(data.decode())
                    if message['type'] == 'task_file':
                        self.receive_task_file(conn, message['task_id'])
                    elif message['type'] == 'task':
                        self.execute_task(message['task_info'], message['part'])
        except Exception as e:
            print(f"Error handling task from {addr}: {e}")

    def receive_task_file(self, conn, task_id):
        task_dir = f"task_{self.node_id}"
        os.makedirs(task_dir, exist_ok=True)
        task_file = f"{task_dir}/task{task_id}.py"
        tmp_filename = self.generate_random_string()

        try:
            with open(task_file, 'wb') as f:
                while True:
                    data = conn.recv(1024)
                    if data == b'quit':
                        break
                    f.write(data)
                    conn.sendall(b'success')
            print(f"Received task file {task_file}")
        except Exception as e:
            print(f"Error receiving task file: {e}")

    def generate_random_string(self, length=8):
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

    def execute_task(self, task_info, task_part):
        task_id = task_info['task_id']
        task_dir = f"task_{self.node_id}"
        task_file = f"{task_dir}/task{task_id}.py"

        # 加载任务模块
        try:
            spec = importlib.util.spec_from_file_location(f"task{task_id}", task_file)
            task_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(task_module)
        except Exception as e:
            print(f"Error loading task module: {e}")
            return

        # 确认模块中存在所需函数
        if not hasattr(task_module, 'find_max'):
            print("Error: Task module does not have the required 'find_max' function.")
            return

        # 执行任务
        try:
            result = task_module.find_max(task_part)
            print(f"Task result: {result}")
            if self.reduce_addr:
                self.send_result_to_reduce(result)
            else:
                print(f"Final result: {result}")
        except Exception as e:
            print(f"Error executing task: {e}")

    def send_result_to_reduce(self, result):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(self.reduce_addr)
                message = json.dumps({'type': 'result', 'node_id': self.node_id, 'result': result}).encode()
                s.sendall(message)
                print(f"Sent result to reduce node at {self.reduce_addr}")
                ack = s.recv(1024)
                if ack == b'success':
                    print(f"Result acknowledged by reduce node")
                    os.remove(f"task_{self.node_id}/task{self.task_info['task_id']}.py")
        except Exception as e:
            print(f"Error sending result to reduce node: {e}")

    def listen_for_results(self):
        threading.Thread(target=self.listen_for_results_thread).start()

    def listen_for_results_thread(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port + 1))
            s.listen()
            print(f"Reduce node listening for results on {self.host}:{self.port + 1}")
            while True:
                conn, addr = s.accept()
                threading.Thread(target=self.handle_result, args=(conn, addr)).start()

    def handle_result(self, conn, addr):
        try:
            print(f"Result connection from {addr}")
            with conn:
                data = conn.recv(1024)
                if data:
                    message = json.loads(data.decode())
                    if message['type'] == 'result':
                        print(f"Received result from node {message['node_id']}: {message['result']}")
                        conn.sendall(b'success')
        except Exception as e:
            print(f"Error handling result from {addr}: {e}")

if __name__ == "__main__":
    compute_node = ComputeNode(control_host='127.0.0.1', control_port=5000)
    compute_node.start()















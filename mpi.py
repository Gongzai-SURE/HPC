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
        self.reduce_conn = None
        self.control_conn = None
        # only for reduce node to use
        self.result_from_nodes = {}
        self.connected_computer_node = 0

    def get_random_port(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', 0))
            return s.getsockname()[1]

    def start(self):
        self.register_with_control()
        threading.Thread(target=self.listen_to_control).start()
        if self.node_id == 0:
            self.listen_for_other_computer_nodes()
        else :
            self.connect_to_reduce_node()
    
    '''
    computer node communication with control node
    '''
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
    
    def listen_to_control(self):
        while True:
            try:
                message = self.control_conn.recv(1024)
                if message:
                    print(message.decode())
                    if message == b'please accept task':
                        self.receive_task_file()
                        self.handle_task()
                    elif message == b'close':
                        self.control_conn.close()
                        os._exit(0)
            except Exception as e:
                print(f"Error listening to control node: {e}")
                os._exit(0)

    def handle_task(self):
        try:
            data = self.control_conn.recv(1024)
            if data:
                message = json.loads(data.decode())
                if message['type'] == 'task':
                    self.execute_task(message['task_info'],message['joint_node'], message['part'])
        except Exception as e:
            print(f"Error handling task from {addr}: {e}")

    def receive_task_file(self):
        task_dir = f"task_node{self.node_id}"
        os.makedirs(task_dir, exist_ok=True)
        task_file = f"{task_dir}/task.py"
        try:
            with open(task_file, 'wb') as f:
                while True:
                    data = self.control_conn.recv(1024)
                    if data == b'quit':
                        break
                    f.write(data)
                    self.control_conn.send('success'.encode())
            print(f"Received task file {task_file}")
        except Exception as e:
            print(f"Error receiving task file: {e}")

    def generate_random_string(self, length=8):
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

    def execute_task(self, task_info,node_num, task_part):
        task_id = task_info['task_id']
        task_dir = f"task_node{self.node_id}"
        task_file = f"{task_dir}/task.py"

        # 加载任务模块
        try:
            spec = importlib.util.spec_from_file_location(f"task{task_id}", task_file)
            task_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(task_module)
        except Exception as e:
            print(f"Error loading task module: {e}")
            return

        # 执行任务
        try:
            if task_part:
                result = task_module.do_task1(task_part)
            else:
                #如果没有数据内容，那么根据自己的id号和总计算节点个数进行计算
                result = task_module.do_task1(self.node_id,node_num)
            print(f"Task result: {result}")
            self.result_from_nodes[self.node_id] = result
            if self.node_id != 0:
                self.send_result_to_reduce(result)
            else:
                while True:
                    if self.connected_computer_node == len(self.result_from_nodes) -1:
                        final_result = task_module.do_task2(self.result_from_nodes.values())
                        print(f"Final result: {final_result}")
                        self.send_result_to_control(final_result)
                        self.result_from_nodes = {}
                        os.remove(f"task_node{self.node_id}/task.py")
                        break
        except Exception as e:
            print(f"Error executing task: {e}")

    def send_result_to_control(self,final_result):
        try:
            message = json.dumps({'type': 'result', 'result': final_result}).encode()
            self.control_conn.sendall(message)
            print(f"Sent final result to control node")
        except Exception as e:
            print(f"Error sending final result to control node: {e}")
    
    '''
    computer node communication with each other
    '''
    def send_result_to_reduce(self, result):
        try:
            message = json.dumps({'type': 'result', 'node_id': self.node_id, 'result': result}).encode()
            self.reduce_conn.sendall(message)
            print(f"Sent result to reduce node at {self.reduce_addr}")
            ack = self.reduce_conn.recv(1024)
            if ack == b'success':
                print(f"Result acknowledged by reduce node")
                os.remove(f"task_node{self.node_id}/task.py")
        except Exception as e:
            print(f"Error sending result to reduce node: {e}")

    def connect_to_reduce_node(self):
        while self.reduce_addr:
            try:
                self.reduce_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.reduce_conn.connect((self.reduce_addr[0], self.reduce_addr[1] + 1))
                self.reduce_conn.sendall(json.dumps({'type': 'connect', 'node_id': self.node_id}).encode())
                print('connect to reduce node successfully！')
                break
            except Exception as e:
                print(f"Error connecting to reduce node: {e}")
                time.sleep(5)

    def listen_for_other_computer_nodes(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port + 1))
            s.listen()
            print(f"Reduce node listening for results on {self.host}:{self.port + 1}")
            while True:
                conn, addr = s.accept()
                message = conn.recv(1024)
                if message:
                    response = json.loads(message.decode())
                    id = response['node_id']
                    print(f'node_id {id} connect into this reduce node!')
                    self.connected_computer_node += 1
                threading.Thread(target=self.handle_result,args=(conn,id)).start()

    def handle_result(self,conn,node_id):
        while True:
            data = conn.recv(1024)
            if data:
                print(f'Received message from node {node_id} : {data.decode()}')
                message = json.loads(data.decode())
                if message['type'] == 'result':
                    print(f"Received result from node {message['node_id']}: {message['result']}")
                    self.result_from_nodes[message['node_id']] = message['result']
                    conn.sendall(b'success')

if __name__ == "__main__":
    compute_node = ComputeNode(control_host='127.0.0.1', control_port=5000)
    compute_node.start()















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
        self.computer_time = 0.0
        # only for reduce node to use
        self.result_from_nodes = {}
        self.connected_computer_node = 0
        self.middle_result = None
        self.send_middle_result = {}

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
                # 系统限制了control_node与computer_node通信规则，具体为以下几点
                if message:
                    print(message.decode())
                    if message == b'please accept task':
                        self.receive_task_file()
                    elif message == b'please accept data':
                        self.receive_set_up_file()
                    elif message == b'close':
                        self.control_conn.close()
                        os._exit(0)
                    else:
                        try:
                            message = json.loads(message.decode())
                            if message['type'] == 'task':
                                self.execute_task(message['task_id'],message['joint_node'],message['rank'])
                        except Exception as e:
                            print(f"Error handling task from : {e}")
            except Exception as e:
                print(f"Error listening to control node: {e}")
                os._exit(0)

    def receive_set_up_file(self):
        setup_dir = f"task_node{self.node_id}"
        setup_file = f"{setup_dir}/setup.txt"
        try:
            with open(setup_file, 'wb') as f:
                while True:
                    data = self.control_conn.recv(1024)
                    if data == b'quit':
                        break
                    f.write(data)
            print(f"Received setup file {setup_file}")
        except Exception as e:
            print(f"Error receiving setup file: {e}")

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

    def execute_task(self, task_id, node_num,rank):
        t1 = time.time()
        task_dir = f"task_node{self.node_id}"
        task_file = f"{task_dir}/task.py"
        data_file = f"{task_dir}/setup.txt"
        task_part = self.get_node_data(data_file,rank,node_num)
        # 加载任务模块
        try:
            spec = importlib.util.spec_from_file_location(f"task{task_id}", task_file)
            task_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(task_module)
        except Exception as e:
            print(f"Error loading task module: {e}")
            return

        # 如果task_module中存在名为do_task1_2的函数
        if hasattr(task_module, 'do_task1_2') and task_part:
            # 执行多轮次归约任务
            try:
                result1 = task_module.do_task1_1(task_part)
                print(f"Task temp result: {result1}")
                self.result_from_nodes[self.node_id] = result1
                # 第一轮归约
                if self.node_id != 0:
                    self.send_result_to_reduce(result1)
                else:
                    while True:
                        if self.connected_computer_node == len(self.result_from_nodes) -1 and self.middle_result == None:
                            middle_result = task_module.do_task2_1(self.result_from_nodes.values())
                            print(f"middle result: {middle_result}")
                            self.middle_result = middle_result
                            self.result_from_nodes = {}
                            break
                while self.middle_result == None:
                    pass
                result = task_module.do_task1_2(task_part,self.middle_result)
                print(f"Task result: {result}")
                self.result_from_nodes[self.node_id] = result
                # 第二轮规约
                if self.node_id != 0:
                    self.send_result_to_reduce(result)
                    self.middle_result = None
                else:
                    while True:
                        if self.connected_computer_node == len(self.result_from_nodes) -1:
                            final_result = task_module.do_task2_2(self.result_from_nodes.values())
                            print(f"Final result: {final_result}")
                            t2 = time.time()
                            self.computer_time = t2 - t1
                            self.send_result_to_control(final_result)
                            self.result_from_nodes = {}
                            os.remove(f"task_node{self.node_id}/task.py")
                            os.remove(f"task_node{self.node_id}/setup.txt")
                            self.middle_result = None
                            break
            except Exception as e:
                print(f"Error executing task: {e}")
        else:   
            # 执行单轮次归约任务
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
                            t2 = time.time()
                            self.computer_time = t2 - t1
                            self.send_result_to_control(final_result)
                            self.result_from_nodes = {}
                            os.remove(f"task_node{self.node_id}/task.py")
                            os.remove(f"task_node{self.node_id}/setup.txt")
                            break
            except Exception as e:
                print(f"Error executing task: {e}")
        

    def send_result_to_control(self,final_result):
        try:
            message = json.dumps({'type': 'result', 'result': final_result , 'Single_node_time':self.computer_time}).encode()
            self.control_conn.sendall(message)
            print(f"Sent final result to control node")
        except Exception as e:
            print(f"Error sending final result to control node: {e}")
            
    def get_node_data(self, data_file, rank, n):
        try:
            with open(data_file, 'r') as f:
                data_info = json.load(f)
        except Exception as e:
            print(f"Error reading setup file {data_file}: {e}")
            return
        data = data_info['data']
        if data:
            if not 0 <= rank < n:
                raise ValueError("Rank must be between 0 and n-1 inclusive.")
            # Calculate the size of each chunk
            chunk_size = len(data) // n
            remainder = len(data) % n

            # Calculate the starting and ending index for the rank-th chunk
            start_index = (chunk_size + 1) * min(rank, remainder) + chunk_size * max(0, rank - remainder)
            end_index = start_index + chunk_size + (1 if rank < remainder else 0)
            node_data = data[start_index:end_index]
            print(node_data)
            return node_data
        else:
            return None
    
    '''
    computer node communication with each other
    '''
    def send_result_to_reduce(self, result):
        try:
            message = json.dumps({'type': 'result', 'node_id': self.node_id, 'result': result}).encode()
            self.reduce_conn.sendall(message)
            print(f"Sent result: {result} to reduce node at {self.reduce_addr}") 
        except Exception as e:
            print(f"Error sending result to reduce node: {e}")

    def connect_to_reduce_node(self):
        while self.reduce_addr:
            try:
                self.reduce_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.reduce_conn.connect((self.reduce_addr[0], self.reduce_addr[1] + 1))
                self.reduce_conn.sendall(json.dumps({'type': 'connect', 'node_id': self.node_id}).encode())
                print('connect to reduce node successfully！')
                threading.Thread(target=self.listen_to_reduce_node,args=()).start()
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
                    self.send_middle_result[id] = False
                threading.Thread(target=self.handle_node_message,args=(conn,id)).start()
                threading.Thread(target=self.send_middle_result_to_node,args=(conn,id)).start()

    def listen_to_reduce_node(self):
        while True:
            data = self.reduce_conn.recv(1024)
            if data:
                if data == b'success':
                    print(f"Result acknowledged by reduce node")
                    if self.middle_result:
                        os.remove(f"task_node{self.node_id}/task.py")
                        os.remove(f"task_node{self.node_id}/setup.txt")
                else:
                    message = json.loads(data.decode())
                    if message['type'] == 'middle_result':
                        print(f"Received middle result from reduce node: {message['result']}")
                        self.middle_result = message['result']

    def send_middle_result_to_node(self,conn,node_id):
        while True:
            if self.middle_result==None and self.send_middle_result[node_id] == True:
                self.send_middle_result[node_id] = False
                pass
                
            if self.middle_result and self.send_middle_result[node_id] == False:
                try:
                    message = json.dumps({'type': 'middle_result', 'result': self.middle_result}).encode()
                    conn.sendall(message)
                    print(f"Sent middle result to {node_id} node")
                    self.send_middle_result[node_id] = True
                    pass
                except Exception as e:
                    print(f"Error sending middle result to {node_id} node: {e}")
    
    def handle_node_message(self,conn,node_id):
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















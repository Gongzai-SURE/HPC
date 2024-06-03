import socket
import threading
import json
import os
import time

class ControlNode:
    def __init__(self, host='0.0.0.0', port=5000):
        self.host = host
        self.port = port
        self.compute_nodes = {}
        self.compute_conns = []
        self.task_queue = []
        self.lock = threading.Lock()

    def start(self):
        self.listen_for_cmd()
        self.listen_for_nodes()

    def close_all_resource(self):
        self.close_connection()
        os._exit(0)
        
    def listen_for_cmd(self):
        threading.Thread(target=self.listen_for_cmd_thread).start()

    def listen_for_cmd_thread(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen()
            print(f"Control node listening for commands on {self.host}:{self.port}")
            while True:
                conn, addr = s.accept()
                threading.Thread(target=self.handle_cmd, args=(conn, addr)).start()

    def handle_cmd(self, conn, addr):
        try:
            data = conn.recv(1024)
            if data:
                command = json.loads(data.decode())
                print(f"Received command: {command}")
                if command['type'] == 'start_task':
                    self.handle_start_task(command['task_data'])
                elif command['type'] == 'end_task':
                    conn.close()
                    self.close_all_resource()
        except Exception as e:
            print(f"Error handling command from {addr}: {e}")

    def handle_start_task(self, task_data):
        try:
            task_id = task_data['task_id']
            task_file_path = f"task/task{task_id}.py"
            setup_file_path = f"task/setup{task_id}.txt"

            # 读取任务设置文件
            try:
                with open(setup_file_path, 'r') as f:
                    setup_info = json.load(f)
            except Exception as e:
                print(f"Error reading setup file {setup_file_path}: {e}")
                return

            task_parts = self.split_task(setup_info)

            # 分发任务文件和任务数据到计算节点
            for i, node_id in enumerate(self.compute_nodes):
                try:
                    conn = self.compute_conns[node_id]
                    conn.send("please accept task".encode())
                    time.sleep(0.1)
                    self.send_task_file(node_id, task_file_path)
                    time.sleep(0.1)
                    if task_parts:
                        self.send_task(node_id, {'task_id': task_id, 'setup_info': setup_info}, task_parts[i])
                    else:
                        self.send_task(node_id, {'task_id': task_id, 'setup_info': setup_info}, None)
                except Exception as e:
                    print(f"Error sending task to node {node_id}: {e}")
            
            #从reduce计算节点接收最终结果
            while True:
                message = self.compute_conns[0].recv(1024)
                if message: 
                    result = json.loads(message.decode())
                    print('Received final result:',result['result'])
                    break
        except KeyError as e:
            print(f"Error: missing key {e} in task_data")
        except Exception as e:
            print(f"Error handling start task: {e}")

    def split_task(self, setup_info):
        # 假设 setup_info 包含需要划分的数据
        data = setup_info['data']
        if data:
            num_nodes = len(self.compute_nodes)
            
            # 创建一个字典来存储每个节点的数据
            node_data = {i: [] for i in range(num_nodes)}
            
            # 使用哈希函数来分配数据
            for item in data:
                node_id = hash(item) % num_nodes
                node_data[node_id].append(item)
            return node_data
        else:
            return None

    def send_task_file(self, node_id, task_file_path):
        conn = self.compute_conns[node_id]
        try:
            with open(task_file_path, 'rb') as f:
                for i in f:
                    conn.send(i)
                    data = conn.recv(1024)
                    if data != b'success':
                        print(f"Error: Task file not acknowledged by node {node_id}")
                        return
            conn.send('quit'.encode())
            print(f"Sent task file {task_file_path} to node {node_id}")
        except FileNotFoundError:
            print(f"Error: Task file {task_file_path} not found.")
        except Exception as e:
            print(f"Error sending task file {task_file_path} to node {node_id}: {e}")

    def send_task(self, node_id, task_info, task_part):
        conn = self.compute_conns[node_id]
        try:
            message = json.dumps({'type': 'task', 'task_info': task_info, 'joint_node': len(self.compute_nodes), 'part': task_part}).encode()
            conn.sendall(message)
            print(f"Sent task part to node {node_id}")
        except Exception as e:
            print(f"Error sending task to node {node_id}: {e}")

    def listen_for_nodes(self):
        threading.Thread(target=self.listen_for_nodes_thread).start()

    def listen_for_nodes_thread(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port + 1))
            s.listen()
            print(f"Control node listening for compute nodes on {self.host}:{self.port + 1}")
            while True:
                conn, addr = s.accept()
                threading.Thread(target=self.handle_node, args=(conn, addr)).start()

    def handle_node(self, conn, addr):
        try:
            data = conn.recv(1024)
            if data:
                message = json.loads(data.decode())
                if message['type'] == 'register':
                    with self.lock:
                        node_id = len(self.compute_nodes)
                        self.compute_nodes[node_id] = {'id': node_id, 'host': message['host'], 'port': message['port']}
                        self.compute_conns.append(conn)
                        response = {'id': node_id, 'reduce_addr': self.get_reduce_addr()}
                        conn.sendall(json.dumps(response).encode())
                        print(f"Registered node {node_id} from {addr}")
        except Exception as e:
            print(f"Error handling node {addr}: {e}")

    def get_reduce_addr(self):
        for node_id in self.compute_nodes:
            if node_id == 0:
                node_info = self.compute_nodes[node_id]
                return (node_info['host'], node_info['port'])
        return None
    
    def close_connection(self):
        for conn in self.compute_conns:
            conn.send('close'.encode())
            conn.close()
        
        
        

if __name__ == "__main__":
    control_node = ControlNode()
    control_node.start()








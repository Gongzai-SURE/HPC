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
                    self.handle_start_task(command['task_data'],conn)
                elif command['type'] == 'end_task':
                    conn.close()
                    self.close_all_resource()
        except Exception as e:
            print(f"Error handling command from {addr}: {e}")

    def handle_start_task(self, task_data, cmd_conn):
        try:
            task_id = task_data['task_id']
            task_file_path = f"task/task{task_id}.py"
            setup_file_path = f"task/setup{task_id}.txt"
            
            # 分发任务文件和任务数据到计算节点
            for i, node_id in enumerate(self.compute_nodes):
                try:
                    conn = self.compute_conns[node_id]
                    conn.send("please accept task".encode())
                    time.sleep(0.3)
                    self.send_task_file(node_id, task_file_path)
                    time.sleep(0.3)
                    conn.send("please accept data".encode())
                    time.sleep(0.5)
                    self.send_set_up_file(node_id, setup_file_path)
                    time.sleep(0.75)
                    self.send_task(i,node_id,task_id)
                except Exception as e:
                    print(f"Error sending task to node {node_id}: {e}")

            t_start = time.time()
            #从reduce计算节点接收最终结果
            while True:
                message = self.compute_conns[0].recv(1024)
                if message: 
                    result = json.loads(message.decode())
                    print('Received final result:',result['result'])
                    t_finish = time.time()
                    try: 
                        message = json.dumps({'type': 'result', 'result': result['result'],'computer_node_num': len(self.compute_nodes), 'time_cost': t_finish - t_start}).encode()
                        cmd_conn.send(message)
                    except Exception as e:
                        print(f"Error sending final result to cmd: {e}")
                    break
        except KeyError as e:
            print(f"Error: missing key {e} in task_data")
        except Exception as e:
            print(f"Error handling start task: {e}")

    def send_set_up_file(self, node_id, setup_file_path):
        conn = self.compute_conns[node_id]
        try:
            with open(setup_file_path, 'rb') as f:
                for i in f:
                    conn.send(i)
            time.sleep(0.75)
            conn.send('quit'.encode())
            print(f"Sent Set up file {setup_file_path} to node {node_id}")
        except FileNotFoundError:
            print(f"Error: Set up file {setup_file_path} not found.")
        except Exception as e:
            print(f"Error sending Set up file {setup_file_path} to node {node_id}: {e}")
            
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

    def send_task(self,i,node_id, task_id):
        conn = self.compute_conns[node_id]
        try:
            message = json.dumps({'type': 'task', 'task_id': task_id ,'joint_node': len(self.compute_nodes),'rank':i}).encode()
            conn.sendall(message)
            print(f"Sent start task to node {node_id}")
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








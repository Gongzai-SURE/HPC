import socket
import json

def send_start_task_command(control_host, control_port, task_id):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((control_host, control_port))
            command = {'type': 'start_task', 'task_data': {'task_id': task_id}}
            s.sendall(json.dumps(command).encode())
            print(f"Sent start task command for task_id {task_id}")
            while True:
                message = s.recv(1024)
                if message:
                    result = json.loads(message.decode())
                    print(f"Received final result: {result['result']},computer_node_num: {result['computer_node_num']}, time cost: {result['total_time_cost']} ,Single node time :{result['Single_node_time']}, Speedup ratio : {result['Speedup ratio']}")
                    break
    except Exception as e:
        print(f"Error sending start task command: {e}")

def send_end_command(control_host, control_port):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((control_host, control_port))
            command = {'type': 'end_task'}
            s.sendall(json.dumps(command).encode())
    except Exception as e:
        print(f"Error sending end command: {e}")

if __name__ == "__main__":
    control_host = '127.0.0.1'
    control_port = 5000
    #根据外部输入决定发送指令信息，如果是大于0的数字发送指定数字号的指令，如果是'-1'发送结束指令
    try:
        while True:
            task_id = input("Enter a task id to start or -1 to end: ")
            if task_id == '-1':
                send_end_command(control_host, control_port)
                print("Sent end command. Exiting.")
                break
            elif task_id.isdigit() and int(task_id) > 0:
                send_start_task_command(control_host, control_port, task_id)
                continue
            else:
                print("Invalid task id. Please enter a positive integer or -1 to end.")
    except KeyboardInterrupt:
        print("Exiting.")
        pass
    
    
    




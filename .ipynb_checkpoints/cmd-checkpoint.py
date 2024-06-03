import socket
import json

def send_start_task_command(control_host, control_port, task_id):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((control_host, control_port))
            command = {'type': 'start_task', 'task_data': {'task_id': task_id}}
            s.sendall(json.dumps(command).encode())
            print(f"Sent start task command for task_id {task_id}")
    except Exception as e:
        print(f"Error sending start task command: {e}")

if __name__ == "__main__":
    control_host = '127.0.0.1'
    control_port = 5000
    task_id = 1
    send_start_task_command(control_host, control_port, task_id)




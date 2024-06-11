import math

def do_task1_1(data):
    if not data:
        return None
    return max(data)

def do_task2_1(data):
    if not data:
        return None
    return max(data)

def do_task1_2(data, max_n):
    # Remove max_n from the data
    data = [num for num in data if num != max_n]
    
    # Sort data in descending order
    data.sort(reverse=True)
    
    # Find the largest number that is coprime with max_n
    for num in data:
        if math.gcd(num, max_n) == 1:
            return num
    
    return None  # In case there is no coprime number

def do_task2_2(data):
    if not data:
        return None
    return max(data)


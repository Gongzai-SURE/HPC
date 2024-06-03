max_n = 10000
def is_prime(n):
    if n <= 1:
        return False
    if n <= 3:
        return True
    if n % 2 == 0 or n % 3 == 0:
        return False
    i = 5
    while i * i <= n:
        if n % i == 0 or n % (i + 2) == 0:
            return False
        i += 6
    return True

def do_task1(id,len_n):
    res = 0
    while id<max_n:
        if is_prime(id):
            res = res+1
        id = id + len_n
    return res

def do_task2(data):
    return sum(data)
    
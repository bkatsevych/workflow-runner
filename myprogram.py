def cpu_heavy_task():
    result = 0
    for i in range(10000000):
        result += i * i

if __name__ == "__main__":
    for _ in range(10):
        cpu_heavy_task()
    
    large_list = [0] * (1024**3 // 4)  # Allocate approximately 1GB of memory
    
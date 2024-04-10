import sys

def cpu_heavy_task():
    # Perform a computationally heavy task
    result = 0
    for i in range(10000000):
        result += i * i

def memory_heavy_task():
    # Allocate a large amount of memory
    large_list = [0] * (1024**3 // 4)  # Allocate approximately 1GB of memory

if __name__ == "__main__":
    # Run CPU-heavy tasks
    for _ in range(10):
        cpu_heavy_task()
    
    # Run Memory-heavy tasks
    for _ in range(10):
        memory_heavy_task()

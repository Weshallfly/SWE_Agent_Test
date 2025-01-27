import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

class TaskQueue:
    def __init__(self, max_workers=3):
        self.max_workers = max_workers
        self.tasks = []  # List to store all tasks
        self.task_states = {}  # Dictionary to track task statuses
        self.lock = Lock()  # Lock for thread safety

    def add_task(self, task_id, func, *args, **kwargs):
        """
        Add a task to the queue.
        """
        with self.lock:
            if task_id in self.task_states:
                raise ValueError(f"Task ID {task_id} already exists!")  # Prevent duplicate task IDs
            self.tasks.append((task_id, func, args, kwargs))
            self.task_states[task_id] = "PENDING"  # Initial state

    def process_tasks(self):
        """
        Process tasks in parallel using ThreadPoolExecutor.
        """
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_task = {
                executor.submit(func, *args, **kwargs): task_id
                for task_id, func, args, kwargs in self.tasks
            }

            for future in as_completed(future_to_task):
                task_id = future_to_task[future]
                try:
                    result = future.result()  # This could raise an exception
                    with self.lock:
                        self.task_states[task_id] = f"COMPLETED: {result}"
                except Exception as e:
                    with self.lock:
                        self.task_states[task_id] = f"FAILED: {e}"

    def get_task_state(self, task_id):
        """
        Get the state of a specific task.
        """
        with self.lock:
            return self.task_states.get(task_id, "Task not found")

# Simulated task functions
def simulated_task(task_id, duration):
    """
    Simulates a task by sleeping for a random amount of time.
    """
    if random.random() < 0.3:  # Simulate a 30% failure rate
        raise RuntimeError(f"Task {task_id} failed due to a simulated error.")
    time.sleep(duration)
    return f"Task {task_id} completed in {duration:.2f} seconds."

if __name__ == "__main__":
    queue = TaskQueue(max_workers=3)

    # Adding tasks
    for i in range(1, 6):
        duration = random.uniform(0.5, 2.0)  # Random duration between 0.5 and 2 seconds
        queue.add_task(task_id=f"Task_{i}", func=simulated_task, task_id=f"Task_{i}", duration=duration)

    # Processing tasks
    queue.process_tasks()

    # Retrieving task states
    for i in range(1, 6):
        task_id = f"Task_{i}"
        print(f"{task_id}: {queue.get_task_state(task_id)}")

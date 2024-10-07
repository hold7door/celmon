import time
from celery import Celery
from celery.events import EventReceiver
from celery.events.state import State
from collections import defaultdict
from prometheus_client import start_http_server, Gauge, Counter

app = Celery('tasks')
app.config_from_object('celeryconfig')  # Assuming you have a celeryconfig.py file

class CeleryMonitor:
    def __init__(self):
        self.state = State()
        self.workers = defaultdict(lambda: {'active_tasks': 0, 'completed_tasks': 0, 'prefetched_tasks': 0, 'status': 'unknown'})
        self.tasks = defaultdict(lambda: {'total': 0, 'success': 0, 'failure': 0, 'durations': []})

        # Prometheus metrics
        self.worker_active_tasks = Gauge('celery_worker_active_tasks', 'Number of active tasks per worker', ['worker'])
        self.worker_completed_tasks = Counter('celery_worker_completed_tasks', 'Number of completed tasks per worker', ['worker'])
        self.worker_prefetched_tasks = Gauge('celery_worker_prefetched_tasks', 'Number of prefetched tasks per worker', ['worker'])
        self.worker_status = Gauge('celery_worker_status', 'Status of the worker (1 for online, 0 for offline)', ['worker'])
        self.task_total = Counter('celery_task_total', 'Total number of tasks', ['task'])
        self.task_success = Counter('celery_task_success', 'Number of successful tasks', ['task'])
        self.task_failure = Counter('celery_task_failure', 'Number of failed tasks', ['task'])
        self.task_duration = Gauge('celery_task_duration', 'Task duration in seconds', ['task'])

    def __call__(self, event):
        self.state.event(event)
        event_type = event['type']

        if event_type.startswith('task-'):
            self.handle_task_event(event)
        elif event_type.startswith('worker-'):
            self.handle_worker_event(event)

    def handle_task_event(self, event):
        task_id = event['uuid']
        task_name = event.get('name', 'unknown')
        worker = event['hostname']

        if event['type'] == 'task-received':
            self.workers[worker]['active_tasks'] += 1
            self.tasks[task_name]['total'] += 1
            self.worker_active_tasks.labels(worker=worker).inc()
            self.task_total.labels(task=task_name).inc()
        elif event['type'] == 'task-succeeded':
            self.workers[worker]['active_tasks'] -= 1
            self.workers[worker]['completed_tasks'] += 1
            self.tasks[task_name]['success'] += 1
            self.tasks[task_name]['durations'].append(event['runtime'])
            self.worker_active_tasks.labels(worker=worker).dec()
            self.worker_completed_tasks.labels(worker=worker).inc()
            self.task_success.labels(task=task_name).inc()
            self.task_duration.labels(task=task_name).set(event['runtime'])
        elif event['type'] == 'task-failed':
            self.workers[worker]['active_tasks'] -= 1
            self.workers[worker]['completed_tasks'] += 1
            self.tasks[task_name]['failure'] += 1
            self.worker_active_tasks.labels(worker=worker).dec()
            self.worker_completed_tasks.labels(worker=worker).inc()
            self.task_failure.labels(task=task_name).inc()

    def handle_worker_event(self, event):
        worker = event['hostname']
        if event['type'] == 'worker-online':
            print(f"Worker {worker} is online")
            self.workers[worker]['status'] = 'online'
            self.worker_status.labels(worker=worker).set(1)
        elif event['type'] == 'worker-offline':
            print(f"Worker {worker} went offline")
            self.workers[worker]['status'] = 'offline'
            self.worker_status.labels(worker=worker).set(0)
        elif event['type'] == 'worker-heartbeat':
            self.workers[worker]['prefetched_tasks'] = event.get('prefetched', 0)
            self.worker_prefetched_tasks.labels(worker=worker).set(self.workers[worker]['prefetched_tasks'])

    def print_stats(self):
        print("\nWorker Stats:")
        for worker, stats in self.workers.items():
            print(f"{worker}: Active Tasks: {stats['active_tasks']}, Completed Tasks: {stats['completed_tasks']}, Prefetched Tasks: {stats['prefetched_tasks']}, Status: {stats['status']}")

        print("\nTask Stats:")
        for task, stats in self.tasks.items():
            success_rate = (stats['success'] / stats['total']) * 100 if stats['total'] > 0 else 0
            avg_duration = sum(stats['durations']) / len(stats['durations']) if stats['durations'] else 0
            print(f"{task}: Total: {stats['total']}, Success Rate: {success_rate:.2f}%, Avg Duration: {avg_duration:.2f}s")

def monitor_celery():
    start_http_server(8000)  # Start Prometheus metrics server
    monitor = CeleryMonitor()
    with app.connection() as connection:
        recv = EventReceiver(connection, handlers={
            '*': monitor,
        })
        print("Starting Celery monitor...")
        try:
            recv.capture(limit=None, timeout=None, wakeup=True)
        except KeyboardInterrupt:
            print("Stopping Celery monitor...")
        finally:
            monitor.print_stats()

if __name__ == '__main__':
    monitor_celery()

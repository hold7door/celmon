import time
import json
from celery import Celery
from celery.events import EventReceiver
from celery.events.state import State
from collections import defaultdict
from prometheus_client import start_http_server, Gauge, Counter, Histogram
import redis
import os
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

app = Celery('salarybox_backend')
app.config_from_object('celeryconfig')  # Assuming you have a celeryconfig.py file

class CeleryMonitor:
    def __init__(self):
        self.state = State()
        persistence_url = os.environ.get('CELERY_PERSISTENCE_URL', "redis://localhost:6379")
        self.redis_client = redis.Redis.from_url(persistence_url)
        self.workers = self.load_state('workers', lambda data: defaultdict(lambda: {'active_tasks': 0, 'completed_tasks': 0, 'prefetched_tasks': 0, 'status': 'unknown'}, data))
        self.tasks = self.load_state('tasks', lambda data: defaultdict(lambda: {'total': 0, 'success': 0, 'failure': 0, 'durations': []}, data))
        self.tasks_id_to_name = self.load_state('tasks_id_to_name', lambda data: defaultdict(lambda: 'unknown', data))

        # Prometheus metrics
        # TODO:
        # self.worker_active_tasks = Gauge('celery_worker_active_tasks', 'Number of active tasks per worker', ['worker'])
        self.worker_completed_tasks = Counter('celery_worker_completed_tasks', 'Number of completed tasks per worker', ['worker'])
        self.worker_prefetched_tasks = Gauge('celery_worker_prefetched_tasks', 'Number of prefetched tasks per worker', ['worker'])
        self.worker_status = Gauge('celery_worker_status', 'Status of the worker (1 for online, 0 for offline)', ['worker'])
        self.task_total = Counter('celery_task_total', 'Total number of tasks', ['task'])
        self.task_success = Counter('celery_task_success', 'Number of successful tasks', ['task'])
        self.task_failure = Counter('celery_task_failure', 'Number of failed tasks', ['task'])
        self.per_task_duration = Histogram('celery_task_duration_per_task', 'Per task duration in seconds', ['task'])

        self.restore_metrics()

        logger.info("Monitor initialized")

    def load_state(self, key, default_factory):
        state_json = self.redis_client.get(key)
        if state_json:
            return default_factory(json.loads(state_json))
        return default_factory({})

    def save_state(self):
        self.redis_client.set('workers', json.dumps(self.workers))
        self.redis_client.set('tasks', json.dumps(self.tasks))
        self.redis_client.set('tasks_id_to_name', json.dumps(self.tasks_id_to_name))

    def restore_metrics(self):
        for worker, stats in self.workers.items():
            # self.worker_active_tasks.labels(worker=worker).set(stats['active_tasks'])
            self.worker_completed_tasks.labels(worker=worker)._value.set(stats['completed_tasks'])
            self.worker_prefetched_tasks.labels(worker=worker).set(stats['prefetched_tasks'])
            self.worker_status.labels(worker=worker).set(1 if stats['status'] == 'online' else 0)

        for task, stats in self.tasks.items():
            self.task_total.labels(task=task)._value.set(stats['total'])
            self.task_success.labels(task=task)._value.set(stats['success'])
            self.task_failure.labels(task=task)._value.set(stats['failure'])

    def __call__(self, event):
        self.state.event(event)
        event_type = event['type']

        if event_type.startswith('task-'):
            self.handle_task_event(event)
        elif event_type.startswith('worker-'):
            self.handle_worker_event(event)

        self.save_state()

    def handle_task_event(self, event):
        task_id = event['uuid']
        task_name = event.get('name', self.tasks_id_to_name[task_id])
        worker = event['hostname']

        logger.info(f"handling task: {task_id}, {task_name}, {event['type']}, {self.tasks_id_to_name}")

        if event['type'] == 'task-received':
            self.workers[worker]['active_tasks'] += 1
            self.tasks_id_to_name[task_id] = task_name
            self.tasks[task_name]['total'] += 1
            # self.worker_active_tasks.labels(worker=worker).inc()
            self.task_total.labels(task=task_name).inc()
        elif event['type'] == 'task-started':
            # TODO: what needs to done here?
            pass
        elif event['type'] == 'task-succeeded':
            self.workers[worker]['active_tasks'] -= 1
            self.workers[worker]['completed_tasks'] += 1
            self.tasks[task_name]['success'] += 1
            self.tasks[task_name]['durations'].append(event['runtime'])
            # self.worker_active_tasks.labels(worker=worker).dec()
            self.worker_completed_tasks.labels(worker=worker).inc()
            self.task_success.labels(task=task_name).inc()
            self.per_task_duration.labels(task=task_name).observe(event['runtime'])
        elif event['type'] == 'task-failed':
            self.workers[worker]['active_tasks'] -= 1
            self.workers[worker]['completed_tasks'] += 1
            self.tasks[task_name]['failure'] += 1
            # self.worker_active_tasks.labels(worker=worker).dec()
            self.worker_completed_tasks.labels(worker=worker).inc()
            self.task_failure.labels(task=task_name).inc()

    def handle_worker_event(self, event):
        worker = event['hostname']
        if event['type'] == 'worker-online':
            logger.info(f"Worker {worker} is online")
            self.workers[worker]['status'] = 'online'
            self.worker_status.labels(worker=worker).set(1)
        elif event['type'] == 'worker-offline':
            logger.info(f"Worker {worker} went offline")
            self.workers[worker]['status'] = 'offline'
            self.worker_status.labels(worker=worker).set(0)
        elif event['type'] == 'worker-heartbeat':
            self.workers[worker]['prefetched_tasks'] = event.get('prefetched', 0)
            self.worker_prefetched_tasks.labels(worker=worker).set(self.workers[worker]['prefetched_tasks'])

    def print_stats(self):
        logger.info("\nWorker Stats:")
        for worker, stats in self.workers.items():
            logger.info(f"{worker}: Active Tasks: {stats['active_tasks']}, Completed Tasks: {stats['completed_tasks']}, Prefetched Tasks: {stats['prefetched_tasks']}, Status: {stats['status']}")

        logger.info("\nTask Stats:")
        for task, stats in self.tasks.items():
            success_rate = (stats['success'] / stats['total']) * 100 if stats['total'] > 0 else 0
            avg_duration = sum(stats['durations']) / len(stats['durations']) if stats['durations'] else 0
            logger.info(f"{task}: Total: {stats['total']}, Success Rate: {success_rate:.2f}%, Avg Duration: {avg_duration:.2f}s")

def monitor_celery():
    start_http_server(8099)  # Start Prometheus metrics server
    logger.info("Started prometheus metrics server on port 8099")
    monitor = CeleryMonitor()
    with app.connection() as connection:
        logger.info("Connection established with redis broker")
        recv = EventReceiver(connection, handlers={
            '*': monitor,
        })
        try:
            recv.capture(limit=None, timeout=None, wakeup=True)
        except KeyboardInterrupt:
            logger.info("Stopping Celery monitor...")
        finally:
            monitor.print_stats()
            monitor.save_state()

if __name__ == '__main__':
    logger.info(f"Starting celmon...")
    monitor_celery()

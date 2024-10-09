import os

broker_url = os.environ.get('CELERY_BROKER_URL', "redis://localhost:6379")

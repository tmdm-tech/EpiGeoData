"""Resource-aware Gunicorn settings for the 512 MiB Render instance.

Gunicorn automatically discovers gunicorn.conf.py from the working directory.
One worker avoids duplicated GeoPandas/Matplotlib memory across processes.
Two threads keep light health/API requests responsive during one map render.
"""
workers = 1
worker_class = "gthread"
threads = 2
timeout = 120
graceful_timeout = 30
keepalive = 5
max_requests = 60
max_requests_jitter = 10
accesslog = "-"
errorlog = "-"

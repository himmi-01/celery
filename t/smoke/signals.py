"""Signal Handlers for the smoke test."""

import logging
from celery.signals import worker_init, worker_process_init, worker_process_shutdown, worker_ready, worker_shutdown

# Configure logger for signal handlers
logger = logging.getLogger(__name__)


@worker_init.connect
def worker_init_handler(sender, **kwargs):
    print("worker_init_handler")
def worker_process_init_handler(sender, **kwargs):
    print("worker_process_init_handler")


    print("worker_process_init_handler")

@worker_ready.connect
def worker_ready_handler(sender, **kwargs):
    print("worker_ready_handler")
    print("worker_process_shutdown_handler")
    print("worker_shutdown_handler")



    print("worker_ready_handler")




    print("worker_shutdown_handler")
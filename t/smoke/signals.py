"""Signal Handlers for the smoke test."""

import logging
from celery.signals import worker_init, worker_process_init, worker_process_shutdown, worker_ready, worker_shutdown

# Configure logger for signal handlers
logger = logging.getLogger(__name__)


@worker_init.connect
def worker_init_handler(sender, **kwargs):
    try:
        print("worker_init_handler")
    except Exception as e:
        logger.error("Error in worker_init_handler: %s", e, exc_info=True)


@worker_process_init.connect
def worker_process_init_handler(sender, **kwargs):
    try:
        print("worker_process_init_handler")
    except Exception as e:
        logger.error("Error in worker_process_init_handler: %s", e, exc_info=True)


@worker_process_shutdown.connect
def worker_process_shutdown_handler(sender, pid, exitcode, **kwargs):
    try:
        print("worker_process_shutdown_handler")
    except Exception as e:
        logger.error("Error in worker_process_shutdown_handler: %s", e, exc_info=True)


@worker_ready.connect
def worker_ready_handler(sender, **kwargs):
    try:
        print("worker_ready_handler")
    except Exception as e:
        logger.error("Error in worker_ready_handler: %s", e, exc_info=True)


@worker_shutdown.connect
def worker_shutdown_handler(sender, **kwargs):
    try:
        print("worker_shutdown_handler")
    except Exception as e:
        logger.error("Error in worker_shutdown_handler: %s", e, exc_info=True)

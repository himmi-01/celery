"""Signal Handlers for the smoke test."""

from celery.signals import worker_init, worker_process_init, worker_process_shutdown, worker_ready, worker_shutdown


@worker_init.connect
def worker_init_handler(sender, **kwargs):
    """Handle worker initialization signal.
    
    This handler is called when a Celery worker is first initialized,
    before any worker processes are spawned. It's triggered once per
    worker instance startup.
    
    Args:
        sender: The worker instance that sent the signal
        **kwargs: Additional keyword arguments from the signal
    """
    print("worker_init_handler")


@worker_process_init.connect
def worker_process_init_handler(sender, **kwargs):
    """Handle worker process initialization signal.
    
    This handler is called when each individual worker process is
    initialized. In a multi-process worker setup, this will be called
    once for each worker process that gets spawned.
    
    Args:
        sender: The worker instance that sent the signal
        **kwargs: Additional keyword arguments from the signal
    """
    print("worker_process_init_handler")


@worker_process_shutdown.connect
def worker_process_shutdown_handler(sender, pid, exitcode, **kwargs):
    """Handle worker process shutdown signal.
    
    This handler is called when a worker process is shutting down.
    It provides information about the process that's terminating,
    including its PID and exit code.
    
    Args:
        sender: The worker instance that sent the signal
        pid (int): Process ID of the shutting down worker process
        exitcode (int): Exit code of the worker process (0 for normal shutdown)
        **kwargs: Additional keyword arguments from the signal
    """
    print("worker_process_shutdown_handler")


@worker_ready.connect
def worker_ready_handler(sender, **kwargs):
    """Handle worker ready signal.
    
    This handler is called when the worker is fully initialized and
    ready to accept tasks. This signal is sent after all worker
    processes have been spawned and are ready to process tasks.
    
    Args:
        sender: The worker instance that sent the signal
        **kwargs: Additional keyword arguments from the signal
    """
    print("worker_ready_handler")


@worker_shutdown.connect
def worker_shutdown_handler(sender, **kwargs):
    """Handle worker shutdown signal.
    
    This handler is called when the worker is shutting down gracefully.
    This is triggered when the worker receives a shutdown signal and
    begins its shutdown process, before any processes are terminated.
    
    Args:
        sender: The worker instance that sent the signal
        **kwargs: Additional keyword arguments from the signal
    """
    print("worker_shutdown_handler")

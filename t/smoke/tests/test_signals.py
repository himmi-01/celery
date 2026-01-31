import pytest
from pytest_celery import CeleryBackendCluster, CeleryTestSetup
from unittest.mock import patch, MagicMock

from celery.signals import after_task_publish, before_task_publish
from t.smoke.tasks import noop


@pytest.fixture
def default_worker_signals(default_worker_signals: set) -> set:
    from t.smoke import signals

    default_worker_signals.add(signals)
    yield default_worker_signals


@pytest.fixture
def celery_backend_cluster() -> CeleryBackendCluster:
    # Disable backend
    return None


class test_signals:
    @pytest.mark.parametrize(
        "log, control",
        [
            ("worker_init_handler", None),
            ("worker_process_init_handler", None),
            ("worker_ready_handler", None),
            ("worker_process_shutdown_handler", "shutdown"),
            ("worker_shutdown_handler", "shutdown"),
        ],
    )
    def test_sanity(self, celery_setup: CeleryTestSetup, log: str, control: str):
        if control:
            celery_setup.app.control.broadcast(control)
        celery_setup.worker.wait_for_log(log)


class test_before_task_publish:
    def test_sanity(self, celery_setup: CeleryTestSetup):
        @before_task_publish.connect
        def before_task_publish_handler(*args, **kwargs):
            nonlocal signal_was_called
            signal_was_called = True

        signal_was_called = False
        noop.s().apply_async(queue=celery_setup.worker.worker_queue)
        assert signal_was_called is True


class test_worker_init_handler:
    def test_normal_operation(self):
        from t.smoke.signals import worker_init_handler
        
        with patch('builtins.print') as mock_print:
            worker_init_handler(sender=MagicMock())
            mock_print.assert_called_once_with("worker_init_handler")
    
    def test_with_kwargs(self):
        from t.smoke.signals import worker_init_handler
        
        with patch('builtins.print') as mock_print:
            worker_init_handler(sender=MagicMock(), extra_arg="test", another_kwarg=123)
            mock_print.assert_called_once_with("worker_init_handler")
    
    def test_print_failure(self):
        from t.smoke.signals import worker_init_handler
        
        with patch('builtins.print', side_effect=IOError("Print failed")):
            with pytest.raises(IOError, match="Print failed"):
                worker_init_handler(sender=MagicMock())
    
    def test_none_sender(self):
        from t.smoke.signals import worker_init_handler
        
        with patch('builtins.print') as mock_print:
            worker_init_handler(sender=None)
            mock_print.assert_called_once_with("worker_init_handler")


class test_worker_process_init_handler:
    def test_normal_operation(self):
        from t.smoke.signals import worker_process_init_handler
        
        with patch('builtins.print') as mock_print:
            worker_process_init_handler(sender=MagicMock())
            mock_print.assert_called_once_with("worker_process_init_handler")
    
    def test_with_kwargs(self):
        from t.smoke.signals import worker_process_init_handler
        
        with patch('builtins.print') as mock_print:
            worker_process_init_handler(sender=MagicMock(), pool_opts={"processes": 4}, extra="value")
            mock_print.assert_called_once_with("worker_process_init_handler")
    
    def test_print_failure(self):
        from t.smoke.signals import worker_process_init_handler
        
        with patch('builtins.print', side_effect=OSError("Output error")):
            with pytest.raises(OSError, match="Output error"):
                worker_process_init_handler(sender=MagicMock())
    
    def test_none_sender(self):
        from t.smoke.signals import worker_process_init_handler
        
        with patch('builtins.print') as mock_print:
            worker_process_init_handler(sender=None)
            mock_print.assert_called_once_with("worker_process_init_handler")


class test_worker_process_shutdown_handler:
    def test_normal_operation(self):
        from t.smoke.signals import worker_process_shutdown_handler
        
        with patch('builtins.print') as mock_print:
            worker_process_shutdown_handler(sender=MagicMock(), pid=1234, exitcode=0)
            mock_print.assert_called_once_with("worker_process_shutdown_handler")
    
    def test_with_error_exitcode(self):
        from t.smoke.signals import worker_process_shutdown_handler
        
        with patch('builtins.print') as mock_print:
            worker_process_shutdown_handler(sender=MagicMock(), pid=5678, exitcode=1)
            mock_print.assert_called_once_with("worker_process_shutdown_handler")
    
    def test_with_kwargs(self):
        from t.smoke.signals import worker_process_shutdown_handler
        
        with patch('builtins.print') as mock_print:
            worker_process_shutdown_handler(
                sender=MagicMock(), 
                pid=9999, 
                exitcode=-1, 
                signal_name="SIGTERM",
                extra_info="test"
            )
            mock_print.assert_called_once_with("worker_process_shutdown_handler")
    
    def test_print_failure(self):
        from t.smoke.signals import worker_process_shutdown_handler
        
        with patch('builtins.print', side_effect=Exception("Print exception")):
            with pytest.raises(Exception, match="Print exception"):
                worker_process_shutdown_handler(sender=MagicMock(), pid=1111, exitcode=0)
    
    def test_none_values(self):
        from t.smoke.signals import worker_process_shutdown_handler
        
        with patch('builtins.print') as mock_print:
            worker_process_shutdown_handler(sender=None, pid=None, exitcode=None)
            mock_print.assert_called_once_with("worker_process_shutdown_handler")


class test_worker_ready_handler:
    def test_normal_operation(self):
        from t.smoke.signals import worker_ready_handler
        
        with patch('builtins.print') as mock_print:
            worker_ready_handler(sender=MagicMock())
            mock_print.assert_called_once_with("worker_ready_handler")
    
    def test_with_kwargs(self):
        from t.smoke.signals import worker_ready_handler
        
        with patch('builtins.print') as mock_print:
            worker_ready_handler(sender=MagicMock(), hostname="worker1", ready_time=123456)
            mock_print.assert_called_once_with("worker_ready_handler")
    
    def test_print_failure(self):
        from t.smoke.signals import worker_ready_handler
        
        with patch('builtins.print', side_effect=RuntimeError("Runtime error")):
            with pytest.raises(RuntimeError, match="Runtime error"):
                worker_ready_handler(sender=MagicMock())
    
    def test_none_sender(self):
        from t.smoke.signals import worker_ready_handler
        
        with patch('builtins.print') as mock_print:
            worker_ready_handler(sender=None)
            mock_print.assert_called_once_with("worker_ready_handler")


class test_worker_shutdown_handler:
    def test_normal_operation(self):
        from t.smoke.signals import worker_shutdown_handler
        
        with patch('builtins.print') as mock_print:
            worker_shutdown_handler(sender=MagicMock())
            mock_print.assert_called_once_with("worker_shutdown_handler")
    
    def test_with_kwargs(self):
        from t.smoke.signals import worker_shutdown_handler
        
        with patch('builtins.print') as mock_print:
            worker_shutdown_handler(
                sender=MagicMock(), 
                sig="SIGTERM", 
                how="warm", 
                exitcode=0,
                reason="shutdown requested"
            )
            mock_print.assert_called_once_with("worker_shutdown_handler")
    
    def test_print_failure(self):
        from t.smoke.signals import worker_shutdown_handler
        
        with patch('builtins.print', side_effect=KeyboardInterrupt("Interrupted")):
            with pytest.raises(KeyboardInterrupt, match="Interrupted"):
                worker_shutdown_handler(sender=MagicMock())
    
    def test_none_sender(self):
        from t.smoke.signals import worker_shutdown_handler
        
        with patch('builtins.print') as mock_print:
            worker_shutdown_handler(sender=None)
            mock_print.assert_called_once_with("worker_shutdown_handler")
    
    def test_empty_kwargs(self):
        from t.smoke.signals import worker_shutdown_handler
        
        with patch('builtins.print') as mock_print:
            worker_shutdown_handler(sender=MagicMock(), **{})
            mock_print.assert_called_once_with("worker_shutdown_handler")


class test_after_task_publish:
    def test_sanity(self, celery_setup: CeleryTestSetup):
        @after_task_publish.connect
        def after_task_publish_handler(*args, **kwargs):
            nonlocal signal_was_called
            signal_was_called = True

        signal_was_called = False
        noop.s().apply_async(queue=celery_setup.worker.worker_queue)
        assert signal_was_called is True

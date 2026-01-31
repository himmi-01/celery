"""Unit tests for celery.beat module."""

import copy
import datetime
import os
import shelve
import tempfile
import time
import unittest
from unittest.mock import Mock, MagicMock, patch, PropertyMock

from celery import Celery
from celery.beat import (
    BeatLazyFunc, ScheduleEntry, Scheduler, PersistentScheduler,
    Service, EmbeddedService, SchedulingError, _Threaded, _Process,
    _evaluate_entry_args, _evaluate_entry_kwargs
)
from celery.schedules import crontab, schedule


class TestBeatLazyFunc(unittest.TestCase):
    """Test BeatLazyFunc class."""

    def test_delay(self):
        """Test delay method."""
        mock_func = Mock(return_value="test_result")
        lazy_func = BeatLazyFunc(mock_func, "arg1", "arg2", kwarg1="value1")
        
        result = lazy_func.delay()
        
        mock_func.assert_called_once_with("arg1", "arg2", kwarg1="value1")
        self.assertEqual(result, "test_result")

    def test_call(self):
        """Test __call__ method."""
        mock_func = Mock(return_value="test_result")
        lazy_func = BeatLazyFunc(mock_func, "arg1")
        
        result = lazy_func()
        
        mock_func.assert_called_once_with("arg1")
        self.assertEqual(result, "test_result")


class TestScheduleEntry(unittest.TestCase):
    """Test ScheduleEntry class."""

    def setUp(self):
        """Set up test fixtures."""
        self.app = Celery('test')
        self.schedule_obj = schedule(run_every=60)
        
    def test_default_now(self):
        """Test default_now method."""
        entry = ScheduleEntry(
            name='test_task',
            task='test.task',
            schedule=self.schedule_obj,
            app=self.app
        )
        
        now = entry.default_now()
        self.assertIsInstance(now, datetime.datetime)

    def test_update(self):
        """Test update method."""
        entry1 = ScheduleEntry(
            name='test_task',
            task='test.task1',
            schedule=self.schedule_obj,
            args=('arg1',),
            kwargs={'key1': 'value1'},
            options={'queue': 'queue1'},
            app=self.app
        )
        
        entry2 = ScheduleEntry(
            name='test_task',
            task='test.task2',
            schedule=schedule(run_every=120),
            args=('arg2',),
            kwargs={'key2': 'value2'},
            options={'queue': 'queue2'},
            app=self.app
        )
        
        entry1.update(entry2)
        
        self.assertEqual(entry1.task, 'test.task2')
        self.assertEqual(entry1.args, ('arg2',))
        self.assertEqual(entry1.kwargs, {'key2': 'value2'})
        self.assertEqual(entry1.options, {'queue': 'queue2'})

    def test_is_due(self):
        """Test is_due method."""
        entry = ScheduleEntry(
            name='test_task',
            task='test.task',
            schedule=self.schedule_obj,
            app=self.app
        )
        
        is_due, next_time = entry.is_due()
        self.assertIsInstance(is_due, bool)
        self.assertIsInstance(next_time, (int, float))

    def test_editable_fields_equal(self):
        """Test editable_fields_equal method."""
        entry1 = ScheduleEntry(
            name='test_task',
            task='test.task',
            schedule=self.schedule_obj,
            args=('arg1',),
            kwargs={'key1': 'value1'},
            options={'queue': 'queue1'},
            app=self.app
        )
        
        entry2 = ScheduleEntry(
            name='test_task',
            task='test.task',
            schedule=self.schedule_obj,
            args=('arg1',),
            kwargs={'key1': 'value1'},
            options={'queue': 'queue1'},
            app=self.app
        )
        
        self.assertTrue(entry1.editable_fields_equal(entry2))
        
        entry2.task = 'different.task'
        self.assertFalse(entry1.editable_fields_equal(entry2))


class TestScheduler(unittest.TestCase):
    """Test Scheduler class."""

    def setUp(self):
        """Set up test fixtures."""
        self.app = Celery('test')
        self.app.conf.beat_schedule = {}
        self.scheduler = Scheduler(self.app, lazy=True)

    def test_install_default_entries(self):
        """Test install_default_entries method."""
        data = {}
        self.app.conf.result_expires = 3600
        self.app.backend = Mock()
        self.app.backend.supports_autoexpire = False
        
        self.scheduler.install_default_entries(data)
        
        # Should add backend cleanup task
        self.assertIn('celery.backend_cleanup', self.scheduler.schedule)

    def test_apply_entry(self):
        """Test apply_entry method."""
        entry = ScheduleEntry(
            name='test_task',
            task='test.task',
            schedule=self.schedule_obj,
            app=self.app
        )
        
        mock_producer = Mock()
        
        with patch.object(self.scheduler, 'apply_async') as mock_apply:
            mock_apply.return_value = Mock(id='task_id')
            self.scheduler.apply_entry(entry, producer=mock_producer)
            
            mock_apply.assert_called_once_with(entry, producer=mock_producer, advance=False)

    def test_adjust(self):
        """Test adjust method."""
        result = self.scheduler.adjust(10)
        self.assertAlmostEqual(result, 9.99, places=2)
        
        result = self.scheduler.adjust(0)
        self.assertEqual(result, 0)
        
        result = self.scheduler.adjust(-5)
        self.assertEqual(result, -5)

    def test_is_due(self):
        """Test is_due method."""
        entry = ScheduleEntry(
            name='test_task',
            task='test.task',
            schedule=self.schedule_obj,
            app=self.app
        )
        
        result = self.scheduler.is_due(entry)
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 2)

    def test_populate_heap(self):
        """Test populate_heap method."""
        entry = ScheduleEntry(
            name='test_task',
            task='test.task',
            schedule=self.schedule_obj,
            app=self.app
        )
        
        self.scheduler.schedule = {'test_task': entry}
        self.scheduler.populate_heap()
        
        self.assertIsNotNone(self.scheduler._heap)
        self.assertEqual(len(self.scheduler._heap), 1)

    def test_tick(self):
        """Test tick method."""
        entry = ScheduleEntry(
            name='test_task',
            task='test.task',
            schedule=self.schedule_obj,
            app=self.app
        )
        
        self.scheduler.schedule = {'test_task': entry}
        self.scheduler.producer = Mock()
        
        with patch.object(self.scheduler, 'apply_entry'):
            result = self.scheduler.tick()
            
            self.assertIsInstance(result, (int, float))

    def test_schedules_equal(self):
        """Test schedules_equal method."""
        entry1 = ScheduleEntry(
            name='test_task',
            task='test.task',
            schedule=self.schedule_obj,
            app=self.app
        )
        
        schedule1 = {'test_task': entry1}
        schedule2 = {'test_task': entry1}
        
        self.assertTrue(self.scheduler.schedules_equal(schedule1, schedule2))
        
        schedule2['different_task'] = entry1
        self.assertFalse(self.scheduler.schedules_equal(schedule1, schedule2))

    def test_should_sync(self):
        """Test should_sync method."""
        # First call should return True (no last sync)
        self.assertTrue(self.scheduler.should_sync())
        
        # After setting last sync, should return False
        self.scheduler._last_sync = time.monotonic()
        self.assertFalse(self.scheduler.should_sync())

    def test_reserve(self):
        """Test reserve method."""
        entry = ScheduleEntry(
            name='test_task',
            task='test.task',
            schedule=self.schedule_obj,
            app=self.app
        )
        
        self.scheduler.schedule = {'test_task': entry}
        
        new_entry = self.scheduler.reserve(entry)
        
        self.assertIsInstance(new_entry, ScheduleEntry)
        self.assertEqual(new_entry.total_run_count, entry.total_run_count + 1)

    def test_apply_async(self):
        """Test apply_async method."""
        entry = ScheduleEntry(
            name='test_task',
            task='test.task',
            schedule=self.schedule_obj,
            app=self.app
        )
        
        self.scheduler.schedule = {'test_task': entry}
        
        mock_task = Mock()
        mock_task.apply_async.return_value = Mock(id='task_id')
        self.app.tasks = {'test.task': mock_task}
        
        with patch.object(self.scheduler, '_do_sync'):
            result = self.scheduler.apply_async(entry)
            
            mock_task.apply_async.assert_called_once()
            self.assertIsNotNone(result)

    def test_send_task(self):
        """Test send_task method."""
        with patch.object(self.app, 'send_task') as mock_send:
            mock_send.return_value = Mock(id='task_id')
            
            result = self.scheduler.send_task('test.task', [], {})
            
            mock_send.assert_called_once_with('test.task', [], {})
            self.assertIsNotNone(result)

    def test_setup_schedule(self):
        """Test setup_schedule method."""
        with patch.object(self.scheduler, 'install_default_entries') as mock_install:
            with patch.object(self.scheduler, 'merge_inplace') as mock_merge:
                self.scheduler.setup_schedule()
                
                mock_install.assert_called_once()
                mock_merge.assert_called_once()

    def test_sync(self):
        """Test sync method."""
        # Base scheduler sync does nothing
        self.scheduler.sync()
        # Should not raise any exception

    def test_close(self):
        """Test close method."""
        with patch.object(self.scheduler, 'sync') as mock_sync:
            self.scheduler.close()
            mock_sync.assert_called_once()

    def test_add(self):
        """Test add method."""
        entry = self.scheduler.add(
            name='test_task',
            task='test.task',
            schedule=self.schedule_obj
        )
        
        self.assertIsInstance(entry, ScheduleEntry)
        self.assertEqual(entry.name, 'test_task')
        self.assertIn('test_task', self.scheduler.schedule)

    def test_update_from_dict(self):
        """Test update_from_dict method."""
        schedule_dict = {
            'test_task': {
                'task': 'test.task',
                'schedule': 60,
            }
        }
        
        self.scheduler.update_from_dict(schedule_dict)
        
        self.assertIn('test_task', self.scheduler.schedule)
        self.assertIsInstance(self.scheduler.schedule['test_task'], ScheduleEntry)

    def test_merge_inplace(self):
        """Test merge_inplace method."""
        # Add existing entry
        existing_entry = ScheduleEntry(
            name='existing_task',
            task='existing.task',
            schedule=self.schedule_obj,
            app=self.app
        )
        self.scheduler.schedule = {'existing_task': existing_entry}
        
        # Merge new schedule
        new_schedule = {
            'new_task': {
                'task': 'new.task',
                'schedule': 120,
            }
        }
        
        self.scheduler.merge_inplace(new_schedule)
        
        self.assertIn('new_task', self.scheduler.schedule)
        self.assertNotIn('existing_task', self.scheduler.schedule)

    def test_get_schedule(self):
        """Test get_schedule method."""
        test_data = {'test': 'data'}
        self.scheduler.data = test_data
        
        result = self.scheduler.get_schedule()
        self.assertEqual(result, test_data)

    def test_set_schedule(self):
        """Test set_schedule method."""
        test_data = {'test': 'data'}
        
        self.scheduler.set_schedule(test_data)
        self.assertEqual(self.scheduler.data, test_data)

    def test_connection(self):
        """Test connection property."""
        with patch.object(self.app, 'connection_for_write') as mock_conn:
            mock_conn.return_value = Mock()
            
            connection = self.scheduler.connection
            
            mock_conn.assert_called_once()
            self.assertIsNotNone(connection)

    def test_producer(self):
        """Test producer property."""
        with patch.object(self.scheduler, '_ensure_connected') as mock_ensure:
            mock_ensure.return_value = Mock()
            
            producer = self.scheduler.producer
            
            self.assertIsNotNone(producer)

    def test_info(self):
        """Test info property."""
        info = self.scheduler.info
        self.assertEqual(info, '')


class TestPersistentScheduler(unittest.TestCase):
    """Test PersistentScheduler class."""

    def setUp(self):
        """Set up test fixtures."""
        self.app = Celery('test')
        self.temp_file = tempfile.NamedTemporaryFile(delete=False)
        self.temp_file.close()
        self.scheduler = PersistentScheduler(
            self.app,
            schedule_filename=self.temp_file.name,
            lazy=True
        )

    def tearDown(self):
        """Clean up test fixtures."""
        try:
            os.unlink(self.temp_file.name)
        except OSError:
            pass
        # Clean up any shelve files
        for suffix in self.scheduler.known_suffixes:
            try:
                os.unlink(self.temp_file.name + suffix)
            except OSError:
                pass

    def test_setup_schedule(self):
        """Test setup_schedule method."""
        with patch.object(self.scheduler, '_open_schedule') as mock_open:
            mock_store = {}
            mock_open.return_value = mock_store
            mock_store.keys = Mock(return_value=[])
            mock_store.get = Mock(return_value=None)
            mock_store.setdefault = Mock(return_value={})
            mock_store.update = Mock()
            mock_store.sync = Mock()
            
            self.scheduler.setup_schedule()
            
            mock_open.assert_called()

    def test_get_schedule(self):
        """Test get_schedule method."""
        mock_store = {'entries': {'test': 'data'}}
        self.scheduler._store = mock_store
        
        result = self.scheduler.get_schedule()
        self.assertEqual(result, {'test': 'data'})

    def test_set_schedule(self):
        """Test set_schedule method."""
        mock_store = {'entries': {}}
        self.scheduler._store = mock_store
        
        test_schedule = {'test': 'data'}
        self.scheduler.set_schedule(test_schedule)
        
        self.assertEqual(mock_store['entries'], test_schedule)

    def test_sync(self):
        """Test sync method."""
        mock_store = Mock()
        self.scheduler._store = mock_store
        
        self.scheduler.sync()
        
        mock_store.sync.assert_called_once()

    def test_close(self):
        """Test close method."""
        mock_store = Mock()
        self.scheduler._store = mock_store
        
        self.scheduler.close()
        
        mock_store.sync.assert_called_once()
        mock_store.close.assert_called_once()

    def test_info(self):
        """Test info property."""
        info = self.scheduler.info
        self.assertIn(self.temp_file.name, info)


class TestService(unittest.TestCase):
    """Test Service class."""

    def setUp(self):
        """Set up test fixtures."""
        self.app = Celery('test')
        self.service = Service(self.app)

    def test_start(self):
        """Test start method."""
        with patch.object(self.service, 'scheduler') as mock_scheduler:
            mock_scheduler.tick.return_value = 0.1
            mock_scheduler.should_sync.return_value = False
            
            # Mock the shutdown event to stop after one iteration
            self.service._is_shutdown.set()
            
            with patch('time.sleep') as mock_sleep:
                self.service.start()
                
                mock_scheduler.tick.assert_called()

    def test_sync(self):
        """Test sync method."""
        with patch.object(self.service, 'scheduler') as mock_scheduler:
            self.service.sync()
            
            mock_scheduler.close.assert_called_once()
            self.assertTrue(self.service._is_stopped.is_set())

    def test_stop(self):
        """Test stop method."""
        self.service.stop()
        
        self.assertTrue(self.service._is_shutdown.is_set())

    def test_get_scheduler(self):
        """Test get_scheduler method."""
        scheduler = self.service.get_scheduler(lazy=True)
        
        self.assertIsInstance(scheduler, PersistentScheduler)

    def test_scheduler_property(self):
        """Test scheduler property."""
        scheduler = self.service.scheduler
        
        self.assertIsInstance(scheduler, PersistentScheduler)


class TestEmbeddedService(unittest.TestCase):
    """Test EmbeddedService function."""

    def setUp(self):
        """Set up test fixtures."""
        self.app = Celery('test')

    def test_threaded_service(self):
        """Test EmbeddedService with thread=True."""
        service = EmbeddedService(self.app, thread=True)
        
        self.assertIsInstance(service, _Threaded)

    @unittest.skipIf(_Process is None, "multiprocessing not available")
    def test_process_service(self):
        """Test EmbeddedService with process."""
        service = EmbeddedService(self.app, thread=False)
        
        self.assertIsInstance(service, _Process)


class TestThreaded(unittest.TestCase):
    """Test _Threaded class."""

    def setUp(self):
        """Set up test fixtures."""
        self.app = Celery('test')
        self.threaded = _Threaded(self.app)

    def test_run(self):
        """Test run method."""
        with patch.object(self.app, 'set_current') as mock_set_current:
            with patch.object(self.threaded.service, 'start') as mock_start:
                self.threaded.run()
                
                mock_set_current.assert_called_once()
                mock_start.assert_called_once()

    def test_stop(self):
        """Test stop method."""
        with patch.object(self.threaded.service, 'stop') as mock_stop:
            self.threaded.stop()
            
            mock_stop.assert_called_once_with(wait=True)


@unittest.skipIf(_Process is None, "multiprocessing not available")
class TestProcess(unittest.TestCase):
    """Test _Process class."""

    def setUp(self):
        """Set up test fixtures."""
        self.app = Celery('test')
        self.process = _Process(self.app)

    def test_run(self):
        """Test run method."""
        with patch('celery.beat.reset_signals') as mock_reset:
            with patch('celery.beat.platforms.close_open_fds') as mock_close:
                with patch.object(self.app, 'set_default') as mock_set_default:
                    with patch.object(self.app, 'set_current') as mock_set_current:
                        with patch.object(self.process.service, 'start') as mock_start:
                            self.process.run()
                            
                            mock_reset.assert_called_once()
                            mock_close.assert_called_once()
                            mock_set_default.assert_called_once()
                            mock_set_current.assert_called_once()
                            mock_start.assert_called_once_with(embedded_process=True)

    def test_stop(self):
        """Test stop method."""
        with patch.object(self.process.service, 'stop') as mock_stop:
            with patch.object(self.process, 'terminate') as mock_terminate:
                self.process.stop()
                
                mock_stop.assert_called_once()
                mock_terminate.assert_called_once()


class TestEvaluateEntryFunctions(unittest.TestCase):
    """Test _evaluate_entry_args and _evaluate_entry_kwargs functions."""

    def test_evaluate_entry_args_with_lazy_func(self):
        """Test _evaluate_entry_args with BeatLazyFunc."""
        mock_func = Mock(return_value="lazy_result")
        lazy_func = BeatLazyFunc(mock_func)
        
        args = ["normal_arg", lazy_func, "another_arg"]
        result = _evaluate_entry_args(args)
        
        expected = ["normal_arg", "lazy_result", "another_arg"]
        self.assertEqual(result, expected)
        mock_func.assert_called_once()

    def test_evaluate_entry_args_empty(self):
        """Test _evaluate_entry_args with empty args."""
        result = _evaluate_entry_args(None)
        self.assertEqual(result, [])
        
        result = _evaluate_entry_args([])
        self.assertEqual(result, [])

    def test_evaluate_entry_kwargs_with_lazy_func(self):
        """Test _evaluate_entry_kwargs with BeatLazyFunc."""
        mock_func = Mock(return_value="lazy_result")
        lazy_func = BeatLazyFunc(mock_func)
        
        kwargs = {"normal_key": "normal_value", "lazy_key": lazy_func}
        result = _evaluate_entry_kwargs(kwargs)
        
        expected = {"normal_key": "normal_value", "lazy_key": "lazy_result"}
        self.assertEqual(result, expected)
        mock_func.assert_called_once()

    def test_evaluate_entry_kwargs_empty(self):
        """Test _evaluate_entry_kwargs with empty kwargs."""
        result = _evaluate_entry_kwargs(None)
        self.assertEqual(result, {})
        
        result = _evaluate_entry_kwargs({})
        self.assertEqual(result, {})


class TestSchedulingError(unittest.TestCase):
    """Test SchedulingError exception."""

    def test_scheduling_error(self):
        """Test SchedulingError can be raised and caught."""
        with self.assertRaises(SchedulingError):
            raise SchedulingError("Test error message")


if __name__ == '__main__':
    unittest.main()
"""The periodic task scheduler."""

import copy
import dbm
import errno
import heapq
import os
import shelve
import sys
import time
import traceback
from calendar import timegm
from collections import namedtuple
from functools import total_ordering
from pickle import UnpicklingError
from threading import Event, Thread

from billiard import ensure_multiprocessing
from billiard.common import reset_signals
from billiard.context import Process
from kombu.utils.functional import maybe_evaluate, reprcall
from kombu.utils.objects import cached_property

from . import __version__, platforms, signals
from .exceptions import reraise
from .schedules import crontab, maybe_schedule
from .utils.functional import is_numeric_value
from .utils.imports import load_extension_class_names, symbol_by_name
from .utils.log import get_logger, iter_open_logger_fds
from .utils.time import humanize_seconds, maybe_make_aware

__all__ = (
    'SchedulingError', 'ScheduleEntry', 'Scheduler',
    'PersistentScheduler', 'Service', 'EmbeddedService',
)

event_t = namedtuple('event_t', ('time', 'priority', 'entry'))

logger = get_logger(__name__)
debug, info, error, warning = (logger.debug, logger.info,
                               logger.error, logger.warning)

DEFAULT_MAX_INTERVAL = 300  # 5 minutes


class SchedulingError(Exception):
    """An error occurred while scheduling a task."""


class BeatLazyFunc:
    """A lazy function declared in 'beat_schedule' and called before sending to worker.

    Example:

        beat_schedule = {
            'test-every-5-minutes': {
                'task': 'test',
                'schedule': 300,
                'kwargs': {
                    "current": BeatCallBack(datetime.datetime.now)
                }
            }
        }

    """

    def __init__(self, func, *args, **kwargs):
        self._func = func
        self._func_params = {
            "args": args,
            "kwargs": kwargs
        }

    def __call__(self):
        return self.delay()

    def delay(self):
        return self._func(*self._func_params["args"], **self._func_params["kwargs"])


@total_ordering
class ScheduleEntry:
    """An entry in the scheduler.

    Arguments:
        name (str): see :attr:`name`.
        schedule (~celery.schedules.schedule): see :attr:`schedule`.
        args (Tuple): see :attr:`args`.
        kwargs (Dict): see :attr:`kwargs`.
        options (Dict): see :attr:`options`.
        last_run_at (~datetime.datetime): see :attr:`last_run_at`.
        total_run_count (int): see :attr:`total_run_count`.
        relative (bool): Is the time relative to when the server starts?
    """

    #: The task name
    name = None

    #: The schedule (:class:`~celery.schedules.schedule`)
    schedule = None

    #: Positional arguments to apply.
    args = None

    #: Keyword arguments to apply.
    kwargs = None

    #: Task execution options.
    options = None

    #: The time and date of when this task was last scheduled.
    last_run_at = None

    #: Total number of times this task has been scheduled.
    total_run_count = 0

    def __init__(self, name=None, task=None, last_run_at=None,
                 total_run_count=None, schedule=None, args=(), kwargs=None,
                 options=None, relative=False, app=None):
        self.app = app
        self.name = name
        self.task = task
        self.args = args
        self.kwargs = kwargs if kwargs else {}
        self.options = options if options else {}
        self.schedule = maybe_schedule(schedule, relative, app=self.app)
        self.last_run_at = last_run_at or self.default_now()
        self.total_run_count = total_run_count or 0

    def default_now(self):
        return self.schedule.now() if self.schedule else self.app.now()
    _default_now = default_now  # compat

    def _next_instance(self, last_run_at=None):
        """Return new instance, with date and count fields updated."""
        return self.__class__(**dict(
            self,
            last_run_at=last_run_at or self.default_now(),
            total_run_count=self.total_run_count + 1,
        ))
    __next__ = next = _next_instance  # for 2to3

    def __reduce__(self):
        return self.__class__, (
            self.name, self.task, self.last_run_at, self.total_run_count,
            self.schedule, self.args, self.kwargs, self.options,
        )

    def update(self, other):
        """Update values from another entry.

        Will only update "editable" fields:
            ``task``, ``schedule``, ``args``, ``kwargs``, ``options``.
        """
        self.__dict__.update({
            'task': other.task, 'schedule': other.schedule,
            'args': other.args, 'kwargs': other.kwargs,
            'options': other.options,
        })

    def is_due(self):
        """See :meth:`~celery.schedules.schedule.is_due`."""
        return self.schedule.is_due(self.last_run_at)

    def __iter__(self):
        return iter(vars(self).items())

    def __repr__(self):
        return '<{name}: {0.name} {call} {0.schedule}'.format(
            self,
            call=reprcall(self.task, self.args or (), self.kwargs or {}),
            name=type(self).__name__,
        )

    def __lt__(self, other):
        if isinstance(other, ScheduleEntry):
            # How the object is ordered doesn't really matter, as
            # in the scheduler heap, the order is decided by the
            # preceding members of the tuple ``(time, priority, entry)``.
            #
            # If all that's left to order on is the entry then it can
            # just as well be random.
            return id(self) < id(other)
        return NotImplemented

    def editable_fields_equal(self, other):
        for attr in ('task', 'args', 'kwargs', 'options', 'schedule'):
            if getattr(self, attr) != getattr(other, attr):
                return False
        return True

    def __eq__(self, other):
        """Test schedule entries equality.

        Will only compare "editable" fields:
        ``task``, ``schedule``, ``args``, ``kwargs``, ``options``.
        """
        return self.editable_fields_equal(other)


def _evaluate_entry_args(entry_args):
    if not entry_args:
        return []
    return [
        v() if isinstance(v, BeatLazyFunc) else v
        for v in entry_args
    ]


def _evaluate_entry_kwargs(entry_kwargs):
    if not entry_kwargs:
        return {}
    return {
        k: v() if isinstance(v, BeatLazyFunc) else v
        for k, v in entry_kwargs.items()
    }


class Scheduler:
    """Scheduler for periodic tasks.

    The :program:`celery beat` program may instantiate this class
    multiple times for introspection purposes, but then with the
    ``lazy`` argument set.  It's important for subclasses to
    be idempotent when this argument is set.

    Arguments:
        schedule (~celery.schedules.schedule): see :attr:`schedule`.
        max_interval (int): see :attr:`max_interval`.
        lazy (bool): Don't set up the schedule.
    """

    Entry = ScheduleEntry

    #: The schedule dict/shelve.
    schedule = None

    #: Maximum time to sleep between re-checking the schedule.
    max_interval = DEFAULT_MAX_INTERVAL

    #: How often to sync the schedule (3 minutes by default)
    sync_every = 3 * 60

    #: How many tasks can be called before a sync is forced.
    sync_every_tasks = None

    _last_sync = None
    _tasks_since_sync = 0

    logger = logger  # compat

    def __init__(self, app, schedule=None, max_interval=None,
                 Producer=None, lazy=False, sync_every_tasks=None, **kwargs):
        self.app = app
        self.data = maybe_evaluate({} if schedule is None else schedule)
        self.max_interval = (max_interval or
                             app.conf.beat_max_loop_interval or
                             self.max_interval)
        self.Producer = Producer or app.amqp.Producer
        self._heap = None
        self.old_schedulers = None
        self.sync_every_tasks = (
            app.conf.beat_sync_every if sync_every_tasks is None
            else sync_every_tasks)
        if not lazy:
            self.setup_schedule()

    def install_default_entries(self, data):
        entries = {}
        if self.app.conf.result_expires and \
                not self.app.backend.supports_autoexpire:
            if 'celery.backend_cleanup' not in data:
                entries['celery.backend_cleanup'] = {
                    'task': 'celery.backend_cleanup',
                    'schedule': crontab('0', '4', '*'),
                    'options': {'expires': 12 * 3600}}
        self.update_from_dict(entries)

    def apply_entry(self, entry, producer=None):
        info('Scheduler: Sending due task %s (%s)', entry.name, entry.task)
        try:
            result = self.apply_async(entry, producer=producer, advance=False)
        except Exception as exc:  # pylint: disable=broad-except
            error('Message Error: %s\n%s',
                  exc, traceback.format_stack(), exc_info=True)
        else:
            if result and hasattr(result, 'id'):
                debug('%s sent. id->%s', entry.task, result.id)
            else:
                debug('%s sent.', entry.task)

    def adjust(self, n, drift=-0.010):
        if n and n > 0:
            return n + drift
        return n

    def is_due(self, entry):
        return entry.is_due()

    def _when(self, entry, next_time_to_run, mktime=timegm):
        """Return a utc timestamp, make sure heapq in correct order."""
        adjust = self.adjust

        as_now = maybe_make_aware(entry.default_now())

        return (mktime(as_now.utctimetuple()) +
                as_now.microsecond / 1e6 +
                (adjust(next_time_to_run) or 0))

    def populate_heap(self, event_t=event_t, heapify=heapq.heapify):
        """Populate the heap with the data contained in the schedule."""
        priority = 5
        self._heap = []
        for entry in self.schedule.values():
            is_due, next_call_delay = entry.is_due()
            self._heap.append(event_t(
                self._when(
                    entry,
                    0 if is_due else next_call_delay
                ) or 0,
                priority, entry
            ))
        heapify(self._heap)

    # pylint disable=redefined-outer-name
    def tick(self, event_t=event_t, min=min, heappop=heapq.heappop,
             heappush=heapq.heappush):
        """Run a tick - one iteration of the scheduler.

        Executes one due task per call.

        Returns:
            float: preferred delay in seconds for next call.
        """
        adjust = self.adjust
        max_interval = self.max_interval

        if (self._heap is None or
                not self.schedules_equal(self.old_schedulers, self.schedule)):
            self.old_schedulers = copy.copy(self.schedule)
            self.populate_heap()

        H = self._heap

        if not H:
            return max_interval

        event = H[0]
        entry = event[2]
        is_due, next_time_to_run = self.is_due(entry)
        if is_due:
            verify = heappop(H)
            if verify is event:
                next_entry = self.reserve(entry)
                self.apply_entry(entry, producer=self.producer)
                heappush(H, event_t(self._when(next_entry, next_time_to_run),
                                    event[1], next_entry))
                return 0
            else:
                heappush(H, verify)
                return min(verify[0], max_interval)
        adjusted_next_time_to_run = adjust(next_time_to_run)
        return min(adjusted_next_time_to_run if is_numeric_value(adjusted_next_time_to_run) else max_interval,
                   max_interval)

    def schedules_equal(self, old_schedules, new_schedules):
        if old_schedules is new_schedules is None:
            return True
        if old_schedules is None or new_schedules is None:
            return False
        if set(old_schedules.keys()) != set(new_schedules.keys()):
            return False
        for name, old_entry in old_schedules.items():
            new_entry = new_schedules.get(name)
            if not new_entry:
                return False
            if new_entry != old_entry:
                return False
        return True

    def should_sync(self):
        return (
            (not self._last_sync or
             (time.monotonic() - self._last_sync) > self.sync_every) or
            (self.sync_every_tasks and
             self._tasks_since_sync >= self.sync_every_tasks)
        )

    def reserve(self, entry):
        new_entry = self.schedule[entry.name] = next(entry)
        return new_entry

    def apply_async(self, entry, producer=None, advance=True, **kwargs):
        # Update time-stamps and run counts before we actually execute,
        # so we have that done if an exception is raised (doesn't schedule
        # forever.)
        entry = self.reserve(entry) if advance else entry
        task = self.app.tasks.get(entry.task)

        try:
            entry_args = _evaluate_entry_args(entry.args)
            entry_kwargs = _evaluate_entry_kwargs(entry.kwargs)
            if task:
                return task.apply_async(entry_args, entry_kwargs,
                                        producer=producer,
                                        **entry.options)
            else:
                return self.send_task(entry.task, entry_args, entry_kwargs,
                                      producer=producer,
                                      **entry.options)
        except Exception as exc:  # pylint: disable=broad-except
            reraise(SchedulingError, SchedulingError(
                "Couldn't apply scheduled task {0.name}: {exc}".format(
                    entry, exc=exc)), sys.exc_info()[2])
        finally:
            self._tasks_since_sync += 1
            if self.should_sync():
                self._do_sync()

    def send_task(self, *args, **kwargs):
        return self.app.send_task(*args, **kwargs)

    def setup_schedule(self):
        self.install_default_entries(self.data)
        self.merge_inplace(self.app.conf.beat_schedule)

    def _do_sync(self):
        try:
            debug('beat: Synchronizing schedule...')
            self.sync()
        finally:
            self._last_sync = time.monotonic()
            self._tasks_since_sync = 0

    def sync(self):
        pass

    def close(self):
        self.sync()

    def add(self, **kwargs):
        entry = self.Entry(app=self.app, **kwargs)
        self.schedule[entry.name] = entry
        return entry

    def _maybe_entry(self, name, entry):
        if isinstance(entry, self.Entry):
            entry.app = self.app
            return entry
        return self.Entry(**dict(entry, name=name, app=self.app))

    def update_from_dict(self, dict_):
        self.schedule.update({
            name: self._maybe_entry(name, entry)
            for name, entry in dict_.items()
        })

    def merge_inplace(self, b):
        schedule = self.schedule
        A, B = set(schedule), set(b)

        # Remove items from disk not in the schedule anymore.
        for key in A ^ B:
            schedule.pop(key, None)

        # Update and add new items in the schedule
        for key in B:
            entry = self.Entry(**dict(b[key], name=key, app=self.app))
            if schedule.get(key):
                schedule[key].update(entry)
            else:
                schedule[key] = entry

    def _ensure_connected(self):
        # callback called for each retry while the connection
        # can't be established.
        def _error_handler(exc, interval):
            error('beat: Connection error: %s. '
                  'Trying again in %s seconds...', exc, interval)

        return self.connection.ensure_connection(
            _error_handler, self.app.conf.broker_connection_max_retries
        )

    def get_schedule(self):
        return self.data

    def set_schedule(self, schedule):
        self.data = schedule
    schedule = property(get_schedule, set_schedule)

    @cached_property
    def connection(self):
        return self.app.connection_for_write()

    @cached_property
    def producer(self):
        return self.Producer(self._ensure_connected(), auto_declare=False)

    @property
    def info(self):
        return ''


class PersistentScheduler(Scheduler):
    """Scheduler backed by :mod:`shelve` database."""

    persistence = shelve
    known_suffixes = ('', '.db', '.dat', '.bak', '.dir')

    _store = None

    def __init__(self, *args, **kwargs):
        self.schedule_filename = kwargs.get('schedule_filename')
        super().__init__(*args, **kwargs)

    def _remove_db(self):
        for suffix in self.known_suffixes:
            with platforms.ignore_errno(errno.ENOENT):
                os.remove(self.schedule_filename + suffix)

    def _open_schedule(self):
        return self.persistence.open(self.schedule_filename, writeback=True)

    def _destroy_open_corrupted_schedule(self, exc):
        error('Removing corrupted schedule file %r: %r',
              self.schedule_filename, exc, exc_info=True)
        self._remove_db()
        return self._open_schedule()

    def setup_schedule(self):
        try:
            self._store = self._open_schedule()
            # In some cases there may be different errors from a storage
            # backend for corrupted files.  Example - DBPageNotFoundError
            # exception from bsddb.  In such case the file will be
            # successfully opened but the error will be raised on first key
            # retrieving.
            self._store.keys()
        except Exception as exc:  # pylint: disable=broad-except
            self._store = self._destroy_open_corrupted_schedule(exc)

        self._create_schedule()

        tz = self.app.conf.timezone
        stored_tz = self._store.get('tz')
        if stored_tz is not None and stored_tz != tz:
            warning('Reset: Timezone changed from %r to %r', stored_tz, tz)
            self._store.clear()   # Timezone changed, reset db!
        utc = self.app.conf.enable_utc
        stored_utc = self._store.get('utc_enabled')
        if stored_utc is not None and stored_utc != utc:
            choices = {True: 'enabled', False: 'disabled'}
            warning('Reset: UTC changed from %s to %s',
                    choices[stored_utc], choices[utc])
            self._store.clear()   # UTC setting changed, reset db!
        entries = self._store.setdefault('entries', {})
        self.merge_inplace(self.app.conf.beat_schedule)
        self.install_default_entries(self.schedule)
        self._store.update({
            '__version__': __version__,
            'tz': tz,
            'utc_enabled': utc,
        })
        self.sync()
        debug('Current schedule:\n' + '\n'.join(
            repr(entry) for entry in entries.values()))

    def _create_schedule(self):
        for _ in (1, 2):
            try:
                self._store['entries']
            except (KeyError, UnicodeDecodeError, TypeError, UnpicklingError):
                # new schedule db
                try:
                    self._store['entries'] = {}
                except (KeyError, UnicodeDecodeError, TypeError, UnpicklingError) + dbm.error as exc:
                    self._store = self._destroy_open_corrupted_schedule(exc)
                    continue
            else:
                if '__version__' not in self._store:
                    warning('DB Reset: Account for new __version__ field')
                    self._store.clear()   # remove schedule at 2.2.2 upgrade.
                elif 'tz' not in self._store:
                    warning('DB Reset: Account for new tz field')
                    self._store.clear()   # remove schedule at 3.0.8 upgrade
                elif 'utc_enabled' not in self._store:
                    warning('DB Reset: Account for new utc_enabled field')
                    self._store.clear()   # remove schedule at 3.0.9 upgrade
            break

    def get_schedule(self):
        return self._store['entries']

    def set_schedule(self, schedule):
        self._store['entries'] = schedule
    schedule = property(get_schedule, set_schedule)

    def sync(self):
        if self._store is not None:
            self._store.sync()

    def close(self):
        self.sync()
        self._store.close()

    @property
    def info(self):
        return f'    . db -> {self.schedule_filename}'


class Service:
    """Celery periodic task service."""

    scheduler_cls = PersistentScheduler

    def __init__(self, app, max_interval=None, schedule_filename=None,
                 scheduler_cls=None):
        self.app = app
        self.max_interval = (max_interval or
                             app.conf.beat_max_loop_interval)
        self.scheduler_cls = scheduler_cls or self.scheduler_cls
        self.schedule_filename = (
            schedule_filename or app.conf.beat_schedule_filename)

        self._is_shutdown = Event()
        self._is_stopped = Event()

    def __reduce__(self):
        return self.__class__, (self.max_interval, self.schedule_filename,
                                self.scheduler_cls, self.app)

    def start(self, embedded_process=False):
        info('beat: Starting...')
        debug('beat: Ticking with max interval->%s',
              humanize_seconds(self.scheduler.max_interval))

        signals.beat_init.send(sender=self)
        if embedded_process:
            signals.beat_embedded_init.send(sender=self)
            platforms.set_process_title('celery beat')

        try:
            while not self._is_shutdown.is_set():
                interval = self.scheduler.tick()
                if interval and interval > 0.0:
                    debug('beat: Waking up %s.',
                          humanize_seconds(interval, prefix='in '))
                    time.sleep(interval)
                    if self.scheduler.should_sync():
                        self.scheduler._do_sync()
        except (KeyboardInterrupt, SystemExit):
            self._is_shutdown.set()
        finally:
            self.sync()

    def sync(self):
        self.scheduler.close()
        self._is_stopped.set()

    def stop(self, wait=False):
        info('beat: Shutting down...')
        self._is_shutdown.set()
        wait and self._is_stopped.wait()  # block until shutdown done.

    def get_scheduler(self, lazy=False,
                      extension_namespace='celery.beat_schedulers'):
        filename = self.schedule_filename
        aliases = dict(load_extension_class_names(extension_namespace))
        return symbol_by_name(self.scheduler_cls, aliases=aliases)(
            app=self.app,
            schedule_filename=filename,
            max_interval=self.max_interval,
            lazy=lazy,
        )

    @cached_property
    def scheduler(self):
        return self.get_scheduler()


class _Threaded(Thread):
    """Embedded task scheduler using threading."""

    def __init__(self, app, **kwargs):
        super().__init__()
        self.app = app
        self.service = Service(app, **kwargs)
        self.daemon = True
        self.name = 'Beat'

    def run(self):
        self.app.set_current()
        self.service.start()

    def stop(self):
        self.service.stop(wait=True)


try:
    ensure_multiprocessing()
except NotImplementedError:     # pragma: no cover
    _Process = None
else:
    class _Process(Process):

        def __init__(self, app, **kwargs):
            super().__init__()
            self.app = app
            self.service = Service(app, **kwargs)
            self.name = 'Beat'

        def run(self):
            reset_signals(full=False)
            platforms.close_open_fds([
                sys.__stdin__, sys.__stdout__, sys.__stderr__,
            ] + list(iter_open_logger_fds()))
            self.app.set_default()
            self.app.set_current()
            self.service.start(embedded_process=True)

        def stop(self):
            self.service.stop()
            self.terminate()


def EmbeddedService(app, max_interval=None, **kwargs):
    """Return embedded clock service.

    Arguments:
        thread (bool): Run threaded instead of as a separate process.
            Uses :mod:`multiprocessing` by default, if available.
    """
    if kwargs.pop('thread', False) or _Process is None:
        # Need short max interval to be able to stop thread
        # in reasonable time.
        return _Threaded(app, max_interval=1, **kwargs)
    return _Process(app, max_interval=max_interval, **kwargs)

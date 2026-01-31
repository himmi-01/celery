"""Comprehensive unit tests for celery.bootsteps module."""

import pytest
from unittest.mock import Mock, MagicMock, patch, call
from threading import Event
from collections import deque

from celery.bootsteps import (
    Blueprint, Step, StartStopStep, ConsumerStep, StepFormatter,
    RUN, CLOSE, TERMINATE, _label, _pre
)
from celery.utils.graph import DependencyGraph


class TestStepFormatter:
    """Test cases for StepFormatter class."""

    def setup_method(self):
        self.formatter = StepFormatter()

    def test_label_with_step_label(self):
        """Test label method when step has a label."""
        step = Mock()
        step.label = 'test_label'
        step.last = False
        step.conditional = False
        
        result = self.formatter.label(step)
        assert result == 'test_label'

    def test_label_without_step_label(self):
        """Test label method when step has no label."""
        step = Mock()
        step.label = None
        step.name = 'module.TestStep'
        step.last = False
        step.conditional = False
        
        result = self.formatter.label(step)
        assert result == 'TestStep'

    def test_label_with_last_step(self):
        """Test label method with last step prefix."""
        step = Mock()
        step.label = 'test_label'
        step.last = True
        step.conditional = False
        
        result = self.formatter.label(step)
        assert result == f'{self.formatter.blueprint_prefix}test_label'

    def test_label_with_conditional_step(self):
        """Test label method with conditional step prefix."""
        step = Mock()
        step.label = 'test_label'
        step.last = False
        step.conditional = True
        
        result = self.formatter.label(step)
        assert result == f'{self.formatter.conditional_prefix}test_label'

    def test_node_with_last_step(self):
        """Test node method with last step."""
        step = Mock()
        step.last = True
        
        with patch.object(self.formatter, 'draw_node') as mock_draw:
            self.formatter.node(step)
            mock_draw.assert_called_once_with(
                step, self.formatter.blueprint_scheme, {}
            )

    def test_node_with_regular_step(self):
        """Test node method with regular step."""
        step = Mock()
        step.last = False
        
        with patch.object(self.formatter, 'draw_node') as mock_draw:
            self.formatter.node(step)
            mock_draw.assert_called_once_with(
                step, self.formatter.node_scheme, {}
            )

    def test_edge_with_last_step(self):
        """Test edge method with last step."""
        step_a = Mock()
        step_a.last = True
        step_b = Mock()
        
        with patch.object(self.formatter, 'draw_edge') as mock_draw:
            self.formatter.edge(step_a, step_b)
            expected_attrs = {'arrowhead': 'none', 'color': 'darkseagreen3'}
            mock_draw.assert_called_once_with(
                step_a, step_b, self.formatter.edge_scheme, expected_attrs
            )

    def test_edge_with_regular_step(self):
        """Test edge method with regular step."""
        step_a = Mock()
        step_a.last = False
        step_b = Mock()
        
        with patch.object(self.formatter, 'draw_edge') as mock_draw:
            self.formatter.edge(step_a, step_b)
            mock_draw.assert_called_once_with(
                step_a, step_b, self.formatter.edge_scheme, {}
            )


class TestBlueprint:
    """Test cases for Blueprint class."""

    def setup_method(self):
        self.blueprint = Blueprint()

    def test_init_default_values(self):
        """Test Blueprint initialization with default values."""
        bp = Blueprint()
        assert bp.name is not None
        assert bp.state is None
        assert bp.started == 0
        assert isinstance(bp.types, set)
        assert isinstance(bp.shutdown_complete, Event)
        assert bp.steps == {}

    def test_init_with_parameters(self):
        """Test Blueprint initialization with parameters."""
        on_start = Mock()
        on_close = Mock()
        on_stopped = Mock()
        steps = ['step1', 'step2']
        
        bp = Blueprint(
            steps=steps,
            name='test_blueprint',
            on_start=on_start,
            on_close=on_close,
            on_stopped=on_stopped
        )
        
        assert bp.name == 'test_blueprint'
        assert bp.on_start == on_start
        assert bp.on_close == on_close
        assert bp.on_stopped == on_stopped
        assert 'step1' in bp.types
        assert 'step2' in bp.types

    def test_start(self):
        """Test Blueprint start method."""
        parent = Mock()
        step1 = Mock()
        step1.alias = 'step1'
        step2 = Mock()
        step2.alias = 'step2'
        parent.steps = [step1, step2]
        
        on_start = Mock()
        self.blueprint.on_start = on_start
        
        self.blueprint.start(parent)
        
        assert self.blueprint.state == RUN
        assert self.blueprint.started == 2
        on_start.assert_called_once()
        step1.start.assert_called_once_with(parent)
        step2.start.assert_called_once_with(parent)

    def test_start_without_callback(self):
        """Test Blueprint start method without callback."""
        parent = Mock()
        parent.steps = []
        
        self.blueprint.start(parent)
        
        assert self.blueprint.state == RUN
        assert self.blueprint.started == 0

    def test_human_state(self):
        """Test human_state method."""
        assert self.blueprint.human_state() == 'initializing'
        
        self.blueprint.state = RUN
        assert self.blueprint.human_state() == 'running'
        
        self.blueprint.state = CLOSE
        assert self.blueprint.human_state() == 'closing'
        
        self.blueprint.state = TERMINATE
        assert self.blueprint.human_state() == 'terminating'

    def test_info(self):
        """Test info method."""
        parent = Mock()
        step1 = Mock()
        step1.info.return_value = {'step1_info': 'value1'}
        step2 = Mock()
        step2.info.return_value = {'step2_info': 'value2'}
        parent.steps = [step1, step2]
        
        result = self.blueprint.info(parent)
        
        expected = {'step1_info': 'value1', 'step2_info': 'value2'}
        assert result == expected
        step1.info.assert_called_once_with(parent)
        step2.info.assert_called_once_with(parent)

    def test_close(self):
        """Test close method."""
        parent = Mock()
        on_close = Mock()
        self.blueprint.on_close = on_close
        
        with patch.object(self.blueprint, 'send_all') as mock_send:
            self.blueprint.close(parent)
            
            on_close.assert_called_once()
            mock_send.assert_called_once_with(
                parent, 'close', 'closing', reverse=False
            )

    def test_restart(self):
        """Test restart method."""
        parent = Mock()
        
        with patch.object(self.blueprint, 'send_all') as mock_send:
            self.blueprint.restart(parent, method='restart', description='restarting')
            
            mock_send.assert_called_once_with(
                parent, 'restart', 'restarting', propagate=False
            )

    def test_send_all_forward(self):
        """Test send_all method in forward direction."""
        parent = Mock()
        step1 = Mock()
        step1.alias = 'step1'
        step1.test_method = Mock()
        step2 = Mock()
        step2.alias = 'step2'
        step2.test_method = Mock()
        parent.steps = [step1, step2]
        
        self.blueprint.send_all(parent, 'test_method', reverse=False)
        
        step1.test_method.assert_called_once_with(parent)
        step2.test_method.assert_called_once_with(parent)

    def test_send_all_reverse(self):
        """Test send_all method in reverse direction."""
        parent = Mock()
        step1 = Mock()
        step1.alias = 'step1'
        step1.test_method = Mock()
        step2 = Mock()
        step2.alias = 'step2'
        step2.test_method = Mock()
        parent.steps = [step1, step2]
        
        self.blueprint.send_all(parent, 'test_method', reverse=True)
        
        # Should be called in reverse order
        step1.test_method.assert_called_once_with(parent)
        step2.test_method.assert_called_once_with(parent)

    def test_send_all_with_exception_propagate(self):
        """Test send_all method with exception and propagate=True."""
        parent = Mock()
        step1 = Mock()
        step1.alias = 'step1'
        step1.test_method = Mock(side_effect=Exception('test error'))
        parent.steps = [step1]
        
        with pytest.raises(Exception):
            self.blueprint.send_all(parent, 'test_method', propagate=True)

    def test_send_all_with_exception_no_propagate(self):
        """Test send_all method with exception and propagate=False."""
        parent = Mock()
        step1 = Mock()
        step1.alias = 'step1'
        step1.test_method = Mock(side_effect=Exception('test error'))
        parent.steps = [step1]
        
        # Should not raise exception
        self.blueprint.send_all(parent, 'test_method', propagate=False)

    def test_stop_not_started(self):
        """Test stop method when blueprint is not fully started."""
        parent = Mock()
        parent.steps = [Mock(), Mock()]
        self.blueprint.state = RUN
        self.blueprint.started = 1  # Less than len(parent.steps)
        
        self.blueprint.stop(parent)
        
        assert self.blueprint.state == TERMINATE
        assert self.blueprint.shutdown_complete.is_set()

    def test_stop_fully_started(self):
        """Test stop method when blueprint is fully started."""
        parent = Mock()
        parent.steps = [Mock(), Mock()]
        self.blueprint.state = RUN
        self.blueprint.started = 2
        on_stopped = Mock()
        self.blueprint.on_stopped = on_stopped
        
        with patch.object(self.blueprint, 'close') as mock_close:
            with patch.object(self.blueprint, 'restart') as mock_restart:
                self.blueprint.stop(parent)
                
                mock_close.assert_called_once_with(parent)
                mock_restart.assert_called_once_with(
                    parent, 'stop', description='stopping', propagate=False
                )
                on_stopped.assert_called_once()
                assert self.blueprint.state == TERMINATE
                assert self.blueprint.shutdown_complete.is_set()

    def test_stop_with_terminate(self):
        """Test stop method with terminate=True."""
        parent = Mock()
        parent.steps = [Mock(), Mock()]
        self.blueprint.state = RUN
        self.blueprint.started = 2
        
        with patch.object(self.blueprint, 'close') as mock_close:
            with patch.object(self.blueprint, 'restart') as mock_restart:
                self.blueprint.stop(parent, terminate=True)
                
                mock_restart.assert_called_once_with(
                    parent, 'terminate', description='terminating', propagate=False
                )

    def test_stop_already_closing(self):
        """Test stop method when already in CLOSE state."""
        parent = Mock()
        self.blueprint.state = CLOSE
        
        with patch.object(self.blueprint, 'close') as mock_close:
            self.blueprint.stop(parent)
            mock_close.assert_not_called()

    def test_join(self):
        """Test join method."""
        self.blueprint.shutdown_complete.set()
        
        # Should not block since event is set
        self.blueprint.join(timeout=0.1)

    def test_apply(self):
        """Test apply method."""
        parent = Mock()
        parent.steps = []
        
        with patch.object(self.blueprint, 'claim_steps') as mock_claim:
            with patch.object(self.blueprint, '_finalize_steps') as mock_finalize:
                mock_claim.return_value = {}
                
                # Create mock step class
                MockStepClass = Mock()
                mock_step_instance = Mock()
                mock_step_instance.name = 'test_step'
                mock_step_instance.alias = 'test_step'
                MockStepClass.return_value = mock_step_instance
                mock_finalize.return_value = [MockStepClass]
                
                result = self.blueprint.apply(parent)
                
                assert result == self.blueprint
                assert 'test_step' in self.blueprint.steps
                mock_step_instance.include.assert_called_once_with(parent)

    def test_connect_with(self):
        """Test connect_with method."""
        other = Mock()
        other.graph.adjacent = {'other': 'data'}
        other.order = [Mock()]
        
        self.blueprint.graph = Mock()
        self.blueprint.graph.adjacent = {}
        self.blueprint.order = [Mock()]
        
        self.blueprint.connect_with(other)
        
        assert self.blueprint.graph.adjacent == {'other': 'data'}
        self.blueprint.graph.add_edge.assert_called_once()

    def test_getitem(self):
        """Test __getitem__ method."""
        step = Mock()
        self.blueprint.steps = {'test_step': step}
        
        assert self.blueprint['test_step'] == step

    def test_claim_steps(self):
        """Test claim_steps method."""
        self.blueprint.types = {'step1', 'step2'}
        
        with patch.object(self.blueprint, 'load_step') as mock_load:
            mock_load.side_effect = [('step1', 'Step1Class'), ('step2', 'Step2Class')]
            
            result = self.blueprint.claim_steps()
            
            expected = {'step1': 'Step1Class', 'step2': 'Step2Class'}
            assert result == expected

    def test_load_step(self):
        """Test load_step method."""
        with patch('celery.bootsteps.symbol_by_name') as mock_symbol:
            mock_step = Mock()
            mock_step.name = 'test_step'
            mock_symbol.return_value = mock_step
            
            name, step = self.blueprint.load_step('module.TestStep')
            
            assert name == 'test_step'
            assert step == mock_step
            mock_symbol.assert_called_once_with('module.TestStep')

    def test_alias_property(self):
        """Test alias property."""
        self.blueprint.name = 'module.TestBlueprint'
        assert self.blueprint.alias == 'TestBlueprint'


class TestStep:
    """Test cases for Step class."""

    def setup_method(self):
        self.step = Step(Mock())

    def test_init(self):
        """Test Step initialization."""
        parent = Mock()
        step = Step(parent, test_arg='test_value')
        # Should not raise any exceptions

    def test_include_if_enabled(self):
        """Test include_if method when enabled."""
        parent = Mock()
        self.step.enabled = True
        
        assert self.step.include_if(parent) is True

    def test_include_if_disabled(self):
        """Test include_if method when disabled."""
        parent = Mock()
        self.step.enabled = False
        
        assert self.step.include_if(parent) is False

    def test_instantiate(self):
        """Test instantiate method."""
        with patch('celery.bootsteps.instantiate') as mock_instantiate:
            mock_instantiate.return_value = 'instantiated_object'
            
            result = self.step.instantiate('test.Class', 'arg1', kwarg1='value1')
            
            assert result == 'instantiated_object'
            mock_instantiate.assert_called_once_with('test.Class', 'arg1', kwarg1='value1')

    def test_should_include_enabled(self):
        """Test _should_include method when step should be included."""
        parent = Mock()
        created_obj = Mock()
        
        with patch.object(self.step, 'include_if', return_value=True):
            with patch.object(self.step, 'create', return_value=created_obj):
                should_include, obj = self.step._should_include(parent)
                
                assert should_include is True
                assert obj == created_obj

    def test_should_include_disabled(self):
        """Test _should_include method when step should not be included."""
        parent = Mock()
        
        with patch.object(self.step, 'include_if', return_value=False):
            should_include, obj = self.step._should_include(parent)
            
            assert should_include is False
            assert obj is None

    def test_include(self):
        """Test include method."""
        parent = Mock()
        
        with patch.object(self.step, '_should_include', return_value=(True, Mock())):
            result = self.step.include(parent)
            assert result is True

    def test_create(self):
        """Test create method."""
        parent = Mock()
        # Should not raise any exceptions
        result = self.step.create(parent)
        assert result is None

    def test_repr(self):
        """Test __repr__ method."""
        self.step.label = 'test_label'
        result = repr(self.step)
        assert 'test_label' in result

    def test_alias_with_label(self):
        """Test alias property with label."""
        self.step.label = 'test_label'
        assert self.step.alias == 'test_label'

    def test_alias_without_label(self):
        """Test alias property without label."""
        self.step.label = None
        self.step.name = 'module.TestStep'
        assert self.step.alias == 'TestStep'

    def test_info(self):
        """Test info method."""
        obj = Mock()
        result = self.step.info(obj)
        assert result is None


class TestStartStopStep:
    """Test cases for StartStopStep class."""

    def setup_method(self):
        self.step = StartStopStep(Mock())

    def test_start_with_obj(self):
        """Test start method with obj."""
        parent = Mock()
        self.step.obj = Mock()
        
        self.step.start(parent)
        
        self.step.obj.start.assert_called_once()

    def test_start_without_obj(self):
        """Test start method without obj."""
        parent = Mock()
        self.step.obj = None
        
        result = self.step.start(parent)
        assert result is None

    def test_stop_with_obj(self):
        """Test stop method with obj."""
        parent = Mock()
        self.step.obj = Mock()
        
        self.step.stop(parent)
        
        self.step.obj.stop.assert_called_once()

    def test_stop_without_obj(self):
        """Test stop method without obj."""
        parent = Mock()
        self.step.obj = None
        
        result = self.step.stop(parent)
        assert result is None

    def test_close(self):
        """Test close method."""
        parent = Mock()
        # Should not raise any exceptions
        self.step.close(parent)

    def test_terminate_with_obj_having_terminate(self):
        """Test terminate method with obj that has terminate method."""
        parent = Mock()
        self.step.obj = Mock()
        self.step.obj.terminate = Mock()
        
        self.step.terminate(parent)
        
        self.step.obj.terminate.assert_called_once()

    def test_terminate_with_obj_without_terminate(self):
        """Test terminate method with obj that doesn't have terminate method."""
        parent = Mock()
        self.step.obj = Mock()
        del self.step.obj.terminate  # Remove terminate method
        
        self.step.terminate(parent)
        
        self.step.obj.stop.assert_called_once()

    def test_terminate_without_obj(self):
        """Test terminate method without obj."""
        parent = Mock()
        self.step.obj = None
        
        result = self.step.terminate(parent)
        assert result is None

    def test_include_should_include(self):
        """Test include method when step should be included."""
        parent = Mock()
        parent.steps = []
        created_obj = Mock()
        
        with patch.object(self.step, '_should_include', return_value=(True, created_obj)):
            result = self.step.include(parent)
            
            assert result is True
            assert self.step.obj == created_obj
            assert self.step in parent.steps

    def test_include_should_not_include(self):
        """Test include method when step should not be included."""
        parent = Mock()
        parent.steps = []
        
        with patch.object(self.step, '_should_include', return_value=(False, None)):
            result = self.step.include(parent)
            
            assert result is False
            assert self.step not in parent.steps


class TestConsumerStep:
    """Test cases for ConsumerStep class."""

    def setup_method(self):
        self.step = ConsumerStep(Mock())

    def test_get_consumers_not_implemented(self):
        """Test get_consumers method raises NotImplementedError."""
        channel = Mock()
        
        with pytest.raises(NotImplementedError):
            self.step.get_consumers(channel)

    def test_start(self):
        """Test start method."""
        c = Mock()
        channel = Mock()
        c.connection.channel.return_value = channel
        
        consumer1 = Mock()
        consumer2 = Mock()
        consumers = [consumer1, consumer2]
        
        with patch.object(self.step, 'get_consumers', return_value=consumers):
            self.step.start(c)
            
            assert self.step.consumers == consumers
            consumer1.consume.assert_called_once()
            consumer2.consume.assert_called_once()

    def test_start_no_consumers(self):
        """Test start method with no consumers."""
        c = Mock()
        channel = Mock()
        c.connection.channel.return_value = channel
        
        with patch.object(self.step, 'get_consumers', return_value=None):
            self.step.start(c)
            
            assert self.step.consumers is None

    def test_stop(self):
        """Test stop method."""
        c = Mock()
        
        with patch.object(self.step, '_close') as mock_close:
            self.step.stop(c)
            mock_close.assert_called_once_with(c, True)

    def test_shutdown(self):
        """Test shutdown method."""
        c = Mock()
        
        with patch.object(self.step, '_close') as mock_close:
            self.step.shutdown(c)
            mock_close.assert_called_once_with(c, False)

    def test_close_with_consumers(self):
        """Test _close method with consumers."""
        c = Mock()
        
        consumer1 = Mock()
        consumer1.channel = Mock()
        consumer2 = Mock()
        consumer2.channel = Mock()
        self.step.consumers = [consumer1, consumer2]
        
        with patch('celery.bootsteps.ignore_errors') as mock_ignore:
            self.step._close(c, cancel_consumers=True)
            
            # Should call ignore_errors for each consumer cancel and channel close
            assert mock_ignore.call_count >= 2

    def test_close_without_cancel_consumers(self):
        """Test _close method without canceling consumers."""
        c = Mock()
        
        consumer1 = Mock()
        consumer1.channel = Mock()
        self.step.consumers = [consumer1]
        
        with patch('celery.bootsteps.ignore_errors') as mock_ignore:
            self.step._close(c, cancel_consumers=False)
            
            # Should only call ignore_errors for channel close, not consumer cancel
            mock_ignore.assert_called()

    def test_close_no_consumers(self):
        """Test _close method with no consumers."""
        c = Mock()
        self.step.consumers = None
        
        with patch('celery.bootsteps.ignore_errors') as mock_ignore:
            self.step._close(c, cancel_consumers=True)
            
            # Should not call ignore_errors since no consumers
            mock_ignore.assert_not_called()


class TestUtilityFunctions:
    """Test cases for utility functions."""

    def test_label_function(self):
        """Test _label function."""
        class MockStep:
            name = 'module.submodule.StepName'
        
        step = MockStep()
        result = _label(step)
        assert result == 'StepName'

    def test_label_function_no_dots(self):
        """Test _label function with name containing no dots."""
        class MockStep:
            name = 'StepName'
        
        step = MockStep()
        result = _label(step)
        assert result == 'StepName'

    def test_pre_function(self):
        """Test _pre function."""
        ns = Mock()
        ns.alias = 'test_alias'
        
        result = _pre(ns, 'test message')
        assert result == '| test_alias: test message'


class TestStepType:
    """Test cases for StepType metaclass."""

    def test_step_creation_with_metaclass(self):
        """Test step creation with StepType metaclass."""
        from celery.bootsteps import StepType
        
        class TestStep(metaclass=StepType):
            pass
        
        # Should have name attribute set
        assert hasattr(TestStep, 'name')
        assert TestStep.name is not None

    def test_step_str_representation(self):
        """Test step string representation."""
        from celery.bootsteps import StepType
        
        class TestStep(metaclass=StepType):
            name = 'test.TestStep'
        
        assert str(TestStep) == 'test.TestStep'

    def test_step_repr_representation(self):
        """Test step repr representation."""
        from celery.bootsteps import StepType
        
        class TestStep(metaclass=StepType):
            name = 'test.TestStep'
            requires = ('dep1', 'dep2')
        
        result = repr(TestStep)
        assert 'test.TestStep' in result
        assert 'dep1' in result or 'dep2' in result


class TestBlueprintIntegration:
    """Integration tests for Blueprint with actual Step classes."""

    def test_blueprint_with_real_steps(self):
        """Test Blueprint with real Step instances."""
        class TestStep1(Step):
            name = 'test.Step1'
            
            def create(self, parent):
                return Mock()
        
        class TestStep2(StartStopStep):
            name = 'test.Step2'
            requires = ('test.Step1',)
            
            def create(self, parent):
                return Mock()
        
        # Mock the symbol_by_name to return our test classes
        with patch('celery.bootsteps.symbol_by_name') as mock_symbol:
            def symbol_side_effect(name):
                if name == 'test.Step1':
                    return TestStep1
                elif name == 'test.Step2':
                    return TestStep2
                return Mock()
            
            mock_symbol.side_effect = symbol_side_effect
            
            bp = Blueprint(steps=['test.Step1', 'test.Step2'])
            parent = Mock()
            parent.steps = []
            
            bp.apply(parent)
            
            # Should have created both steps
            assert len(bp.order) == 2
            assert bp.steps['test.Step1']
            assert bp.steps['test.Step2']
"""A directed acyclic graph of reusable components."""

from collections import deque
from threading import Event

from kombu.common import ignore_errors
from kombu.utils.encoding import bytes_to_str
from kombu.utils.imports import symbol_by_name

from .utils.graph import DependencyGraph, GraphFormatter
from .utils.imports import instantiate, qualname
from .utils.log import get_logger

try:
    from greenlet import GreenletExit
except ImportError:
    IGNORE_ERRORS = ()
else:
    IGNORE_ERRORS = (GreenletExit,)

__all__ = ('Blueprint', 'Step', 'StartStopStep', 'ConsumerStep')

#: States
RUN = 0x1
CLOSE = 0x2
TERMINATE = 0x3

logger = get_logger(__name__)


def _pre(ns, fmt):
    return f'| {ns.alias}: {fmt}'


def _label(s):
    return s.name.rsplit('.', 1)[-1]


class StepFormatter(GraphFormatter):
    """Graph formatter for :class:`Blueprint`."""

    blueprint_prefix = 'â§'
    conditional_prefix = 'â'
    blueprint_scheme = {
        'shape': 'parallelogram',
        'color': 'slategray4',
        'fillcolor': 'slategray3',
    }

    def label(self, step):
        return step and '{}{}'.format(
            self._get_prefix(step),
            bytes_to_str(
                (step.label or _label(step)).encode('utf-8', 'ignore')),
        )

    def _get_prefix(self, step):
        if step.last:
            return self.blueprint_prefix
        if step.conditional:
            return self.conditional_prefix
        return ''

    def node(self, obj, **attrs):
        scheme = self.blueprint_scheme if obj.last else self.node_scheme
        return self.draw_node(obj, scheme, attrs)

    def edge(self, a, b, **attrs):
        if a.last:
            attrs.update(arrowhead='none', color='darkseagreen3')
        return self.draw_edge(a, b, self.edge_scheme, attrs)


class Blueprint:
    """Blueprint containing bootsteps that can be applied to objects.

    Arguments:
        steps Sequence[Union[str, Step]]: List of steps.
        name (str): Set explicit name for this blueprint.
        on_start (Callable): Optional callback applied after blueprint start.
        on_close (Callable): Optional callback applied before blueprint close.
        on_stopped (Callable): Optional callback applied after
            blueprint stopped.
    """

    GraphFormatter = StepFormatter

    name = None
    state = None
    started = 0
    default_steps = set()
    state_to_name = {
        0: 'initializing',
        RUN: 'running',
        CLOSE: 'closing',
        TERMINATE: 'terminating',
    }

    def __init__(self, steps=None, name=None,
                 on_start=None, on_close=None, on_stopped=None):
        self.name = name or self.name or qualname(type(self))
        self.types = set(steps or []) | set(self.default_steps)
        self.on_start = on_start
        self.on_close = on_close
        self.on_stopped = on_stopped
        self.shutdown_complete = Event()
        self.steps = {}

    def start(self, parent):
        self.state = RUN
        if self.on_start:
            self.on_start()
        for i, step in enumerate(s for s in parent.steps if s is not None):
            self._debug('Starting %s', step.alias)
            self.started = i + 1
            step.start(parent)
            logger.debug('^-- substep ok')

    def human_state(self):
        return self.state_to_name[self.state or 0]

    def info(self, parent):
        info = {}
        for step in parent.steps:
            info.update(step.info(parent) or {})
        return info

    def close(self, parent):
        if self.on_close:
            self.on_close()
        self.send_all(parent, 'close', 'closing', reverse=False)

    def restart(self, parent, method='stop',
                description='restarting', propagate=False):
        self.send_all(parent, method, description, propagate=propagate)

    def send_all(self, parent, method,
                 description=None, reverse=True, propagate=True, args=()):
        description = description or method.replace('_', ' ')
        steps = reversed(parent.steps) if reverse else parent.steps
        for step in steps:
            if step:
                fun = getattr(step, method, None)
                if fun is not None:
                    self._debug('%s %s...',
                                description.capitalize(), step.alias)
                    try:
                        fun(parent, *args)
                    except Exception as exc:  # pylint: disable=broad-except
                        if propagate:
                            raise
                        logger.exception(
                            'Error on %s %s: %r', description, step.alias, exc)

    def stop(self, parent, close=True, terminate=False):
        what = 'terminating' if terminate else 'stopping'
        if self.state in (CLOSE, TERMINATE):
            return

        if self.state != RUN or self.started != len(parent.steps):
            # Not fully started, can safely exit.
            self.state = TERMINATE
            self.shutdown_complete.set()
            return
        self.close(parent)
        self.state = CLOSE

        self.restart(
            parent, 'terminate' if terminate else 'stop',
            description=what, propagate=False,
        )

        if self.on_stopped:
            self.on_stopped()
        self.state = TERMINATE
        self.shutdown_complete.set()

    def join(self, timeout=None):
        try:
            # Will only get here if running green,
            # makes sure all greenthreads have exited.
            self.shutdown_complete.wait(timeout=timeout)
        except IGNORE_ERRORS:
            pass

    def apply(self, parent, **kwargs):
        """Apply the steps in this blueprint to an object.

        This will apply the ``__init__`` and ``include`` methods
        of each step, with the object as argument::

            step = Step(obj)
            ...
            step.include(obj)

        For :class:`StartStopStep` the services created
        will also be added to the objects ``steps`` attribute.
        """
        self._debug('Preparing bootsteps.')
        order = self.order = []
        steps = self.steps = self.claim_steps()

        self._debug('Building graph...')
        for S in self._finalize_steps(steps):
            step = S(parent, **kwargs)
            steps[step.name] = step
            order.append(step)
        self._debug('New boot order: {%s}',
                    ', '.join(s.alias for s in self.order))
        for step in order:
            step.include(parent)
        return self

    def connect_with(self, other):
        self.graph.adjacent.update(other.graph.adjacent)
        self.graph.add_edge(type(other.order[0]), type(self.order[-1]))

    def __getitem__(self, name):
        return self.steps[name]

    def _find_last(self):
        return next((C for C in self.steps.values() if C.last), None)

    def _firstpass(self, steps):
        for step in steps.values():
            step.requires = [symbol_by_name(dep) for dep in step.requires]
        stream = deque(step.requires for step in steps.values())
        while stream:
            for node in stream.popleft():
                node = symbol_by_name(node)
                if node.name not in self.steps:
                    steps[node.name] = node
                stream.append(node.requires)

    def _finalize_steps(self, steps):
        last = self._find_last()
        self._firstpass(steps)
        it = ((C, C.requires) for C in steps.values())
        G = self.graph = DependencyGraph(
            it, formatter=self.GraphFormatter(root=last),
        )
        if last:
            for obj in G:
                if obj != last:
                    G.add_edge(last, obj)
        try:
            return G.topsort()
        except KeyError as exc:
            raise KeyError('unknown bootstep: %s' % exc)

    def claim_steps(self):
        return dict(self.load_step(step) for step in self.types)

    def load_step(self, step):
        step = symbol_by_name(step)
        return step.name, step

    def _debug(self, msg, *args):
        return logger.debug(_pre(self, msg), *args)

    @property
    def alias(self):
        return _label(self)


class StepType(type):
    """Meta-class for steps."""

    name = None
    requires = None

    def __new__(cls, name, bases, attrs):
        module = attrs.get('__module__')
        qname = f'{module}.{name}' if module else name
        attrs.update(
            __qualname__=qname,
            name=attrs.get('name') or qname,
        )
        return super().__new__(cls, name, bases, attrs)

    def __str__(cls):
        return cls.name

    def __repr__(cls):
        return 'step:{0.name}{{{0.requires!r}}}'.format(cls)


class Step(metaclass=StepType):
    """A Bootstep.

    The :meth:`__init__` method is called when the step
    is bound to a parent object, and can as such be used
    to initialize attributes in the parent object at
    parent instantiation-time.
    """

    #: Optional step name, will use ``qualname`` if not specified.
    name = None

    #: Optional short name used for graph outputs and in logs.
    label = None

    #: Set this to true if the step is enabled based on some condition.
    conditional = False

    #: List of other steps that that must be started before this step.
    #: Note that all dependencies must be in the same blueprint.
    requires = ()

    #: This flag is reserved for the workers Consumer,
    #: since it is required to always be started last.
    #: There can only be one object marked last
    #: in every blueprint.
    last = False

    #: This provides the default for :meth:`include_if`.
    enabled = True

    def __init__(self, parent, **kwargs):
        pass

    def include_if(self, parent):
        """Return true if bootstep should be included.

        You can define this as an optional predicate that decides whether
        this step should be created.
        """
        return self.enabled

    def instantiate(self, name, *args, **kwargs):
        return instantiate(name, *args, **kwargs)

    def _should_include(self, parent):
        if self.include_if(parent):
            return True, self.create(parent)
        return False, None

    def include(self, parent):
        return self._should_include(parent)[0]

    def create(self, parent):
        """Create the step."""

    def __repr__(self):
        return f'<step: {self.alias}>'

    @property
    def alias(self):
        return self.label or _label(self)

    def info(self, obj):
        pass


class StartStopStep(Step):
    """Bootstep that must be started and stopped in order."""

    #: Optional obj created by the :meth:`create` method.
    #: This is used by :class:`StartStopStep` to keep the
    #: original service object.
    obj = None

    def start(self, parent):
        if self.obj:
            return self.obj.start()

    def stop(self, parent):
        if self.obj:
            return self.obj.stop()

    def close(self, parent):
        pass

    def terminate(self, parent):
        if self.obj:
            return getattr(self.obj, 'terminate', self.obj.stop)()

    def include(self, parent):
        inc, ret = self._should_include(parent)
        if inc:
            self.obj = ret
            parent.steps.append(self)
        return inc


class ConsumerStep(StartStopStep):
    """Bootstep that starts a message consumer."""

    requires = ('celery.worker.consumer:Connection',)
    consumers = None

    def get_consumers(self, channel):
        raise NotImplementedError('missing get_consumers')

    def start(self, c):
        channel = c.connection.channel()
        self.consumers = self.get_consumers(channel)
        for consumer in self.consumers or []:
            consumer.consume()

    def stop(self, c):
        self._close(c, True)

    def shutdown(self, c):
        self._close(c, False)

    def _close(self, c, cancel_consumers=True):
        channels = set()
        for consumer in self.consumers or []:
            if cancel_consumers:
                ignore_errors(c.connection, consumer.cancel)
            if consumer.channel:
                channels.add(consumer.channel)
        for channel in channels:
            ignore_errors(c.connection, channel.close)

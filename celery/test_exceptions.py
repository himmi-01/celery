"""Unit tests for celery.exceptions module."""

import numbers
import sys
import unittest
from datetime import datetime

from celery.exceptions import Retry, reraise


class TestReraise(unittest.TestCase):
    """Test cases for the reraise function."""

    def test_reraise_with_same_traceback(self):
        """Test reraise when value.__traceback__ is the same as tb parameter."""
        try:
            raise ValueError("test error")
        except ValueError as e:
            original_tb = e.__traceback__
            with self.assertRaises(ValueError) as cm:
                reraise(ValueError, e, original_tb)
            self.assertEqual(str(cm.exception), "test error")

    def test_reraise_with_different_traceback(self):
        """Test reraise when value.__traceback__ differs from tb parameter."""
        try:
            raise ValueError("test error")
        except ValueError as e:
            # Create a different traceback
            try:
                raise RuntimeError("different error")
            except RuntimeError:
                different_tb = sys.exc_info()[2]
            
            with self.assertRaises(ValueError) as cm:
                reraise(ValueError, e, different_tb)
            self.assertEqual(str(cm.exception), "test error")

    def test_reraise_with_none_traceback(self):
        """Test reraise with None traceback parameter."""
        try:
            raise ValueError("test error")
        except ValueError as e:
            with self.assertRaises(ValueError) as cm:
                reraise(ValueError, e, None)
            self.assertEqual(str(cm.exception), "test error")

    def test_reraise_preserves_exception_type(self):
        """Test that reraise preserves the original exception type."""
        custom_exception = RuntimeError("custom message")
        with self.assertRaises(RuntimeError) as cm:
            reraise(RuntimeError, custom_exception)
        self.assertEqual(str(cm.exception), "custom message")


class TestRetryHumanize(unittest.TestCase):
    """Test cases for the Retry.humanize method."""

    def test_humanize_with_number_when(self):
        """Test humanize with numeric when value."""
        retry = Retry(when=30)
        result = retry.humanize()
        self.assertEqual(result, "in 30s")

    def test_humanize_with_float_when(self):
        """Test humanize with float when value."""
        retry = Retry(when=45.5)
        result = retry.humanize()
        self.assertEqual(result, "in 45.5s")

    def test_humanize_with_datetime_when(self):
        """Test humanize with datetime when value."""
        dt = datetime(2023, 12, 25, 15, 30, 0)
        retry = Retry(when=dt)
        result = retry.humanize()
        self.assertEqual(result, f"at {dt}")

    def test_humanize_with_string_when(self):
        """Test humanize with string when value (non-numeric)."""
        retry = Retry(when="tomorrow")
        result = retry.humanize()
        self.assertEqual(result, "at tomorrow")

    def test_humanize_with_none_when(self):
        """Test humanize with None when value."""
        retry = Retry(when=None)
        result = retry.humanize()
        self.assertEqual(result, "at None")

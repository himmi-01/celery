"""Unit tests for docs/conf.py configuration functions."""

import unittest
from unittest.mock import patch, MagicMock
import sys
import os

# Add the docs directory to the path to import conf
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import conf


class TestConfigFunctions(unittest.TestCase):
    """Test cases for configuration checking functions."""

    def setUp(self):
        """Set up test fixtures."""
        # Reset settings before each test
        conf.settings.clear()

    def tearDown(self):
        """Clean up after each test."""
        conf.settings.clear()

    @patch('conf.NAMESPACES')
    @patch('conf.flatten')
    def test_configcheck_project_settings_success(self, mock_flatten, mock_namespaces):
        """Test configcheck_project_settings returns correct settings set."""
        # Mock the flatten function to return a list of tuples
        mock_flatten.return_value = [
            ('broker_url', MagicMock()),
            ('result_backend', MagicMock()),
            ('task_serializer', MagicMock()),
            ('result_serializer', MagicMock()),
        ]
        
        result = conf.configcheck_project_settings()
        
        # Verify flatten was called with NAMESPACES
        mock_flatten.assert_called_once_with(mock_namespaces)
        
        # Verify the result is a set containing the expected keys
        expected_settings = {'broker_url', 'result_backend', 'task_serializer', 'result_serializer'}
        self.assertEqual(result, expected_settings)
        
        # Verify settings dict was updated
        self.assertEqual(set(conf.settings.keys()), expected_settings)

    @patch('conf.NAMESPACES')
    @patch('conf.flatten')
    def test_configcheck_project_settings_empty(self, mock_flatten, mock_namespaces):
        """Test configcheck_project_settings with empty namespaces."""
        mock_flatten.return_value = []
        
        result = conf.configcheck_project_settings()
        
        self.assertEqual(result, set())
        self.assertEqual(len(conf.settings), 0)

    @patch('conf.NAMESPACES')
    @patch('conf.flatten')
    def test_configcheck_project_settings_updates_global_settings(self, mock_flatten, mock_namespaces):
        """Test that configcheck_project_settings updates the global settings dict."""
        mock_setting = MagicMock()
        mock_flatten.return_value = [('test_setting', mock_setting)]
        
        conf.configcheck_project_settings()
        
        self.assertIn('test_setting', conf.settings)
        self.assertEqual(conf.settings['test_setting'], mock_setting)

    def test_is_deprecated_setting_with_deprecated_setting(self):
        """Test is_deprecated_setting returns deprecation info for deprecated setting."""
        # Mock a deprecated setting
        mock_setting = MagicMock()
        mock_setting.deprecate_by = "5.0"
        conf.settings['deprecated_setting'] = mock_setting
        
        result = conf.is_deprecated_setting('deprecated_setting')
        
        self.assertEqual(result, "5.0")

    def test_is_deprecated_setting_with_non_deprecated_setting(self):
        """Test is_deprecated_setting returns None for non-deprecated setting."""
        # Mock a non-deprecated setting
        mock_setting = MagicMock()
        mock_setting.deprecate_by = None
        conf.settings['regular_setting'] = mock_setting
        
        result = conf.is_deprecated_setting('regular_setting')
        
        self.assertIsNone(result)

    def test_is_deprecated_setting_with_unknown_setting(self):
        """Test is_deprecated_setting returns None for unknown setting."""
        result = conf.is_deprecated_setting('unknown_setting')
        
        self.assertIsNone(result)

    def test_configcheck_should_ignore_with_ignored_setting(self):
        """Test configcheck_should_ignore returns True for explicitly ignored settings."""
        # Test with a setting from ignored_settings
        result = conf.configcheck_should_ignore('broker_host')
        
        self.assertTrue(result)

    def test_configcheck_should_ignore_with_deprecated_setting(self):
        """Test configcheck_should_ignore returns True for deprecated settings."""
        # Mock a deprecated setting
        mock_setting = MagicMock()
        mock_setting.deprecate_by = "5.0"
        conf.settings['deprecated_test'] = mock_setting
        
        result = conf.configcheck_should_ignore('deprecated_test')
        
        self.assertTrue(result)

    def test_configcheck_should_ignore_with_regular_setting(self):
        """Test configcheck_should_ignore returns False for regular settings."""
        # Mock a regular setting
        mock_setting = MagicMock()
        mock_setting.deprecate_by = None
        conf.settings['regular_test'] = mock_setting
        
        result = conf.configcheck_should_ignore('regular_test')
        
        self.assertFalse(result)

    def test_configcheck_should_ignore_with_unknown_setting(self):
        """Test configcheck_should_ignore returns False for unknown settings."""
        result = conf.configcheck_should_ignore('completely_unknown_setting')
        
        self.assertFalse(result)

    def test_ignored_settings_contains_expected_values(self):
        """Test that ignored_settings contains expected deprecated settings."""
        expected_ignored = {
            'broker_host', 'broker_user', 'broker_password', 'broker_vhost',
            'chord_propagates', 'mongodb_backend_settings', 'database_url',
            'redis_host', 'redis_port', 'worker_agent'
        }
        
        for setting in expected_ignored:
            self.assertIn(setting, conf.ignored_settings)


if __name__ == '__main__':
    unittest.main()

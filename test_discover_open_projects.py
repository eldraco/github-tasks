#!/usr/bin/env python3
"""
Test for the discover_open_projects function fix.
This test verifies that the function handles None values in the nodes list correctly.
"""

import unittest
from unittest.mock import Mock, patch
import sys
import os

# Add the current directory to the path so we can import gh_task_viewer
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from gh_task_viewer import discover_open_projects


class TestDiscoverOpenProjects(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_session = Mock()
    
    @patch('gh_task_viewer._graphql_with_backoff')
    def test_discover_open_projects_with_none_values(self, mock_graphql):
        """Test that discover_open_projects handles None values in nodes list."""
        # Mock response with None values in the nodes list
        mock_response = {
            "data": {
                "organization": {
                    "projectsV2": {
                        "nodes": [
                            {"number": 1, "title": "Project 1", "closed": False},
                            None,  # This None value was causing the AttributeError
                            {"number": 2, "title": "Project 2", "closed": True},
                            {"number": 3, "title": "Project 3", "closed": False},
                            None,  # Another None value
                        ]
                    }
                }
            }
        }
        mock_graphql.return_value = mock_response
        
        # Call the function
        result = discover_open_projects(self.mock_session, "org", "test-org")
        
        # Verify that:
        # 1. None values are filtered out
        # 2. Closed projects are filtered out
        # 3. Only open projects remain
        expected = [
            {"number": 1, "title": "Project 1", "closed": False},
            {"number": 3, "title": "Project 3", "closed": False},
        ]
        
        self.assertEqual(result, expected)
        self.assertEqual(len(result), 2)
        
        # Verify that all returned items are not None and not closed
        for project in result:
            self.assertIsNotNone(project)
            self.assertFalse(project.get("closed", False))
    
    @patch('gh_task_viewer._graphql_with_backoff')
    def test_discover_open_projects_user_type(self, mock_graphql):
        """Test that discover_open_projects works for user type."""
        # Mock response for user projects
        mock_response = {
            "data": {
                "user": {
                    "projectsV2": {
                        "nodes": [
                            {"number": 1, "title": "User Project 1", "closed": False},
                            None,
                            {"number": 2, "title": "User Project 2", "closed": True},
                        ]
                    }
                }
            }
        }
        mock_graphql.return_value = mock_response
        
        # Call the function with user type
        result = discover_open_projects(self.mock_session, "user", "test-user")
        
        # Should only return the open project, filtering out None and closed
        expected = [
            {"number": 1, "title": "User Project 1", "closed": False},
        ]
        
        self.assertEqual(result, expected)
        self.assertEqual(len(result), 1)
    
    @patch('gh_task_viewer._graphql_with_backoff')
    def test_discover_open_projects_all_none(self, mock_graphql):
        """Test that discover_open_projects handles all None values."""
        # Mock response with all None values
        mock_response = {
            "data": {
                "organization": {
                    "projectsV2": {
                        "nodes": [None, None, None]
                    }
                }
            }
        }
        mock_graphql.return_value = mock_response
        
        # Call the function
        result = discover_open_projects(self.mock_session, "org", "test-org")
        
        # Should return empty list
        self.assertEqual(result, [])
        self.assertEqual(len(result), 0)
    
    @patch('gh_task_viewer._graphql_with_backoff')
    def test_discover_open_projects_empty_nodes(self, mock_graphql):
        """Test that discover_open_projects handles empty nodes list."""
        # Mock response with empty nodes
        mock_response = {
            "data": {
                "organization": {
                    "projectsV2": {
                        "nodes": []
                    }
                }
            }
        }
        mock_graphql.return_value = mock_response
        
        # Call the function
        result = discover_open_projects(self.mock_session, "org", "test-org")
        
        # Should return empty list
        self.assertEqual(result, [])
        self.assertEqual(len(result), 0)


if __name__ == '__main__':
    unittest.main()

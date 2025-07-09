import os
import pytest
from unittest.mock import patch

# Import the function to be tested
from src.helpers.db_utils import get_db_connection

@patch('src.helpers.db_utils.psycopg2.connect')
def test_get_db_connection(mock_connect):
    """
    Unit test for get_db_connection to ensure it correctly uses environment variables
    and calls psycopg2.connect with expected arguments.
    """

    # 1. Define test enviroment variables
    test_env = {
        'RDS_HOST': 'test-host',
        'RDS_PORT': '5433',
        'RDS_DATABASE': 'test_db',
        'RDS_USER': 'test_user',
        'RDS_PASSWORD': 'test_password'
    }

    # 2. Temporarily override os.environ with test_env for the duration of this test
    with patch.dict(os.environ, test_env):
        # 3. Mock return value of psycopg2.connect
        mock_conn = mock_connect.return_value

        # 4. Call the function to be tested
        conn = get_db_connection()


        # 5. Verify that psycopg2.connect was called with expected arguments
        mock_connect.assert_called_once_with(
            host='test-host',
            port='5433',
            dbname='test_db',
            user='test_user',
            password='test_password'
        )

        # 6. Verify that the function returns the expected connection object
        assert conn == mock_conn




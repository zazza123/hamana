import pytest
import logging
from sqlite3 import Connection

from hamana.core.db import (
    HamanaDatabase, 
    HamanaDatabaseNotInitialised,
    HamanaDatabaseAlreadyInitialised
)

# Initialize the database
hamana_db = HamanaDatabase()

def test_get_instance_initialized():
    """Test that get_instance returns the instance if the database is initialized."""
    # Get the instance
    instance = HamanaDatabase.get_instance()

    global hamana_db

    assert instance is hamana_db

def test_get_instance_not_initialized():
    """Test that get_instance raises HamanaDatabaseNotInitialised if the database is not initialized."""
    # Ensure the database is not initialized
    HamanaDatabase._instances = {}

    # Attempt to get the instance without initializing the database
    with pytest.raises(HamanaDatabaseNotInitialised):
        HamanaDatabase.get_instance()

    HamanaDatabase()

def test_get_connection_success():
    """Test that get_connection returns a connection object."""
    # Initialize the database
    db = HamanaDatabase.get_instance()

    # Get the connection
    connection = db.get_connection()

    # Assert that the connection is an instance of sqlite3.Connection
    assert isinstance(connection, Connection)

def test_connect_success():
    """Test that _connect connects to the database successfully."""
    # Initialize the database
    db = HamanaDatabase.get_instance()

    # Test if the connection is successful
    connection = db._connect()
    assert connection is not None
    assert connection == db.get_connection()

    # close connection
    db.close()

def test_connect_failure():
    """Test that _connect raises HamanaDatabaseNotInitialised if the connection fails."""
    # Create an instance without initializing the connection
    db = HamanaDatabase.get_instance()
    db._connection = None  # Manually set the connection to None to simulate failure

    # Test if the appropriate exception is raised
    with pytest.raises(HamanaDatabaseNotInitialised):
        db._connect()

def test_singleton_behavior():
    """Test that only one instance of HamanaDatabase is created."""
    # Attempt to initialize another instance should raise an error
    with pytest.raises(HamanaDatabaseAlreadyInitialised):
        HamanaDatabase()

def test_close_connection():
    """Test that close closes the connection."""
    # Initialize the database
    db = HamanaDatabase.get_instance()

    # Close the connection
    db.close()

    # Check if the connection is closed
    assert db._connection is None

def test_close_already_closed_connection(caplog):
    """Test that close logs a warning if the connection is already closed."""
    # Initialize the database
    db = HamanaDatabase.get_instance()

    # Close the connection again
    with caplog.at_level(logging.WARNING):
        db.close()

def test_get_connection_not_initialized():
    """Test that get_connection raises HamanaDatabaseNotInitialised if the database is not initialized."""
    # Ensure the database is not initialized
    HamanaDatabase._instances = {}
    
    # Attempt to get the connection without initializing the database
    with pytest.raises(HamanaDatabaseNotInitialised):
        HamanaDatabase.get_instance().get_connection()
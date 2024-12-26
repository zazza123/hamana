import pytest
import logging
from sqlite3 import Connection

from hamana import HamanaDatabase
from hamana.core.db import (
    HamanaDatabaseNotInitialised,
    HamanaDatabaseAlreadyInitialised
)

def test_get_instance_initialized():
    """Test that get_instance returns the instance if the database is initialized."""

    db = HamanaDatabase()
    instance = HamanaDatabase.get_instance()
    assert instance is db

    db.close()
    return

def test_get_instance_not_initialized():
    """Test that get_instance raises HamanaDatabaseNotInitialised if the database is not initialized."""

    with pytest.raises(HamanaDatabaseNotInitialised):
        HamanaDatabase.get_instance()

    return

def test_get_connection_success():
    """Test that get_connection returns a connection object."""

    db = HamanaDatabase()
    connection = db.get_connection()
    assert isinstance(connection, Connection)

    db.close()
    return

def test_connect_success():
    """Test that _connect connects to the database successfully."""

    db = HamanaDatabase()
    connection = db._connect()
    assert connection is not None
    assert connection == db.get_connection()

    db.close()
    return

def test_singleton_behavior():
    """Test that only one instance of HamanaDatabase is created."""

    db = HamanaDatabase()
    with pytest.raises(HamanaDatabaseAlreadyInitialised):
        HamanaDatabase()

    db.close()
    return

def test_close_connection():
    """Test that close closes the connection."""

    db = HamanaDatabase()
    db.close()
    assert db._connection is None

    return

def test_close_already_closed_connection(caplog):
    """Test that close logs a warning if the connection is already closed."""

    db = HamanaDatabase()
    db.close()
    with caplog.at_level(logging.WARNING):
        db.close()
    return

def test_get_connection_not_initialized():
    """Test that get_connection raises HamanaDatabaseNotInitialised if the database is not initialized."""
    # Ensure the database is not initialized
    HamanaDatabase._instances = {}
    
    # Attempt to get the connection without initializing the database
    with pytest.raises(HamanaDatabaseNotInitialised):
        HamanaDatabase.get_instance().get_connection()

def test_connect_failure():
    """Test that _connect raises HamanaDatabaseNotInitialised if the connection fails."""

    db = HamanaDatabase()
    db._connection = None
    with pytest.raises(HamanaDatabaseNotInitialised):
        db._connect()

    db.close()
    return
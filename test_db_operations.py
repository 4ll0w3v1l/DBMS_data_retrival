import asyncio
import json
from unittest.mock import Mock, patch
import pytest
from db_operations import DBOperations
import server

db = DBOperations()

pytest_plugins = ('pytest_asyncio',)


# MOCK DB OPERATIONS
@pytest.mark.asyncio
async def mock_dql(query=None):
    return [(1, 'data 1'), (2, 'data 2'), (3, 'data 3')]


@pytest.mark.asyncio
async def broken_mock_dql(query=None):
    return [(1, 'data 1'), None, (3, 'data 3')]


@pytest.mark.asyncio
async def empty_mock_dql(query=None):
    return []


@pytest.mark.asyncio
async def timed_out_dql(query=None):
    await asyncio.sleep(10)
    return [(1, 'data 1'), (2, 'data 2'), (3, 'data 3')]


# DB OPERATIONS TESTS

@pytest.mark.asyncio
async def test_execute_dql():
    db.execute_dql = Mock(side_effect=mock_dql)
    result = await db.execute_dql("SELECT * FROM data_1")
    assert result == [(1, 'data 1'), (2, 'data 2'), (3, 'data 3')]


# FETCH TABLES TESTS

@pytest.mark.asyncio
async def test_fetch_tables():
    with patch('db_operations.DBOperations.execute_dql', side_effect=mock_dql):
        result = await server.fetch_tables()
        assert result == [[(1, 'data 1'), (2, 'data 2'), (3, 'data 3')] for _ in range(3)]
        # range 3 -> number of tables = [f"data_{x}" for x in range(1, 4)] in server.py


@pytest.mark.asyncio
async def test_fetch_tables_timeout():
    with patch('db_operations.DBOperations.execute_dql', side_effect=timed_out_dql):
        result = await server.fetch_tables()
        assert result == []


@pytest.mark.asyncio
async def test_empty_fetch_tables():
    with patch('db_operations.DBOperations.execute_dql', side_effect=empty_mock_dql):
        result = await server.fetch_tables()
        assert result == [[] for _ in range(3)]
        # range 3 -> number of tables = [f"data_{x}" for x in range(1, 4)] in server.py


# GET DATA TESTS

@pytest.mark.asyncio
async def test_broken_get_data():
    with patch('server.fetch_tables', side_effect=broken_mock_dql):
        result = await server.get_data()
        result = json.loads(result.body.decode("utf-8"))
        assert result == [{'id': 1, 'name': 'data 1'}, {'id': 3, 'name': 'data 3'}]


@pytest.mark.asyncio
async def test_empty_get_data():
    with patch('server.fetch_tables', side_effect=empty_mock_dql):
        result = await server.get_data()
        result = json.loads(result.body.decode("utf-8"))
        assert result == []

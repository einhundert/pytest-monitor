# -*- coding: utf-8 -*-
import datetime
import sqlite3
import subprocess
import os
import pytest

try:
    import psycopg
except ImportError:
    import psycopg2 as psycopg

from pytest_monitor.sys_utils import determine_scm_revision
from pytest_monitor.handler import SqliteDBHandler
from pytest_monitor.handler import PostgresDBHandler


# helper function
def reset_db(cleanup_cursor):
    cleanup_cursor.execute("DROP DATABASE postgres")
    cleanup_cursor.execute("CREATE DATABASE postgres")

    # cleanup_cursor.execute("CREATE SCHEMA public;")
    # cleanup_cursor.execute("ALTER DATABASE postgres SET search_path TO public;")
    # cleanup_cursor.execute("ALTER ROLE postgres SET search_path TO public;")
    # cleanup_cursor.execute("ALTER SCHEMA public OWNER to postgres;")
    # cleanup_cursor.execute("GRANT ALL ON SCHEMA public TO postgres;")
    # cleanup_cursor.execute("GRANT ALL ON SCHEMA public TO public;")


@pytest.fixture
def sqlite_empty_mock_db() -> sqlite3.Connection:
    mockdb = sqlite3.connect(":memory:")
    yield mockdb
    mockdb.close()


@pytest.fixture
def prepared_mocked_SqliteDBHandler(sqlite_empty_mock_db) -> SqliteDBHandler:
    mockdb = sqlite_empty_mock_db
    db_cursor = mockdb.cursor()
    db_cursor.execute(
        """
CREATE TABLE IF NOT EXISTS TEST_SESSIONS(
    SESSION_H varchar(64) primary key not null unique, -- Session identifier
    RUN_DATE varchar(64), -- Date of test run
    SCM_ID varchar(128), -- SCM change id
    RUN_DESCRIPTION json
);"""
    )
    db_cursor.execute(
        """
CREATE TABLE IF NOT EXISTS EXECUTION_CONTEXTS (
   ENV_H varchar(64) primary key not null unique,
   CPU_COUNT integer,
   CPU_FREQUENCY_MHZ integer,
   CPU_TYPE varchar(64),
   CPU_VENDOR varchar(256),
   RAM_TOTAL_MB integer,
   MACHINE_NODE varchar(512),
   MACHINE_TYPE varchar(32),
   MACHINE_ARCH varchar(16),
   SYSTEM_INFO varchar(256),
   PYTHON_INFO varchar(512)
);
"""
    )

    db_cursor.execute(
        """
CREATE TABLE IF NOT EXISTS TEST_METRICS (
    SESSION_H varchar(64), -- Session identifier
    ENV_H varchar(64), -- Environment description identifier
    ITEM_START_TIME varchar(64), -- Effective start time of the test
    ITEM_PATH varchar(4096), -- Path of the item, following Python import specification
    ITEM varchar(2048), -- Name of the item
    ITEM_VARIANT varchar(2048), -- Optional parametrization of an item.
    ITEM_FS_LOC varchar(2048), -- Relative path from pytest invocation directory to the item's module.
    KIND varchar(64), -- Package, Module or function
    COMPONENT varchar(512) NULL, -- Tested component if any
    TOTAL_TIME float, -- Total time spent running the item
    USER_TIME float, -- time spent in user space
    KERNEL_TIME float, -- time spent in kernel space
    CPU_USAGE float, -- cpu usage
    MEM_USAGE float, -- Max resident memory used.
    FOREIGN KEY (ENV_H) REFERENCES EXECUTION_CONTEXTS(ENV_H),
    FOREIGN KEY (SESSION_H) REFERENCES TEST_SESSIONS(SESSION_H)
);"""
    )

    db_cursor.execute(
        "insert into TEST_SESSIONS(SESSION_H, RUN_DATE, SCM_ID, RUN_DESCRIPTION)"
        " values (?,?,?,?)",
        (
            "1",
            datetime.datetime.now().isoformat(),
            determine_scm_revision(),
            '{ "descr": "Test Session" }',
        ),
    )

    db_cursor.execute(
        "insert into EXECUTION_CONTEXTS(CPU_COUNT,CPU_FREQUENCY_MHZ,CPU_TYPE,CPU_VENDOR,"
        "RAM_TOTAL_MB,MACHINE_NODE,MACHINE_TYPE,MACHINE_ARCH,SYSTEM_INFO,"
        "PYTHON_INFO,ENV_H) values (?,?,?,?,?,?,?,?,?,?,?)",
        (
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            1,
        ),
    )

    # insert old style entry
    db_cursor.execute(
        "insert into TEST_METRICS(SESSION_H,ENV_H,ITEM_START_TIME,ITEM,"
        "ITEM_PATH,ITEM_VARIANT,ITEM_FS_LOC,KIND,COMPONENT,TOTAL_TIME,"
        "USER_TIME,KERNEL_TIME,CPU_USAGE,MEM_USAGE) "
        "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "1",
            "1",
            "Startdate",
            "name of item",
            "Item path",
            "Optional Param",
            "relative path",
            None,
            None,
            42,
            42,
            42,
            42,
            42,
        ),
    )
    db = SqliteDBHandler(":memory:")
    db.__cnx = mockdb
    db._SqliteDBHandler__cnx = mockdb
    db._SqliteDBHandler__db = "mockdb"

    return db


@pytest.fixture
def postgres_empty_db_mock_cursor():
    # set up databse
    os.environ["PYTEST_MONITOR_DB_NAME"] = "postgres"
    os.environ["PYTEST_MONITOR_DB_USER"] = "postgres"
    os.environ["PYTEST_MONITOR_DB_PASSWORD"] = "testing_db"
    os.environ["PYTEST_MONITOR_DB_HOST"] = "localhost"
    os.environ["PYTEST_MONITOR_DB_PORT"] = "5432"
    connection_string = "dbname='postgres' user='postgres' password='testing_db' host='localhost' port='5432'"
    mockdb = psycopg.connect(connection_string)
    mockdb.autocommit = True

    # yield cursor to test context
    yield mockdb.cursor()

    # cleanup db
    cleanup_cursor = mockdb.cursor()
    reset_db(cleanup_cursor)
    cleanup_cursor.close()
    mockdb.close()


# prepare db with tables and example session, execution context, test entry inserted
@pytest.fixture
def prepared_mock_db_cursor_postgres(
    postgres_empty_db_mock_cursor,
) -> psycopg.extensions.cursor:
    db_cursor = postgres_empty_db_mock_cursor
    db_cursor.execute(
        """
CREATE TABLE IF NOT EXISTS TEST_SESSIONS(
    SESSION_H varchar(64) primary key not null unique, -- Session identifier
    RUN_DATE varchar(64), -- Date of test run
    SCM_ID varchar(128), -- SCM change id
    RUN_DESCRIPTION json
);"""
    )
    db_cursor.execute(
        """
CREATE TABLE IF NOT EXISTS EXECUTION_CONTEXTS (
   ENV_H varchar(64) primary key not null unique,
   CPU_COUNT integer,
   CPU_FREQUENCY_MHZ integer,
   CPU_TYPE varchar(64),
   CPU_VENDOR varchar(256),
   RAM_TOTAL_MB integer,
   MACHINE_NODE varchar(512),
   MACHINE_TYPE varchar(32),
   MACHINE_ARCH varchar(16),
   SYSTEM_INFO varchar(256),
   PYTHON_INFO varchar(512)
);
"""
    )

    db_cursor.execute(
        """
CREATE TABLE IF NOT EXISTS TEST_METRICS (
    SESSION_H varchar(64), -- Session identifier
    ENV_H varchar(64), -- Environment description identifier
    ITEM_START_TIME varchar(64), -- Effective start time of the test
    ITEM_PATH varchar(4096), -- Path of the item, following Python import specification
    ITEM varchar(2048), -- Name of the item
    ITEM_VARIANT varchar(2048), -- Optional parametrization of an item.
    ITEM_FS_LOC varchar(2048), -- Relative path from pytest invocation directory to the item's module.
    KIND varchar(64), -- Package, Module or function
    COMPONENT varchar(512) NULL, -- Tested component if any
    TOTAL_TIME float, -- Total time spent running the item
    USER_TIME float, -- time spent in user space
    KERNEL_TIME float, -- time spent in kernel space
    CPU_USAGE float, -- cpu usage
    MEM_USAGE float, -- Max resident memory used.
    FOREIGN KEY (ENV_H) REFERENCES EXECUTION_CONTEXTS(ENV_H),
    FOREIGN KEY (SESSION_H) REFERENCES TEST_SESSIONS(SESSION_H)
);"""
    )

    db_cursor.execute(
        "insert into TEST_SESSIONS(SESSION_H, RUN_DATE, SCM_ID, RUN_DESCRIPTION)"
        " values (%s,%s,%s,%s)",
        (
            "1",
            datetime.datetime.now().isoformat(),
            determine_scm_revision(),
            '{ "descr": "Test Session" }',
        ),
    )

    db_cursor.execute(
        "insert into EXECUTION_CONTEXTS(CPU_COUNT,CPU_FREQUENCY_MHZ,CPU_TYPE,CPU_VENDOR,"
        "RAM_TOTAL_MB,MACHINE_NODE,MACHINE_TYPE,MACHINE_ARCH,SYSTEM_INFO,"
        "PYTHON_INFO,ENV_H) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            1,
        ),
    )

    # insert old style entry
    db_cursor.execute(
        "insert into TEST_METRICS(SESSION_H,ENV_H,ITEM_START_TIME,ITEM,"
        "ITEM_PATH,ITEM_VARIANT,ITEM_FS_LOC,KIND,COMPONENT,TOTAL_TIME,"
        "USER_TIME,KERNEL_TIME,CPU_USAGE,MEM_USAGE) "
        "values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (
            "1",
            "1",
            "Startdate",
            "name of item",
            "Item path",
            "Optional Param",
            "relative path",
            None,
            None,
            42,
            42,
            42,
            42,
            42,
        ),
    )

    yield db_cursor

    db_cursor.close()


@pytest.fixture
def connected_PostgresDBHandler(postgres_empty_db_mock_cursor):
    os.environ["PYTEST_MONITOR_DB_NAME"] = "postgres"
    os.environ["PYTEST_MONITOR_DB_USER"] = "postgres"
    os.environ["PYTEST_MONITOR_DB_PASSWORD"] = "testing_db"
    os.environ["PYTEST_MONITOR_DB_HOST"] = "localhost"
    os.environ["PYTEST_MONITOR_DB_PORT"] = "5432"
    db = PostgresDBHandler()
    yield db
    reset_db(db._PostgresDBHandler__cnx.cursor())
    db.__cnx.close()


def test_sqlite_handler_check_create_test_passed_column(
    pytester, prepared_mocked_SqliteDBHandler
):
    # mockedDBHandler with old style database attached
    mockedHandler = prepared_mocked_SqliteDBHandler
    mock_cursor = mockedHandler.__cnx.cursor()

    # test for old style db
    mock_cursor.execute("PRAGMA table_info(TEST_METRICS)")
    has_test_column = any(
        column[1] == "TEST_PASSED" for column in mock_cursor.fetchall()
    )
    assert not has_test_column

    try:
        # run function to test (migration)
        mockedHandler.check_create_test_passed_column()

        # check for new column
        mock_cursor = mockedHandler.__cnx.cursor()
        mock_cursor.execute("PRAGMA table_info(TEST_METRICS)")
        has_test_column = any(
            column[1] == "TEST_PASSED" for column in mock_cursor.fetchall()
        )
        assert has_test_column

        # check for default value TRUE in existing entry
        mock_cursor.execute("SELECT TEST_PASSED FROM TEST_METRICS LIMIT 1")
        default_is_passed = mock_cursor.fetchone()

        # default value true(1) for entries after migration
        assert default_is_passed[0] == 1

    except Exception:
        raise


def test_sqlite_handler_check_new_db_setup(pytester):
    # db handler
    db = SqliteDBHandler(":memory:")
    table_cols = db.query("PRAGMA table_info(TEST_METRICS)", (), many=True)
    assert any(column[1] == "TEST_PASSED" for column in table_cols)


def test_postgres_handler_check_create_test_passed_column(
    prepared_mock_db_cursor_postgres,
):
    # get empty db fixture
    mockdb_cursor = prepared_mock_db_cursor_postgres

    # check for test passed column existent (should not exist)
    mockdb_cursor.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'test_metrics'"
    )
    columns = mockdb_cursor.fetchall()
    has_test_column = any(column[0] == "test_passed" for column in columns)
    assert not has_test_column

    # attach new PostgresHandler
    pghandler = PostgresDBHandler()

    # check for test passed column again (should exist now, auto-migration feature)
    mockdb_cursor.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'test_metrics'"
    )
    columns = mockdb_cursor.fetchall()
    has_test_column = any(column[0] == "test_passed" for column in columns)
    assert has_test_column

    # check for default value TRUE in existing entry
    mockdb_cursor.execute("SELECT TEST_PASSED FROM TEST_METRICS LIMIT 1")
    default_is_passed = mockdb_cursor.fetchone()

    # default value true(1) for entries after migration
    assert default_is_passed[0] == 1
    pghandler.__cnx.close()


def test_postgres_handler_check_new_db_setup(connected_PostgresDBHandler):
    db = connected_PostgresDBHandler
    columns = db.query(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'test_metrics'",
        (),
        many=True,
    )
    assert any(column[0] == "test_passed" for column in columns)

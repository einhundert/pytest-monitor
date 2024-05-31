# -*- coding: utf-8 -*-
import sqlite3


def test_sqlite_handler_check_create_test_passed_column(pytester):
    import datetime

    from pytest_monitor.handler import SqliteDBHandler as DBHandler
    from pytest_monitor.sys_utils import determine_scm_revision

    # import os

    def prepare_mock_db(conn: sqlite3.Connection):
        cursor = conn.cursor()
        cursor.execute(
            """
CREATE TABLE IF NOT EXISTS TEST_SESSIONS(
    SESSION_H varchar(64) primary key not null unique, -- Session identifier
    RUN_DATE varchar(64), -- Date of test run
    SCM_ID varchar(128), -- SCM change id
    RUN_DESCRIPTION json
);"""
        )
        cursor.execute(
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

        cursor.execute(
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
        conn.commit()
        return conn

    # open database in memory
    mockdb = sqlite3.connect(":memory:")
    # prepare old database format
    mockdb = prepare_mock_db(mockdb)

    # attach mocked db to DBHandler object
    db = DBHandler(":memory:")
    db.__cnx = mockdb
    db._DBHandler__cnx = mockdb
    db._DBHandler__db = "mockdb"

    # insert old style entry
    run_date = datetime.datetime.now().isoformat()
    db.insert_session("1", run_date, determine_scm_revision(), "Test Session")
    db.__cnx.cursor().execute(
        "insert into TEST_METRICS(SESSION_H,ENV_H,ITEM_START_TIME,ITEM,"
        "ITEM_PATH,ITEM_VARIANT,ITEM_FS_LOC,KIND,COMPONENT,TOTAL_TIME,"
        "USER_TIME,KERNEL_TIME,CPU_USAGE,MEM_USAGE) "
        "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "1",
            "Environment",
            "Startdate",
            "name of item",
            "Item path",
            "Optional Param",
            "relative path",
            "NULL",
            "NULL",
            42,
            42,
            42,
            42,
            42,
        ),
    )
    db.__cnx.commit()

    mcursor = db.__cnx.cursor()
    mcursor.execute("PRAGMA table_info(TEST_METRICS)")
    has_test_column = any(column[1] == "TEST_PASSED" for column in mcursor.fetchall())
    mcursor = None

    try:
        assert not has_test_column

        # run function to test
        db.check_create_test_passed_column()

        # check for new column
        mcursor = db.__cnx.cursor()
        mcursor.execute("PRAGMA table_info(TEST_METRICS)")
        has_test_column = any(column[1] == "TEST_PASSED" for column in mcursor.fetchall())
        assert has_test_column

        # check for default value TRUE in existing entry
        mcursor.execute("SELECT TEST_PASSED FROM TEST_METRICS LIMIT 1")
        default_is_passed = mcursor.fetchone()

        # default value true(1) for entries after migration
        assert default_is_passed[0] == 1

    except Exception:
        raise


def test_sqlite_handler_check_new_db_setup(pytester):
    from pytest_monitor.handler import SqliteDBHandler as DBHandler

    # db handler
    db = DBHandler(":memory:")
    table_cols = db.query("PRAGMA table_info(TEST_METRICS)", (), many=True)
    assert any(column[1] == "TEST_PASSED" for column in table_cols)

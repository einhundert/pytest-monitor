# -*- coding: utf-8 -*-
import json
import pathlib
import sqlite3

import pytest


def test_monitor_basic_test(testdir):
    """Make sure that pytest-monitor does the job without impacting user tests."""
    # create a temporary pytest test module
    testdir.makepyfile(
        """
    import time


    def test_ok():
        time.sleep(0.5)
        x = ['a' * i for i in range(100)]
        assert len(x) == 100

"""
    )

    # run pytest with the following cmd args
    result = testdir.runpytest("-vv", "--tag", "version=12.3.5")

    # fnmatch_lines does an assertion internally
    result.stdout.fnmatch_lines(["*::test_ok PASSED*"])

    pymon_path = pathlib.Path(str(testdir)) / ".pymon"
    assert pymon_path.exists()

    # make sure that that we get a '0' exit code for the test suite
    result.assert_outcomes(passed=1)

    db = sqlite3.connect(str(pymon_path))
    cursor = db.cursor()
    cursor.execute("SELECT ITEM FROM TEST_METRICS;")
    assert len(cursor.fetchall()) == 1
    cursor = db.cursor()
    tags = json.loads(cursor.execute("SELECT RUN_DESCRIPTION FROM TEST_SESSIONS;").fetchone()[0])
    assert "description" not in tags
    assert "version" in tags
    assert tags["version"] == "12.3.5"


def test_monitor_basic_test_description(testdir):
    """Make sure that pytest-monitor does the job without impacting user tests."""
    # create a temporary pytest test module
    testdir.makepyfile(
        """
    import time


    def test_ok():
        time.sleep(0.5)
        x = ['a' * i for i in range(100)]
        assert len(x) == 100

"""
    )

    # run pytest with the following cmd args
    result = testdir.runpytest("-vv", "--description", '"Test"', "--tag", "version=12.3.5")

    # fnmatch_lines does an assertion internally
    result.stdout.fnmatch_lines(["*::test_ok PASSED*"])

    pymon_path = pathlib.Path(str(testdir)) / ".pymon"
    assert pymon_path.exists()

    # make sure that that we get a '0' exit code for the test suite
    result.assert_outcomes(passed=1)

    db = sqlite3.connect(str(pymon_path))
    cursor = db.cursor()
    cursor.execute("SELECT ITEM FROM TEST_METRICS;")
    assert len(cursor.fetchall()) == 1
    cursor = db.cursor()
    tags = json.loads(cursor.execute("SELECT RUN_DESCRIPTION FROM TEST_SESSIONS;").fetchone()[0])
    assert "description" in tags
    assert tags["description"] == '"Test"'
    assert "version" in tags
    assert tags["version"] == "12.3.5"


def test_monitor_pytest_skip_marker(testdir):
    """Make sure that pytest-monitor does the job without impacting user tests."""
    # create a temporary pytest test module
    testdir.makepyfile(
        """
    import pytest
    import time

    @pytest.mark.skip("Some reason")
    def test_skipped():
        assert True

"""
    )

    # run pytest with the following cmd args
    result = testdir.runpytest("-v")

    # fnmatch_lines does an assertion internally
    result.stdout.fnmatch_lines(["*::test_skipped SKIPPED*"])

    pymon_path = pathlib.Path(str(testdir)) / ".pymon"
    assert pymon_path.exists()

    # make sure that that we get a '0' exit code for the testsuite
    result.assert_outcomes(skipped=1)

    db = sqlite3.connect(str(pymon_path))
    cursor = db.cursor()
    cursor.execute("SELECT ITEM FROM TEST_METRICS;")
    assert not len(cursor.fetchall())


def test_monitor_pytest_skip_marker_on_fixture(testdir):
    """Make sure that pytest-monitor does the job without impacting user tests."""
    # create a temporary pytest test module
    testdir.makepyfile(
        """
    import pytest
    import time

    @pytest.fixture
    def a_fixture():
        pytest.skip("because this is the scenario being tested")

    def test_skipped(a_fixture):
        assert True

"""
    )

    # run pytest with the following cmd args
    result = testdir.runpytest("-v")

    # fnmatch_lines does an assertion internally
    result.stdout.fnmatch_lines(["*::test_skipped SKIPPED*"])

    pymon_path = pathlib.Path(str(testdir)) / ".pymon"
    assert pymon_path.exists()

    # make sure that that we get a '0' exit code for the testsuite
    result.assert_outcomes(skipped=1)

    db = sqlite3.connect(str(pymon_path))
    cursor = db.cursor()
    cursor.execute("SELECT ITEM FROM TEST_METRICS;")
    assert not len(cursor.fetchall())


def test_bad_markers(testdir):
    """Make sure that pytest-monitor warns about unknown markers."""
    # create a temporary pytest test module
    testdir.makepyfile(
        """
        import pytest
        import time


        @pytest.mark.monitor_bad_marker
        def test_ok():
            time.sleep(0.1)
            x = ['a' * i for i in range(100)]
            assert len(x) == 100

    """
    )

    # run pytest with the following cmd args
    result = testdir.runpytest("-v")

    # fnmatch_lines does an assertion internally
    result.stdout.fnmatch_lines(["*::test_ok PASSED*", "*Nothing known about marker monitor_bad_marker*"])

    pymon_path = pathlib.Path(str(testdir)) / ".pymon"
    assert pymon_path.exists()

    # make sure that that we get a '0' exit code for the testsuite
    result.assert_outcomes(passed=1)

    db = sqlite3.connect(str(pymon_path))
    cursor = db.cursor()
    cursor.execute("SELECT ITEM FROM TEST_METRICS;")
    assert len(cursor.fetchall()) == 1


def test_monitor_skip_module(testdir):
    """Make sure that pytest-monitor correctly understand the monitor_skip_test marker."""
    # create a temporary pytest test module
    testdir.makepyfile(
        """
import pytest
import time

pytestmark = pytest.mark.monitor_skip_test

def test_ok_not_monitored():
    time.sleep(0.1)
    x = ['a' * i for i in range(100)]
    assert len(x) == 100

def test_another_function_ok_not_monitored():
    assert True
"""
    )

    # run pytest with the following cmd args
    result = testdir.runpytest("-v")

    # fnmatch_lines does an assertion internally
    result.stdout.fnmatch_lines(
        [
            "*::test_ok_not_monitored PASSED*",
            "*::test_another_function_ok_not_monitored PASSED*",
        ]
    )

    pymon_path = pathlib.Path(str(testdir)) / ".pymon"
    assert pymon_path.exists()

    # make sure that that we get a '0' exit code for the testsuite
    result.assert_outcomes(passed=2)

    db = sqlite3.connect(str(pymon_path))
    cursor = db.cursor()
    cursor.execute("SELECT ITEM FROM TEST_METRICS;")
    assert not len(cursor.fetchall())  # Nothing ran


def test_monitor_skip_test(testdir):
    """Make sure that pytest-monitor correctly understand the monitor_skip_test marker."""
    # create a temporary pytest test module
    testdir.makepyfile(
        """
    import pytest
    import time


    @pytest.mark.monitor_skip_test
    def test_not_monitored():
        time.sleep(0.1)
        x = ['a' * i for i in range(100)]
        assert len(x) == 100

"""
    )

    # run pytest with the following cmd args
    result = testdir.runpytest("-v")

    # fnmatch_lines does an assertion internally
    result.stdout.fnmatch_lines(["*::test_not_monitored PASSED*"])

    pymon_path = pathlib.Path(str(testdir)) / ".pymon"
    assert pymon_path.exists()

    # make sure that that we get a '0' exit code for the testsuite
    result.assert_outcomes(passed=1)

    db = sqlite3.connect(str(pymon_path))
    cursor = db.cursor()
    cursor.execute("SELECT ITEM FROM TEST_METRICS;")
    assert not len(cursor.fetchall())  # nothing monitored


def test_monitor_skip_test_if(testdir):
    """Make sure that pytest-monitor correctly understand the monitor_skip_test_if marker."""
    # create a temporary pytest test module
    testdir.makepyfile(
        """
    import pytest
    import time


    @pytest.mark.monitor_skip_test_if(True)
    def test_not_monitored():
        time.sleep(0.1)
        x = ['a' * i for i in range(100)]
        assert len(x) == 100


    @pytest.mark.monitor_skip_test_if(False)
    def test_monitored():
        time.sleep(0.1)
        x = ['a' *i for i in range(100)]
        assert len(x) == 100

"""
    )

    # run pytest with the following cmd args
    result = testdir.runpytest("-v")

    # fnmatch_lines does an assertion internally
    result.stdout.fnmatch_lines(["*::test_not_monitored PASSED*", "*::test_monitored PASSED*"])

    pymon_path = pathlib.Path(str(testdir)) / ".pymon"
    assert pymon_path.exists()

    # make sure that that we get a '0' exit code for the testsuite
    result.assert_outcomes(passed=2)

    db = sqlite3.connect(str(pymon_path))
    cursor = db.cursor()
    cursor.execute("SELECT ITEM FROM TEST_METRICS;")
    assert len(cursor.fetchall()) == 1


def test_monitor_no_db(testdir):
    """Make sure that pytest-monitor correctly understand the monitor_skip_test_if marker."""
    # create a temporary pytest test module
    testdir.makepyfile(
        """
    import pytest
    import time


    def test_it():
        time.sleep(0.1)
        x = ['a' * i for i in range(100)]
        assert len(x) == 100


    def test_that():
        time.sleep(0.1)
        x = ['a' *i for i in range(100)]
        assert len(x) == 100

"""
    )

    wrn = "pytest-monitor: No storage specified but monitoring is requested. Disabling monitoring."
    with pytest.warns(UserWarning, match=wrn):
        # run pytest with the following cmd args
        result = testdir.runpytest("--no-db", "-v")

    # fnmatch_lines does an assertion internally
    result.stdout.fnmatch_lines(["*::test_it PASSED*", "*::test_that PASSED*"])

    pymon_path = pathlib.Path(str(testdir)) / ".pymon"
    assert not pymon_path.exists()

    # make sure that that we get a '0' exit code for the testsuite
    result.assert_outcomes(passed=2)


def test_monitor_basic_output(testdir):
    """Make sure that pytest-monitor does not repeat captured output (issue #26)."""
    # create a temporary pytest test module
    testdir.makepyfile(
        """
        def test_it():
            print('Hello World')
    """
    )

    wrn = "pytest-monitor: No storage specified but monitoring is requested. Disabling monitoring."
    with pytest.warns(UserWarning, match=wrn):
        # run pytest with the following cmd args
        result = testdir.runpytest("--no-db", "-s", "-vv")

    # fnmatch_lines does an assertion internally
    result.stdout.fnmatch_lines(["*::test_it Hello World*"])
    assert "Hello World" != result.stdout.get_lines_after("*Hello World")[0]

    # make sure that that we get a '0' exit code for the testsuite
    result.assert_outcomes(passed=1)


def test_monitor_with_doctest(testdir):
    """Make sure that pytest-monitor does not fail to run doctest."""
    # create a temporary pytest test module
    testdir.makepyfile(
        '''
        def run(a, b):
            """
            >>> run(3, 30)
            33
            """
            return a + b
    '''
    )

    # run pytest with the following cmd args
    result = testdir.runpytest("--doctest-modules", "-vv")

    # make sure that that we get a '0' exit code for the testsuite
    result.assert_outcomes(passed=1)
    pymon_path = pathlib.Path(str(testdir)) / ".pymon"
    assert pymon_path.exists()

    db = sqlite3.connect(str(pymon_path))
    cursor = db.cursor()
    cursor.execute("SELECT ITEM FROM TEST_METRICS;")
    assert not len(cursor.fetchall())

    pymon_path.unlink()
    result = testdir.runpytest("--doctest-modules", "--no-monitor", "-vv")

    # make sure that that we get a '0' exit code for the testsuite
    result.assert_outcomes(passed=1)
    assert not pymon_path.exists()


def test_monitor_DBHandler_check_create_test_passed_column(pytester):
    import datetime

    from pytest_monitor.handler import DBHandler
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


def test_monitor_DBHandler_check_new_db_setup(pytester):
    from pytest_monitor.handler import DBHandler

    # db handler
    db = DBHandler(":memory:")
    table_cols = db.query("PRAGMA table_info(TEST_METRICS)", (), many=True)
    assert any(column[1] == "TEST_PASSED" for column in table_cols)    

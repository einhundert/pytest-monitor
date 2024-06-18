import os

import pytest

from pytest_monitor.session import PyTestMonitorSession


@pytest.fixture()
def _setup_environment_postgres():
    os.environ["PYTEST_MONITOR_DB_NAME"] = "postgres"
    os.environ["PYTEST_MONITOR_DB_USER"] = "postgres"
    os.environ["PYTEST_MONITOR_DB_PASSWORD"] = "testing_db"
    os.environ["PYTEST_MONITOR_DB_HOST"] = "localhost"
    os.environ["PYTEST_MONITOR_DB_PORT"] = "5432"


def test_pytestmonitorsession_close_connection(_setup_environment_postgres):
    session = PyTestMonitorSession(":memory:")
    db = session._PyTestMonitorSession__db

    try:
        db.query("SELECT * FROM sqlite_master LIMIT 1", ())
    except Exception:
        pytest.fail("Database should be available")

    session.close()

    try:
        db.query("SELECT * FROM sqlite_master LIMIT 1", ())
        assert False
    except Exception:
        assert True

    session = PyTestMonitorSession(use_postgres=True)
    db = session._PyTestMonitorSession__db
    assert db._PostgresDBHandler__cnx.closed == 0
    session.close()
    assert db._PostgresDBHandler__cnx.closed > 0

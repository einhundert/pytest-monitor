import os
import sqlite3

try:
    import psycopg
except ImportError:
    import psycopg2 as psycopg


class SqliteDBHandler:
    def __init__(self, db_path):
        self.__db = db_path
        self.__cnx = sqlite3.connect(self.__db) if db_path else None
        self.prepare()
        # check if new table column is existent, if not create it
        self.check_create_test_passed_column()

    def check_create_test_passed_column(self):
        cursor = self.__cnx.cursor()
        # check for test_passed column,
        # table exists bc call happens after prepare()
        cursor.execute("PRAGMA table_info(TEST_METRICS)")
        has_test_column = any(
            column[1] == "TEST_PASSED" for column in cursor.fetchall()
        )
        if not has_test_column:
            cursor.execute(
                "ALTER TABLE TEST_METRICS ADD COLUMN TEST_PASSED BOOLEAN DEFAULT TRUE;"
            )
            self.__cnx.commit()

    def close(self):
        self.__cnx.close()

    def __del__(self):
        self.__cnx.close()

    def query(self, what, bind_to, many=False):
        cursor = self.__cnx.cursor()
        cursor.execute(what, bind_to)
        return cursor.fetchall() if many else cursor.fetchone()

    def insert_session(self, h, run_date, scm_id, description):
        self.__cnx.execute(
            "insert into TEST_SESSIONS(SESSION_H, RUN_DATE, SCM_ID, RUN_DESCRIPTION)"
            " values (?,?,?,?)",
            (h, run_date, scm_id, description),
        )
        self.__cnx.commit()

    def insert_metric(
        self,
        session_id,
        env_id,
        item_start_date,
        item,
        item_path,
        item_variant,
        item_loc,
        kind,
        component,
        total_time,
        user_time,
        kernel_time,
        cpu_usage,
        mem_usage,
        passed: bool,
    ):
        self.__cnx.execute(
            "insert into TEST_METRICS(SESSION_H,ENV_H,ITEM_START_TIME,ITEM,"
            "ITEM_PATH,ITEM_VARIANT,ITEM_FS_LOC,KIND,COMPONENT,TOTAL_TIME,"
            "USER_TIME,KERNEL_TIME,CPU_USAGE,MEM_USAGE,TEST_PASSED) "
            "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                session_id,
                env_id,
                item_start_date,
                item,
                item_path,
                item_variant,
                item_loc,
                kind,
                component,
                total_time,
                user_time,
                kernel_time,
                cpu_usage,
                mem_usage,
                passed,
            ),
        )
        self.__cnx.commit()

    def insert_execution_context(self, exc_context):
        env_h = exc_context.compute_hash()
        self.__cnx.execute(
            "insert into EXECUTION_CONTEXTS(CPU_COUNT,CPU_FREQUENCY_MHZ,CPU_TYPE,CPU_VENDOR,"
            "RAM_TOTAL_MB,MACHINE_NODE,MACHINE_TYPE,MACHINE_ARCH,SYSTEM_INFO,"
            "PYTHON_INFO,ENV_H) SELECT ?,?,?,?,?,?,?,?,?,?,?"
            " WHERE NOT EXISTS (SELECT 1 FROM EXECUTION_CONTEXTS WHERE ENV_H = ?)",
            (
                exc_context.cpu_count,
                exc_context.cpu_frequency,
                exc_context.cpu_type,
                exc_context.cpu_vendor,
                exc_context.ram_total,
                exc_context.fqdn,
                exc_context.machine,
                exc_context.architecture,
                exc_context.system_info,
                exc_context.python_info,
                env_h,
                env_h,
            ),
        )
        self.__cnx.commit()

    def prepare(self):
        cursor = self.__cnx.cursor()
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
    TEST_PASSED boolean, -- boolean indicating if test passed
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
        self.__cnx.commit()

    def get_env_id(self, env_hash):
        query_result = self.query(
            "SELECT ENV_H FROM EXECUTION_CONTEXTS WHERE ENV_H= ?", (env_hash,)
        )
        return query_result[0] if query_result else None


class PostgresDBHandler:
    def __init__(self):
        self.__db = os.getenv("PYTEST_MONITOR_DB_NAME")
        if not self.__db:
            raise Exception(
                "Please provide the postgres db name using the PYTEST_MONITOR_DB_NAME environment variable."
            )
        self.__user = os.getenv("PYTEST_MONITOR_DB_USER")
        if not self.__user:
            raise Exception(
                "Please provide the postgres user name using the PYTEST_MONITOR_DB_USER environment variable."
            )
        self.__password = os.getenv("PYTEST_MONITOR_DB_PASSWORD")
        if not self.__password:
            raise Exception(
                "Please provide the postgres user password using the PYTEST_MONITOR_DB_PASSWORD environment variable."
            )
        self.__host = os.getenv("PYTEST_MONITOR_DB_HOST")
        if not self.__host:
            raise Exception(
                "Please provide the postgres hostname using the PYTEST_MONITOR_DB_HOST environment variable."
            )
        self.__port = os.getenv("PYTEST_MONITOR_DB_PORT")
        if not self.__port:
            raise Exception(
                "Please provide the postgres port using the PYTEST_MONITOR_DB_PORT environment variable."
            )
        self.__cnx = self.connect()
        self.prepare()
        self.check_create_test_passed_column()

    def check_create_test_passed_column(self):
        cursor = self.__cnx.cursor()
        # check for test_passed column,
        # table exists bc call happens after prepare()
        cursor.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'test_metrics'"
        )
        columns = cursor.fetchall()
        has_test_column = any(column[0] == "test_passed" for column in columns)
        if not has_test_column:
            cursor.execute(
                "ALTER TABLE TEST_METRICS ADD COLUMN TEST_PASSED BOOLEAN DEFAULT TRUE;"
            )
            self.__cnx.commit()

    def close(self):
        self.__cnx.close()

    def __del__(self):
        self.__cnx.close()

    def connect(self):
        connection_string = (
            f"dbname='{self.__db}' user='{self.__user}' password='{self.__password}' "
            + f"host='{self.__host}' port='{self.__port}'"
        )
        return psycopg.connect(connection_string)

    def query(self, what, bind_to, many=False):
        cursor = self.__cnx.cursor()
        cursor.execute(what, bind_to)
        return cursor.fetchall() if many else cursor.fetchone()

    def insert_session(self, h, run_date, scm_id, description):
        self.__cnx.cursor().execute(
            "insert into TEST_SESSIONS(SESSION_H, RUN_DATE, SCM_ID, RUN_DESCRIPTION)"
            " values (%s,%s,%s,%s)",
            (h, run_date, scm_id, description),
        )
        self.__cnx.commit()

    def insert_metric(
        self,
        session_id,
        env_id,
        item_start_date,
        item,
        item_path,
        item_variant,
        item_loc,
        kind,
        component,
        total_time,
        user_time,
        kernel_time,
        cpu_usage,
        mem_usage,
        passed: bool,
    ):
        self.__cnx.cursor().execute(
            "insert into TEST_METRICS(SESSION_H,ENV_H,ITEM_START_TIME,ITEM,"
            "ITEM_PATH,ITEM_VARIANT,ITEM_FS_LOC,KIND,COMPONENT,TOTAL_TIME,"
            "USER_TIME,KERNEL_TIME,CPU_USAGE,MEM_USAGE,TEST_PASSED) "
            "values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                session_id,
                env_id,
                item_start_date,
                item,
                item_path,
                item_variant,
                item_loc,
                kind,
                component,
                total_time,
                user_time,
                kernel_time,
                cpu_usage,
                mem_usage,
                passed,
            ),
        )
        self.__cnx.commit()

    def insert_execution_context(self, exc_context):
        env_h = exc_context.compute_hash()
        self.__cnx.cursor().execute(
            "insert into EXECUTION_CONTEXTS(CPU_COUNT,CPU_FREQUENCY_MHZ,CPU_TYPE,CPU_VENDOR,"
            "RAM_TOTAL_MB,MACHINE_NODE,MACHINE_TYPE,MACHINE_ARCH,SYSTEM_INFO,"
            "PYTHON_INFO,ENV_H) SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s"
            " WHERE NOT EXISTS (SELECT 1 FROM EXECUTION_CONTEXTS WHERE ENV_H = %s)",
            (
                exc_context.cpu_count,
                exc_context.cpu_frequency,
                exc_context.cpu_type,
                exc_context.cpu_vendor,
                exc_context.ram_total,
                exc_context.fqdn,
                exc_context.machine,
                exc_context.architecture,
                exc_context.system_info,
                exc_context.python_info,
                env_h,
                env_h,
            ),
        )
        self.__cnx.commit()

    def prepare(self):
        cursor = self.__cnx.cursor()
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
    TEST_PASSED boolean, -- boolean indicating if test passed
    FOREIGN KEY (ENV_H) REFERENCES EXECUTION_CONTEXTS(ENV_H),
    FOREIGN KEY (SESSION_H) REFERENCES TEST_SESSIONS(SESSION_H)
);"""
        )

        self.__cnx.commit()

    def get_env_id(self, env_hash):
        query_result = self.query(
            "select ENV_H from EXECUTION_CONTEXTS where ENV_H = %s", (env_hash,)
        )
        return query_result[0] if query_result else None

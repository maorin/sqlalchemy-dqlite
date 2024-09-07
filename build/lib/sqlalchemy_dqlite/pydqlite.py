from __future__ import absolute_import

from sqlalchemy.dialects.sqlite.base import SQLiteDialect, DATETIME, DATE
from sqlalchemy import exc, pool
from sqlalchemy import types as sqltypes
from sqlalchemy import util
from sqlalchemy import expression

import os


class _SQLite_dqliteTimeStamp(DATETIME):
    def bind_processor(self, dialect):
        if dialect.native_datetime:
            return None
        else:
            return DATETIME.bind_processor(self, dialect)

    def result_processor(self, dialect, coltype):
        if dialect.native_datetime:
            return None
        else:
            return DATETIME.result_processor(self, dialect, coltype)


class _SQLite_dqliteDate(DATE):
    def bind_processor(self, dialect):
        if dialect.native_datetime:
            return None
        else:
            return DATE.bind_processor(self, dialect)

    def result_processor(self, dialect, coltype):
        if dialect.native_datetime:
            return None
        else:
            return DATE.result_processor(self, dialect, coltype)


class SQLiteDialect_dqlite(SQLiteDialect):
    default_paramstyle = 'qmark'

    colspecs = util.update_copy(
        SQLiteDialect.colspecs,
        {
            sqltypes.Date: _SQLite_dqliteDate,
            sqltypes.TIMESTAMP: _SQLite_dqliteTimeStamp,
        }
    )

    if not util.py2k:
        description_encoding = None

    driver = 'pydqlite'

    # pylint: disable=method-hidden
    @classmethod
    def dbapi(cls):
        try:
            # pylint: disable=no-name-in-module
            from pydqlite import dbapi2 as sqlite
            #from sqlite3 import dbapi2 as sqlite  # try 2.5+ stdlib name.
        except ImportError:
            #raise e
            raise
        return sqlite

    @classmethod
    def get_pool_class(cls, url):
        if url.database and url.database != ':memory:':
            return pool.NullPool
        else:
            return pool.SingletonThreadPool
    
    # def _check_unicode_returns(self, connection, additional_tests=None):
    #     # 定义了一组 Unicode 检查的测试
    #     tests = [
    #         expression.cast(expression.literal_column("'test plain returns'"), sqltypes.VARCHAR(60)),
    #         expression.cast(expression.literal_column("'test unicode returns'"), sqltypes.Unicode(60)),
    #     ]
        
    #     return False

    
    def create_connect(self, *args, **kwargs):
        # 你的连接代码
        from pydqlite import dbapi2 as sqlite
        return sqlite.connect(
            host='192.168.214.101',
            port=9001,
            database='hci_db'
        )
        
    def create_connect_args(self, url):
        opts = url.query.copy()
        util.coerce_kw_type(opts, 'connect_timeout', float)
        util.coerce_kw_type(opts, 'detect_types', int)
        util.coerce_kw_type(opts, 'max_redirects', int)
        opts['port'] = url.port
        opts['host'] = url.host
        
        if url.username:
            opts['user'] = url.username

        if url.password:
            opts['password'] = url.password

        return ([], opts)
    
    def get_isolation_level(self, connection):
        # 执行 PRAGMA 读取未提交状态
        result = connection.execute("PRAGMA read_uncommitted").fetchone()
        print(f"Isolation level: {result[0]}")
            # 将结果转换为字符串并去除空格和换行符
        result_str = str(result[0]).strip()
        
        if result_str == "0":
            return "READ COMMITTED"
        elif result_str == "1":
            return "READ UNCOMMITTED"
        else:
            raise AssertionError(f"Unknown isolation level {result[0]}")


    def do_execute(self, cursor, statement, parameters, context=None):
        # 执行查询并确保游标能够正常处理
        # 执行 SQL 查询并返回游标
        print(f"Executing: {statement}")
        cursor.execute(statement, parameters or ())
        print(f"Cursor3333333: {cursor}")

    def do_fetchall(self, cursor):
        # 提供 fetchall 的标准接口
        print(f"Cursor22222: {cursor}")
        return cursor.fetchall()

    def do_fetchone(self, cursor):
        # 提供 fetchone 的标准接口
        print(f"Cursor111111: {cursor}")
        return cursor.fetchone()

    def do_rollback(self, connection):
        # 处理事务回滚
        print("Rolling back in do_rollback ......")
        connection.rollback()
    
    def do_close(self, cursor):
        print("Closing cursor")
        cursor.close()
        
    def is_disconnect(self, e, connection, cursor):
        return False

dialect = SQLiteDialect_dqlite

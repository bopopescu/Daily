# coding:utf8
import re
import pymysql
#from ..error.exceptions import MySQLDBException

JYDB = {
    'NAME': 'sqlname',
    'HOST': '***.***.***.***',
    'PORT': 3306,
    'USER': 'user',
    'PASSWORD': 'passwd',
    'CHARSET': 'gbk'
}


class MyDictCursor(pymysql.cursors.DictCursor):
    pass


class MySQLConnection():
    """
      connection that can specify the cursor type
    """
    dbpool_args = dict(use_unicode=True, init_command='SET time_zone = "+8:00"',sql_mode="TRADITIONAL")
    dbpool_args["host"] = JYDB['HOST']
    dbpool_args["port"] = JYDB['PORT']
    dbpool_args["user"] = JYDB['USER']
    dbpool_args["passwd"] = JYDB['PASSWORD']
    dbpool_args["db"] = JYDB['NAME']
    dbpool_args["charset"] = JYDB['CHARSET']
    dbpool_args["use_unicode"] = False


    def __init__(self,sharable = False, args=None):
        self.cursor_type = None
        self.sharable = 1 if sharable else 0
        self._db = None
        self.args = args

    def __del__(self):
        self.close()

    def close(self):
        """Closes this database connection."""
        if getattr(self, "_db", None) is not None:
            cursor = self._cursor()
            cursor.close()
            cursor = None
            self._db.close()
            self._db = None

    def set_cursor_type(self,cursor_type):
        self.cursor_type = cursor_type


    def _ensure_connected(self):
        if self._db is None:
            if not self.args:
                self.reconnect(self.dbpool_args)
            else:
                self.reconnect(self.args)

    def _cursor(self):
        self._ensure_connected()
        return self._db.cursor(self.cursor_type) if self.cursor_type else self._db.cursor()

    def cursor(self):
        self._ensure_connected()
        return self._db.cursor(self.cursor_type) if self.cursor_type else self._db.cursor()

    def set_sharable(self,sharable):
        self.sharable = 1 if sharable else 0


    def reconnect(self, args=None):
        self.close()
        try:
            #self._db = MySQLConnection._pool.connection(self.sharable)
            self._db = pymysql.connect(host=args["host"],
                                          port=args["port"],
                                          user=args["user"],
                                          password=args["passwd"],
                                          database=args["db"],
                                          charset=args["charset"]
                                       )
            # !!!Notice: in multi-thread environment, donot use 'pool.connection().cursor().execute(...)'
            # it will cause the pool to release the connection earlier
            cursor = self._db.cursor()
            if not self.validate_conn(cursor.connection):
                #log_text('------lost server connection,try to reconnect------')
                self._db._con.close()
                self._db._con._store(self._db._con._create())
                cursor = self._db.cursor()
            cursor.connection.autocommit(1)
        except Exception as e:
            raise e

    def validate_conn(self,raw_con):
        """
         Validate the connection:
          in some case,like wire connection is out or mysql close the connection, the db pool cannot
          recover from that automatically
        """
        try:
            raw_con.ping()
            return True
        except Exception:
            return False

    def auto_commit(self, param = 1):
        self._ensure_connected()
        cursor = self._db.cursor()
        if param:
            cursor.connection.autocommit(1)
        else:
            cursor.connection.autocommit(0)

    def commit(self):
        if self._db:
            self._db.commit()
    def rollback(self):
        if self._db:
            self._db.rollback()



class MySQLDBService():
    """
     MySQL database access wrapper
     it can be used with 'With' statement, so it's transactional
    """
    def __init__(self, sharable=False, args=None):
        self.connection = None
        self.args = args
        # specify whether the connection can be shared between threads, some operation like
        # last_row_id() is connection-based, so shared connection may cause problem
        self.sharable = sharable

    def __enter__(self):
        self.init_connection(auto_commit=False)
        return self

    def __exit__(self, type, value, traceback):
        self.release_connection(value is None)
        return False

    def init_connection(self,auto_commit = True):
        if self.connection is None:
            self.connection = MySQLConnection(self.sharable, self.args)
            self.connection.auto_commit(1 if auto_commit else 0)

    def release_connection(self,commit = True):
        if self.connection:
            if commit:
                self.connection.commit()
            else:
                self.connection.rollback()

            self.connection.close()
            self.connection = None

    def close(self):
        self.connection.close()

    def get_ver(self):
        return self.fetch_one("select version()")

    def fetch_one(self,sql,params=None):
        """
         excute the sql and fetch one result
         @ sql: sql statement
         @ params: sql params
         = returns dict
        """
        # it may throw an exception
        self.init_connection()
        result = None
        try:
            self.connection.set_cursor_type(MyDictCursor)
            cursor = self.connection._cursor()
            count = cursor.execute(sql,params)
            if count > 0:
                result = cursor.fetchone()
        except pymysql.Error as e:
            pass
            #print(e)
            #raise MySQLDBException(-251003,"%d: %s"%(e.args[0],e.args[1]))
        finally:
            pass
        return result

    def get_connection(self):
        self.init_connection()
        #self.connection.set_cursor_type(MyDictCursor)
        return self.connection


    def fetch_all(self,sql,params=None):
        """
         execute the sql and fetch all result
         @ sql: sql statement
         @ params: sql params
         = returns dict
        """
        # it may throw an exception
        self.init_connection()
        #
        result = None
        try:
            self.connection.set_cursor_type(MyDictCursor)
            cursor = self.connection._cursor()
            count = cursor.execute(sql,params)
            if count > 0:
                result = cursor.fetchall()
        except pymysql.Error as e:
            pass
            # print(e)
            #raise MySQLDBException(-251004,"%d: %s"%(e.args[0],e.args[1]))
        finally:
            pass
        return result

    def fetch_many(self,sql,num,params=None):
        """
         execute the sql and fetch num result
         @ sql: sql statement
         @ params: sql params
         = returns dict
        """
        # it may throw an exception
        self.init_connection()
        #
        result = None
        try:
            self.connection.set_cursor_type(MyDictCursor)
            cursor = self.connection._cursor()
            count = cursor.execute(sql,params)
            if count > 0:
                result = cursor.fetchmany(size=num)
        except pymysql.Error as e:
            pass
            #print(e)
            #raise MySQLDBException(-251005,"%d: %s"%(e.args[0],e.args[1]))
        finally:
            pass
        return result


    def execute(self,sql,params=None):
        """
         execute the sql and returns affected row count
         @ sql: sql statement
         = returns row count
        """
        # it may throw an exception
        self.init_connection()
        #
        try:
            self.connection.set_cursor_type(MyDictCursor)
            cursor = self.connection._cursor()
            count = cursor.execute(sql,params)
        except pymysql.Error as e:
            pass
            #raise MySQLDBException(-251006,"%d: %s"%(e.args[0],e.args[1]))
        finally:
            pass
        return count

    def execute_many(self,sql,params):
        """
         execute the sql many times and returns affected row count
         @ sql: sql statement
         = returns row count
        """
        # it may throw an exception
        self.init_connection()
        #
        try:
            self.connection.set_cursor_type(MyDictCursor)
            cursor = self.connection._cursor()
            count = cursor.executemany(sql, params)
        except pymysql.Error as e:
            pass
            #raise MySQLDBException(-251006,"%d: %s"%(e.args[0],e.args[1]))
        except Exception as e:
            #log_text(e.message)
            raise e
        finally:
            pass
        return count


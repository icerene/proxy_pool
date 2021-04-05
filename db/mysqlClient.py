# -*- coding: utf-8 -*-
"""
-----------------------------------------------------
   File Name：     mysqlClient.py
   Description :   添加mysql支持
   Author :        Irene
   date：          2021/4/3
------------------------------------------------------
   Change Activity:
                   2021/04/03: 添加mysql支持
------------------------------------------------------
"""
__author__ = 'Irene'

import mysql.connector
from handler.logHandler import LogHandler

class MysqlClient(object):
    """
    Mysql client
    
    """

    def __init__(self, **kwargs):
        """
        init
        :param host: host
        :param port: port
        :param user: username
        :param password: password
        :param database: db
        :return:
        """
        self.name = ""
        self.db_name = kwargs['db']
        kwargs.pop("port")
        self.host = kwargs['host']
        self.user = kwargs['username']
        self.password = kwargs['password']
        """
        :field proxy: varchar(30)
        :field fail_count: int
        :field region: varchar
        :field type: varchar
        :field source: varchar(50)
        :field check_count: 0
        :last_status: varchar
        :last_time: varchar
        """
        self.__fields_tuple = ("proxy", "fail_count", "region", "type", "source", "check_count", "last_status", "last_time")
        self.__fields_string = ", ".join(self.__fields_tuple)
        
    def connect_db(self):
        self.__mydb = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.db_name
        )
        self.__conn = self.__mydb.cursor()

    def close_db(self):
        self.__conn.close()
        self.__mydb.close()

    def get(self):
        """
        返回一个代理
        :return:
        """
        self.connect_db()
        sql = "SELECT " + self.__fields_string + " FROM " + self.name + " ORDER BY RAND() LIMIT 1"
        self.__conn.execute(sql)
        proxy = self.__conn.fetchone()
        self.close_db()
        if proxy:
            return json.dumps(dict(zip(self.__fields_tuple,proxy)))
        else:
            return False

    def put(self, proxy_obj):
        """
        将代理放入hash, 使用changeTable指定hash name
        :param proxy_obj: Proxy obj
        :return:
        """
        self.connect_db()
        sql = "INSERT INTO " + self.db_name + " (" + self.__fields_string + ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        proxy_dict = proxy_obj.to_dict
        proxy_tuple = tuple(proxy_dict.values())
        self.__conn.execute(sql, proxy_tuple)
        self.__mydb.commit()
        rowid = self.__conn.lastrowid
        self.close_db()
        return rowid

    def pop(self):
        """
        弹出一个代理
        :return: dict {proxy: value}
        """
        self.connect_db()
        sql = "SELECT " + self.__fields_string + " FROM " + self.name + " LIMIT 1"
        self.__conn.execute(sql)
        proxy = self.__conn.fetchone()
        proxy_dict = dict(zip(self.__fields_tuple,proxy))
        if proxy:
            sql = "DELETE FROM " + self.db_name + " WHERE proxy = '" + proxy_dict.get("proxy", None) + "'"
            self.__conn.execute(sql)
            self.__mydb.commit()
            self.close_db()
            return json.dumps(proxy_dict)
        else:
            self.close_db()
            return False

    def delete(self, proxy_str):
        """
        移除指定代理, 使用changeTable指定hash name
        :param proxy_str: proxy str
        :return:
        """
        self.connect_db()
        sql = "DELETE FROM " + self.db_name + " WHERE proxy = '" + proxy_str + "'"
        self.__conn.execute(sql)
        self.__mydb.commit()
        rowcount = self.__conn.rowcount
        self.close_db()
        return rowcount

    def exists(self, proxy_str):
        """
        判断指定代理是否存在, 使用changeTable指定hash name
        :param proxy_str: proxy str
        :return:
        """
        self.connect_db()
        self.__conn.execute("SELECT count(*) FROM " + self.name + "  WHERE proxy = '" + proxy_str + "'")
        result = self.__conn.fetchone()
        self.close_db()
        count = result[0]
        return count

    def update(self, proxy_obj):
        """
        更新 proxy 属性
        :param proxy_obj:
        :return:
        """
        self.connect_db()
        sql = "UPDATE " + self.db_name + " SET proxy = %s, fail_count = %s, region = %s, type = %s, source = %s, check_count = %s, last_status = %s, last_time = %s WHERE proxy = %s"
        proxy_dict = proxy_obj.to_dict
        proxy_str = proxy_dict.get('proxy')
        proxy_tuple = tuple(proxy_dict.values())
        proxy_values = (*proxy_tuple, proxy_str)
        self.__conn.execute(sql, proxy_values)
        self.__mydb.commit()
        rowcount = self.__conn.rowcount
        self.close_db()
        return rowcount

    def getAll(self):
        """
        字典形式返回所有代理, 使用changeTable指定hash name
        :return:
        """
        self.connect_db()
        self.__conn.execute("SELECT " + self.__fields_string + " FROM " + self.name)
        results = self.__conn.fetchall()
        proxies_dict = {}
        for proxy in results:
            proxy_dict = dict(zip(self.__fields_tuple,proxy))
            proxies[proxy_dict.get('proxy')] = json.dumps(proxy_dict)
        self.close_db()
        return proxies_dict

    def clear(self):
        """
        清空所有代理, 使用changeTable指定hash name
        :return:
        """
        self.connect_db()
        sql = "TRUNCATE TABLE " + self.name
        self.__conn.execute(sql)
        self.__mydb.commit()
        rowcount = self.__conn.rowcount
        self.close_db()
        return rowcount

    def getCount(self):
        """
        返回代理数量
        :return:
        """
        self.connect_db()
        sql = "SELECT COUNT(*) FROM " + self.name
        self.__conn.execute(sql)
        result = self.__conn.fetchone()
        self.close_db()
        return result[0]

    def changeTable(self, name):
        """
        切换操作对象
        :param name:
        :return:
        """
        self.connect_db()
        self.name = name
        sql = "SELECT count(*) FROM information_schema.TABLES WHERE TABLE_NAME = %s AND TABLE_SCHEMA = %s"
        self.__conn.execute(sql, (name, self.db_name))
        result = self.__conn.fetchone()
        count = result[0]
        if 0 == count:
            self.__conn.execute("CREATE TABLE " + name + " (id INT AUTO_INCREMENT PRIMARY KEY, proxy VARCHAR(255), fail_count int unsigned, region VARCHAR(255), type VARCHAR(255), source VARCHAR(255), check_count int unsigned, last_status VARCHAR(255), last_time VARCHAR(255))")
        self.close_db()

    def test(self):
        log = LogHandler('mysql_client')
        try:
            self.getCount()
        except TimeoutError as e:
            log.error('mysql connection time out: %s' % str(e), exc_info=True)
            return e
        except ConnectionError as e:
            log.error('mysql connection error: %s' % str(e), exc_info=True)
            return e
        except mysql.connector.Error as err:
            log.error("Something went wrong: {}".format(err), exc_info=True)

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import duckdb
import pandas as pd
from typing import Optional, Dict, Any, List, Union

# 数据库文件路径
DB_FILE = "instockdb.duckdb"
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", DB_FILE)

class DuckDBManager:
    def __init__(self):
        self.conn = None
        self.connect()

    def connect(self):
        """连接到DuckDB数据库"""
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        self.conn = duckdb.connect(DB_PATH)
        
    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()
            self.conn = None

    def insert_df(self, data: pd.DataFrame, table_name: str, if_exists: str = 'append') -> None:
        """插入DataFrame到数据库表"""
        self.conn.register('temp_df', data)
        if if_exists == 'replace':
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} AS 
            SELECT * FROM temp_df WHERE 1=0
        """)
        self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")

    def query_df(self, query: str) -> pd.DataFrame:
        """执行查询并返回DataFrame"""
        return self.conn.execute(query).df()

    def execute(self, query: str) -> None:
        """执行SQL语句"""
        self.conn.execute(query)

    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        result = self.conn.execute(f"""
            SELECT count(*) 
            FROM information_schema.tables 
            WHERE table_name = '{table_name}'
        """).fetchone()[0]
        return result > 0

# 全局数据库实例
db = DuckDBManager()


# 替换 insert_db_from_df
def insert_db_from_df(data: pd.DataFrame, table_name: str, 
                     cols_type: Optional[Dict] = None,
                     write_index: bool = False,
                     primary_keys: Optional[List[str]] = None) -> None:
    """插入DataFrame到DuckDB表"""
    if write_index:
        data = data.reset_index()
    db.insert_df(data, table_name)

# 替换 executeSql
def execute_sql(sql: str) -> None:
    """执行SQL语句"""
    db.execute(sql)

# 替换 executeSqlFetch
def execute_sql_fetch(sql: str) -> pd.DataFrame:
    """执行查询并返回结果"""
    return db.query_df(sql)

# 替换 checkTableIsExist
def check_table_exists(table_name: str) -> bool:
    """检查表是否存在"""
    return db.table_exists(table_name)




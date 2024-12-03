#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import os
import duckdb
import pandas as pd
from pathlib import Path

__author__ = 'myh '
__date__ = '2023/3/10 '

# 数据库文件路径
db_file = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data', 'instockdb.duckdb')
os.makedirs(os.path.dirname(db_file), exist_ok=True)

# 使用环境变量获得数据库路径
_db_file = os.environ.get('db_file')
if _db_file is not None:
    db_file = _db_file

logging.info(f"数据库文件路径：{db_file}")

def get_connection():
    """获取数据库连接"""
    try:
        return duckdb.connect(db_file)
    except Exception as e:
        logging.error(f"database.get_connection处理异常：{e}")
    return None

def insert_db_from_df(data, table_name, cols_type, write_index, primary_keys, indexs=None):
    """插入DataFrame到数据库表"""
    insert_other_db_from_df(None, data, table_name, cols_type, write_index, primary_keys, indexs)

def insert_other_db_from_df(to_db, data, table_name, cols_type, write_index, primary_keys, indexs=None):
    """插入DataFrame到指定数据库表"""
    try:
        conn = get_connection()
        if conn is None:
            return

        # 处理索引
        if write_index and data.index.name:
            data = data.reset_index()

        # 创建表（如果不存在）
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("
        columns = []
        for col in data.columns:
            col_type = "VARCHAR"  # 默认类型
            if cols_type and col in cols_type:
                col_type = str(cols_type[col]).upper().replace("NVARCHAR", "VARCHAR")
            columns.append(f"{col} {col_type}")
        
        # 添加主键
        if primary_keys:
            columns.append(f"PRIMARY KEY ({primary_keys})")
        
        create_table_sql += ", ".join(columns) + ")"
        conn.execute(create_table_sql)

        # 插入数据
        conn.execute(f"INSERT INTO {table_name} SELECT * FROM data", {"data": data})
        conn.commit()

    except Exception as e:
        logging.error(f"database.insert_other_db_from_df处理异常：{table_name}表{e}")
    finally:
        if conn:
            conn.close()

def update_db_from_df(data, table_name, where):
    """更新数据库表"""
    try:
        conn = get_connection()
        if conn is None:
            return

        data = data.where(pd.notnull, None)
        for _, row in data.iterrows():
            update_cols = []
            where_conditions = []
            
            for col in data.columns:
                value = row[col]
                if col in where:
                    if value is None:
                        where_conditions.append(f"{col} IS NULL")
                    elif isinstance(value, str):
                        where_conditions.append(f"{col} = '{value}'")
                    else:
                        where_conditions.append(f"{col} = {value}")
                else:
                    if value is None:
                        update_cols.append(f"{col} = NULL")
                    elif isinstance(value, str):
                        update_cols.append(f"{col} = '{value}'")
                    else:
                        update_cols.append(f"{col} = {value}")
            
            if update_cols and where_conditions:
                sql = f"UPDATE {table_name} SET {', '.join(update_cols)} WHERE {' AND '.join(where_conditions)}"
                conn.execute(sql)
        
        conn.commit()
    except Exception as e:
        logging.error(f"database.update_db_from_df处理异常：{e}")
    finally:
        if conn:
            conn.close()

def checkTableIsExist(tableName):
    """检查表是否存在"""
    try:
        conn = get_connection()
        if conn is None:
            return False
            
        result = conn.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{tableName}'").fetchone()
        return result[0] > 0
    except Exception as e:
        logging.error(f"database.checkTableIsExist处理异常：{e}")
        return False
    finally:
        if conn:
            conn.close()

def executeSql(sql, params=()):
    """执行SQL语句"""
    try:
        conn = get_connection()
        if conn is None:
            return
            
        if params:
            conn.execute(sql, params)
        else:
            conn.execute(sql)
        conn.commit()
    except Exception as e:
        logging.error(f"database.executeSql处理异常：{sql}{e}")
    finally:
        if conn:
            conn.close()

def executeSqlFetch(sql, params=()):
    """执行SQL查询并返回结果"""
    try:
        conn = get_connection()
        if conn is None:
            return None
            
        if params:
            result = conn.execute(sql, params).fetchall()
        else:
            result = conn.execute(sql).fetchall()
        return result
    except Exception as e:
        logging.error(f"database.executeSqlFetch处理异常：{sql}{e}")
        return None
    finally:
        if conn:
            conn.close()

def executeSqlCount(sql, params=()):
    """执行SQL计数查询"""
    try:
        conn = get_connection()
        if conn is None:
            return 0
            
        if params:
            result = conn.execute(sql, params).fetchone()
        else:
            result = conn.execute(sql).fetchone()
        return result[0] if result else 0
    except Exception as e:
        logging.error(f"database.executeSqlCount处理异常：{sql}{e}")
        return 0
    finally:
        if conn:
            conn.close()
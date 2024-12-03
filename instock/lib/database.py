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



def create_tables(conn):
    """创建必要的表"""
    try:
        # 创建 cn_stock_selection 表
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cn_stock_selection (
                date DATE,
                code VARCHAR(6),
                name VARCHAR(20),
                new_price DOUBLE,
                change_rate DOUBLE,
                volume BIGINT,
                deal_amount BIGINT,
                total_market_cap BIGINT,
                industry_name VARCHAR(20),
                pe_ttm DOUBLE,
                basic_eps DOUBLE,
                predict_netprofit_ratio DOUBLE,
                predict_income_ratio DOUBLE
            )
        """)
        
        # 创建 cn_stock_spot 表
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cn_stock_spot (
                date DATE,
                code VARCHAR(6),
                name VARCHAR(20),
                new_price DOUBLE,
                change_rate DOUBLE,
                volume BIGINT,
                deal_amount BIGINT,
                total_market_cap BIGINT,
                industry_name VARCHAR(20)
            )
        """)
        
        # 创建 cn_stock_fund_flow 表
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cn_stock_fund_flow (
                date DATE,
                code VARCHAR(6),
                name VARCHAR(20),
                new_price DOUBLE,
                change_rate DOUBLE,
                fund_amount BIGINT,
                fund_rate DOUBLE
            )
        """)
        
        # 创建 cn_stock_attention 表
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cn_stock_attention (
                datetime TIMESTAMP,
                code VARCHAR(6)
            )
        """)
        
        # 插入一些测试数据到 cn_stock_selection
        conn.execute("""
            INSERT INTO cn_stock_selection 
            VALUES 
            ('2024-12-03', '000001', '平安银行', 10.5, 2.5, 1000000, 10500000, 2000000000, '银行', 8.5, 1.2, 15.5, 10.2),
            ('2024-12-03', '000002', '万科A', 15.8, -1.2, 800000, 12640000, 1800000000, '房地产', 7.2, 0.9, 8.5, 5.8),
            ('2024-12-03', '000003', '国光电器', 25.3, 3.8, 500000, 12650000, 500000000, '电子', 12.5, 0.8, 20.5, 15.3)
            ON CONFLICT DO NOTHING
        """)
        
        logging.info("成功创建所有必要的表并插入测试数据")
    except Exception as e:
        logging.error(f"创建表时出错：{e}")
        raise


def _convert_sqlalchemy_type(sa_type):
    """转换SQLAlchemy类型到DuckDB类型"""
    type_str = str(sa_type)
    if 'DATE' in type_str:
        return 'DATE'
    elif 'FLOAT' in type_str:
        return 'DOUBLE'
    elif 'BIGINT' in type_str:
        return 'BIGINT'
    elif 'INTEGER' in type_str:
        return 'INTEGER'
    elif 'VARCHAR' in type_str:
        # 提取VARCHAR的长度
        import re
        length = re.search(r'VARCHAR\((\d+)\)', type_str)
        if length:
            return f'VARCHAR({length.group(1)})'
        return 'VARCHAR'
    else:
        return 'VARCHAR'


def get_connection():
    """获取数据库连接"""
    try:
        conn = duckdb.connect(db_file)
        create_tables(conn)
        return conn
    except Exception as e:
        logging.error(f"连接数据库出错：{e}")
        raise

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

        # 处理数据中的特殊字符
        for col in data.columns:
            if data[col].dtype == 'object':
                data[col] = data[col].replace('<', '').replace('>', '')

        # 创建表（如果不存在）
        create_table_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ('
        columns = []
        for col in data.columns:
            if cols_type and col in cols_type:
                col_type = _convert_sqlalchemy_type(cols_type[col])
            else:
                # 根据DataFrame的数据类型推断SQL类型
                dtype = str(data[col].dtype)
                if 'int' in dtype:
                    col_type = 'BIGINT'
                elif 'float' in dtype:
                    col_type = 'DOUBLE'
                elif 'datetime' in dtype:
                    col_type = 'TIMESTAMP'
                else:
                    col_type = 'VARCHAR'
            
            columns.append(f'"{col}" {col_type}')
        
        # 添加主键
        if primary_keys:
            # 移除所有反引号和多余的空格，只保留列名
            pk_cols = []
            for col in primary_keys.split(','):
                # 清理列名，移除反引号和空格
                clean_col = col.strip().replace('`', '').replace('"', '')
                pk_cols.append(f'"{clean_col}"')
            
            # 添加主键约束
            columns.append(f"PRIMARY KEY ({','.join(pk_cols)})")
            
            # 输出调试信息
            logging.info(f"主键列: {pk_cols}")
        
        create_table_sql += ", ".join(columns) + ")"
        
        # 输出完整的建表语句用于调试
        logging.info(f"建表SQL: {create_table_sql}")
        
        try:
            conn.execute(create_table_sql)
        except Exception as e:
            logging.error(f"创建表失败: {create_table_sql}, 错误: {e}")
            return

        # 插入数据
        try:
            # 使用DuckDB的DataFrame接口直接插入数据
            conn.register('temp_data', data)
            conn.execute(f'INSERT INTO "{table_name}" SELECT * FROM temp_data')
            conn.unregister('temp_data')
            conn.commit()
        except Exception as e:
            logging.error(f"插入数据失败: {e}")
            return

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
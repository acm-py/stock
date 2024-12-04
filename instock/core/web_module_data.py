#!/usr/bin/env python3
# -*- coding: utf-8 -*-

class web_module_data:
    def __init__(self, mode, type, ico, name, table_name, columns, column_names, primary_key, is_realtime, order_columns=None, order_by=None):
        self.mode = mode  # 模式，query，editor 查询和编辑模式
        self.type = type
        self.ico = ico
        self.name = name
        self.table_name = table_name
        self.columns = columns
        self.column_names = column_names
        self.primary_key = primary_key
        self.is_realtime = is_realtime
        self.order_by = order_by
        self.order_columns = order_columns
        self.url = f"/instock/data?table_name={self.table_name}"

    def get_sql(self, date=None):
        """生成DuckDB兼容的SQL查询"""
        # 构建基本查询
        sql = f'SELECT * FROM "{self.table_name}"'

        # 添加日期过滤
        if date:
            sql += f' WHERE date = \'{date}\''

        # 添加排序
        if self.order_by:
            # 移除原始的ORDER BY，因为我们会在后面添加
            clean_order = self.order_by.replace('ORDER BY', '').strip()
            sql += f' ORDER BY {clean_order}'

        return sql
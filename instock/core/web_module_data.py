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
        # 构建SELECT部分
        select_parts = ["*"]
        if self.order_columns:
            # 将MySQL风格的子查询改为DuckDB风格
            attention_subquery = self.order_columns.replace('`', '"')
            select_parts.append(attention_subquery)

        # 构建WHERE部分
        where_clause = ""
        if date:
            where_clause = f' WHERE "date" = \'{date}\''

        # 构建ORDER BY部分
        order_clause = ""
        if self.order_by:
            order_clause = self.order_by.replace('`', '"')

        # 组合SQL
        sql = f'SELECT {", ".join(select_parts)} FROM "{self.table_name}"{where_clause}{order_clause}'
        return sql
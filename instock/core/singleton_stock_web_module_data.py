#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import instock.core.tablestructure as tbs
from instock.lib.singleton_type import singleton_type
import instock.core.web_module_data as wmd

class stock_web_module_data(metaclass=singleton_type):
    def __init__(self):
        self.data_list = [
            wmd.web_module_data(
                mode="query",
                type="综合选股",
                ico="fa fa-desktop",
                name=tbs.TABLE_CN_STOCK_SELECTION['cn'],
                table_name=tbs.TABLE_CN_STOCK_SELECTION['name'],
                columns=tuple(tbs.TABLE_CN_STOCK_SELECTION['columns']),
                column_names=tbs.get_field_cns(tbs.TABLE_CN_STOCK_SELECTION['columns']),
                primary_key=[],
                is_realtime=False,
                order_columns=None,
                order_by='date DESC'
            ),
            wmd.web_module_data(
                mode="query",
                type="股票基本数据",
                ico="fa fa-book",
                name=tbs.TABLE_CN_STOCK_SPOT['cn'],
                table_name=tbs.TABLE_CN_STOCK_SPOT['name'],
                columns=tuple(tbs.TABLE_CN_STOCK_SPOT['columns']),
                column_names=tbs.get_field_cns(tbs.TABLE_CN_STOCK_SPOT['columns']),
                primary_key=[],
                is_realtime=True,
                order_columns=None,
                order_by='date DESC'
            ),
            wmd.web_module_data(
                mode="query",
                type="股票基本数据",
                ico="fa fa-book",
                name=tbs.TABLE_CN_STOCK_FUND_FLOW['cn'],
                table_name=tbs.TABLE_CN_STOCK_FUND_FLOW['name'],
                columns=tuple(tbs.TABLE_CN_STOCK_FUND_FLOW['columns']),
                column_names=tbs.get_field_cns(tbs.TABLE_CN_STOCK_FUND_FLOW['columns']),
                primary_key=[],
                is_realtime=True,
                order_columns=None,
                order_by='date DESC'
            )
        ]
        # 初始化_data字典，用于快速查找表数据
        self._data = {item.table_name: item for item in self.data_list}

        
    def get_data(self, table_name):
        if table_name in self._data:
            return self._data[table_name]
        return None

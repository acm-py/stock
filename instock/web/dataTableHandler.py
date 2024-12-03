#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import datetime
import logging
import pandas as pd
from tornado import gen
import tornado.web
import instock.lib.trade_time as trd
import instock.core.singleton_stock_web_module_data as sswmd
from instock.web.web_service import BaseHandler

class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, pd.Timestamp):
            return obj.strftime('%Y-%m-%d')
        return json.JSONEncoder.default(self, obj)

class GetStockHtmlHandler(BaseHandler):
    @gen.coroutine
    def get(self):
        name = self.get_argument("table_name", default=None, strip=False)
        logging.info(f"请求的表名: {name}")
        
        web_module_data = sswmd.stock_web_module_data().get_data(name)
        if web_module_data is None:
            logging.error(f"未找到表名为 {name} 的模块数据")
            logging.info(f"可用的表名: {[item.table_name for item in sswmd.stock_web_module_data().data_list]}")
            self.write_error(404)
            return
            
        run_date, run_date_nph = trd.get_trade_date_last()
        date_now_str = run_date_nph.strftime("%Y-%m-%d") if web_module_data.is_realtime else run_date.strftime("%Y-%m-%d")
        
        self.render("stock_web.html", 
                   web_module_data=web_module_data, 
                   date_now=date_now_str,
                   leftMenu=self.get_left_menu(self.request.uri))

class GetStockDataHandler(BaseHandler):
    def get(self):
        try:
            name = self.get_argument("name", default=None, strip=False)
            if not name:
                self.write_json({"error": "name parameter is required"})
                return

            date_str = self.get_argument("date", default=None, strip=False)
            if date_str:
                date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
            else:
                date = datetime.date.today()

            web_module_data = sswmd.stock_web_module_data().get_data(name)
            if web_module_data is None:
                self.write_json({"error": f"No data found for name: {name}"})
                return

            conn = self.db
            if not conn:
                self.write_json({"error": "Failed to connect to database"})
                return

            try:
                sql = f'SELECT * FROM "{web_module_data.table_name}"'
                result = conn.execute(sql).fetchdf()
                if not result.empty:
                    self.write_json({
                        "draw": 1,
                        "recordsTotal": len(result),
                        "recordsFiltered": len(result),
                        "data": result.to_dict(orient='records')
                    })
                else:
                    self.write_json({
                        "draw": 1,
                        "recordsTotal": 0,
                        "recordsFiltered": 0,
                        "data": []
                    })
            finally:
                conn.close()
        except Exception as e:
            logging.error(f"GetStockDataHandler处理异常：{e}")
            self.write_json({
                "draw": 1,
                "recordsTotal": 0,
                "recordsFiltered": 0,
                "data": [],
                "error": str(e)
            })
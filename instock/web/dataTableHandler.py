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
        # logging.info(f"当前日期: {date_now_str}")
        # logging.info(f"当前数据 is {web_module_data}")
        self.render("stock_web.html", 
                   web_module_data=web_module_data, 
                   date_now=date_now_str,
                   leftMenu=self.get_left_menu(self.request.uri))

class GetStockDataHandler(BaseHandler):
    def get(self):
        try:
            name = self.get_argument("table_name", default=None, strip=False)  # 修改为table_name
            if not name:
                self.write_json({"error": "table_name parameter is required"})
                return

            date_str = self.get_argument("date", default=None, strip=False)
            
            web_module_data = sswmd.stock_web_module_data().get_data(name)
            if web_module_data is None:
                self.write_json({"error": f"No data found for table_name: {name}"})
                return

            conn = self.db
            if not conn:
                self.write_json({"error": "Failed to connect to database"})
                return

            try:
                # 使用web_module_data的get_sql方法生成SQL
                sql = web_module_data.get_sql(date_str)
                logging.info(f"执行SQL: {sql}")
                
                result = conn.execute(sql).fetchdf()
                if not result.empty:
                    # 将日期列转换为字符串
                    if 'date' in result.columns:
                        result['date'] = result['date'].dt.strftime('%Y-%m-%d')
                    
                    # 转换数据为GC.Spread.Sheets期望的格式
                    records = result.to_dict(orient='records')
                    response_data = {
                        "code": 0,  # 0表示成功
                        "data": {
                            "items": records,  # 实际数据放在items字段中
                            "length": len(records),  # 总记录数
                            "offset": 0,  # 起始位置
                            "total": len(records)  # 总记录数
                        },
                        "message": "success"
                    }
                    self.write_json(response_data)
                else:
                    # 返回空数据但保持相同的格式
                    self.write_json({
                        "code": 0,
                        "data": {
                            "items": [],
                            "length": 0,
                            "offset": 0,
                            "total": 0
                        },
                        "message": "success"
                    })
            finally:
                conn.close()
        except Exception as e:
            logging.error(f"GetStockDataHandler处理异常：{e}")
            self.write_json({
                "code": 1,  # 1表示错误
                "data": {
                    "items": [],
                    "length": 0,
                    "offset": 0,
                    "total": 0
                },
                "message": str(e)
            })

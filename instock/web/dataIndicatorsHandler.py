#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import datetime
from tornado import gen
import instock.core.stockfetch as stf
import instock.core.kline.visualization as vis
import instock.core.tablestructure as tbs
from instock.web.base import BaseHandler

class GetDataIndicatorsHandler(BaseHandler):
    @gen.coroutine
    def get(self):
        code = self.get_argument("code", default=None, strip=False)
        date = self.get_argument("date", default=None, strip=False)
        name = self.get_argument("name", default=None, strip=False)
        comp_list = []
        
        try:
            if code.startswith(('1', '5')):
                stock = stf.fetch_etf_hist((date, code))
            else:
                stock = stf.fetch_stock_hist((date, code))
                
            if stock is None:
                return

            pk = vis.get_plot_kline(code, stock, date, name)
            if pk is None:
                return

            comp_list.append(pk)
        except Exception as e:
            logging.error(f"dataIndicatorsHandler.GetDataIndicatorsHandler处理异常：{e}")

        self.render("stock_indicators.html", 
                   comp_list=comp_list,
                   leftMenu=self.get_left_menu(self.request.uri))

class SaveCollectHandler(BaseHandler):
    @gen.coroutine
    def get(self):
        code = self.get_argument("code", default=None, strip=False)
        otype = self.get_argument("otype", default=None, strip=False)
        
        try:
            conn = self.db
            now = datetime.datetime.now()
            
            if otype == "1":  # 添加关注
                sql = f"""
                INSERT INTO {tbs.TABLE_NAME_STOCK_ATTENTION} (code, create_date) 
                VALUES (?, ?)
                """
                conn.execute(sql, [code, now])
            else:  # 删除关注
                sql = f"""
                DELETE FROM {tbs.TABLE_NAME_STOCK_ATTENTION} 
                WHERE code = ?
                """
                conn.execute(sql, [code])
                
            self.write("true")
        except Exception as e:
            logging.error(f"SaveCollectHandler处理异常：{e}")
            self.write("false")
        finally:
            if conn:
                conn.close()
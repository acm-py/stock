#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os.path
import sys
import logging
import json
import datetime
import asyncio
import tornado.web
import tornado.ioloop
import tornado.httpserver
import tornado.platform.asyncio
from tornado.options import define, options

# 设置项目路径
cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)

# 设置日志
log_path = os.path.join(cpath_current, 'log')
if not os.path.exists(log_path):
    os.makedirs(log_path)
logging.basicConfig(
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler(os.path.join(log_path, 'stock_web.log')),
        logging.StreamHandler()
    ]
)

import instock.lib.database as db
import instock.lib.version as version
import instock.web.dataTableHandler as dataTableHandler
import instock.web.dataIndicatorsHandler as dataIndicatorsHandler
from instock.core.singleton_stock import stock_hist_data

# define("port", default=9988, help="run on the given port", type=int)



class MenuItem:
    def __init__(self, url, name, item_type, active=""):
        self.url = url
        self.name = name
        self.type = item_type
        self.active = active
        self.ico = "fa fa-list"  # 默认图标

class LeftMenu:
    def __init__(self, items):
        self.leftMenuList = items

class BaseHandler(tornado.web.RequestHandler):
    @property
    def db(self):
        """获取数据库连接"""
        return db.get_connection()

    def write_json(self, data):
        self.write(json.dumps(data, default=str))

    def get_left_menu(self, uri):
        """获取左侧菜单"""
        menu_list = [
            MenuItem("/", "首页", "main"),
            MenuItem(f"/instock/data?table_name=cn_stock_selection", "综合选股", "data"),
            MenuItem(f"/instock/data?table_name=cn_stock_spot", "股票基本数据", "data"),
            MenuItem(f"/instock/data?table_name=cn_stock_fund_flow", "股票资金流向", "data"),
            MenuItem("/instock/data/indicators", "指标中心", "data"),
        ]
        
        # 设置活动状态
        for item in menu_list:
            if item.url in uri:
                item.active = "active"
        
        return LeftMenu(menu_list)

class HomeHandler(BaseHandler):
    async def get(self):
        self.render("index.html",
                   stockVersion=version.__version__,
                   leftMenu=self.get_left_menu(self.request.uri))

class StockHistHandler(BaseHandler):
    async def get(self):
        try:
            date_str = self.get_argument("date", None)
            if date_str:
                date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
            else:
                date = datetime.date.today()
                
            data = stock_hist_data(date=date).get_data()
            if data is not None:
                self.write_json({"data": data.to_dict(orient='records')})
            else:
                self.write_json({"data": []})
        except Exception as e:
            logging.error(f"StockHistHandler处理异常：{e}")
            self.write_json({"error": str(e)})
        finally:
            if hasattr(self, '_db') and self._db:
                self._db.close()

class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/", HomeHandler),
            (r"/instock/", HomeHandler),
            (r"/stock/hist", StockHistHandler),
            # 使用datatable展示报表数据模块
            (r"/instock/api_data", dataTableHandler.GetStockDataHandler),
            (r"/instock/data", dataTableHandler.GetStockHtmlHandler),
            # 获得股票指标数据
            (r"/instock/data/indicators", dataIndicatorsHandler.GetDataIndicatorsHandler),
            # 加入关注
            (r"/instock/control/attention", dataIndicatorsHandler.SaveCollectHandler),
        ]
        settings = dict(
            template_path=os.path.join(os.path.dirname(__file__), "templates"),
            static_path=os.path.join(os.path.dirname(__file__), "static"),
            xsrf_cookies=False,
            cookie_secret="027bb1b670eddf0392cdda8709268a17b58b7",
            debug=True
        )
        super(Application, self).__init__(handlers, **settings)

async def main():
    define("port", default=9988, help="run on the given port", type=int)
    # 设置事件循环策略
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    tornado.options.parse_command_line()
    app = Application()
    app.listen(options.port)
    
    logging.info(f'Server is running on http://localhost:{options.port}')
    
    # 等待服务器关闭
    shutdown_event = asyncio.Event()
    await shutdown_event.wait()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("服务器正在关闭...")
    except Exception as e:
        logging.error(f"服务器发生错误：{e}")
    finally:
        logging.info("服务器已关闭")
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import logging
from typing import Dict, Optional, Union
__author__ = 'myh '
__date__ = '2023/3/24 '


def get_pattern_recognitions(
    data: pd.DataFrame,
    stock_column: Dict[str, Dict[str, Union[str, int, callable]]],
    end_date: Optional[str] = None,
    threshold: Optional[int] = 120,
    calc_threshold: Optional[int] = None
) -> Optional[pd.DataFrame]:
    """计算股票的K线形态识别指标

    该函数对输入的股票数据进行K线形态分析，可以识别多种K线形态，如两只乌鸦、三只乌鸦等。
    每种形态的计算结果为整数值：
    - 100：强烈买入信号
    - -100：强烈卖出信号
    - 0：无信号

    Args:
        data (pd.DataFrame): 股票数据，必须包含以下列：
            - date: 日期
            - open: 开盘价
            - high: 最高价
            - low: 最低价
            - close: 收盘价
        stock_column (Dict[str, Dict]): K线形态配置字典，包含：
            - 形态名称
            - 计算函数
            - 显示设置
        end_date (Optional[str], optional): 结束日期，格式：'YYYY-MM-DD'。默认为None
        threshold (Optional[int], optional): 返回的数据条数限制。默认为120
        calc_threshold (Optional[int], optional): 计算时使用的数据条数。默认为None

    Returns:
        Optional[pd.DataFrame]: 包含K线形态识别结果的DataFrame，如果计算失败返回None
            - 每列对应一种K线形态
            - 值为该形态的识别结果(-100到100的整数)

    Examples:
        >>> import pandas as pd
        >>> from instock.core.tablestructure import STOCK_KLINE_PATTERN_DATA
        >>> # 准备股票数据
        >>> stock_data = pd.DataFrame({
        ...     'date': ['2023-01-01', '2023-01-02'],
        ...     'open': [10, 11], 'high': [12, 13],
        ...     'low': [9, 10], 'close': [11, 12]
        ... })
        >>> # 计算K线形态
        >>> patterns = get_pattern_recognitions(
        ...     stock_data,
        ...     STOCK_KLINE_PATTERN_DATA['columns'],
        ...     end_date='2023-01-02'
        ... )
        >>> if patterns is not None:
        ...     print("成功识别K线形态")

    Notes:
        1. 所有形态识别使用TA-Lib库实现
        2. 确保输入数据包含足够的历史数据以进行形态识别
        3. 部分形态可能需要较多的历史数据才能得到有效结果
    """
    isCopy = False
    if end_date is not None:
        mask = (data['date'] <= end_date)
        data = data.loc[mask]
        isCopy = True
    if calc_threshold is not None:
        data = data.tail(n=calc_threshold)
        isCopy = True
    if isCopy:
        data = data.copy()

    for k in stock_column:
        try:
            data.loc[:, k] = stock_column[k]['func'](data['open'].values, data['high'].values, data['low'].values, data['close'].values)
        except Exception as e:
            pass

    if data is None or len(data.index) == 0:
        return None

    if threshold is not None:
        data = data.tail(n=threshold).copy()

    return data


def get_pattern_recognitions(
    data: pd.DataFrame,
    stock_column: Dict[str, Dict[str, Union[str, int, callable]]],
    end_date: Optional[str] = None,
    threshold: Optional[int] = 120,
    calc_threshold: Optional[int] = None
) -> Optional[pd.DataFrame]:
    """计算股票的K线形态识别指标

    该函数对输入的股票数据进行K线形态分析，可以识别多种K线形态，如两只乌鸦、三只乌鸦等。
    每种形态的计算结果为整数值：
    - 100：强烈买入信号
    - -100：强烈卖出信号
    - 0：无信号

    Args:
        data (pd.DataFrame): 股票数据，必须包含以下列：
            - date: 日期
            - open: 开盘价
            - high: 最高价
            - low: 最低价
            - close: 收盘价
        stock_column (Dict[str, Dict]): K线形态配置字典，包含：
            - 形态名称
            - 计算函数
            - 显示设置
        end_date (Optional[str], optional): 结束日期，格式：'YYYY-MM-DD'。默认为None
        threshold (Optional[int], optional): 返回的数据条数限制。默认为120
        calc_threshold (Optional[int], optional): 计算时使用的数据条数。默认为None

    Returns:
        Optional[pd.DataFrame]: 包含K线形态识别结果的DataFrame，如果计算失败返回None
            - 每列对应一种K线形态
            - 值为该形态的识别结果(-100到100的整数)

    Examples:
        >>> import pandas as pd
        >>> from instock.core.tablestructure import STOCK_KLINE_PATTERN_DATA
        >>> # 准备股票数据
        >>> stock_data = pd.DataFrame({
        ...     'date': ['2023-01-01', '2023-01-02'],
        ...     'open': [10, 11], 'high': [12, 13],
        ...     'low': [9, 10], 'close': [11, 12]
        ... })
        >>> # 计算K线形态
        >>> patterns = get_pattern_recognitions(
        ...     stock_data,
        ...     STOCK_KLINE_PATTERN_DATA['columns'],
        ...     end_date='2023-01-02'
        ... )
        >>> if patterns is not None:
        ...     print("成功识别K线形态")

    Notes:
        1. 所有形态识别使用TA-Lib库实现
        2. 确保输入数据包含足够的历史数据以进行形态识别
        3. 部分形态可能需要较多的历史数据才能得到有效结果
    """
    try:
        # 增加空判断，如果是空返回 0 数据。
        if date is None:
            end_date = code_name[0]
        else:
            end_date = date.strftime("%Y-%m-%d")

        code = code_name[1]
        # 设置返回数组。
        # 增加空判断，如果是空返回 0 数据。
        if len(data.index) <= 1:
            return None

        stockStat = get_pattern_recognitions(data, stock_column, end_date=end_date, threshold=1,
                                             calc_threshold=calc_threshold)

        if stockStat is None:
            return None

        isHas = False
        for k in stock_column:
            if stockStat.iloc[0][k] != 0:
                isHas = True
                break

        if isHas:
            stockStat.loc[:, 'code'] = code
            return stockStat.iloc[0, -(len(stock_column) + 1):]

    except Exception as e:
        logging.error(f"pattern_recognitions.get_pattern_recognition处理异常：{code}代码{e}")

    return None

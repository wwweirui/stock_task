import pandas as pd
import numpy as np
import os
import time


def combine():
    # 市值 和 时间线 合并demo
    # 小时线数据源
    path_hour = "D:/硕士研究/代码类/2020-08-12/JoinQuant/小时数据/"
    # 市值线数据源
    path_capitalization = "D:/硕士研究/代码类/2020-08-12/JoinQuant/市值表/"
    # 存放结果
    path_result = "D:/硕士研究/代码类/2020-08-12/JoinQuant/小时线合并市值数据/"

    # 小时数据
    list_hour = os.listdir(path_hour)
    # 聚宽 市值 数据
    list_capitalization = os.listdir(path_capitalization)

    # 结合表的顺序
    '''
    capitalization  总股本 万股
    market_cap      总市值 亿元
    circulating_cap     流通股本 万股
    circulating_market_cap  流通市值 亿元
    turnover_ratio 换手率 

    '''
    order = ['date', 'open', 'high', 'low', 'close', 'volume', 'money',
             'capitalization', 'market_cap', 'circulating_cap', 'circulating_market_cap',
             'turnover_ratio']
    for em in list_hour:
        # 对应
        if em in list_capitalization:
            # 读
            df_hour = pd.read_csv(path_hour + em)
            df_cap = pd.read_csv(path_capitalization + em, encoding="gbk")
            df_hour["date2"] = df_hour["date"]
            # 时间格式处理
            for i in range(len(df_hour)):
                _time = time.strptime(str(df_hour["date"][i])[:10], "%Y-%m-%d")
                # print(time.strftime("%Y/%m/%d", _time))
                df_hour.loc[i, "date2"] = time.strftime("%Y-%m-%d", _time)
                # 合并
            comb = pd.merge(df_hour, df_cap, how="inner", left_on="date2", right_on="day")
            # --  axis为0时表示删除行，axis为1时表示删除列
            comb["turnover_ratio"] = (comb["volume"] / (comb["circulating_cap"] * 100)).round(decimals=4)
            comb = comb[order]
            comb.to_csv(path_result + em, index=False)
            print(em)


# 两小时线 数据的处理
def two_hour():
    path_sour = "D:/硕士研究/代码类/2020-08-12/JoinQuant/小时线合并市值数据/"
    path_result = "D:/硕士研究/代码类/2020-08-12/JoinQuant/小时线(两小时)合并数据/"
    # # 小时线数据 位置
    list_two_hour = os.listdir(path_sour)
    for list_two in list_two_hour:
        df_two = pd.read_csv(path_sour + list_two, encoding="gbk")
        # 简化测试
        df_two_res = pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'volume', 'money',
                                           'capitalization', 'market_cap', 'circulating_cap',
                                           'circulating_market_cap', 'turnover_ratio'])
        # df_two = pd.read_csv('D:/硕士研究/代码类/2020-08-12/JoinQuant/小时线合并市值数据/000001.SZ.csv', encoding="gbk")
        list_capitalization = []
        list_market_cap = []
        list_circulating_cap = []
        list_circulating_market_cap = []
        list_date = []
        list_open = []
        list_close = []
        list_high = []
        list_low = []
        list_volume = []
        list_money = []
        for i in range(len(df_two)):
            # 上下时间线 进行比较
            if i % 2 == 0:
                # print(i + 1, df_two["date"][i + 1])
                list_date.append(df_two["date"][i + 1])
                list_open.append(df_two["open"][i])
                list_close.append(df_two["close"][i + 1])
                # 最高价
                if df_two['high'][i] <= df_two['high'][i + 1]:
                    list_high.append(df_two['high'][i + 1])
                else:
                    list_high.append(df_two['high'][i])
                # 最低价
                if df_two['low'][i] >= df_two['low'][i + 1]:
                    list_low.append(df_two['low'][i + 1])
                else:
                    list_low.append(df_two['low'][i])
                list_money.append(df_two['money'][i] + df_two['money'][i + 1])
                list_volume.append(df_two['volume'][i] + df_two['volume'][i + 1])
                list_capitalization.append(df_two['capitalization'][i])
                list_market_cap.append(df_two['market_cap'][i])
                list_circulating_cap.append(df_two['circulating_cap'][i])
                list_circulating_market_cap.append(df_two['circulating_market_cap'][i])

        df_two_res['date'] = list_date
        df_two_res['open'] = list_open
        df_two_res['close'] = list_close
        df_two_res['high'] = list_high
        df_two_res['low'] = list_low
        df_two_res['volume'] = list_volume
        df_two_res['money'] = list_money
        df_two_res['capitalization'] = list_capitalization
        df_two_res['market_cap'] = list_market_cap
        df_two_res['circulating_cap'] = list_circulating_cap
        df_two_res['circulating_market_cap'] = list_circulating_market_cap
        df_two_res["turnover_ratio"] = \
            (df_two_res["volume"] / (df_two_res["circulating_cap"] * 100)).round(decimals=4)
        df_two_res.to_csv(path_result + list_two, index=False)


if __name__ == '__main__':
    # combine()

    two_hour()

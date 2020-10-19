from jqdatasdk import *
import pandas as pd
import numpy as np
import os
from datetime import datetime

auth('13687083589', 'JoinQuant123456')
# ID是申请时所填写的手机号；Password为聚宽官    网登录密码，新申请用户默认为手机号后6位
# 验证是否连接成功
# is_auth = is_auth()
# print(is_auth)
count = get_query_count()


# get_price - 获取行情数据
# get_price(security, start_date=None, end_date=None, frequency='daily',
# fields=None, skip_paused=False, fq='pre', count=None, panel=True, fill_paused=True)
# df = get_price('000001.XSHE')  # 获取000001.XSHE的2015年的按天数据

# 获取日线数据
def get_daily_stock():
    # df = get_price('000001.XSHE', start_date='2005-01-01', end_date='2020-08-31', fq="pre")
    df = get_price('600000.XSHG', start_date='2005-01-01', end_date='2020-08-31', fq="pre")
    df = get_price('000016.XSHE', start_date='2005-01-01', end_date='2020-08-31', fq="pre")
    print(df)
    # df.to_csv("D:/硕士研究/代码类/2020-08-12/JoinQuant/000001.csv", encoding="utf_8_sig")
    df.to_csv("D:/硕士研究/代码类/2020-08-12/JoinQuant/600000.csv", encoding="utf_8_sig")
    df.to_csv("D:/硕士研究/代码类/2020-08-12/JoinQuant/000016.csv", encoding="utf_8_sig")


# 获取市值数据
def get_shizhi(stock_code):
    # 查询'000001.XSHE'的所有市值数据,
    for lines in stock_code:
        lines = lines.replace("SZ", "XSHE")
        # lines = lines.replace("SH", "XSHG")

        q = query(
            valuation
        ).filter(
            valuation.code == lines
        )
        df = get_fundamentals_continuously(q, end_date='2020-09-01', count=10000)
        # 打印出总市值

        lines = lines.replace("XSHE", "SZ")
        # lines = lines.replace("XSHG", "SH")
        print(lines)
        df.to_csv("D:/硕士研究/代码类/2020-08-12/JoinQuant/市值表sz/" + lines + ".csv", encoding="utf_8_sig")


# 获取财务数据
def get_financial_indicators(stock_code):
    # 查询'000001.XSHE'的所有市值数据,
    for lines in stock_code:
        lines = lines.replace("SZ", "XSHE")
        # lines = lines.replace("SH", "XSHG")
        q = query(
            indicator
        ).filter(
            indicator.code == lines
        )
        df = get_fundamentals_continuously(q, end_date='2020-09-01', count=10000)
        # 打印出总市值     往前批量获取

        lines = lines.replace("XSHE", "SZ")
        print(lines)
        # lines = lines.replace("XSHG", "SH")
        df.to_csv("D:/硕士研究/代码类/2020-08-12/JoinQuant/财务指标表sz/" + lines + ".csv", encoding="utf_8_sig", index=None)
        # df.to_csv("D:/硕士研究/代码类/2020-08-12/JoinQuant/财务指标表sh/" + lines + ".csv", encoding="utf_8_sig")
        # print(df)


# 获取所有股票的 名称
def get_all_stock_inf():
    stocks = list(get_all_securities(['stock']).index)
    filelist = open('stock_list.txt', 'w')
    for st in stocks:
        st = st.replace("XSHE", "SZ")
        st = st.replace("XSHG", "SH")
        filelist.write(st)
        filelist.write('\n')
    filelist.close()
    return stocks

    # stocks = get_all_securities(['stock'])
    # 一共 是由4080支股票
    # print(stocks)


# 读取 处理 所有股票信息
def read_stock_base_txt():
    file_name = "stock_list.txt"
    data_stock_sh = []
    data_stock_sz = []
    file = open(file_name, mode='r')
    for line in file:
        line = line.strip('\n')
        if line.split('.')[1] == "SZ":
            data_stock_sz.append(line)
        else:
            data_stock_sh.append(line)
    file.close()
    # print(data_stock_sz)
    # print("______________")
    # print(data_stock_sh)
    return data_stock_sh, data_stock_sz


# 雪球热度数据
# https://www.joinquant.com/help/api/help?name=public
def xueqiu():
    # n 最大为3000
    xqdata = finance.run_query(query(finance.STK_XUEQIU_PUBLIC).filter
                               (finance.STK_XUEQIU_PUBLIC.code == code).limit(n))
    print(xqdata)


# 新闻联播文本数据
# https://www.joinquant.com/help/api/help?name=Public
def news():
    newsdata = finance.run_query(query(finance.CCTV_NEWS).filter
                                 (finance.CCTV_NEWS.day == '2019-02-19').limit(n))
    print(newsdata)


# 小时线，分钟线数据下载
def minute(stock_code):
    for down_list in stock_code:
        # down_list = down_list.replace("SZ", "XSHE")
        down_list = down_list.replace("SH", "XSHG")
        # unit bar的时间单位为 '1m', '5m', '15m', '30m', '60m', '120m', '1d', '1w'(一周), '1M'（一月）标准bar时
        # security 股票代码
        # count 获取bar的个数
        # fields: 获取数据的字段， 支持如下值：'date', 'open', 'close', 'high', 'low', 'volume',
        # 'money', 'open_interest'（期货持仓量），factor(复权因子)。
        # fq_ref_date 设置为datetime.datetime.now()即返回前复权数据 ;
        hour_data = get_bars(down_list, 40000, unit='60m',
                             fields=['date', 'open', 'high', 'low', 'close', 'volume', 'money'],
                             include_now=False, fq_ref_date=datetime.now(), df=True)
        # 例子
        # df = get_bars('000001.XSHG', 10, unit='1d', fields=['date', 'open', 'high', 'low', 'close'],
        #               include_now=False, end_dt='2018-12-05')

        # down_list = down_list.replace("XSHE", "SZ")
        down_list = down_list.replace("XSHG", "SH")
        print(down_list)
        hour_data.to_csv("D:/硕士研究/代码类/2020-08-12/JoinQuant/小时数据sh/" + down_list + ".csv",
                         encoding="utf_8_sig", index=False)
        # df.to_csv("D:/硕士研究/代码类/2020-08-12/JoinQuant/财务指标表sh/" + lines + ".csv", encoding="utf_8_sig")


if __name__ == '__main__':
    # get_daily_stock()

    stock_list_sh = []
    stock_list_sz = []
    stock_list_sh, stock_list_sz = read_stock_base_txt()
    # 断点 继续
    # print(stock_list_sh.index("600295.SH"))
    # print(stock_list_sz.index("300499.SZ"))
    # get_shizhi(stock_list_sz)

    # get_shizhi()
    # get_all_stock_inf()
    # get_financial_indicators(stock_list_sz)

    # 小时线下载  前天下载到了 002251.SZ,次日切片处理 (SH未下载) 9/14
    # 小时线下载  前天下载到了 ,次日切片处理 600030.SH  9/15
    # print(stock_list_sh.index("600660.SH"))
    minute(stock_list_sh)
    print(count)

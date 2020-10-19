import pandas as pd
import os
import sys
import time
import numpy as np


def all_in():
    # path = "D:/硕士研究/代码类/2020-08-12/combine_stock/data_comb/"
    path = "D:/硕士研究/毕业设计准备/不复权淘宝数据/不复权淘宝数据"
    #
    list_filename = os.listdir(path)
    # 整合all_in_one
    with open("all_in_one.csv", "w", encoding="utf-8") as fout:
        fout.write(
            "ts_code,trade_date,open,high,low,close,vol,amount,换手率（%）,换手率（自由流通股）,量比,市盈率（总市值/净利润， 亏损的PE为空）,市盈率（TTM，亏损的PE为空）,市净率（总市值/净资产）,市销率,市销率（TTM）,股息率 （%）,股息率（TTM）（%）,总股本 （万股）,流通股本 （万股）,自由流通股本 （万）,总市值 （万元）,流通市值（万元）,ma5,ma10,ma20,ma30,ma60,ma120,ma250,kdj_K,kdj_D,kdj_J,fivedayhs,updown_rate,BBI1,BBI2,BBI3\n")
        for filename in list_filename:
            with open(path + filename, "r", encoding="utf-8") as fin:
                for iline, line in enumerate(fin):
                    if iline != 0:
                        fout.write(filename[:-4] + "," + line)
            print(filename)


def combine_none_fq():
    path = "D:/硕士研究/毕业设计准备/不复权淘宝数据/不复权淘宝数据"
    year_filename = os.listdir(path)
    end_year = []
    stock_list = []
    i = 1
    dfs = []
    # 年限数据
    for li in os.listdir(path + '/' + '2020'):
        # stock_list 股票列表
        stock_list.append(li[:-5])

    # 每个年限循环
    for year_path in year_filename:
        path_tem = path + "/" + year_path
        # list_filename = os.listdir(path_tem)
        end_year.append(path_tem)
        # # 对应年限 循环下的 股票 list
        # for address in list_filename:
        #     dfs = []
        #     if address in stock_list:
        #         address_last = path + "/" + address
        #         dfs.append(pd.read_excel(address_last, encoding="gbk"))
        #         df = pd.concat(dfs)
    # 股票循环
    for sl in stock_list:
        # 年份 循环
        for number_year in end_year:
            # 对应 年份下所有数据 list
            address = os.listdir(number_year)
            # 对应 股票list
            for add in address:
                add_er = add[:-5]
                if sl in add_er:
                    dfs.append(pd.read_excel(number_year + '/' + sl + '.xlsx'))
                    df = pd.concat(dfs)
                    if number_year == "D:/硕士研究/毕业设计准备/不复权淘宝数据/不复权淘宝数据/2020":
                        df.to_excel("D:/硕士研究/毕业设计准备/不复权淘宝数据/all_data/" + add, index=None)
                        print("D:/硕士研究/毕业设计准备/不复权淘宝数据/all_data/" + add)
                        dfs = []
                    break
        # print(number_year + '/' + sl)
        # dfs.append(pd.read_excel(number_year + '/' + sl + '.xlsx'))
    # # print(address_last)
    # year_list = get_file_list(path)
    # print(year_list)


def combine():
    # 市值合并demo
    # 日线数据
    path_eastmoney = "D:/硕士研究/代码类/2020-08-12/pacong/data_parsed/"

    path_jointquant = "D:/硕士研究/代码类/2020-08-12/combine_stock/data_jq/"
    # path_result = "./data_comb/"
    path_result = "D:/硕士研究/毕业设计准备/不复权淘宝数据/data_comb/"

    # 不复权的日线数据
    list_eastmoney = os.listdir(path_eastmoney)
    # print(list_eastmoney)
    # 聚宽% 第一次的市值 数据
    list_jq = []
    list_jqq = os.listdir(path_jointquant)
    for jq in list_jqq:
        list_jq.append(jq[:-4])
        # print(list_jq)
    # 结合表的顺序
    # order = ['date', 'time', 'open', 'high', 'low', 'close']
    order = ["trade_date", "open", "high", "low", "close", "vol", "amount", "换手率（%）",
             "换手率（自由流通股）", "量比", "市盈率（总市值/净利润， 亏损的PE为空）", "市盈率（TTM，亏损的PE为空）",
             "市净率（总市值/净资产）", "市销率", "市销率（TTM）", "股息率 （%）", "股息率（TTM）（%）",
             "总股本 （万股）", "流通股本 （万股）", "自由流通股本 （万）", "总市值 （万元）",
             "流通市值（万元）", "ma5", "ma10", "ma20", "ma30", "ma60", "ma120", "ma250",
             "kdj_K", "kdj_D", "kdj_J", "fivedayhs", "updown_rate", "BBI1", "BBI2", "BBI3"]
    # order = ["ma5", "ma10", "ma20", "ma30", "ma60", "ma120", "ma250",
    #          "kdj_K", "kdj_D", "kdj_J", "fivedayhs", "BBI1", "BBI2", "BBI3"]
    count = 1
    # 遍历
    for em in list_eastmoney:  # 不复权
        # 对应
        em = em[:-5]
        # print(em) 处理后缀
        if em in list_jq:
            # 读
            df_em = pd.read_excel(path_eastmoney + em + ".xlsx")  # 不复权
            print(path_eastmoney + em + ".xlsx")
            df_jq = pd.read_csv(path_jointquant + em + ".csv", encoding="gbk")
            for i in range(len(df_jq)):
                _time = time.strptime(str(df_jq["交易日期"][i]), "%Y%m%d")
                df_jq.loc[i, "交易日期"] = time.strftime("%Y-%m-%d", _time)
            # 合并
            comb = pd.merge(df_em, df_jq, how="left",

                            left_on="交易所行情日期（格式：YYYY-MM-DD）", right_on="交易日期")
            # --  axis为0时表示删除行，axis为1时表示删除列
            comb = comb.drop(["交易日期", "换手率（%）"], axis=1)
            comb.rename(columns={'交易所行情日期（格式：YYYY-MM-DD）': 'trade_date',
                                 '成交量（累计 单位：股）': 'vol',
                                 '成交额（精度：小数点后4位；单位：人民币元）': 'amount',
                                 '换手率（精度：小数点后6位；单位：%，[指定交易日的成交量(股)/指定交易日的股票的流通股总股数(股)]*100%）': '换手率（%）',
                                 '涨跌幅（百分比，精度：小数点后6位  涨跌幅=[(区间最后交易日收盘价-区间首个交易日前收盘价)/区间首个交易日前收盘价]*100%）': 'updown_rate',
                                 '今开盘价格（精度：小数点后4位；单位：人民币元）': 'open',
                                 '最高价（精度：小数点后4位；单位：人民币元）': 'high',
                                 '最低价（精度：小数点后4位；单位：人民币元）': 'low',
                                 '收盘价（精度：小数点后4位；单位：人民币元）': 'close'
                                 }

                        , inplace=True)
            comb['ma5'] = comb['close'].rolling(window=5).mean()
            comb['ma10'] = comb['close'].rolling(window=10).mean()
            comb['ma20'] = comb['close'].rolling(window=20).mean()
            comb['ma30'] = comb['close'].rolling(window=30).mean()
            comb['ma60'] = comb['close'].rolling(window=60).mean()
            comb['ma120'] = comb['close'].rolling(window=120).mean()
            comb['ma250'] = comb['close'].rolling(window=250).mean()

            N, M1, M2 = 10, 9, 9
            low_list = comb['low'].rolling(N, min_periods=N).min()
            low_list.fillna(value=comb['low'].expanding().min(), inplace=True)
            high_list = comb['high'].rolling(N, min_periods=N).max()
            high_list.fillna(value=comb['high'].expanding().max(), inplace=True)
            rsv = (comb['close'] - low_list) / (high_list - low_list) * 100

            # KDJ 计算
            # comb['kdj_K'] = pd.DataFrame(rsv).ewm(com=M1).mean()
            comb['kdj_K'] = pd.DataFrame(rsv).ewm(adjust=False, alpha=1 / M1).mean()
            # comb['kdj_D'] = comb['kdj_K'].ewm(com=M2).mean()
            comb['kdj_D'] = comb['kdj_K'].ewm(adjust=False, alpha=1 / M2).mean()
            comb['kdj_J'] = 3 * comb['kdj_K'] - 2 * comb['kdj_D']
            # print(comb['K'], comb['D'], comb['J'])

            # 五日区间换手率： 今天再往前数4天 的平均换手率
            comb['fivedayhs'] = comb['换手率（%）'].rolling(window=5).mean()
            # print(comb['fivedayhs'])

            # 涨跌幅计算 计算变化率：（后一个值-前一个值）／前一个值
            # comb['updown_rate'] = comb['close'].pct_change() * 100
            # print(comb['updown_rate'])
            #
            # 计算BBI 分三个字段
            # BBI=(3日均价+6日均价+12日均价+24日均价)÷4
            comb['BBI1'] = (comb['close'].rolling(window=3).mean() + comb['close'].rolling(window=6).mean()
                            + comb['close'].rolling(window=12).mean() + comb['close'].rolling(window=24).mean()) / 4
            # BBI   6/12/24/48
            comb['BBI2'] = (comb['close'].rolling(window=6).mean() + comb['close'].rolling(window=12).mean()
                            + comb['close'].rolling(window=24).mean() + comb['close'].rolling(window=48).mean()) / 4
            # BBI   7/14/28/56
            comb['BBI3'] = (comb['close'].rolling(window=7).mean() + comb['close'].rolling(window=14).mean()
                            + comb['close'].rolling(window=28).mean() + comb['close'].rolling(window=56).mean()) / 4

            comb = comb[order]
            # print(comb['kdj_K'], comb['kdj_D'], comb['kdj_J'])
            # 合并写入 data_comb中
            print("loading__ {0}", format(count / len(list_eastmoney)))
            count += 1
            comb.to_csv(path_result + em + ".csv", index=False)


def multi_daily():
    # 日线目标数据
    path_result = "D:/硕士研究/毕业设计准备/不复权淘宝数据/data_comb/"
    path_end = "D:/硕士研究/代码类/2020-08-12/combine_stock/data_multi/"
    # list 日线数据
    list_daily = os.listdir(path_result)
    # 遍历
    for em in list_daily:
        comb = pd.read_csv(path_result + em)
        # print(em)
        '''
        均线分布 对应数列
        2  4  8   12  24  48  100
        3  6  12  18  36  72  150 
        4  8  16  24  48  96  200
        6  12 24  36  72  144 300
        7  14 28  42  84  168 350     
        8  16 32  48  96  192 400
        '''
        comb['ma2'] = comb['close'].rolling(window=2).mean()
        comb['ma4'] = comb['close'].rolling(window=4).mean()
        comb['ma8'] = comb['close'].rolling(window=8).mean()
        comb['ma12'] = comb['close'].rolling(window=12).mean()
        comb['ma24'] = comb['close'].rolling(window=24).mean()
        comb['ma48'] = comb['close'].rolling(window=48).mean()
        comb['ma100'] = comb['close'].rolling(window=100).mean()
        comb['ma3'] = comb['close'].rolling(window=3).mean()
        comb['ma6'] = comb['close'].rolling(window=6).mean()
        comb['ma18'] = comb['close'].rolling(window=18).mean()
        comb['ma36'] = comb['close'].rolling(window=36).mean()
        comb['ma72'] = comb['close'].rolling(window=72).mean()
        comb['ma150'] = comb['close'].rolling(window=150).mean()
        comb['ma4'] = comb['close'].rolling(window=4).mean()
        comb['ma16'] = comb['close'].rolling(window=16).mean()
        comb['ma96'] = comb['close'].rolling(window=96).mean()
        comb['ma200'] = comb['close'].rolling(window=200).mean()
        comb['ma6'] = comb['close'].rolling(window=6).mean()
        comb['ma144'] = comb['close'].rolling(window=144).mean()
        comb['ma300'] = comb['close'].rolling(window=300).mean()
        comb['ma7'] = comb['close'].rolling(window=7).mean()
        comb['ma14'] = comb['close'].rolling(window=14).mean()
        comb['ma28'] = comb['close'].rolling(window=28).mean()
        comb['ma42'] = comb['close'].rolling(window=42).mean()
        comb['ma84'] = comb['close'].rolling(window=84).mean()
        comb['ma168'] = comb['close'].rolling(window=168).mean()
        comb['ma350'] = comb['close'].rolling(window=350).mean()
        comb['ma8'] = comb['close'].rolling(window=8).mean()
        comb['ma32'] = comb['close'].rolling(window=32).mean()
        comb['ma48'] = comb['close'].rolling(window=48).mean()
        comb['ma192'] = comb['close'].rolling(window=192).mean()
        comb['ma400'] = comb['close'].rolling(window=400).mean()
        comb['hs_2'] = comb['换手率（%）'].rolling(window=2).mean()
        comb['hs_3'] = comb['换手率（%）'].rolling(window=3).mean()
        comb['hs_4'] = comb['换手率（%）'].rolling(window=4).mean()
        comb['hs_5'] = comb['换手率（%）'].rolling(window=5).mean()
        comb['hs_6'] = comb['换手率（%）'].rolling(window=6).mean()
        comb['hs_7'] = comb['换手率（%）'].rolling(window=7).mean()
        comb['hs_8'] = comb['换手率（%）'].rolling(window=8).mean()
        comb['hs_9'] = comb['换手率（%）'].rolling(window=9).mean()
        comb['hs_10'] = comb['换手率（%）'].rolling(window=10).mean()
        print(em)
        comb.to_csv(path_end + em, index=False)


if __name__ == '__main__':
    # combine()
    multi_daily()

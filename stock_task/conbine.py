import pandas as pd
import numpy as np
import os
import time

# 市值合并demo
path_eastmoney = "./data_parsed/"
path_jointquant = "./data_jq/"
path_result = "./data_comb/"

# 东方财富数据
list_eastmoney = os.listdir(path_eastmoney)
# 聚宽% 第一次的市值 数据
list_jq = os.listdir(path_jointquant)
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
for em in list_eastmoney:
    # 对应
    if em in list_jq:
        # 读
        df_em = pd.read_csv(path_eastmoney + em)
        df_jq = pd.read_csv(path_jointquant + em, encoding="gbk")
        for i in range(len(df_jq)):
            _time = time.strptime(str(df_jq["交易日期"][i]), "%Y%m%d")
            df_jq.loc[i, "交易日期"] = time.strftime("%Y-%m-%d", _time)
        # 合并
        comb = pd.merge(df_em, df_jq, how="left",
                        
                        left_on="time", right_on="交易日期")
        # --  axis为0时表示删除行，axis为1时表示删除列
        comb = comb.drop(["交易日期", "amplitude", "Change", "当日收盘价", "换手率（%）"], axis=1)
        comb.rename(columns={'time': 'trade_date', 'Volume': 'vol',
                             'Turnover': 'amount', 'Turnover_rate': '换手率（%）',
                             'Increase': 'updown_rate'}
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
        print("loading__  {:.2f}", format(count / len(list_eastmoney)))
        count += 1
        comb.to_csv(path_result + em, index=False)

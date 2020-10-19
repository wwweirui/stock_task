import os
import pandas as pd


def all_in():
    # 不复权 爬取的地址
    path = "D:/硕士研究/毕业设计准备/不复权淘宝数据/data_recomb/"
    #   D:/硕士研究/毕业设计准备/不复权淘宝数据/data_comb/
    # D:/硕士研究/毕业设计准备/不复权淘宝数据/data_recomb/
    list_filename = os.listdir(path)
    # 整合all_in_one
    with open("all_in_one_nonefq.csv", "w", encoding="utf-8") as fout:
        fout.write(
            "ts_code,trade_date,open,high,low,close,vol,amount,"
            "换手率（%）,换手率（自由流通股）,量比,市盈率（总市值/净利润， 亏损的PE为空）,"
            "市盈率（TTM，亏损的PE为空）,市净率（总市值/净资产）,市销率,市销率（TTM）,股息率 （%）,"
            "股息率（TTM）（%）,总股本 （万股）,流通股本 （万股）,自由流通股本 （万）,总市值 （万元）,"
            "流通市值（万元）,ma5,ma10,ma20,ma30,ma60,ma120,ma250,kdj_K,kdj_D,kdj_J,fivedayhs,"
            "updown_rate,BBI1,BBI2,BBI3\n")
        for filename in list_filename:
            with open(path + filename, "r", encoding="utf-8") as fin:
                for iline, line in enumerate(fin):
                    if iline != 0:
                        fout.write(filename[:-4] + "," + line)
            print(filename)


def rewrite():
    with open("all_in_one2.csv", "w", encoding="gbk") as fout:
        fout.write(
            r"\ts_code\",\"trade_date\",\"open\",\"high\",\"low\",\"close\",\"vol\",\"amount\",\"换手率（%）\",\"换手率（自由流通股）\",\"量比\",\"市盈率（总市值/净利润， 亏损的PE为空）\",\"市盈率（TTM，亏损的PE为空）\",\"市净率（总市值/净资产）\",\"市销率\",\"市销率（TTM）\",\"股息率 （%）\",\"股息率（TTM）（%）\",\"总股本 （万股）\",\"流通股本 （万股）\",\"自由流通股本 （万）\",\"总市值 （万元）\",\"流通市值（万元）\",\"ma5\",\"ma10\",\"ma20\",\"ma30\",\"ma60\",\"ma120\",\"ma250\",\"kdj_K\",\"kdj_D\",\"kdj_J\",\"fivedayhs\",\"updown_rate\",\"BBI1\",\"BBI2\",\"BBI3\"\n")
        with open("all_in_one.csv", "r", encoding="gbk") as fin:
            for i, line in enumerate(fin):
                if i != 0:
                    fout.write(line)


def all_in_hour():
    path = "D:/硕士研究/代码类/2020-08-12/JoinQuant/小时线合并市值数据/"
    list_filename = os.listdir(path)
    # 整合all_in_one
    with open("all_in_one_hour_daily.csv", "w", encoding="utf-8") as fout:
        fout.write(
            "ts_code,date,open,high,low,close,volume,money,capitalization, "
            "market_cap,circulating_cap,circulating_market_cap,turnover_ratio\n")
        for filename in list_filename:
            with open(path + filename, "r", encoding="utf-8") as fin:
                for iline, line in enumerate(fin):
                    if iline != 0:
                        fout.write(filename[:-4] + "," + line)
            print(filename)


def all_in_two_hour():
    path = "D:/硕士研究/代码类/2020-08-12/JoinQuant/小时线(两小时)合并数据/"
    list_filename = os.listdir(path)
    # 整合all_in_one
    with open("all_in_two_hour_daily.csv", "w", encoding="utf-8") as fout:
        fout.write(
            "ts_code,date,open,high,low,close,volume,money,capitalization, "
            "market_cap,circulating_cap,circulating_market_cap,turnover_ratio\n")
        for filename in list_filename:
            with open(path + filename, "r", encoding="utf-8") as fin:
                for iline, line in enumerate(fin):
                    if iline != 0:
                        fout.write(filename[:-4] + "," + line)
            print(filename)


def all_in_multi_hour():
    path = "D:/硕士研究/代码类/2020-08-12/combine_stock/data_multi/"
    list_filename = os.listdir(path)
    # 整合all_in_one
    with open("all_in_multi_daily.csv", "w", encoding="utf-8") as fout:
        fout.write(
            "ts_code,trade_date,open,high,low,close,vol,amount,"
            "换手率（%）,换手率（自由流通股）,量比,市盈率（总市值/净利润， 亏损的PE为空）,"
            "市盈率（TTM，亏损的PE为空）,市净率（总市值/净资产）,市销率,市销率（TTM）,股息率 （%）,"
            "股息率（TTM）（%）,总股本 （万股）,流通股本 （万股）,自由流通股本 （万）,总市值 （万元）,"
            "流通市值（万元）,ma5,ma10,ma20,ma30,ma60,ma120,ma250,kdj_K,kdj_D,kdj_J,fivedayhs,"
            "updown_rate,BBI1,BBI2,BBI3,ma2,ma4,ma8,ma12,ma24,ma48,ma100,ma3,ma6,ma18,ma36,"
            "ma72,ma150,ma16,ma96,ma200,ma144,ma300,ma7,ma14,ma28,ma42,ma84,ma168,ma350,"
            "ma32,ma192,ma400,hs_2,hs_3,hs_4,hs_5,hs_6,hs_7,hs_8,hs_9,hs_10\n")
        for filename in list_filename:
            with open(path + filename, "r", encoding="utf-8") as fin:
                for iline, line in enumerate(fin):
                    if iline != 0:
                        fout.write(filename[:-4] + "," + line)
            print(filename)


if __name__ == '__main__':
    all_in()

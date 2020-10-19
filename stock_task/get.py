import requests
import pandas as pd
import time

# 获取不复权数据 爬虫
# fqt 前复权是 1 不复权是 0
url = "http://push2his.eastmoney.com/api/qt/stock/kline/get?fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5%2Cf6&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58%2Cf59%2Cf60%2Cf61&klt=101&fqt=0&secid={0}.{1}&beg=0&end=20500000"

# 股票 list 列表
len_flist = len(open("stocklist.txt", "r").readlines())
with open("stocklist.txt") as flist:
    for i_stock, stock in enumerate(flist):
        parts = stock.split(".")
        now = parts[0]
        stocktype = parts[1].replace("\n", "").upper()
        print("[%d / %d] - %s.%s" % (i_stock + 1, len_flist, now, stocktype))
        if stocktype == "SH":
            ntype = 1
        elif stocktype == "SZ":
            ntype = 0
        else:
            continue
        # response = requests.get(url.format(ntype, now))
        # fout = open("./data/%s.%s.json" % (now, stocktype), "w")
        # fout.write(response.text)
        # fout.close()
        # response.close()
        # time.sleep(0.8)

        # with _enter_ 报错，晚些再次尝试
        with requests.get(url.format(ntype, now)) as response:
            with open("./data/%s.%s.json" % (now, stocktype), "w") as fout:
                fout.write(response.text)
        time.sleep(0.8)

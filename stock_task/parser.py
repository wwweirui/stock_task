from concurrent.futures import ProcessPoolExecutor
import multiprocessing
import pandas as pd
import json
import os

data_folder = "./data/"
parsed_folder = "./data_parsed/"


def parse(filelist):
    # 重复处理
    plist = os.listdir(parsed_folder)
    for file in filelist:
        if (file[:-5] + ".csv") in plist:
            continue
        else:
            print(file)
            with open(data_folder + file) as f:
                dic = json.load(f)
                # if dic["data"]: 判断json 若为空 跳出
                lines = dic["data"]["klines"]
                # 时间 开盘 收盘 最高 最低 成交量 成交额 换手率 振幅 涨跌幅 change(不用)
                df = pd.DataFrame(
                    columns=["time", "open", "close", "high", "low", "Volume", "Turnover", "Turnover_rate", "amplitude",
                             "Increase", "Change"])
                for line in lines:
                    parts = line.split(",")
                    df = df.append([{"time": parts[0], "open": parts[1], "close": parts[2],
                                     "high": parts[3], "low": parts[4], "Volume": parts[5],
                                     "Turnover": parts[6], "Turnover_rate": parts[10], "amplitude": parts[7],
                                     "Increase": parts[8], "Change": parts[9]}], ignore_index=True)
                df.to_csv(parsed_folder + file[:-5] + ".csv", index=False)


# print("%d stock parsed!" % len(filelist))


if __name__ == "__main__":
    flist = os.listdir(data_folder)
    len_flist = len(flist)
    parse(flist)
    # 多进程处理
    # n_proc = multiprocessing.cpu_count()
    # processes = ProcessPoolExecutor(n_proc)
    # n_perprocess = len_flist // n_proc
    # if len_flist % n_proc != 0:
    #     n_perprocess += 1
    # for i in range(n_proc):
    #     index = i * n_perprocess
    #     processes.submit(parse, flist[index:index + n_perprocess if len_flist > index + n_perprocess else len_flist])
    # processes.shutdown(wait=True)

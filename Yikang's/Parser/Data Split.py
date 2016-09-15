import pandas as pd
import os
from tqdm import *

os.chdir("/Users/luoy2/OneDrive/Documents/UIUC/CME Fall 2016")
# os.chdir("D:/SkyDrive/Documents/UIUC/CME Fall 2016")
# create unique list of names
code_map = pd.read_table('data/grouped/stock_located.txt', sep='\t')


def find_name(code):
    return (code_map.loc[code_map['Stock Locate'] == code].iloc[0, 1])


for data in tqdm(pd.read_csv('data/07292016.NASDAQ_ITCH50output.txt',
                             header=None,
                             names=['EventCode', 'StockLocate', 'Tracking', 'Time',
                                    'x5', 'x6', 'x7', 'x8', 'x9', 'x10'],
                             sep=',',
                             chunksize=1000000)):
    UniqueNames = data.StockLocate.unique()
    # create a data frame dictionary to store your data frames
    DataFrameDict = {elem: pd.DataFrame for elem in UniqueNames}

    for key in tqdm(DataFrameDict.keys()):
        DataFrameDict[key] = data[:][data.StockLocate == key]
        output_filename = 'data/grouped/' + find_name(int(key)) + '.txt'
        DataFrameDict[key].to_csv(output_filename, index=False, mode='a', header=False)
        DataFrameDict[key] = 0

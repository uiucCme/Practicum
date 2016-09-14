import pandas as pd
import os

#os.chdir("/Users/luoy2/OneDrive/Documents/UIUC/CME Fall 2016/Parser")
os.chdir("D:/SkyDrive/Documents/UIUC/CME Fall 2016/Parser")
data = pd.read_csv('data/07292016.NASDAQ_ITCH50output.csv', header=None,
                    )

#create unique list of names
UniqueNames = data.StockLocate.unique()

#create a data frame dictionary to store your data frames
DataFrameDict = {elem: pd.DataFrame for elem in UniqueNames}

for key in DataFrameDict.keys():
    DataFrameDict[key] = data[:][data.StockLocate == key]
    output_filename = 'data/grouped/' + str(DataFrameDict[key].iloc[1][7]) + '.txt'
    print(output_filename)
    DataFrameDict[key].to_csv(output_filename, index=False)
    DataFrameDict[key] = 0


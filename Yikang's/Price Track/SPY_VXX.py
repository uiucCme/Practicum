import matplotlib as plt
import os
import pandas as pd
import numpy as np
from tqdm import *
from datetime import timedelta
import math

def to_nanosecond(time):
    time_array = time.split(":")
    return ((int(time_array[0]) * 3600 + int(time_array[1]) * 60 + float(time_array[2])) * 1000000)

def move_na(object_array):
    for i in range(len(object_array)):
        if math.isnan(object_array[i]) is False:
            last = object_array[i]
        else:
            object_array[i] = last
    return(object_array)


os.chdir("D:/SkyDrive/Documents/UIUC/CME Fall 2016")
SPY = pd.read_table('data/price/SPY_price_track.txt', sep='', delimiter='\t')
VXX = pd.read_table('data/price/VXX_price_track.txt', sep='', delimiter='\t')
SPY['Time'] = SPY['Time'].map(lambda x: to_nanosecond(x))
VXX['Time'] = VXX['Time'].map(lambda x: to_nanosecond(x))


SPY.drop(['Unnamed: 0', 'Quantity'], axis=1, inplace=True)
VXX.drop(['Unnamed: 0', 'Quantity'], axis=1, inplace=True)
SPY.columns = ['Time', 'SPY Price']
VXX.columns = ['Time', 'VXX Price']
SPY['SPY Net'] = SPY['SPY Price']/SPY['SPY Price'][0]
VXX['VXX Net'] = VXX['VXX Price']/VXX['VXX Price'][0]


merged = pd.merge(SPY, VXX, how='outer')
merged[['Time', 'SPY Net', 'VXX Net']].plot()
merged.sort_values('Time', inplace=True)
merged.reset_index(inplace=True)
merged['SPY Net'][0] = merged['SPY Net'][4]
merged['VXX Net'] = move_na(merged.copy()['VXX Net'])
merged['SPY Net'] = move_na(merged.copy()['SPY Net'])
merged.set_index('Time', drop = True, inplace = True)
merged[['SPY Net', 'VXX Net']].plot()
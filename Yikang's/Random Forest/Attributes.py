import pandas as pd
import os
import math
import csv
from tqdm import *
import numpy as np
import matplotlib.pyplot as plt
import h5py


# function to extract data in time range needed, and delete all 0 difference data
def data_cleaning(df_input):
    start_time = '10:30:00'
    end_time = '15:00:00'
    ftr = [3600, 60, 1]
    start_timestamp = sum([a * b for a, b in zip(ftr, [int(i) for i in start_time.split(":")])]) * 1e+9
    end_timestamp = sum([a * b for a, b in zip(ftr, [int(i) for i in end_time.split(":")])]) * 1e+9

    execution_message = df_input[df_input[1].isin([ord('E'), ord('C')])].reset_index(drop=True)
    execution_message = execution_message[start_timestamp <= execution_message[0]]
    execution_message = execution_message[execution_message[0] <= end_timestamp].reset_index(drop=True)

    execution_time = np.array(execution_message[0])
    diff_time = np.diff(execution_time)
    diff_time = np.insert(diff_time, 0, np.nan)
    execution_message = execution_message[diff_time != 0].reset_index(drop=True)
    return (execution_message)


def get_direction(input_val):
    if input_val == np.nan:
        return np.nan
    elif input_val > 0:
        return 1
    elif input_val == 0:
        return 0
    else:
        return -1


def get_imbalance_rate(df_input, level):
    for i in range(1, (level + 1)):
        try:
            bid_column += df_input['best.bid.q.' + str(i)]
            ask_column += df_input['best.ask.q.' + str(i)]
        except:
            bid_column = df_input['best.bid.q.1']
            ask_column = df_input['best.ask.q.1']
    return bid_column / ask_column


os.chdir("D:/SkyDrive/Documents/UIUC/CME Fall 2016")

# os.chdir("/Users/luoy2/OneDrive/Documents/UIUC/CME Fall 2016")
data_input_dir = os.getcwd() + "/data/order_book/"
data_output_dir = os.getcwd() + "/data/random_forest/"

# read hdf5 file
f = h5py.File(data_input_dir + "/order_book.hdf5", 'r+')
order_book = f['SPY/orderbook'][:]
record = f['SPY/record'][:]


# transfer np.array to pdans dataframe, for easy cleaning purpose
orderbook_df = pd.DataFrame(order_book)
record_df = pd.DataFrame(record)

execution_orderbook_df = data_cleaning(orderbook_df)
execution_record_df = data_cleaning(record_df)
# verify if two data frame is for exactly the same message:
# sum(execution_record_df[0] != execution_orderbook_df[0])


# Start build training attributes
execution_orderbook_df.drop([0, 1, 22], 1, inplace=True)  # no need for time, message type and bid ask divide
execution_record_df.drop([0, 1], 1, inplace=True)  # only need price and quantity
execution_record_pctchange_df = execution_record_df.pct_change(1)
execution_orderbook_pctchange_df = execution_orderbook_df.pct_change(1)

execution_record_df.columns = ['exe.p', 'exe.v']
execution_record_pctchange_df.columns = ['exe.p.d', 'exe.v.c']
execution_record_pctchange_df['exe.p.d'] = execution_record_pctchange_df['exe.p.d'].apply(get_direction)

order_book_column = []
for i in reversed(range(1, 11)):
    order_book_column.append('best.bid.p.' + str(i))
    order_book_column.append('best.bid.q.' + str(i))
for i in range(1, 11):
    order_book_column.append('best.ask.p.' + str(i))
    order_book_column.append('best.ask.q.' + str(i))
execution_orderbook_df.columns = order_book_column

order_book_pctc_column = []
for i in reversed(range(1, 11)):
    order_book_pctc_column.append('best.bid.p.c' + str(i))
    order_book_pctc_column.append('best.bid.q.c' + str(i))
for i in range(1, 11):
    order_book_pctc_column.append('best.ask.p.c' + str(i))
    order_book_pctc_column.append('best.ask.q.c' + str(i))
execution_orderbook_pctchange_df.columns = order_book_pctc_column

# caluclate the imbalance rate
imbalance_df = pd.DataFrame(index=range(len(execution_orderbook_df)))
for i in range(1, 11):
    imbalance_df['imbalance.' + str(i)] = get_imbalance_rate(execution_orderbook_df, i)

# merge output
output_df = execution_record_pctchange_df
output_df = output_df.join(execution_record_df)
output_df = output_df.join(execution_orderbook_df)
output_df = output_df.join(execution_orderbook_pctchange_df)
output_df = output_df.join(imbalance_df)
output_df.to_csv(data_output_dir + "/Attributes10_12.csv")

output_matrix = output_df.as_matrix()
f['SPY/Attributes'] = output_matrix
f.close()







'''
diff_time_sub = diff_time[diff_time < 10000]
plt.hist(diff_time_sub, bins=20)
plt.show()
frequency = []
for i in sorted(set(diff_time_sub)):
    frequency.append(sum(diff_time_sub == i))
for k, v in zip(sorted(set(diff_time_sub)), frequency):
    print(k, v)

hist, bins = np.histogram(diff_time, bins=200)
width = 0.7 * (bins[1] - bins[0])
center = (bins[:-1] + bins[1:]) / 2
plt.bar(center, hist, align='center', width=width)

plt.show()


plt.clf()
'''

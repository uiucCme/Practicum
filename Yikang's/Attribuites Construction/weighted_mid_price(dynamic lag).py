import pandas as pd
import os
import time
import numpy as np
import h5py
from bisect import bisect_left

_LAG = 30000
_QUEUE_TIME = 1000
_MULTISTATE = False

if _MULTISTATE == True:
    classifier_num = '_5state'
else:
    classifier_num = '_3state'


def time_to_nanosecond(input_time_string):
    ftr = [3600, 60, 1]
    nanosecond_output = sum([a * b for a, b in zip(ftr, [int(i)
                                                         for i in
                                                         input_time_string.split(
                                                             ":")])]) * 1e+9
    return nanosecond_output


# function to extract data in time range needed, and delete all 0 difference data
def data_cleaning(df_input, start_time='10:30:00', end_time='15:00:00',
                  execution=False):
    """
    :param df_input:
    :param start_time:
    :param end_time:
    :return pandas dataframe:
    """
    start_timestamp = time_to_nanosecond(start_time)
    end_timestamp = time_to_nanosecond(end_time)
    if execution:
        df_input = df_input[
            df_input[1].isin([ord('E'), ord('C')])].reset_index(drop=True)
    df_output = df_input[
        start_timestamp <= df_input[0]]
    df_output = df_output[
        df_output[0] <= end_timestamp].reset_index(drop=True)

    execution_time = np.array(df_output[0])
    diff_time = np.diff(execution_time)
    diff_time = np.insert(diff_time, 0, np.nan)
    df_output = df_output[
        diff_time != 0].reset_index(drop=True)
    return df_output


def get_direction(input_val, high, low, multistate = True):
    """

    :param input_val:
    :return:
    """
    if multistate == True:
        if input_val == np.nan:
            return np.nan
        elif input_val >= high:
            return 2
        elif input_val > 0:
            return 1
        elif input_val == 0:
            return 0
        elif input_val <= low:
            return -2
        elif input_val < 0:
            return -1
        else:
            return np.nan
    else:
        if input_val == np.nan:
            return np.nan
        elif input_val > 0:
            return 1
        elif input_val == 0:
            return 0
        elif input_val < 0:
            return -1
        else:
            return np.nan



def get_imbalance_rate(df_input, level):
    for i in range(1, (level + 1)):
        try:
            bid_column += df_input['best.bid.q.' + str(i)]
            ask_column += df_input['best.ask.q.' + str(i)]
        except:
            bid_column = df_input['best.bid.q.1'].copy()
            ask_column = df_input['best.ask.q.1'].copy()
    return bid_column / ask_column


# function to find the latest

def takeClosest(myNumber, myList):
    """
    Assumes myList is sorted. Returns closest value to myNumber.

    If two numbers are equally close, return the smallest number.
    """
    pos = bisect_left(myList, myNumber)
    if pos == 0:
        return myList[0]
    if pos == len(myList):
        return myList[-1]
    if myList[pos] == myNumber:
        return myNumber
    before = myList[pos - 1]
    return before


def weighted_midprice(message, depth=10):
    # mid_price_position = 22
    price_lvl = 0
    volumn_lvl = []
    mid_price_bylvl = []
    while price_lvl <= 18:
        mid_price_bylvl.append((message[
                                    20 - price_lvl] + \
                                message[
                                    23 + price_lvl]) * 0.5)
        volumn_lvl.append(
            message[21 - price_lvl] + message[
                24 + price_lvl])
        price_lvl += 2
    volumn_lvl = np.array(volumn_lvl)[:depth]
    mid_price_bylvl = np.array(mid_price_bylvl)[:depth]
    total_volumn = np.sum(volumn_lvl, 0)
    volumn_factor = volumn_lvl / total_volumn
    mid_price = sum(mid_price_bylvl * volumn_factor)
    return mid_price


# os.chdir("D:/SkyDrive/Documents/UIUC/CME Fall 2016")
os.chdir("/Users/luoy2/OneDrive/Documents/UIUC/CME Fall 2016")
data_input_dir = os.getcwd() + "/data/order_book/"
data_output_dir = os.getcwd() + "/data/Attributes/"

# read hdf5 file
print("reading hdf5 file...")
with h5py.File(data_input_dir + "/order_book.hdf5", 'r') as f:
    order_book = f['SPY/orderbook'][:]
    record = f['SPY/record'][:]

# transfer np.array to pdans dataframe, for easy cleaning purpose
orderbook_df = pd.DataFrame(order_book)
record_df = pd.DataFrame(record)
orderbook_df[22] = 0.5 * (orderbook_df[20] + orderbook_df[23])
print("finished calculating weighted mid price!")

# step 1: get the data and transfer it to data frame, from 10:30:00 to 15:30:00
time_to_start = '10:30:00'
time_to_end = '15:00:00'
execution_orderbook_df = data_cleaning(orderbook_df, time_to_start,
                                       time_to_end, True)
execution_record_df = data_cleaning(record_df, time_to_start,
                                    time_to_end, True)

orderbook_df.set_index(0, inplace=True)
execution_orderbook_df.set_index(0, inplace=True)
execution_record_df.set_index(0, inplace=True)

# In[3]: len(execution_orderbook_df_index)
# Out[3]: 1620000

# drop column 1 which stands for order type
execution_orderbook_df.drop(1, 1, inplace=True)
execution_record_df.drop(1, 1, inplace=True)

# step 2: Start build training attributes
execution_orderbook_df = execution_orderbook_df[[22]
                                                + list(range(2, 22))
                                                + list(
    range(23, execution_orderbook_df.columns[-1] + 1))]

# a) calculate weighted mid price
execution_orderbook_df[22] = execution_orderbook_df.apply(
    weighted_midprice, 1)

# b) eliminate the queueing execution order
execution_gap = np.diff(execution_orderbook_df.index)
execution_gap_position = np.insert(execution_gap, len(execution_gap), np.nan)

timestamp_needed = execution_record_df.index[
    execution_gap_position >= _QUEUE_TIME]
execution_orderbook_df = execution_orderbook_df.loc[timestamp_needed]
execution_record_df = execution_record_df.loc[timestamp_needed]

# c) calcluate percentage change of execution order

# 66 : buy signal  83 : sell signal
# 0.2575: buy to sell
# -0.2048: sell to buy
execution_orderbook_pctchange_df = execution_orderbook_df.pct_change(1)
execution_record_pctchange_df = execution_record_df.pct_change(1)

# d) calculate the future weighted mid price
Y = pd.DataFrame(index=execution_record_df.index,
                 columns=['future.mid.p', 'direction.by.midp'])

timestamp_needed = []

# e) add the dynamic lag of future prediction, assume the message gap
# is the same as previous
new_execution_gap = np.diff(execution_orderbook_df.index)
dynamic_lag = np.insert(new_execution_gap, 0, 0)
for i in np.array(execution_record_df.index) + dynamic_lag:
    timestamp_needed.append(takeClosest(i, orderbook_df.index))
print('found all future timestamp!')
search_df = orderbook_df.groupby(orderbook_df.index).last()
Y['future.mid.p'] = search_df.loc[list(timestamp_needed)].apply(
    weighted_midprice, 1).tolist()

# step 3: clean the output
execution_record_df.columns = ['exe.p', 'exe.v', 'bs.indicator']
execution_record_pctchange_df.columns = ['exe.p.d', 'exe.v.c', 'bs.c']

order_book_column = []
order_book_column.append("mid.price")
for i in reversed(range(1, 11)):
    order_book_column.append('best.bid.p.' + str(i))
    order_book_column.append('best.bid.q.' + str(i))
for i in range(1, 11):
    order_book_column.append('best.ask.p.' + str(i))
    order_book_column.append('best.ask.q.' + str(i))
execution_orderbook_df.columns = order_book_column

order_book_pctc_column = []
order_book_pctc_column.append("mid.price.c")
for i in reversed(range(1, 11)):
    order_book_pctc_column.append('best.bid.p.c' + str(i))
    order_book_pctc_column.append('best.bid.q.c' + str(i))
for i in range(1, 11):
    order_book_pctc_column.append('best.ask.p.c' + str(i))
    order_book_pctc_column.append('best.ask.q.c' + str(i))
execution_orderbook_pctchange_df.columns = order_book_pctc_column

# caluclate the imbalance rate
print("calulating  imbalance rate...")
imbalance_df = pd.DataFrame(index=execution_orderbook_df.index)
for i in range(1, 11):
    imbalance_df['imbalance.' +
                 str(i)] = np.array(
        get_imbalance_rate(execution_orderbook_df, i))

# merge output
output_df = Y.join(execution_record_df)
output_df = output_df.join(execution_record_pctchange_df)
output_df = output_df.join(execution_orderbook_df)
output_df = output_df.join(execution_orderbook_pctchange_df)
output_df = output_df.join(imbalance_df)

output_col = list(output_df.columns)
output_col.remove('mid.price')
output_col.remove('exe.p')
output_df = output_df[['mid.price'] + ['exe.p'] + output_col]

price_diff = np.array(output_df['future.mid.p'] - output_df['mid.price'])
high_q = np.median(price_diff[price_diff>=0])
low_q = np.median(price_diff[price_diff<=0])

vgetdirection = np.vectorize(get_direction)
output_df['direction.by.midp'] = vgetdirection(price_diff, high_q, low_q, _MULTISTATE)

# drop the first row, since the dynamic lag of the first message is unknown
output_df.drop(output_df.index[0], 0, inplace=True)
output_name = data_output_dir + "Attributes" + classifier_num + time.strftime(
    "_%m_%d") + '_dynamic_lag.csv'
output_df.to_csv(output_name)

# ouput the pure dataset
pure_data = output_df.drop(['mid.price', 'exe.p', 'future.mid.p'], 1)
pure_data_colname = pure_data.columns.tolist()
pure_data_colname[0] = 'y'
pure_data.columns = pure_data_colname
pure_data.to_csv(data_output_dir + "trainset" + classifier_num + time.strftime(
    "_%m_%d") + '_dynamic_lag.csv', index=False)
# output_matrix = output_df.as_matrix()
# with h5py.File(data_input_dir + "/order_book.hdf5", 'r+') as f:
#     f["SPY/Attributes" + time.strftime("_%m_%d")] = output_matrix

print("finished task!")







# diff_time_sub = diff_time[diff_time < 10000]
# plt.hist(diff_time_sub, bins=20)
# plt.show()
# frequency = []
# for i in sorted(set(diff_time_sub)):
#     frequency.append(sum(diff_time_sub == i))
# for k, v in zip(sorted(set(diff_time_sub)), frequency):
#     print(k, v)
#
# hist, bins = np.histogram(diff_time, bins=200)
# width = 0.7 * (bins[1] - bins[0])
# center = (bins[:-1] + bins[1:]) / 2
# plt.bar(center, hist, align='center', width=width)
#
# plt.show()
#
#
# plt.clf()


# lags = [500, 2000, 5000, 10000, 30000, 50000, 100000]
# for _LAG in lags:
#     timestamp_needed = []
#     for i in np.array(execution_record_df.index) + _LAG:
#         timestamp_needed.append(takeClosest(i, orderbook_df.index))
#     print(np.sum(timestamp_needed != execution_record_df.index))
